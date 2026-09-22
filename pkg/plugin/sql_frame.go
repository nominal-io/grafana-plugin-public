package plugin

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

// frameFromArrowStream decodes an Arrow IPC stream into one frame, keeping at most rowLimit rows.
// It converts columns itself because data.FromArrowRecord rejects nested types such as the
// map<string,string> tags column and assumes nanosecond timestamps. When the whole stream fits,
// it also reads r to EOF, so a stream that fails after its last batch reports that error.
func frameFromArrowStream(r io.Reader, name string, rowLimit int64) (*data.Frame, error) {
	reader, err := ipc.NewReader(r, ipc.WithAllocator(memory.DefaultAllocator))
	if errors.Is(err, io.EOF) {
		return nil, errors.New("SQL response was empty")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read Arrow stream: %w", err)
	}
	defer reader.Release()

	fields := reader.Schema().Fields()
	columns := make([]sqlColumn, len(fields))
	for i, field := range fields {
		columns[i] = newSQLColumn(field)
	}
	var rows int64
	truncated := false
	for !truncated && reader.Next() {
		record := reader.RecordBatch()
		n := record.NumRows()
		if rows+n > rowLimit {
			n, truncated = rowLimit-rows, true
		}
		for i, column := range columns {
			if err := column.append(record.Column(i), int(n)); err != nil {
				return nil, fmt.Errorf("failed to read column %q: %w", fields[i].Name, err)
			}
		}
		rows += n
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("failed to read Arrow stream: %w", err)
	}
	if !truncated {
		if _, err := io.Copy(io.Discard, r); err != nil {
			return nil, err
		}
	}

	frame := data.NewFrame(name)
	for i, column := range columns {
		frame.Fields = append(frame.Fields, data.NewField(fields[i].Name, nil, column.values()))
	}
	if truncated {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityWarning,
			Text:     fmt.Sprintf("Results have been limited to %d rows because the SQL row limit was reached", rowLimit),
		})
	}
	return frame, nil
}

// sqlColumn collects one Arrow column across record batches.
type sqlColumn interface {
	append(column arrow.Array, rows int) error
	values() any
}

func newSQLColumn(field arrow.Field) sqlColumn {
	valueType, nullable := field.Type, field.Nullable
	if dictionary, ok := valueType.(*arrow.DictionaryType); ok {
		// A dictionary entry can be null even when the index is not.
		valueType, nullable = dictionary.ValueType, true
	}
	switch valueType.ID() {
	case arrow.NULL:
		return &nullColumn{}
	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64:
		return newTypedColumn(nullable, timeValues)
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64, arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
		return newTypedColumn(nullable, floatValues)
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return newTypedColumn(nullable, intValues)
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return newTypedColumn(nullable, uintValues)
	case arrow.BOOL:
		return newTypedColumn(nullable, boolValues)
	default:
		return newTypedColumn(nullable, stringValues)
	}
}

// valueReader returns a function that reads row i of an Arrow array as T.
type valueReader[T any] func(values arrow.Array) (func(i int) T, error)

type typedColumn[T any] struct {
	read     valueReader[T]
	nullable bool
	vals     []T
	ptrs     []*T
}

func newTypedColumn[T any](nullable bool, read valueReader[T]) *typedColumn[T] {
	return &typedColumn[T]{read: read, nullable: nullable, vals: []T{}, ptrs: []*T{}}
}

func (c *typedColumn[T]) values() any {
	if c.nullable {
		return c.ptrs
	}
	return c.vals
}

func (c *typedColumn[T]) append(column arrow.Array, rows int) error {
	values, index, isNull := column, func(i int) int { return i }, column.IsNull
	if dictionary, ok := column.(*array.Dictionary); ok {
		values, index = dictionary.Dictionary(), dictionary.GetValueIndex
		for i := range rows {
			if j := index(i); !dictionary.IsNull(i) && (j < 0 || j >= values.Len()) {
				return fmt.Errorf("dictionary index %d is out of range", j)
			}
		}
		isNull = func(i int) bool { return dictionary.IsNull(i) || values.IsNull(index(i)) }
	}
	get, err := c.read(values)
	if err != nil {
		return err
	}
	if !c.nullable {
		// Dictionary columns are always nullable, so rows index values directly here.
		c.vals = slices.Grow(c.vals, rows)
		for i := range rows {
			c.vals = append(c.vals, get(i))
		}
		return nil
	}
	c.ptrs = slices.Grow(c.ptrs, rows)
	batch := make([]T, rows)
	for i := range rows {
		if isNull(i) {
			c.ptrs = append(c.ptrs, nil)
			continue
		}
		batch[i] = get(index(i))
		c.ptrs = append(c.ptrs, &batch[i])
	}
	return nil
}

// nullColumn holds the NULL type, whose arrays have no validity bitmap.
type nullColumn struct{ rows int }

func (c *nullColumn) append(_ arrow.Array, rows int) error {
	c.rows += rows
	return nil
}

func (c *nullColumn) values() any {
	return make([]*string, c.rows)
}

func unexpectedArray(values arrow.Array) error {
	return fmt.Errorf("unexpected %s array", values.DataType())
}

func timeValues(values arrow.Array) (func(int) time.Time, error) {
	switch values := values.(type) {
	case *array.Timestamp:
		unit := values.DataType().(*arrow.TimestampType).Unit
		return func(i int) time.Time { return values.Value(i).ToTime(unit).UTC() }, nil
	case *array.Date32:
		return func(i int) time.Time { return values.Value(i).ToTime() }, nil
	case *array.Date64:
		return func(i int) time.Time { return values.Value(i).ToTime() }, nil
	}
	return nil, unexpectedArray(values)
}

func floatValues(values arrow.Array) (func(int) float64, error) {
	switch values := values.(type) {
	case *array.Float64:
		return values.Value, nil
	case *array.Float32:
		return func(i int) float64 { return float64(values.Value(i)) }, nil
	case *array.Float16:
		return func(i int) float64 { return float64(values.Value(i).Float32()) }, nil
	case *array.Decimal32:
		scale := values.DataType().(arrow.DecimalType).GetScale()
		return func(i int) float64 { return values.Value(i).ToFloat64(scale) }, nil
	case *array.Decimal64:
		scale := values.DataType().(arrow.DecimalType).GetScale()
		return func(i int) float64 { return values.Value(i).ToFloat64(scale) }, nil
	case *array.Decimal128:
		scale := values.DataType().(arrow.DecimalType).GetScale()
		return func(i int) float64 { return values.Value(i).ToFloat64(scale) }, nil
	case *array.Decimal256:
		scale := values.DataType().(arrow.DecimalType).GetScale()
		return func(i int) float64 { return values.Value(i).ToFloat64(scale) }, nil
	}
	return nil, unexpectedArray(values)
}

func intValues(values arrow.Array) (func(int) int64, error) {
	switch values := values.(type) {
	case *array.Int64:
		return values.Value, nil
	case *array.Int32:
		return func(i int) int64 { return int64(values.Value(i)) }, nil
	case *array.Int16:
		return func(i int) int64 { return int64(values.Value(i)) }, nil
	case *array.Int8:
		return func(i int) int64 { return int64(values.Value(i)) }, nil
	}
	return nil, unexpectedArray(values)
}

func uintValues(values arrow.Array) (func(int) uint64, error) {
	switch values := values.(type) {
	case *array.Uint64:
		return values.Value, nil
	case *array.Uint32:
		return func(i int) uint64 { return uint64(values.Value(i)) }, nil
	case *array.Uint16:
		return func(i int) uint64 { return uint64(values.Value(i)) }, nil
	case *array.Uint8:
		return func(i int) uint64 { return uint64(values.Value(i)) }, nil
	}
	return nil, unexpectedArray(values)
}

func boolValues(values arrow.Array) (func(int) bool, error) {
	if values, ok := values.(*array.Boolean); ok {
		return values.Value, nil
	}
	return nil, unexpectedArray(values)
}

// stringValues reads text and binary columns as strings and renders every other type, such as
// maps, lists and durations, as Arrow's string form of the value.
func stringValues(values arrow.Array) (func(int) string, error) {
	switch values := values.(type) {
	case *array.String:
		return values.Value, nil
	case *array.LargeString:
		return values.Value, nil
	case *array.StringView:
		return values.Value, nil
	case *array.Binary:
		return values.ValueString, nil
	case *array.LargeBinary:
		return values.ValueString, nil
	}
	return values.ValueStr, nil
}

// shapeSQLFrame returns table results unchanged. Time series results are sorted by time and, when
// long, converted to the wide shape Grafana panels expect, with nulls for missing samples.
func shapeSQLFrame(frame *data.Frame, format sqlutil.FormatQueryOption) (*data.Frame, error) {
	if format == sqlutil.FormatOptionTable || frame.Rows() == 0 {
		return frame, nil
	}
	schema := frame.TimeSeriesSchema()
	if schema.Type == data.TimeSeriesTypeNot {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityInfo,
			Text:     "Result is shown as a table because a time series needs a timestamp column and a numeric column.",
		})
		return frame, nil
	}
	sorted, err := sortFrameByTime(frame, schema.TimeIndex)
	if err != nil {
		return nil, err
	}
	if schema.Type == data.TimeSeriesTypeWide {
		return sorted, nil
	}
	wide, err := data.LongToWide(sorted, &data.FillMissing{Mode: data.FillModeNull})
	if err != nil {
		return nil, fmt.Errorf("failed to convert result to time series: %w", err)
	}
	wide.RefID = frame.RefID
	return wide, nil
}

// sortFrameByTime returns frame ordered by its time field, keeping the order of equal timestamps.
func sortFrameByTime(frame *data.Frame, timeIndex int) (*data.Frame, error) {
	field := frame.Fields[timeIndex]
	times := make([]time.Time, field.Len())
	for i := range times {
		t, ok := field.ConcreteAt(i)
		if !ok {
			return nil, errors.New("time series results cannot contain a null timestamp")
		}
		times[i] = t.(time.Time)
	}
	if slices.IsSortedFunc(times, time.Time.Compare) {
		return frame, nil
	}
	order := make([]int, len(times))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return times[a].Compare(times[b]) })

	sorted := frame.EmptyCopy()
	sorted.Meta = frame.Meta
	sorted.Extend(len(order))
	for col, field := range frame.Fields {
		sorted.Fields[col].Config = field.Config
		for i, row := range order {
			sorted.Fields[col].Set(i, field.At(row))
		}
	}
	return sorted, nil
}
