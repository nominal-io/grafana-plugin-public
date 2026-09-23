package plugin

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
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
	// Batches are kept until the stream ends so that each column is allocated once at its final size.
	var batches []arrow.RecordBatch
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	var rows int64
	truncated := false
	for !truncated && reader.Next() {
		batch := reader.RecordBatch()
		if rows+batch.NumRows() > rowLimit {
			batch, truncated = batch.NewSlice(0, rowLimit-rows), true
		} else {
			batch.Retain()
		}
		batches = append(batches, batch)
		rows += batch.NumRows()
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
	chunks := make([]arrow.Array, len(batches))
	for i, field := range fields {
		for b, batch := range batches {
			chunks[b] = batch.Column(i)
		}
		values, err := newSQLColumn(field)(chunks, int(rows))
		if err != nil {
			return nil, fmt.Errorf("failed to read column %q: %w", field.Name, err)
		}
		frame.Fields = append(frame.Fields, data.NewField(field.Name, nil, values))
	}
	if truncated {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityWarning,
			Text:     fmt.Sprintf("Results have been limited to %d rows because the SQL row limit was reached", rowLimit),
		})
	}
	return frame, nil
}

// sqlColumn converts one Arrow column, split across record batches, to the values of a Grafana
// field.
type sqlColumn func(chunks []arrow.Array, rows int) (any, error)

func newSQLColumn(field arrow.Field) sqlColumn {
	nullable := field.Nullable
	switch field.Type.ID() {
	case arrow.NULL:
		// NULL arrays have no validity bitmap, and every value is null.
		return func(_ []arrow.Array, rows int) (any, error) { return make([]*string, rows), nil }
	case arrow.INT8:
		return typedColumn(nullable, reader((*array.Int8).Value))
	case arrow.INT16:
		return typedColumn(nullable, reader((*array.Int16).Value))
	case arrow.INT32:
		return typedColumn(nullable, reader((*array.Int32).Value))
	case arrow.INT64:
		return typedColumn(nullable, reader((*array.Int64).Value))
	case arrow.UINT8:
		return typedColumn(nullable, reader((*array.Uint8).Value))
	case arrow.UINT16:
		return typedColumn(nullable, reader((*array.Uint16).Value))
	case arrow.UINT32:
		return typedColumn(nullable, reader((*array.Uint32).Value))
	case arrow.UINT64:
		return typedColumn(nullable, reader((*array.Uint64).Value))
	case arrow.FLOAT32:
		return typedColumn(nullable, reader((*array.Float32).Value))
	case arrow.FLOAT64:
		return typedColumn(nullable, reader((*array.Float64).Value))
	case arrow.BOOL:
		return typedColumn(nullable, reader((*array.Boolean).Value))
	case arrow.STRING:
		return typedColumn(nullable, reader((*array.String).Value))
	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64:
		return typedColumn(nullable, timeValues)
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return typedColumn(nullable, decimalValues)
	case arrow.MAP:
		return typedColumn(nullable, mapValues)
	default:
		// Lists and other types without a Grafana field type.
		return typedColumn(nullable, func(values arrow.Array) (func(int) string, error) { return values.ValueStr, nil })
	}
}

// valueReader returns a function that reads row i of an Arrow array as T.
type valueReader[T any] func(values arrow.Array) (func(i int) T, error)

// typedColumn reads a column into a []T or, when it is nullable, a []*T with nil for nulls.
func typedColumn[T any](nullable bool, read valueReader[T]) sqlColumn {
	return func(chunks []arrow.Array, rows int) (any, error) {
		vals := make([]T, rows)
		var ptrs []*T
		if nullable {
			ptrs = make([]*T, rows)
		}
		row := 0
		for _, chunk := range chunks {
			get, err := read(chunk)
			if err != nil {
				return nil, err
			}
			for i := range chunk.Len() {
				switch {
				case !nullable:
					vals[row] = get(i)
				case chunk.IsValid(i):
					vals[row] = get(i)
					ptrs[row] = &vals[row]
				}
				row++
			}
		}
		if nullable {
			return ptrs, nil
		}
		return vals, nil
	}
}

func unexpectedArray(values arrow.Array) error {
	return fmt.Errorf("unexpected %s array", values.DataType())
}

// reader reads arrays of type A, whose values need no conversion for a Grafana field, with value.
func reader[A arrow.Array, T any](value func(A, int) T) valueReader[T] {
	return func(values arrow.Array) (func(int) T, error) {
		typed, ok := values.(A)
		if !ok {
			return nil, unexpectedArray(values)
		}
		return func(i int) T { return value(typed, i) }, nil
	}
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

// mapValues formats maps such as the tags column as key=value pairs, reading the entries in place.
func mapValues(values arrow.Array) (func(int) string, error) {
	m, ok := values.(*array.Map)
	if !ok {
		return nil, unexpectedArray(values)
	}
	keys, items := m.Keys(), m.Items()
	var buf []byte
	return func(i int) string {
		start, end := m.ValueOffsets(i)
		buf = buf[:0]
		for j := int(start); j < int(end); j++ {
			if j > int(start) {
				buf = append(buf, ", "...)
			}
			buf = append(buf, keys.ValueStr(j)...)
			buf = append(buf, '=')
			buf = append(buf, items.ValueStr(j)...)
		}
		return string(buf)
	}, nil
}

func decimalValues(values arrow.Array) (func(int) float64, error) {
	switch values := values.(type) {
	case *array.Decimal128:
		scale := values.DataType().(*arrow.Decimal128Type).Scale
		return func(i int) float64 { return values.Value(i).ToFloat64(scale) }, nil
	case *array.Decimal256:
		scale := values.DataType().(*arrow.Decimal256Type).Scale
		return func(i int) float64 { return values.Value(i).ToFloat64(scale) }, nil
	}
	return nil, unexpectedArray(values)
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
	times, err := fieldTimes(frame.Fields[schema.TimeIndex])
	if err != nil {
		return nil, err
	}
	order := timeOrder(times)
	if schema.Type == data.TimeSeriesTypeLong {
		wide, err := longToWide(frame, schema, times, order)
		if err != nil {
			return nil, err
		}
		wide.RefID = frame.RefID
		return wide, nil
	}
	if order == nil {
		return frame, nil
	}
	return reorderRows(frame, order), nil
}

// fieldTimes reads a time field through PointerAt, which unlike At and ConcreteAt does not allocate.
func fieldTimes(field *data.Field) ([]time.Time, error) {
	times := make([]time.Time, field.Len())
	for i := range times {
		var t *time.Time
		switch p := field.PointerAt(i).(type) {
		case *time.Time:
			t = p
		case **time.Time:
			t = *p
		}
		if t == nil {
			return nil, errors.New("time series results cannot contain a null timestamp")
		}
		times[i] = *t
	}
	return times, nil
}

// timeOrder returns the row order that sorts times, keeping equal times in their order, or nil when
// times are already sorted.
func timeOrder(times []time.Time) []int {
	if slices.IsSortedFunc(times, time.Time.Compare) {
		return nil
	}
	order := make([]int, len(times))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return times[a].Compare(times[b]) })
	return order
}

// reorderRows returns a copy of frame with its rows in order.
func reorderRows(frame *data.Frame, order []int) *data.Frame {
	sorted := frame.EmptyCopy()
	sorted.Meta = frame.Meta
	sorted.Extend(len(order))
	for col, field := range frame.Fields {
		sorted.Fields[col].Config = field.Config
		for i, row := range order {
			sorted.Fields[col].Set(i, field.At(row))
		}
	}
	return sorted
}

// longToWide converts a long time series to the wide shape, as data.LongToWide does with
// FillModeNull, visiting rows in order when it is not nil. Wide cells point at the long frame's
// values, so unlike data.LongToWide it allocates per series rather than per row.
func longToWide(long *data.Frame, schema data.TimeSeriesSchema, times []time.Time, order []int) (*data.Frame, error) {
	rows := len(times)
	wideRow := make([]int, rows)
	series := make([]int, rows)
	var wideTimes []time.Time
	var labels []data.Labels
	seriesIDs := make(map[string]int)
	var key []byte
	for i := range rows {
		row := i
		if order != nil {
			row = order[i]
		}
		if t := times[row]; len(wideTimes) == 0 || t.After(wideTimes[len(wideTimes)-1]) {
			wideTimes = append(wideTimes, t)
		}
		wideRow[row] = len(wideTimes) - 1
		key = key[:0]
		for _, f := range schema.FactorIndices {
			value, err := labelValue(long.Fields[f], row)
			if err != nil {
				return nil, err
			}
			key = append(binary.AppendUvarint(key, uint64(len(value))), value...)
		}
		id, ok := seriesIDs[string(key)]
		if !ok {
			seriesLabels, err := rowLabels(long, schema.FactorIndices, row)
			if err != nil {
				return nil, err
			}
			id = len(labels)
			seriesIDs[string(key)] = id
			labels = append(labels, seriesLabels)
		}
		series[row] = id
	}

	values := schema.ValueIndices
	fields := make([]*data.Field, len(labels)*len(values))
	for id, seriesLabels := range labels {
		for v, index := range values {
			field := data.NewFieldFromFieldType(long.Fields[index].Type().NullableType(), len(wideTimes))
			field.Name, field.Labels = long.Fields[index].Name, seriesLabels
			fields[id*len(values)+v] = field
		}
	}
	for v, index := range values {
		// Both return a *T for these field types: PointerAt into a []T, At from a []*T.
		valueAt := long.Fields[index].PointerAt
		if long.Fields[index].Nullable() {
			valueAt = long.Fields[index].At
		}
		for row := range rows {
			fields[series[row]*len(values)+v].Set(wideRow[row], valueAt(row))
		}
	}
	labelKeys := make([]string, len(schema.FactorIndices))
	for i, f := range schema.FactorIndices {
		labelKeys[i] = long.Fields[f].Name
	}
	slices.SortStableFunc(fields, func(a, b *data.Field) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		for _, k := range labelKeys {
			if c := strings.Compare(a.Labels[k], b.Labels[k]); c != 0 {
				return c
			}
		}
		return 0
	})

	wide := data.NewFrame(long.Name, data.NewField(long.Fields[schema.TimeIndex].Name, nil, wideTimes))
	wide.Fields = append(wide.Fields, fields...)
	wide.Meta = long.Meta
	if wide.Meta == nil {
		wide.Meta = &data.FrameMeta{}
	}
	wide.Meta.Type = data.FrameTypeTimeSeriesWide
	wide.Meta.TypeVersion = data.FrameTypeVersion{0, 1}
	return wide, nil
}

func rowLabels(frame *data.Frame, factors []int, row int) (data.Labels, error) {
	labels := make(data.Labels, len(factors))
	for _, f := range factors {
		field := frame.Fields[f]
		if _, ok := labels[field.Name]; ok {
			return nil, fmt.Errorf("time series results cannot have two label columns named %q", field.Name)
		}
		labels[field.Name], _ = labelValue(field, row)
	}
	return labels, nil
}

// labelValue reads a string or bool label column without allocating.
func labelValue(field *data.Field, i int) (string, error) {
	switch v := field.PointerAt(i).(type) {
	case *string:
		return *v, nil
	case **string:
		if *v != nil {
			return **v, nil
		}
	case *bool:
		return strconv.FormatBool(*v), nil
	case **bool:
		if *v != nil {
			return strconv.FormatBool(**v), nil
		}
	}
	return "", fmt.Errorf("time series label column %q cannot contain nulls", field.Name)
}
