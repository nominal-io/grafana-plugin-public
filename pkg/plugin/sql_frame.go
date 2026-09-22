package plugin

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

func frameFromArrowStream(r io.Reader, name string) (*data.Frame, error) {
	reader, err := ipc.NewReader(r, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, fmt.Errorf("failed to read Arrow stream: %w", err)
	}
	defer reader.Release()
	columns := reader.Schema().Fields()
	accumulators := make([]sqlColumnAccumulator, len(columns))
	for i, column := range columns {
		accumulators[i] = newSQLColumnAccumulator(column)
	}
	for reader.Next() {
		record := reader.Record()
		for column := 0; column < int(record.NumCols()); column++ {
			accumulators[column].append(record.Column(column))
		}
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("Arrow stream read error: %w", err)
	}
	frame := data.NewFrame(name)
	for _, accumulator := range accumulators {
		frame.Fields = append(frame.Fields, data.NewField(accumulator.field.Name, nil, accumulator.values))
	}
	return frame, nil
}

func sqlFieldType(t arrow.DataType, nullable bool) data.FieldType {
	if dictionary, ok := t.(*arrow.DictionaryType); ok {
		return sqlFieldType(dictionary.ValueType, nullable)
	}
	switch t.ID() {
	case arrow.TIMESTAMP:
		if nullable {
			return data.FieldTypeNullableTime
		}
		return data.FieldTypeTime
	case arrow.FLOAT32, arrow.FLOAT64:
		if nullable {
			return data.FieldTypeNullableFloat64
		}
		return data.FieldTypeFloat64
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		if nullable {
			return data.FieldTypeNullableInt64
		}
		return data.FieldTypeInt64
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		if nullable {
			return data.FieldTypeNullableUint64
		}
		return data.FieldTypeUint64
	case arrow.BOOL:
		if nullable {
			return data.FieldTypeNullableBool
		}
		return data.FieldTypeBool
	default:
		if nullable {
			return data.FieldTypeNullableString
		}
		return data.FieldTypeString
	}
}

type sqlColumnAccumulator struct {
	field     arrow.Field
	fieldType data.FieldType
	values    any
}

func sqlAppend[T any](existing, batch []T) []T {
	if len(existing) == 0 {
		return batch
	}
	return append(existing, batch...)
}

func newSQLColumnAccumulator(field arrow.Field) sqlColumnAccumulator {
	fieldType := sqlFieldType(field.Type, field.Nullable)
	var values any
	switch fieldType {
	case data.FieldTypeTime:
		values = []time.Time{}
	case data.FieldTypeNullableTime:
		values = []*time.Time{}
	case data.FieldTypeFloat64:
		values = []float64{}
	case data.FieldTypeNullableFloat64:
		values = []*float64{}
	case data.FieldTypeInt64:
		values = []int64{}
	case data.FieldTypeNullableInt64:
		values = []*int64{}
	case data.FieldTypeUint64:
		values = []uint64{}
	case data.FieldTypeNullableUint64:
		values = []*uint64{}
	case data.FieldTypeBool:
		values = []bool{}
	case data.FieldTypeNullableBool:
		values = []*bool{}
	case data.FieldTypeString:
		values = []string{}
	default:
		values = []*string{}
	}
	return sqlColumnAccumulator{field: field, fieldType: fieldType, values: values}
}

func (a *sqlColumnAccumulator) append(column arrow.Array) {
	n := column.Len()
	switch a.fieldType {
	case data.FieldTypeTime:
		values := make([]time.Time, n)
		for i := range values {
			values[i] = sqlTime(column, i)
		}
		a.values = sqlAppend(a.values.([]time.Time), values)
	case data.FieldTypeNullableTime:
		values := make([]time.Time, n)
		pointers := make([]*time.Time, n)
		for i := range values {
			if !column.IsNull(i) {
				values[i] = sqlTime(column, i)
				pointers[i] = &values[i]
			}
		}
		a.values = sqlAppend(a.values.([]*time.Time), pointers)
	case data.FieldTypeFloat64:
		values := make([]float64, n)
		for i := range values {
			values[i] = sqlFloat(column, i)
		}
		a.values = sqlAppend(a.values.([]float64), values)
	case data.FieldTypeNullableFloat64:
		values := make([]float64, n)
		pointers := make([]*float64, n)
		for i := range values {
			if !column.IsNull(i) {
				values[i] = sqlFloat(column, i)
				pointers[i] = &values[i]
			}
		}
		a.values = sqlAppend(a.values.([]*float64), pointers)
	case data.FieldTypeInt64:
		values := make([]int64, n)
		for i := range values {
			values[i] = sqlInt(column, i)
		}
		a.values = sqlAppend(a.values.([]int64), values)
	case data.FieldTypeNullableInt64:
		values := make([]int64, n)
		pointers := make([]*int64, n)
		for i := range values {
			if !column.IsNull(i) {
				values[i] = sqlInt(column, i)
				pointers[i] = &values[i]
			}
		}
		a.values = sqlAppend(a.values.([]*int64), pointers)
	case data.FieldTypeUint64:
		values := make([]uint64, n)
		for i := range values {
			values[i] = sqlUint(column, i)
		}
		a.values = sqlAppend(a.values.([]uint64), values)
	case data.FieldTypeNullableUint64:
		values := make([]uint64, n)
		pointers := make([]*uint64, n)
		for i := range values {
			if !column.IsNull(i) {
				values[i] = sqlUint(column, i)
				pointers[i] = &values[i]
			}
		}
		a.values = sqlAppend(a.values.([]*uint64), pointers)
	case data.FieldTypeBool:
		values := make([]bool, n)
		for i := range values {
			values[i] = sqlBool(column, i)
		}
		a.values = sqlAppend(a.values.([]bool), values)
	case data.FieldTypeNullableBool:
		values := make([]bool, n)
		pointers := make([]*bool, n)
		for i := range values {
			if !column.IsNull(i) {
				values[i] = sqlBool(column, i)
				pointers[i] = &values[i]
			}
		}
		a.values = sqlAppend(a.values.([]*bool), pointers)
	case data.FieldTypeString:
		values := make([]string, n)
		for i := range values {
			values[i] = sqlString(column, i)
		}
		a.values = sqlAppend(a.values.([]string), values)
	default:
		values := make([]string, n)
		pointers := make([]*string, n)
		for i := range values {
			if !column.IsNull(i) {
				values[i] = sqlString(column, i)
				pointers[i] = &values[i]
			}
		}
		a.values = sqlAppend(a.values.([]*string), pointers)
	}
}

func sqlDictionaryValue(column arrow.Array, row int) (arrow.Array, int) {
	if dictionary, ok := column.(*array.Dictionary); ok {
		return dictionary.Dictionary(), dictionary.GetValueIndex(row)
	}
	return column, row
}

func sqlTime(column arrow.Array, row int) time.Time {
	column, row = sqlDictionaryValue(column, row)
	return column.(*array.Timestamp).Value(row).ToTime(column.(*array.Timestamp).DataType().(*arrow.TimestampType).Unit).UTC()
}
func sqlFloat(column arrow.Array, row int) float64 {
	column, row = sqlDictionaryValue(column, row)
	switch values := column.(type) {
	case *array.Float64:
		return values.Value(row)
	case *array.Float32:
		return float64(values.Value(row))
	default:
		panic("unexpected SQL float column")
	}
}
func sqlInt(column arrow.Array, row int) int64 {
	column, row = sqlDictionaryValue(column, row)
	switch values := column.(type) {
	case *array.Int64:
		return values.Value(row)
	case *array.Int32:
		return int64(values.Value(row))
	case *array.Int16:
		return int64(values.Value(row))
	case *array.Int8:
		return int64(values.Value(row))
	default:
		panic("unexpected SQL integer column")
	}
}
func sqlUint(column arrow.Array, row int) uint64 {
	column, row = sqlDictionaryValue(column, row)
	switch values := column.(type) {
	case *array.Uint64:
		return values.Value(row)
	case *array.Uint32:
		return uint64(values.Value(row))
	case *array.Uint16:
		return uint64(values.Value(row))
	case *array.Uint8:
		return uint64(values.Value(row))
	default:
		panic("unexpected SQL unsigned integer column")
	}
}
func sqlBool(column arrow.Array, row int) bool {
	column, row = sqlDictionaryValue(column, row)
	return column.(*array.Boolean).Value(row)
}
func sqlString(column arrow.Array, row int) string {
	column, row = sqlDictionaryValue(column, row)
	switch values := column.(type) {
	case *array.String:
		return values.Value(row)
	case *array.LargeString:
		return values.Value(row)
	case *array.Binary:
		return string(values.Value(row))
	case *array.LargeBinary:
		return string(values.Value(row))
	default:
		return column.ValueStr(row)
	}
}

func shapeSqlFrame(frame *data.Frame, format sqlutil.FormatQueryOption) (*data.Frame, error) {
	if format == sqlutil.FormatOptionTable || frame.Rows() == 0 {
		return frame, nil
	}
	schema := frame.TimeSeriesSchema()
	if schema.Type == data.TimeSeriesTypeNot {
		frame.AppendNotices(data.Notice{Severity: data.NoticeSeverityInfo, Text: "Result is shown as a table because a TIMESTAMP column is required for a time series."})
		return frame, nil
	}
	for row := 0; row < frame.Rows(); row++ {
		if _, present := frame.Fields[schema.TimeIndex].ConcreteAt(row); !present {
			return nil, fmt.Errorf("time series result contains a null TIMESTAMP")
		}
	}
	sorted := frame
	if !sqlTimeFieldSorted(frame.Fields[schema.TimeIndex]) {
		var err error
		sorted, err = sortSqlLongFrame(frame, schema.TimeIndex)
		if err != nil {
			return nil, err
		}
	}
	if schema.Type == data.TimeSeriesTypeWide {
		return sorted, nil
	}
	// A missing sample is unknown, including for non-nullable SQL results such as COUNT.
	wide, err := data.LongToWide(sorted, &data.FillMissing{Mode: data.FillModeNull})
	if err != nil {
		return nil, fmt.Errorf("failed to convert result to time series: %w", err)
	}
	wide.RefID = frame.RefID
	return wide, nil
}

func sqlTimeFieldSorted(field *data.Field) bool {
	for i := 1; i < field.Len(); i++ {
		left, leftOK := field.ConcreteAt(i - 1)
		right, rightOK := field.ConcreteAt(i)
		if !leftOK || !rightOK || left.(time.Time).After(right.(time.Time)) {
			return false
		}
	}
	return true
}

func sortSqlLongFrame(frame *data.Frame, timeIndex int) (*data.Frame, error) {
	rows, err := frame.RowLen()
	if err != nil {
		return nil, err
	}
	indices := make([]int, rows)
	for i := range indices {
		indices[i] = i
	}
	var sortErr error
	sort.SliceStable(indices, func(i, j int) bool {
		left, leftOK := frame.Fields[timeIndex].ConcreteAt(indices[i])
		right, rightOK := frame.Fields[timeIndex].ConcreteAt(indices[j])
		if !leftOK || !rightOK {
			sortErr = fmt.Errorf("time series result contains a null TIMESTAMP")
			return false
		}
		return left.(time.Time).Before(right.(time.Time))
	})
	if sortErr != nil {
		return nil, sortErr
	}
	out := data.NewFrame(frame.Name)
	out.RefID, out.Meta = frame.RefID, frame.Meta
	for _, field := range frame.Fields {
		out.Fields = append(out.Fields, data.NewFieldFromFieldType(field.Type(), 0))
		out.Fields[len(out.Fields)-1].Name, out.Fields[len(out.Fields)-1].Labels, out.Fields[len(out.Fields)-1].Config = field.Name, field.Labels, field.Config
	}
	out.SetRowCapacity(rows)
	for _, index := range indices {
		for col, field := range frame.Fields {
			out.Fields[col].Append(field.CopyAt(index))
		}
	}
	return out, nil
}
