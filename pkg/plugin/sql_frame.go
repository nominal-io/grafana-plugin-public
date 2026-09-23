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

// shapeSQLFrame returns table results unchanged. A time series result becomes one frame per value
// column and label set, sorted by time, where the result's string and bool columns are the labels.
func shapeSQLFrame(frame *data.Frame, format sqlutil.FormatQueryOption) (data.Frames, error) {
	if format == sqlutil.FormatOptionTable || frame.Rows() == 0 {
		return data.Frames{frame}, nil
	}
	schema := frame.TimeSeriesSchema()
	if schema.Type == data.TimeSeriesTypeNot {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityInfo,
			Text:     "Result is shown as a table because a time series needs a timestamp column and a numeric column.",
		})
		return data.Frames{frame}, nil
	}
	times, err := fieldTimes(frame.Fields[schema.TimeIndex])
	if err != nil {
		return nil, err
	}
	return splitSeries(frame, schema, times, timeOrder(times))
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

// splitSeries returns one frame per value column and label set, ordered by column and then labels,
// visiting rows in order when it is not nil. Value fields point at frame's values, so it allocates
// per series rather than per row.
func splitSeries(frame *data.Frame, schema data.TimeSeriesSchema, times []time.Time, order []int) (data.Frames, error) {
	factors := schema.FactorIndices
	labelValues := make([]string, len(factors))
	seriesIDs := make(map[string]int)
	var seriesLabels []data.Labels
	var seriesRows [][]int
	var key []byte
	for i := range times {
		row := i
		if order != nil {
			row = order[i]
		}
		key = key[:0]
		for j, f := range factors {
			value, err := labelValue(frame.Fields[f], row)
			if err != nil {
				return nil, err
			}
			labelValues[j] = value
			key = append(binary.AppendUvarint(key, uint64(len(value))), value...)
		}
		id, ok := seriesIDs[string(key)]
		if !ok {
			labels := make(data.Labels, len(factors))
			for j, f := range factors {
				labels[frame.Fields[f].Name] = labelValues[j]
			}
			id = len(seriesLabels)
			seriesIDs[string(key)] = id
			seriesLabels = append(seriesLabels, labels)
			seriesRows = append(seriesRows, nil)
		}
		seriesRows[id] = append(seriesRows[id], row)
	}

	ids := make([]int, len(seriesRows))
	labelKeys := make([]string, len(seriesRows))
	for id := range ids {
		ids[id], labelKeys[id] = id, seriesLabels[id].String()
	}
	slices.SortFunc(ids, func(a, b int) int { return strings.Compare(labelKeys[a], labelKeys[b]) })

	meta := frame.Meta
	if meta == nil {
		meta = &data.FrameMeta{}
	}
	meta.Type, meta.TypeVersion = data.FrameTypeTimeSeriesMulti, data.FrameTypeVersion{0, 1}
	timeName := frame.Fields[schema.TimeIndex].Name
	var frames data.Frames
	for _, index := range schema.ValueIndices {
		values := frame.Fields[index]
		// Both return a *T for these fields: PointerAt into a []T, and At from a []*T.
		valueAt := values.PointerAt
		if values.Nullable() {
			valueAt = values.At
		}
		for _, id := range ids {
			rows := seriesRows[id]
			seriesTimes := make([]time.Time, len(rows))
			field := data.NewFieldFromFieldType(values.Type().NullableType(), len(rows))
			field.Name, field.Labels = values.Name, seriesLabels[id]
			for i, row := range rows {
				seriesTimes[i] = times[row]
				field.Set(i, valueAt(row))
			}
			series := data.NewFrame(frame.Name, data.NewField(timeName, nil, seriesTimes), field)
			series.RefID, series.Meta = frame.RefID, meta
			frames = append(frames, series)
		}
	}
	return frames, nil
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
