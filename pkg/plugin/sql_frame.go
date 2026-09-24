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
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

// frameFromArrowStream decodes an Arrow IPC stream into one frame, keeping at most rowLimit rows.
// It converts columns itself because data.FromArrowRecord rejects nested types such as the
// map<string,string> tags column and assumes nanosecond timestamps. When the whole stream fits,
// it also reads r to EOF, so a stream that fails after its last batch reports that error.
func frameFromArrowStream(r io.Reader, name string, rowLimit int64) (*data.Frame, error) {
	reader, err := ipc.NewReader(r)
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
		values, err := columnValues(field, chunks, int(rows))
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

// columnValues converts one Arrow column, split across record batches, to the values of a Grafana
// field: a []T, or a []*T with nil for nulls when the column is nullable.
func columnValues(field arrow.Field, chunks []arrow.Array, rows int) (any, error) {
	n := field.Nullable
	switch t := field.Type.(type) {
	case *arrow.NullType:
		// NULL arrays have no validity bitmap, and every value is null.
		return make([]*string, rows), nil
	case *arrow.Int8Type:
		return column(chunks, rows, n, (*array.Int8).Value)
	case *arrow.Int16Type:
		return column(chunks, rows, n, (*array.Int16).Value)
	case *arrow.Int32Type:
		return column(chunks, rows, n, (*array.Int32).Value)
	case *arrow.Int64Type:
		return column(chunks, rows, n, (*array.Int64).Value)
	case *arrow.Uint8Type:
		return column(chunks, rows, n, (*array.Uint8).Value)
	case *arrow.Uint16Type:
		return column(chunks, rows, n, (*array.Uint16).Value)
	case *arrow.Uint32Type:
		return column(chunks, rows, n, (*array.Uint32).Value)
	case *arrow.Uint64Type:
		return column(chunks, rows, n, (*array.Uint64).Value)
	case *arrow.Float32Type:
		return column(chunks, rows, n, (*array.Float32).Value)
	case *arrow.Float64Type:
		return column(chunks, rows, n, (*array.Float64).Value)
	case *arrow.BooleanType:
		return column(chunks, rows, n, (*array.Boolean).Value)
	case *arrow.StringType:
		return column(chunks, rows, n, (*array.String).Value)
	case *arrow.TimestampType:
		return column(chunks, rows, n, func(a *array.Timestamp, i int) time.Time { return a.Value(i).ToTime(t.Unit) })
	case *arrow.Date32Type:
		return column(chunks, rows, n, func(a *array.Date32, i int) time.Time { return a.Value(i).ToTime() })
	case *arrow.Date64Type:
		return column(chunks, rows, n, func(a *array.Date64, i int) time.Time { return a.Value(i).ToTime() })
	case *arrow.Decimal128Type:
		return column(chunks, rows, n, func(a *array.Decimal128, i int) float64 { return a.Value(i).ToFloat64(t.Scale) })
	case *arrow.Decimal256Type:
		return column(chunks, rows, n, func(a *array.Decimal256, i int) float64 { return a.Value(i).ToFloat64(t.Scale) })
	case *arrow.MapType:
		var buf []byte
		return column(chunks, rows, n, func(a *array.Map, i int) string {
			buf = appendMap(buf[:0], a, i)
			return string(buf)
		})
	default:
		// Lists and other types without a Grafana field type.
		return column(chunks, rows, n, arrow.Array.ValueStr)
	}
}

// column reads chunks, which must be arrays of type A, through value.
func column[A arrow.Array, T any](chunks []arrow.Array, rows int, nullable bool, value func(A, int) T) (any, error) {
	vals := make([]T, rows)
	var ptrs []*T
	if nullable {
		ptrs = make([]*T, rows)
	}
	row := 0
	for _, chunk := range chunks {
		typed, ok := chunk.(A)
		if !ok {
			return nil, fmt.Errorf("unexpected %s array", chunk.DataType())
		}
		for i := range chunk.Len() {
			switch {
			case !nullable:
				vals[row] = value(typed, i)
			case chunk.IsValid(i):
				vals[row] = value(typed, i)
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

// appendMap appends row i of a map, such as the tags column, as key="value" pairs. Values are
// always quoted, and keys when they contain a separator, so distinct maps never format the same.
func appendMap(buf []byte, m *array.Map, i int) []byte {
	keys, items := m.Keys(), m.Items()
	start, end := m.ValueOffsets(i)
	for j := int(start); j < int(end); j++ {
		if j > int(start) {
			buf = append(buf, ", "...)
		}
		if key := keys.ValueStr(j); key != "" && !strings.ContainsAny(key, `=", `) {
			buf = append(buf, key...)
		} else {
			buf = strconv.AppendQuote(buf, key)
		}
		buf = append(buf, '=')
		if items.IsNull(j) {
			buf = append(buf, "null"...)
		} else {
			buf = strconv.AppendQuote(buf, items.ValueStr(j))
		}
	}
	return buf
}

// shapeSQLFrame returns table results unchanged. A time series result becomes one frame per numeric
// column and label set, sorted by time, where the result's string and bool columns are the labels.
// A result without a timestamp and a numeric column is shown as a table with a notice.
func shapeSQLFrame(frame *data.Frame, format sqlutil.FormatQueryOption) (data.Frames, error) {
	if format == sqlutil.FormatOptionTable {
		return data.Frames{frame}, nil
	}
	schema := frame.TimeSeriesSchema()
	var values []int
	var skipped []string
	for _, i := range schema.ValueIndices {
		if frame.Fields[i].Type().Numeric() {
			values = append(values, i)
		} else {
			skipped = append(skipped, frame.Fields[i].Name)
		}
	}
	if schema.Type == data.TimeSeriesTypeNot || len(values) == 0 {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityInfo,
			Text:     "Result is shown as a table because a time series needs a timestamp column and a numeric column.",
		})
		// Explore shows frames that prefer a graph only as a graph.
		frame.Meta.PreferredVisualization = data.VisTypeTable
		return data.Frames{frame}, nil
	}
	if len(skipped) > 0 {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityInfo,
			Text:     "The time series leaves out columns that are neither numbers nor labels: " + strings.Join(skipped, ", "),
		})
	}
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}
	frame.Meta.Type, frame.Meta.TypeVersion = data.FrameTypeTimeSeriesMulti, data.FrameTypeVersion{0, 1}
	if frame.Rows() == 0 {
		// A typed frame without fields is the dataplane's no-data response, which alerting evaluates
		// as no data.
		empty := data.NewFrame(frame.Name)
		empty.RefID, empty.Meta = frame.RefID, frame.Meta
		return data.Frames{empty}, nil
	}
	times, err := fieldTimes(frame.Fields[schema.TimeIndex])
	if err != nil {
		return nil, err
	}
	return splitSeries(frame, schema.TimeIndex, schema.FactorIndices, values, times, timeOrder(times))
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
func splitSeries(frame *data.Frame, timeIndex int, factors, values []int, times []time.Time, order []int) (data.Frames, error) {
	labelValues := make([]string, len(factors))
	seriesIDs := make(map[string]int)
	var seriesLabels []data.Labels
	var seriesNames []string
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
			seriesNames = append(seriesNames, strings.Join(labelValues, " "))
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

	timeName := frame.Fields[timeIndex].Name
	var frames data.Frames
	for _, index := range values {
		column := frame.Fields[index]
		// Both return a *T for these fields: PointerAt into a []T, and At from a []*T.
		valueAt := column.PointerAt
		if column.Nullable() {
			valueAt = column.At
		}
		for _, id := range ids {
			rows := seriesRows[id]
			seriesTimes := make([]time.Time, len(rows))
			field := data.NewFieldFromFieldType(column.Type().NullableType(), len(rows))
			field.Name, field.Labels = column.Name, seriesLabels[id]
			if len(values) > 1 {
				// Alerting identifies a series by its labels alone, so the column goes in a label. The
				// display name stays what Grafana shows without it.
				field.Labels = withColumnLabel(seriesLabels[id], column.Name)
				field.Config = &data.FieldConfig{DisplayNameFromDS: strings.TrimSpace(column.Name + " " + seriesNames[id])}
			}
			for i, row := range rows {
				seriesTimes[i] = times[row]
				field.Set(i, valueAt(row))
			}
			series := data.NewFrame(frame.Name, data.NewField(timeName, nil, seriesTimes), field)
			series.RefID, series.Meta = frame.RefID, frame.Meta
			frames = append(frames, series)
		}
	}
	return frames, nil
}

// seriesColumnLabel is the label that tells apart the series of a result's value columns.
const seriesColumnLabel = "column"

func withColumnLabel(labels data.Labels, column string) data.Labels {
	if _, taken := labels[seriesColumnLabel]; taken {
		return labels
	}
	labels = labels.Copy()
	labels[seriesColumnLabel] = column
	return labels
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
