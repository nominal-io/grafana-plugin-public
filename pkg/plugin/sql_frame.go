package plugin

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
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

// frameFromArrowStream decodes an Arrow IPC stream, keeping at most rowLimit rows, and shapes it for
// format. When the whole stream fits, it also reads r to EOF, so a stream that fails after its last
// batch reports that error. Once decoding succeeds it closes r before building frames, which can
// take a while for a large result.
func frameFromArrowStream(r io.ReadCloser, name string, rowLimit int64, format sqlutil.FormatQueryOption, nonFiniteOK bool) (data.Frames, error) {
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
	_ = r.Close()

	var frame *data.Frame
	if format == sqlutil.FormatOptionLogs {
		frame, err = sqlLogFrame(name, fields, batches, int(rows), nonFiniteOK)
	} else {
		frame, err = tableFrame(name, fields, batches, int(rows), nonFiniteOK)
	}
	if err != nil {
		return nil, err
	}
	if truncated {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityWarning,
			Text:     fmt.Sprintf("Results have been limited to %d rows because the SQL row limit was reached", rowLimit),
		})
	}
	if format == sqlutil.FormatOptionTable || format == sqlutil.FormatOptionLogs {
		return data.Frames{frame}, nil
	}
	return shapeSQLFrame(frame, nonFiniteOK)
}

// tableFrame converts every column of batches to a field of a frame that prefers a table. It
// converts columns itself because data.FromArrowRecord rejects nested types such as the
// map<string,string> tags column and assumes nanosecond timestamps.
func tableFrame(name string, fields []arrow.Field, batches []arrow.RecordBatch, rows int, nonFiniteOK bool) (*data.Frame, error) {
	frame := data.NewFrame(name)
	frame.Meta = &data.FrameMeta{PreferredVisualization: data.VisTypeTable}
	chunks := make([]arrow.Array, len(batches))
	for i, field := range fields {
		for b, batch := range batches {
			chunks[b] = batch.Column(i)
		}
		values, err := columnValues(field, chunks, rows, nonFiniteOK)
		if err != nil {
			return nil, fmt.Errorf("failed to read column %q: %w", field.Name, err)
		}
		frame.Fields = append(frame.Fields, data.NewField(field.Name, nil, values))
	}
	return frame, nil
}

// columnValues converts one Arrow column, split across record batches, to the values of a Grafana
// field: a []*T with nil for nulls when the column has nulls, or NaNs or infinities unless
// nonFiniteOK, else a []T. Grafana decodes a nullable field with one allocation per value, so a
// column without any is sent non-nullable whatever its Arrow type allows.
func columnValues(field arrow.Field, chunks []arrow.Array, rows int, nonFiniteOK bool) (any, error) {
	n := slices.ContainsFunc(chunks, func(c arrow.Array) bool { return c.NullN() > 0 || !nonFiniteOK && hasNonFinite(c) })
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

func hasNonFinite(chunk arrow.Array) bool {
	switch a := chunk.(type) {
	case *array.Float64:
		return slices.ContainsFunc(a.Float64Values(), nonFinite[float64])
	case *array.Float32:
		return slices.ContainsFunc(a.Float32Values(), nonFinite[float32])
	}
	return false
}

// nonFinite reports whether v is NaN or ±Inf.
func nonFinite[F float32 | float64](v F) bool {
	return v != v || math.IsInf(float64(v), 0)
}

// grafanaAcceptsNonFinite reports whether Grafana at version accepts NaN and ±Inf in a
// non-nullable field. Its SQL expressions read both as NULL, and before 13.4.0 fail when they
// write that NULL to a non-nullable column. Builds made before the 13.4.0 release report 13.4.0
// with a "-" suffix and may predate the fix, so they do not count. Security releases, such as
// 13.4.0+security-01, do.
func grafanaAcceptsNonFinite(version string) bool {
	var major, minor, patch int
	var suffix string
	if n, _ := fmt.Sscanf(version, "%d.%d.%d%s", &major, &minor, &patch, &suffix); n < 3 {
		return false
	}
	c := cmp.Or(cmp.Compare(major, 13), cmp.Compare(minor, 4), cmp.Compare(patch, 0))
	return c > 0 || c == 0 && (suffix == "" || strings.HasPrefix(suffix, "+"))
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

// appendMap appends row i of a map, such as the tags column, as key=value pairs. Keys and values
// that could be misread are quoted, so distinct maps never format the same.
func appendMap(buf []byte, m *array.Map, i int) []byte {
	keys, items := m.Keys(), m.Items()
	start, end := m.ValueOffsets(i)
	for j := int(start); j < int(end); j++ {
		if j > int(start) {
			buf = append(buf, ", "...)
		}
		buf = appendMapToken(buf, keys.ValueStr(j))
		buf = append(buf, '=')
		if items.IsNull(j) {
			buf = append(buf, "null"...)
		} else {
			buf = appendMapToken(buf, items.ValueStr(j))
		}
	}
	return buf
}

// appendMapToken quotes s if it is empty, "null", or has a character that Grafana's Extract fields
// "Key+value pairs" parser splits, quotes or escapes on. Inside quotes that parser honors only \,
// so just \ and " are escaped. strconv.AppendQuote would write \n, which reads back as n.
func appendMapToken(buf []byte, s string) []byte {
	if s != "" && s != "null" && !strings.ContainsAny(s, "=:,;&{}\"'\\ \t\r\n") {
		return append(buf, s...)
	}
	buf = append(buf, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' || s[i] == '"' {
			buf = append(buf, '\\')
		}
		buf = append(buf, s[i])
	}
	return append(buf, '"')
}

// shapeSQLFrame splits a time series result into one frame per numeric column and label set, sorted
// by time, where the result's string and bool columns are the labels. A result without a numeric
// column uses its string or bool column named value instead. A result without a timestamp and a
// value column is returned as it came, with a notice.
func shapeSQLFrame(frame *data.Frame, nonFiniteOK bool) (data.Frames, error) {
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
	factors := schema.FactorIndices
	if len(values) == 0 {
		if i := slices.IndexFunc(factors, func(f int) bool { return strings.EqualFold(frame.Fields[f].Name, "value") }); i >= 0 {
			values = []int{factors[i]}
			factors = slices.Delete(factors, i, i+1)
		}
	}
	if len(values) == 0 {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityInfo,
			Text:     "Result is shown as a table because a time series needs a timestamp column and either a numeric column or a string column named value.",
		})
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
	frame.Meta.PreferredVisualization = data.VisTypeGraph
	// Typed time series hold only numeric and bool values, and Grafana's SQL expressions return an
	// empty table for typed string series, so string series stay untyped and fail with an error.
	// Explore cannot graph strings, and its table merges the series into one column, so Explore
	// shows them in a State timeline.
	if frame.Fields[values[0]].Type().NonNullableType() == data.FieldTypeString {
		frame.Meta.PreferredVisualizationPluginID = "state-timeline"
	} else {
		frame.Meta.Type, frame.Meta.TypeVersion = data.FrameTypeTimeSeriesMulti, data.FrameTypeVersion{0, 1}
	}
	if frame.Rows() == 0 {
		// A frame without fields is the no-data response, which alerting evaluates as no data whether
		// or not it is typed.
		empty := data.NewFrame(frame.Name)
		empty.Meta = frame.Meta
		return data.Frames{empty}, nil
	}
	times, err := fieldTimes(frame.Fields[schema.TimeIndex])
	if err != nil {
		return nil, err
	}
	return splitSeries(frame, schema.TimeIndex, factors, values, times, timeOrder(times), nonFiniteOK), nil
}

// nullTimeError is a fault in the query, so sqlErrorResponse returns it as a bad request rather
// than an internal error.
type nullTimeError struct{ column string }

func (e *nullTimeError) Error() string {
	return fmt.Sprintf("time column %q has null values; filter out rows where it is null or put another time column first", e.column)
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
			return nil, &nullTimeError{column: field.Name}
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
func splitSeries(frame *data.Frame, timeIndex int, factors, values []int, times []time.Time, order []int, nonFiniteOK bool) data.Frames {
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
			value := labelValue(frame.Fields[f], row)
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
		var scratch any // Holds a typed slice for this column's series.
		for _, id := range ids {
			rows := seriesRows[id]
			seriesTimes := make([]time.Time, len(rows))
			field := seriesField(column, rows, nonFiniteOK, &scratch)
			field.Name, field.Labels = column.Name, seriesLabels[id]
			name := seriesNames[id]
			if len(values) > 1 {
				// Alerting identifies a series by its labels alone, so the column goes in a label.
				field.Labels = withColumnLabel(seriesLabels[id], column.Name)
				name = column.Name + " " + name
			} else if strings.TrimSpace(name) != "" && !strings.EqualFold(column.Name, "value") {
				// Like Grafana with a field named Value, the name leaves out a column named value.
				name = column.Name + " " + name
			}
			// Without a name, as when there are no labels, Grafana names the series by its column.
			if name = strings.TrimSpace(name); name != "" {
				field.Config = &data.FieldConfig{DisplayNameFromDS: name}
			}
			for i, row := range rows {
				seriesTimes[i] = times[row]
			}
			series := data.NewFrame(frame.Name, data.NewField(timeName, nil, seriesTimes), field)
			series.Meta = frame.Meta
			frames = append(frames, series)
		}
	}
	return frames
}

// seriesField returns a field of column's type holding its values at rows. A series is
// non-nullable when its own rows have no nulls, and no NaNs or infinities unless nonFiniteOK, even
// if column is nullable. Copies are typed so that no value is boxed.
func seriesField(column *data.Field, rows []int, nonFiniteOK bool, scratch *any) *data.Field {
	switch column.Type().NonNullableType() {
	case data.FieldTypeInt8:
		return gather[int8](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeInt16:
		return gather[int16](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeInt32:
		return gather[int32](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeInt64:
		return gather[int64](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeUint8:
		return gather[uint8](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeUint16:
		return gather[uint16](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeUint32:
		return gather[uint32](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeUint64:
		return gather[uint64](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeFloat32:
		return gather[float32](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeFloat64:
		return gather[float64](column, rows, nonFiniteOK, scratch)
	case data.FieldTypeString:
		return gather[string](column, rows, nonFiniteOK, scratch)
	default: // bool, the only other value type
		return gather[bool](column, rows, nonFiniteOK, scratch)
	}
}

// gather copies column's values at rows. The result is nullable only if a value is null, or NaN or
// infinite unless nonFiniteOK.
func gather[T any](column *data.Field, rows []int, nonFiniteOK bool, scratch *any) *data.Field {
	at := column.PointerAt
	if column.Nullable() {
		at = column.At
	}
	ptr := func(row int) *T { return at(row).(*T) }
	values, _ := (*scratch).([]T)
	if cap(values) < len(rows) {
		values = make([]T, len(rows))
		*scratch = values
	}
	values = values[:len(rows)]
	for i, row := range rows {
		p := ptr(row)
		if p == nil || !nonFiniteOK && nonFiniteValue(*p) {
			return gatherNullable(column.Type(), rows, ptr)
		}
		values[i] = *p
	}
	// NewField copies values, so the next series can overwrite the scratch slice.
	return data.NewField("", nil, values)
}

func gatherNullable[T any](t data.FieldType, rows []int, ptr func(int) *T) *data.Field {
	field := data.NewFieldFromFieldType(t.NullableType(), len(rows))
	for i, row := range rows {
		field.Set(i, ptr(row))
	}
	return field
}

func nonFiniteValue[T any](v T) bool {
	switch f := any(v).(type) {
	case float64:
		return nonFinite(f)
	case float32:
		return nonFinite(f)
	}
	return false
}

// prefixSeriesNames puts each frame's name before the series names set on its fields. Grafana does
// this for names it builds when a panel's frames have different names, but not for names a data
// source sets.
func prefixSeriesNames(frames data.Frames) {
	for _, frame := range frames {
		for _, field := range frame.Fields {
			if field.Config != nil && field.Config.DisplayNameFromDS != "" {
				field.Config.DisplayNameFromDS = frame.Name + " " + field.Config.DisplayNameFromDS
			}
		}
	}
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

// labelValue reads a string or bool label column without allocating. A null label is "" or "false", as
// in the plugin SDK's LongToWide.
func labelValue(field *data.Field, i int) string {
	switch v := field.PointerAt(i).(type) {
	case *string:
		return *v
	case **string:
		if *v != nil {
			return **v
		}
	case *bool:
		return strconv.FormatBool(*v)
	case **bool:
		return strconv.FormatBool(*v != nil && **v)
	}
	return ""
}
