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
func frameFromArrowStream(r io.ReadCloser, name string, rowLimit int64, format sqlutil.FormatQueryOption, nulls nullability) (data.Frames, error) {
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
	var columns []any
	if format == sqlutil.FormatOptionLogs {
		frame, err = sqlLogFrame(name, fields, batches, int(rows), nulls)
	} else {
		frame, columns, err = tableFrame(name, fields, batches, int(rows), nulls)
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
	return shapeSQLFrame(frame, columns, nulls)
}

// tableFrame converts every column of batches to a field of a frame that prefers a table. It
// converts columns itself because data.FromArrowRecord rejects nested types such as the
// map<string,string> tags column and assumes nanosecond timestamps. It also returns the slice each
// field was copied from, a []T or []*T, which is cheaper to read than the field.
func tableFrame(name string, fields []arrow.Field, batches []arrow.RecordBatch, rows int, nulls nullability) (*data.Frame, []any, error) {
	frame := data.NewFrame(name)
	frame.Meta = &data.FrameMeta{PreferredVisualization: data.VisTypeTable}
	columns := make([]any, len(fields))
	chunks := make([]arrow.Array, len(batches))
	for i, field := range fields {
		for b, batch := range batches {
			chunks[b] = batch.Column(i)
		}
		values, err := columnValues(field, chunks, rows, nulls)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read column %q: %w", field.Name, err)
		}
		columns[i] = values
		frame.Fields = append(frame.Fields, data.NewField(field.Name, nil, values))
	}
	return frame, columns, nil
}

// columnValues converts one Arrow column, split across record batches, to the values of a Grafana
// field: a []*T with nil for nulls when nulls says the column must be nullable, else a []T.
func columnValues(field arrow.Field, chunks []arrow.Array, rows int, nulls nullability) (any, error) {
	n := field.Nullable && nulls == forExpressions ||
		slices.ContainsFunc(chunks, func(c arrow.Array) bool { return c.NullN() > 0 || nulls != nullOnly && hasNonFinite(c) })
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

// nullability decides which value fields are sent nullable. Grafana decodes a nullable field
// with one allocation per value, so fields are non-nullable where nothing below requires
// otherwise. The zero value is safe for any Grafana.
type nullability int

const (
	// nullOrNonFinite makes a field nullable when it holds a null, NaN or ±Inf. Grafana's SQL
	// expressions read NaN and ±Inf as NULL and, before 13.4.0, fail when they write that NULL to
	// a non-nullable column.
	nullOrNonFinite nullability = iota
	// nullOnly makes a field nullable only when it holds a null, for Grafana 13.4.0 and later.
	nullOnly
	// forExpressions also makes compute and SQL series values nullable, and SQL columns whose
	// Arrow type allows nulls. Grafana's expressions and alerting convert every value to a
	// pointer, and before 13.4.0 a non-nullable field costs them an allocation per value.
	forExpressions
)

// fromExpressionHeader marks a data source request that Grafana makes for an expression.
const fromExpressionHeader = "X-Grafana-From-Expr"

// nullabilityFor picks the nullability for a request from Grafana at version, made for alerting
// or an expression when fromExpressions.
func nullabilityFor(version string, fromExpressions bool) nullability {
	switch {
	case grafana134(version):
		return nullOnly
	case fromExpressions:
		return forExpressions
	}
	return nullOrNonFinite
}

// grafana134 reports whether version is 13.4.0 or later, other than a 13.4.0 pre-release build.
// Builds made before the 13.4.0 release report 13.4.0 with a "-" suffix and may lack its changes.
// Security releases, such as 13.4.0+security-01, count.
func grafana134(version string) bool {
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
// value column is returned as it came, with a notice. columns is nil or holds, for each field, nil
// or a slice with the field's values. Those slices are only read, and a nullable series may point
// into them.
func shapeSQLFrame(frame *data.Frame, columns []any, nulls nullability) (data.Frames, error) {
	if columns == nil {
		columns = make([]any, len(frame.Fields))
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
	times, ok := columns[schema.TimeIndex].([]time.Time)
	if !ok {
		var err error
		if times, err = fieldTimes(frame.Fields[schema.TimeIndex]); err != nil {
			return nil, err
		}
	}
	return splitSeries(frame, columns, schema.TimeIndex, factors, values, times, nulls), nil
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
// with rows sorted by time. It allocates per series rather than per row.
func splitSeries(frame *data.Frame, columns []any, timeIndex int, factors, values []int, times []time.Time, nulls nullability) data.Frames {
	order := timeOrder(times)
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
			var rows []int
			if len(factors) == 0 {
				rows = make([]int, 0, len(times)) // Every row joins this one series.
			}
			seriesRows = append(seriesRows, rows)
		}
		seriesRows[id] = append(seriesRows[id], row)
	}

	ids := make([]int, len(seriesRows))
	labelKeys := make([]string, len(seriesRows))
	for id := range ids {
		ids[id], labelKeys[id] = id, seriesLabels[id].String()
	}
	slices.SortFunc(ids, func(a, b int) int { return strings.Compare(labelKeys[a], labelKeys[b]) })

	timeColumn := frame.Fields[timeIndex]
	var seriesTimes []time.Time // Reused, because NewField copies it.
	var frames data.Frames
	for _, index := range values {
		column := frame.Fields[index]
		gather := seriesGatherer(column, columns[index], nulls)
		for _, id := range ids {
			rows := seriesRows[id]
			// A series that holds every row in its original order can keep frame's fields.
			whole := order == nil && len(rows) == len(times)
			field := gather(rows, whole)
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
			timeField := timeColumn
			// Only the first frame keeps the time column, so that no two frames share a field.
			if !whole || timeColumn.Nullable() || index != values[0] {
				seriesTimes = slices.Grow(seriesTimes[:0], len(rows))
				for _, row := range rows {
					seriesTimes = append(seriesTimes, times[row])
				}
				timeField = data.NewField(timeColumn.Name, nil, seriesTimes)
			}
			series := data.NewFrame(frame.Name, timeField, field)
			series.Meta = frame.Meta
			frames = append(frames, series)
		}
	}
	return frames
}

// seriesGatherer returns a function that copies column's values at rows into a field of its type,
// nullable as nulls decides from those rows rather than from the column. When rows is whole,
// meaning every row in order, it returns column itself if that needs no change. original is nil
// or the slice column was copied from, which is cheaper to read. Copies are typed so that no
// value is boxed.
func seriesGatherer(column *data.Field, original any, nulls nullability) func(rows []int, whole bool) *data.Field {
	switch column.Type().NonNullableType() {
	case data.FieldTypeInt8:
		return gatherer[int8](column, original, nulls)
	case data.FieldTypeInt16:
		return gatherer[int16](column, original, nulls)
	case data.FieldTypeInt32:
		return gatherer[int32](column, original, nulls)
	case data.FieldTypeInt64:
		return gatherer[int64](column, original, nulls)
	case data.FieldTypeUint8:
		return gatherer[uint8](column, original, nulls)
	case data.FieldTypeUint16:
		return gatherer[uint16](column, original, nulls)
	case data.FieldTypeUint32:
		return gatherer[uint32](column, original, nulls)
	case data.FieldTypeUint64:
		return gatherer[uint64](column, original, nulls)
	case data.FieldTypeFloat32:
		return gatherer[float32](column, original, nulls)
	case data.FieldTypeFloat64:
		return gatherer[float64](column, original, nulls)
	case data.FieldTypeString:
		return gatherer[string](column, original, nulls)
	default: // bool, the only other value type
		return gatherer[bool](column, original, nulls)
	}
}

func gatherer[T any](column *data.Field, original any, nulls nullability) func(rows []int, whole bool) *data.Field {
	var ptr func(row int) *T
	switch o := original.(type) {
	case []T:
		ptr = func(row int) *T { return &o[row] }
	case []*T:
		ptr = func(row int) *T { return o[row] }
	default:
		at := column.PointerAt
		if column.Nullable() {
			at = column.At
		}
		ptr = func(row int) *T { return at(row).(*T) }
	}
	var scratch []T // Reused across series, because NewField copies it.
	return func(rows []int, whole bool) *data.Field {
		if nulls == forExpressions {
			return gatherNullable(column.Type(), rows, ptr)
		}
		if whole && !column.Nullable() && (nulls == nullOnly || !slices.ContainsFunc(rows, func(row int) bool { return nonFiniteValue(*ptr(row)) })) {
			return column
		}
		if cap(scratch) < len(rows) {
			scratch = make([]T, len(rows))
		}
		values := scratch[:len(rows)]
		for i, row := range rows {
			p := ptr(row)
			if p == nil || nulls != nullOnly && nonFiniteValue(*p) {
				return gatherNullable(column.Type(), rows, ptr)
			}
			values[i] = *p
		}
		return data.NewField("", nil, values)
	}
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
