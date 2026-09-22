package plugin

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

const testRowLimit = 1_000_000

func sqlArrowStream(t testing.TB, schema *arrow.Schema, batches int, build func(*array.RecordBuilder)) []byte {
	t.Helper()
	var out bytes.Buffer
	writer := ipc.NewWriter(&out, ipc.WithSchema(schema))
	for range batches {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		build(builder)
		record := builder.NewRecordBatch()
		if err := writer.Write(record); err != nil {
			t.Fatalf("writing Arrow batch: %v", err)
		}
		record.Release()
		builder.Release()
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing Arrow writer: %v", err)
	}
	return out.Bytes()
}

// arrowColumnStream writes column as a single-batch Arrow stream.
func arrowColumnStream(t *testing.T, field arrow.Field, column arrow.Array) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(column.Len()))
	defer record.Release()
	var out bytes.Buffer
	writer := ipc.NewWriter(&out, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		t.Fatalf("writing Arrow batch: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing Arrow writer: %v", err)
	}
	return out.Bytes()
}

func arrowArray(t *testing.T, dataType arrow.DataType, values string) arrow.Array {
	t.Helper()
	column, _, err := array.FromJSON(memory.DefaultAllocator, dataType, strings.NewReader(values))
	if err != nil {
		t.Fatalf("array.FromJSON(%s, %s) error = %v", dataType, values, err)
	}
	return column
}

func fieldValues(field *data.Field) []any {
	values := make([]any, field.Len())
	for i := range values {
		if v, ok := field.ConcreteAt(i); ok {
			values[i] = v
		}
	}
	return values
}

func TestFrameFromArrowStreamColumnTypes(t *testing.T) {
	stringDictionary := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	for _, tc := range []struct {
		name     string
		field    arrow.Field
		column   func(t *testing.T) arrow.Array
		wantType data.FieldType
		want     []any
	}{
		{
			name:  "microsecond timestamp",
			field: arrow.Field{Name: "c", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
			column: func(t *testing.T) arrow.Array {
				return arrowArray(t, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, `[1700000000500000]`)
			},
			wantType: data.FieldTypeTime,
			want:     []any{time.Unix(1700000000, 500000000).UTC()},
		},
		{
			name:  "nanosecond timestamp",
			field: arrow.Field{Name: "c", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
			column: func(t *testing.T) arrow.Array {
				return arrowArray(t, &arrow.TimestampType{Unit: arrow.Nanosecond}, `[1700000000123456789]`)
			},
			wantType: data.FieldTypeTime,
			want:     []any{time.Unix(1700000000, 123456789).UTC()},
		},
		{
			name:  "date",
			field: arrow.Field{Name: "c", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
			column: func(t *testing.T) arrow.Array {
				return arrowArray(t, arrow.FixedWidthTypes.Date32, `["2024-01-02", null]`)
			},
			wantType: data.FieldTypeNullableTime,
			want:     []any{time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), nil},
		},
		{
			name:     "nullable float",
			field:    arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			column:   func(t *testing.T) arrow.Array { return arrowArray(t, arrow.PrimitiveTypes.Float32, `[1.5, null]`) },
			wantType: data.FieldTypeNullableFloat64,
			want:     []any{1.5, nil},
		},
		{
			name:     "half float",
			field:    arrow.Field{Name: "c", Type: arrow.FixedWidthTypes.Float16},
			column:   func(t *testing.T) arrow.Array { return arrowArray(t, arrow.FixedWidthTypes.Float16, `[2.5]`) },
			wantType: data.FieldTypeFloat64,
			want:     []any{2.5},
		},
		{
			name:  "decimal",
			field: arrow.Field{Name: "c", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
			column: func(t *testing.T) arrow.Array {
				return arrowArray(t, &arrow.Decimal128Type{Precision: 10, Scale: 2}, `["1.25"]`)
			},
			wantType: data.FieldTypeFloat64,
			want:     []any{1.25},
		},
		{
			name:  "wide decimal",
			field: arrow.Field{Name: "c", Type: &arrow.Decimal256Type{Precision: 40, Scale: 1}},
			column: func(t *testing.T) arrow.Array {
				return arrowArray(t, &arrow.Decimal256Type{Precision: 40, Scale: 1}, `["-3.5"]`)
			},
			wantType: data.FieldTypeFloat64,
			want:     []any{-3.5},
		},
		{
			name:     "signed integer",
			field:    arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int32},
			column:   func(t *testing.T) arrow.Array { return arrowArray(t, arrow.PrimitiveTypes.Int32, `[-7]`) },
			wantType: data.FieldTypeInt64,
			want:     []any{int64(-7)},
		},
		{
			name:  "unsigned integer",
			field: arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			column: func(t *testing.T) arrow.Array {
				return arrowArray(t, arrow.PrimitiveTypes.Uint64, `[18446744073709551615, null]`)
			},
			wantType: data.FieldTypeNullableUint64,
			want:     []any{uint64(18446744073709551615), nil},
		},
		{
			name:     "bool",
			field:    arrow.Field{Name: "c", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			column:   func(t *testing.T) arrow.Array { return arrowArray(t, arrow.FixedWidthTypes.Boolean, `[true, null]`) },
			wantType: data.FieldTypeNullableBool,
			want:     []any{true, nil},
		},
		{
			name:     "binary",
			field:    arrow.Field{Name: "c", Type: arrow.BinaryTypes.Binary, Nullable: true},
			column:   func(t *testing.T) arrow.Array { return arrowArray(t, arrow.BinaryTypes.Binary, `["YQ==", null]`) },
			wantType: data.FieldTypeNullableString,
			want:     []any{"a", nil},
		},
		{
			name:  "map",
			field: arrow.Field{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
			column: func(t *testing.T) arrow.Array {
				return arrowArray(t, arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), `[[{"key": "unit", "value": "C"}]]`)
			},
			wantType: data.FieldTypeString,
			want:     []any{`[{"key":"unit","value":"C"}]`},
		},
		{
			name:     "null type",
			field:    arrow.Field{Name: "c", Type: arrow.Null, Nullable: true},
			column:   func(t *testing.T) arrow.Array { return array.NewNull(2) },
			wantType: data.FieldTypeNullableString,
			want:     []any{nil, nil},
		},
		{
			name:  "dictionary with a null entry",
			field: arrow.Field{Name: "channel", Type: stringDictionary},
			column: func(t *testing.T) arrow.Array {
				return array.NewDictionaryArray(stringDictionary, arrowArray(t, arrow.PrimitiveTypes.Int8, `[1, 0, null]`), arrowArray(t, arrow.BinaryTypes.String, `["a", null]`))
			},
			wantType: data.FieldTypeNullableString,
			want:     []any{nil, "a", nil},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := frameFromArrowStream(bytes.NewReader(arrowColumnStream(t, tc.field, tc.column(t))), "A", testRowLimit)
			if err != nil {
				t.Fatalf("frameFromArrowStream() error = %v", err)
			}
			field := frame.Fields[0]
			if field.Type() != tc.wantType {
				t.Errorf("field type = %s, want %s", field.Type(), tc.wantType)
			}
			if got := fieldValues(field); !equalValues(got, tc.want) {
				t.Errorf("values = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func equalValues(got, want []any) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if gotTime, ok := got[i].(time.Time); ok {
			wantTime, ok := want[i].(time.Time)
			if !ok || !gotTime.Equal(wantTime) {
				return false
			}
			continue
		}
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestFrameFromArrowStreamRejectsBadDictionaryIndex(t *testing.T) {
	dictionary := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Float64}
	column := array.NewDictionaryArray(dictionary, arrowArray(t, arrow.PrimitiveTypes.Int32, `[0, 5]`), arrowArray(t, arrow.PrimitiveTypes.Float64, `[1]`))
	stream := arrowColumnStream(t, arrow.Field{Name: "v", Type: dictionary}, column)
	if _, err := frameFromArrowStream(bytes.NewReader(stream), "A", testRowLimit); err == nil {
		t.Fatal("frameFromArrowStream() error = nil, want an out-of-range dictionary index error")
	}
}

func TestFrameFromArrowStreamConcatenatesBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	stream := sqlArrowStream(t, schema, 3, func(b *array.RecordBuilder) { b.Field(0).(*array.Int64Builder).Append(1) })
	frame, err := frameFromArrowStream(bytes.NewReader(stream), "A", testRowLimit)
	if err != nil || frame.Rows() != 3 {
		t.Fatalf("frameFromArrowStream() = %d rows, %v; want 3 rows", frame.Rows(), err)
	}
	if frame.Meta != nil {
		t.Errorf("frame notices = %+v, want none", frame.Meta.Notices)
	}
}

func TestFrameFromArrowStreamLimitsRows(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	stream := sqlArrowStream(t, schema, 3, func(b *array.RecordBuilder) { b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil) })
	frame, err := frameFromArrowStream(bytes.NewReader(stream), "A", 3)
	if err != nil || frame.Rows() != 3 {
		t.Fatalf("frameFromArrowStream() = %d rows, %v; want 3 rows", frame.Rows(), err)
	}
	if frame.Meta == nil || len(frame.Meta.Notices) != 1 || frame.Meta.Notices[0].Severity != data.NoticeSeverityWarning {
		t.Errorf("frame meta = %+v, want one row-limit warning", frame.Meta)
	}
}

func TestFrameFromArrowStreamEmptyResponse(t *testing.T) {
	_, err := frameFromArrowStream(bytes.NewReader(nil), "A", testRowLimit)
	if err == nil || err.Error() != "SQL response was empty" {
		t.Fatalf("frameFromArrowStream() error = %v, want %q", err, "SQL response was empty")
	}
}

func TestFrameFromArrowStreamSchemaOnly(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64}}, nil)
	frame, err := frameFromArrowStream(bytes.NewReader(sqlArrowStream(t, schema, 0, nil)), "A", testRowLimit)
	if err != nil || frame.Rows() != 0 || len(frame.Fields) != 1 {
		t.Fatalf("frameFromArrowStream() = %d fields, %d rows, %v; want 1 field, 0 rows", len(frame.Fields), frame.Rows(), err)
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestFrameFromArrowStreamReportsErrorAfterLastBatch(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	stream := sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) { b.Field(0).(*array.Int64Builder).Append(1) })
	failure := errors.New("query failed after the last batch")
	_, err := frameFromArrowStream(io.MultiReader(bytes.NewReader(stream), failingReader{failure}), "A", testRowLimit)
	if !errors.Is(err, failure) {
		t.Fatalf("frameFromArrowStream() error = %v, want %v", err, failure)
	}
}

func TestShapeSQLFrameLongToWide(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	frame := data.NewFrame("A",
		data.NewField("time", nil, []time.Time{ts, ts, ts.Add(time.Second)}),
		data.NewField("channel", nil, []string{"b", "a", "a"}),
		data.NewField("value", nil, []float64{2, 1, 3}),
	)
	frame.RefID = "B"
	frame.Meta = &data.FrameMeta{ExecutedQueryString: "SELECT ..."}
	wide, err := shapeSQLFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil {
		t.Fatalf("shapeSQLFrame() error = %v", err)
	}
	if wide.RefID != "B" || wide.Meta == nil || wide.Meta.ExecutedQueryString != "SELECT ..." {
		t.Errorf("RefID, meta = %q, %+v; want the input's", wide.RefID, wide.Meta)
	}
	if len(wide.Fields) != 3 || wide.Fields[1].Labels["channel"] != "a" || wide.Fields[2].Labels["channel"] != "b" {
		t.Fatalf("fields = %v; want time plus one value field for each channel", wide.Fields)
	}
	if got := fieldValues(wide.Fields[2]); !equalValues(got, []any{2.0, nil}) {
		t.Errorf("channel b values = %v, want [2 <nil>] with the missing sample as null", got)
	}
}

func TestShapeSQLFrameSortsByTime(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	frame := data.NewFrame("A", data.NewField("time", nil, []time.Time{ts.Add(time.Second), ts}), data.NewField("value", nil, []float64{2, 1}))
	frame.RefID = "A"
	out, err := shapeSQLFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil {
		t.Fatalf("shapeSQLFrame() error = %v", err)
	}
	if got := fieldValues(out.Fields[1]); !equalValues(got, []any{1.0, 2.0}) || out.RefID != "A" {
		t.Errorf("sorted values, RefID = %v, %q; want [1 2], %q", got, out.RefID, "A")
	}
}

func TestShapeSQLFrameWithoutTimeSeriesColumns(t *testing.T) {
	frame := data.NewFrame("A", data.NewField("time", nil, []time.Time{time.Unix(0, 0)}), data.NewField("channel", nil, []string{"a"}))
	out, err := shapeSQLFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil {
		t.Fatalf("shapeSQLFrame() error = %v", err)
	}
	if out.Meta == nil || len(out.Meta.Notices) != 1 || !strings.Contains(out.Meta.Notices[0].Text, "numeric column") {
		t.Errorf("frame meta = %+v, want a notice that a numeric column is needed", out.Meta)
	}
}

func TestShapeSQLFrameTablePassesThrough(t *testing.T) {
	frame := data.NewFrame("A", data.NewField("value", nil, []float64{1}))
	if got, err := shapeSQLFrame(frame, sqlutil.FormatOptionTable); err != nil || got != frame {
		t.Errorf("shapeSQLFrame(table) = %p, %v; want the input frame %p", got, err, frame)
	}
}

func TestShapeSQLFrameEmptyTimeSeries(t *testing.T) {
	frame := data.NewFrame("A", data.NewField("time", nil, []time.Time{}), data.NewField("channel", nil, []string{}), data.NewField("value", nil, []float64{}))
	frame.RefID = "A"
	out, err := shapeSQLFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil || out.Rows() != 0 || out.RefID != "A" {
		t.Errorf("shapeSQLFrame() = %d rows, RefID %q, %v; want an empty frame for A", out.Rows(), out.RefID, err)
	}
}

func TestShapeSQLFrameRejectsNullTime(t *testing.T) {
	frame := data.NewFrame("A", data.NewField("time", nil, []*time.Time{nil}), data.NewField("value", nil, []float64{1}))
	if _, err := shapeSQLFrame(frame, sqlutil.FormatOptionTimeSeries); err == nil {
		t.Error("shapeSQLFrame() error = nil, want an error for a null timestamp")
	}
}

func BenchmarkFrameFromArrowStream(b *testing.B) {
	const rows = 1_000_000
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}}, {Name: "channel", Type: arrow.BinaryTypes.String}, {Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil)
	stream := sqlArrowStream(b, schema, 1, func(rb *array.RecordBuilder) {
		ts := rb.Field(0).(*array.TimestampBuilder)
		channel := rb.Field(1).(*array.StringBuilder)
		value := rb.Field(2).(*array.Float64Builder)
		for i := range rows {
			ts.Append(arrow.Timestamp(1700000000000000000 + int64(i)))
			channel.Append("x")
			value.Append(float64(i))
		}
	})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := frameFromArrowStream(bytes.NewReader(stream), "A", testRowLimit); err != nil {
			b.Fatal(err)
		}
	}
}
