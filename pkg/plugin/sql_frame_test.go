package plugin

import (
	"bytes"
	"errors"
	"fmt"
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
			wantType: data.FieldTypeNullableFloat32,
			want:     []any{float32(1.5), nil},
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
			wantType: data.FieldTypeInt32,
			want:     []any{int32(-7)},
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
			want:     []any{"YQ==", nil},
		},
		{
			name:  "map",
			field: arrow.Field{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
			column: func(t *testing.T) arrow.Array {
				return arrowArray(t, arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String),
					`[[{"key": "unit", "value": "C"}, {"key": "sensor", "value": "imu"}], [], null]`)
			},
			wantType: data.FieldTypeNullableString,
			want:     []any{"unit=C, sensor=imu", "", nil},
		},
		{
			name:     "null type",
			field:    arrow.Field{Name: "c", Type: arrow.Null, Nullable: true},
			column:   func(t *testing.T) arrow.Array { return array.NewNull(2) },
			wantType: data.FieldTypeNullableString,
			want:     []any{nil, nil},
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

func TestFrameFromArrowStreamConcatenatesBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	values := [][]int64{{1, 2}, {3}, {4, 5}}
	valid := [][]bool{{true, false}, {true}, {false, true}}
	batch := 0
	stream := sqlArrowStream(t, schema, len(values), func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int64Builder).AppendValues(values[batch], valid[batch])
		batch++
	})
	frame, err := frameFromArrowStream(bytes.NewReader(stream), "A", testRowLimit)
	if err != nil {
		t.Fatalf("frameFromArrowStream() error = %v", err)
	}
	if got, want := fieldValues(frame.Fields[0]), []any{int64(1), nil, int64(3), nil, int64(5)}; !equalValues(got, want) {
		t.Errorf("values = %v, want %v", got, want)
	}
	if frame.Meta != nil {
		t.Errorf("frame notices = %+v, want none", frame.Meta.Notices)
	}
}

func TestFrameFromArrowStreamLimitsRows(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	next := int64(0)
	stream := sqlArrowStream(t, schema, 3, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{next + 1, next + 2}, nil)
		next += 2
	})
	for _, tc := range []struct {
		name      string
		limit     int64
		want      []any
		truncated bool
	}{
		{name: "within a batch", limit: 3, want: []any{int64(1), int64(2), int64(3)}, truncated: true},
		{name: "at a batch boundary", limit: 4, want: []any{int64(1), int64(2), int64(3), int64(4)}, truncated: true},
		{name: "whole stream", limit: 6, want: []any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := frameFromArrowStream(bytes.NewReader(stream), "A", tc.limit)
			if err != nil {
				t.Fatalf("frameFromArrowStream() error = %v", err)
			}
			if got := fieldValues(frame.Fields[0]); !equalValues(got, tc.want) {
				t.Errorf("values = %v, want %v", got, tc.want)
			}
			notice := fmt.Sprintf("Results have been limited to %d rows", tc.limit)
			truncated := frame.Meta != nil && len(frame.Meta.Notices) == 1 && strings.HasPrefix(frame.Meta.Notices[0].Text, notice)
			if truncated != tc.truncated {
				t.Errorf("frame meta = %+v, want row-limit notice %v", frame.Meta, tc.truncated)
			}
		})
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

func ref[T any](v T) *T { return &v }

func TestShapeSQLFrameSplitsSeries(t *testing.T) {
	at := func(s int) time.Time { return time.Unix(1700000000+int64(s), 0).UTC() }
	frame := data.NewFrame("A",
		data.NewField("time", nil, []time.Time{at(1), at(0), at(2), at(0)}),
		data.NewField("channel", nil, []string{"b", "a", "a", "b"}),
		data.NewField("avg", nil, []*float64{ref(2.0), ref(1.0), nil, ref(4.0)}),
		data.NewField("max", nil, []int64{20, 10, 30, 40}),
	)
	frame.RefID = "A"
	frame.Meta = &data.FrameMeta{ExecutedQueryString: "SELECT ..."}
	frames, err := shapeSQLFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil {
		t.Fatalf("shapeSQLFrame() error = %v", err)
	}
	want := []struct {
		name, channel string
		times         []any
		values        []any
	}{
		{"avg", "a", []any{at(0), at(2)}, []any{1.0, nil}},
		{"avg", "b", []any{at(0), at(1)}, []any{4.0, 2.0}},
		{"max", "a", []any{at(0), at(2)}, []any{int64(10), int64(30)}},
		{"max", "b", []any{at(0), at(1)}, []any{int64(40), int64(20)}},
	}
	if len(frames) != len(want) {
		t.Fatalf("got %d frames, want %d", len(frames), len(want))
	}
	for i, w := range want {
		f := frames[i]
		if f.RefID != "A" || f.Meta.Type != data.FrameTypeTimeSeriesMulti || f.Meta.ExecutedQueryString != "SELECT ..." {
			t.Errorf("frame %d RefID, meta = %q, %+v; want A, a multi-frame series with the input's query", i, f.RefID, f.Meta)
		}
		value := f.Fields[1]
		if value.Name != w.name || value.Labels["channel"] != w.channel {
			t.Errorf("frame %d = %s%v, want %s{channel=%s}", i, value.Name, value.Labels, w.name, w.channel)
		}
		if got := fieldValues(f.Fields[0]); !equalValues(got, w.times) {
			t.Errorf("frame %d times = %v, want %v", i, got, w.times)
		}
		if got := fieldValues(value); !equalValues(got, w.values) {
			t.Errorf("frame %d values = %v, want %v", i, got, w.values)
		}
	}
}

func TestShapeSQLFrameSortsByTime(t *testing.T) {
	early := time.Unix(1700000000, 0).UTC()
	late := early.Add(time.Second)
	for name, times := range map[string]any{
		"time":          []time.Time{late, early, late},
		"nullable time": []*time.Time{&late, &early, &late},
	} {
		t.Run(name, func(t *testing.T) {
			frame := data.NewFrame("A", data.NewField("time", nil, times), data.NewField("value", nil, []float64{2, 1, 3}))
			frames, err := shapeSQLFrame(frame, sqlutil.FormatOptionTimeSeries)
			if err != nil || len(frames) != 1 {
				t.Fatalf("shapeSQLFrame() = %d frames, %v; want 1 frame", len(frames), err)
			}
			if got, want := fieldValues(frames[0].Fields[0]), []any{early, late, late}; !equalValues(got, want) {
				t.Errorf("times = %v, want %v", got, want)
			}
			if got, want := fieldValues(frames[0].Fields[1]), []any{1.0, 2.0, 3.0}; !equalValues(got, want) {
				t.Errorf("values = %v, want %v with rows at equal times kept in order", got, want)
			}
		})
	}
}

func TestShapeSQLFrameWithoutTimeSeriesColumns(t *testing.T) {
	frame := data.NewFrame("A", data.NewField("time", nil, []time.Time{time.Unix(0, 0)}), data.NewField("channel", nil, []string{"a"}))
	frames, err := shapeSQLFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil || len(frames) != 1 || frames[0] != frame {
		t.Fatalf("shapeSQLFrame() = %v, %v; want the input frame", frames, err)
	}
	if frame.Meta == nil || len(frame.Meta.Notices) != 1 || !strings.Contains(frame.Meta.Notices[0].Text, "numeric column") {
		t.Errorf("frame meta = %+v, want a notice that a numeric column is needed", frame.Meta)
	}
}

func TestShapeSQLFramePassesThrough(t *testing.T) {
	for name, tc := range map[string]struct {
		frame  *data.Frame
		format sqlutil.FormatQueryOption
	}{
		"table":             {data.NewFrame("A", data.NewField("value", nil, []float64{1})), sqlutil.FormatOptionTable},
		"empty time series": {data.NewFrame("A", data.NewField("time", nil, []time.Time{}), data.NewField("value", nil, []float64{})), sqlutil.FormatOptionTimeSeries},
	} {
		t.Run(name, func(t *testing.T) {
			if frames, err := shapeSQLFrame(tc.frame, tc.format); err != nil || len(frames) != 1 || frames[0] != tc.frame {
				t.Errorf("shapeSQLFrame() = %v, %v; want the input frame", frames, err)
			}
		})
	}
}

func TestShapeSQLFrameRejectsNulls(t *testing.T) {
	for name, tc := range map[string]struct {
		frame *data.Frame
		want  string
	}{
		"time": {
			frame: data.NewFrame("A", data.NewField("time", nil, []*time.Time{nil}), data.NewField("value", nil, []float64{1})),
			want:  "null timestamp",
		},
		"label": {
			frame: data.NewFrame("A", data.NewField("time", nil, []time.Time{time.Unix(0, 0)}), data.NewField("channel", nil, []*string{nil}), data.NewField("value", nil, []float64{1})),
			want:  `"channel"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := shapeSQLFrame(tc.frame, sqlutil.FormatOptionTimeSeries); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("shapeSQLFrame() error = %v, want an error containing %s", err, tc.want)
			}
		})
	}
}

func BenchmarkFrameFromArrowStream(b *testing.B) {
	// 1M rows in batches about the size ClickHouse sends.
	const batches, batchRows = 16, 62_500
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{Name: "channel", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
	}, nil)
	row := 0
	stream := sqlArrowStream(b, schema, batches, func(rb *array.RecordBuilder) {
		ts := rb.Field(0).(*array.TimestampBuilder)
		channel := rb.Field(1).(*array.StringBuilder)
		value := rb.Field(2).(*array.Float64Builder)
		tags := rb.Field(3).(*array.MapBuilder)
		keys, items := tags.KeyBuilder().(*array.StringBuilder), tags.ItemBuilder().(*array.StringBuilder)
		for range batchRows {
			ts.Append(arrow.Timestamp(1700000000000000000 + int64(row)))
			channel.Append("x")
			value.Append(float64(row))
			tags.Append(true)
			keys.AppendValues([]string{"vehicle", "env"}, nil)
			items.AppendValues([]string{"car-1", "prod"}, nil)
			row++
		}
	})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := frameFromArrowStream(bytes.NewReader(stream), "A", testRowLimit); err != nil {
			b.Fatal(err)
		}
	}
}
