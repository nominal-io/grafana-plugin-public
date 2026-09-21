package plugin

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

func sqlArrowStream(t testing.TB, schema *arrow.Schema, batches int, build func(*array.RecordBuilder)) []byte {
	t.Helper()
	var out bytes.Buffer
	writer := ipc.NewWriter(&out, ipc.WithSchema(schema))
	for range batches {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		build(builder)
		record := builder.NewRecord()
		if err := writer.Write(record); err != nil {
			t.Fatal(err)
		}
		record.Release()
		builder.Release()
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestFrameFromArrowStreamTypesAndBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}}, {Name: "f", Type: arrow.PrimitiveTypes.Float32, Nullable: true}, {Name: "i", Type: arrow.PrimitiveTypes.Int32}, {Name: "u", Type: arrow.PrimitiveTypes.Uint32}, {Name: "ok", Type: arrow.FixedWidthTypes.Boolean}, {Name: "text", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
	stream := sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) {
		b.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1700000000500000, 1700000001500000}, nil)
		b.Field(1).(*array.Float32Builder).AppendValues([]float32{1.5, 0}, []bool{true, false})
		b.Field(2).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
		b.Field(3).(*array.Uint32Builder).AppendValues([]uint32{3, 4}, nil)
		b.Field(4).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
		b.Field(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("a"), nil}, []bool{true, false})
	})
	frame, err := frameFromArrowStream(bytes.NewReader(stream), "A")
	if err != nil {
		t.Fatal(err)
	}
	if frame.Rows() != 2 || frame.Fields[0].Type() != data.FieldTypeTime || frame.Fields[1].At(1).(*float64) != nil || frame.Fields[5].At(1).(*string) != nil {
		t.Fatalf("unexpected frame %#v", frame)
	}
	if got := frame.Fields[0].At(0).(time.Time); !got.Equal(time.Unix(1700000000, 500000000)) {
		t.Fatal(got)
	}
}

func TestFrameFromArrowStreamMapDictionaryAndEmpty(t *testing.T) {
	mapSchema := arrow.NewSchema([]arrow.Field{{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)}}, nil)
	mapStream := sqlArrowStream(t, mapSchema, 1, func(b *array.RecordBuilder) {
		m := b.Field(0).(*array.MapBuilder)
		m.Append(true)
		m.KeyBuilder().(*array.StringBuilder).Append("key")
		m.ItemBuilder().(*array.StringBuilder).Append("value")
	})
	frame, err := frameFromArrowStream(bytes.NewReader(mapStream), "A")
	if err != nil || frame.Fields[0].Type() != data.FieldTypeString || !bytes.Contains([]byte(frame.Fields[0].At(0).(string)), []byte("key")) {
		t.Fatalf("%v %#v", err, frame)
	}
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}
	dictSchema := arrow.NewSchema([]arrow.Field{{Name: "channel", Type: dictType}}, nil)
	dictStream := sqlArrowStream(t, dictSchema, 1, func(b *array.RecordBuilder) {
		builder := b.Field(0).(*array.BinaryDictionaryBuilder)
		_ = builder.AppendString("a")
		_ = builder.AppendString("b")
	})
	dictFrame, err := frameFromArrowStream(bytes.NewReader(dictStream), "A")
	if err != nil || dictFrame.Fields[0].At(0).(string) != "a" || dictFrame.Fields[0].At(1).(string) != "b" {
		t.Fatalf("%v %#v", err, dictFrame)
	}
	emptySchema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64}}, nil)
	empty, err := frameFromArrowStream(bytes.NewReader(sqlArrowStream(t, emptySchema, 1, func(*array.RecordBuilder) {})), "A")
	if err != nil || empty.Rows() != 0 || len(empty.Fields) != 1 {
		t.Fatalf("%v %#v", err, empty)
	}
}

func TestFrameFromArrowStreamConcatenatesBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	frame, err := frameFromArrowStream(bytes.NewReader(sqlArrowStream(t, schema, 3, func(b *array.RecordBuilder) { b.Field(0).(*array.Int64Builder).Append(1) })), "A")
	if err != nil || frame.Rows() != 3 {
		t.Fatalf("%v %#v", err, frame)
	}
}

func TestFrameFromArrowStreamNanosecondTimestamp(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}}}, nil)
	frame, err := frameFromArrowStream(bytes.NewReader(sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) { b.Field(0).(*array.TimestampBuilder).Append(1700000000123456789) })), "A")
	if err != nil || !frame.Fields[0].At(0).(time.Time).Equal(time.Unix(1700000000, 123456789)) {
		t.Fatalf("%v %#v", err, frame)
	}
}

func TestShapeSqlFrame(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	a, b := "a", "b"
	one, two := 1.0, 2.0
	long := data.NewFrame("A", data.NewField("time", nil, []*time.Time{&ts, &ts}), data.NewField("channel", nil, []*string{&b, &a}), data.NewField("value", nil, []*float64{&two, &one}))
	wide, err := shapeSqlFrame(long, sqlutil.FormatOptionTimeSeries)
	if err != nil || wide.Rows() != 1 || len(wide.Fields) != 3 {
		t.Fatalf("%v %#v", err, wide)
	}
	if wide.Fields[1].Labels["channel"] != "a" || wide.Fields[2].Labels["channel"] != "b" {
		t.Fatalf("%v %v", wide.Fields[1].Labels, wide.Fields[2].Labels)
	}
	noTime := data.NewFrame("A", data.NewField("value", nil, []*float64{&one}))
	out, err := shapeSqlFrame(noTime, sqlutil.FormatOptionTimeSeries)
	if err != nil || out.Meta == nil || len(out.Meta.Notices) != 1 {
		t.Fatal(err)
	}
	if got, _ := shapeSqlFrame(noTime, sqlutil.FormatOptionTable); got != noTime {
		t.Fatal("table should pass through")
	}
}

func BenchmarkFrameFromArrowStream(b *testing.B) {
	rows := 1000000
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}}, {Name: "channel", Type: arrow.BinaryTypes.String}, {Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil)
	stream := sqlArrowStream(b, schema, 1, func(rb *array.RecordBuilder) {
		ts := rb.Field(0).(*array.TimestampBuilder)
		channel := rb.Field(1).(*array.StringBuilder)
		value := rb.Field(2).(*array.Float64Builder)
		for i := 0; i < rows; i++ {
			ts.Append(arrow.Timestamp(1700000000000000000 + int64(i)))
			channel.Append("x")
			value.Append(float64(i))
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := frameFromArrowStream(bytes.NewReader(stream), "A"); err != nil {
			b.Fatal(err)
		}
	}
}

func TestShapeSqlFrameEmptyTimeSeries(t *testing.T) {
	frame := data.NewFrame("A", data.NewField("time", nil, []time.Time{}), data.NewField("channel", nil, []string{}), data.NewField("value", nil, []float64{}))
	frame.RefID = "A"
	out, err := shapeSqlFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil {
		t.Fatalf("an empty time range should return no data, not an error: %v", err)
	}
	if out.Rows() != 0 || out.RefID != "A" {
		t.Fatalf("unexpected empty response: %+v", out)
	}
}

func TestShapeSqlFrameSparseSeries(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	frame := data.NewFrame("A",
		data.NewField("time", nil, []time.Time{ts, ts.Add(time.Second)}),
		data.NewField("channel", nil, []string{"a", "b"}),
		data.NewField("count", nil, []int64{3, 4}),
	)
	frame.RefID = "B"
	frame.Meta = &data.FrameMeta{ExecutedQueryString: "SELECT ..."}
	out, err := shapeSqlFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil {
		t.Fatal(err)
	}
	if out.RefID != "B" || out.Meta.ExecutedQueryString != frame.Meta.ExecutedQueryString {
		t.Errorf("query identity and inspector metadata must survive conversion: %+v", out)
	}
	for col, missingRow := range map[int]int{1: 1, 2: 0} {
		if value, present := out.Fields[col].ConcreteAt(missingRow); present {
			t.Errorf("missing sample became %v; expected null", value)
		}
	}
}

func TestShapeSqlFrameSortsWideSeries(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	frame := data.NewFrame("A", data.NewField("time", nil, []time.Time{ts.Add(time.Second), ts}), data.NewField("value", nil, []float64{2, 1}))
	out, err := shapeSqlFrame(frame, sqlutil.FormatOptionTimeSeries)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Fields[0].At(0).(time.Time).Equal(ts) || out.Fields[1].At(0).(float64) != 1 {
		t.Fatal("time and value rows must be sorted together")
	}
}

func TestShapeSqlFrameRejectsNullTime(t *testing.T) {
	frame := data.NewFrame("A", data.NewField("time", nil, []*time.Time{nil}), data.NewField("value", nil, []float64{1}))
	if _, err := shapeSqlFrame(frame, sqlutil.FormatOptionTimeSeries); err == nil {
		t.Fatal("null timestamps cannot be plotted")
	}
}
