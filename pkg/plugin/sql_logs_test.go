package plugin

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

var testTimestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}

var dictString = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}

// testLogColumn is one nullable column of a Logs test result, with its Arrow JSON values.
type testLogColumn struct {
	name   string
	typ    arrow.DataType
	values string
}

func tsColumn(name, values string) testLogColumn {
	return testLogColumn{name: name, typ: testTimestampType, values: values}
}

func strColumn(name, values string) testLogColumn {
	return testLogColumn{name: name, typ: arrow.BinaryTypes.String, values: values}
}

func mapColumn(name, values string) testLogColumn {
	return testLogColumn{name: name, typ: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), values: values}
}

func logStream(t *testing.T, columns ...testLogColumn) []byte {
	t.Helper()
	fields := make([]arrow.Field, len(columns))
	arrays := make([]arrow.Array, len(columns))
	for i, c := range columns {
		fields[i] = arrow.Field{Name: c.name, Type: c.typ, Nullable: true}
		arrays[i] = arrowArray(t, c.typ, c.values)
		defer arrays[i].Release()
	}
	return arrowColumnsStream(t, fields, arrays)
}

// shapedLogFrame decodes stream in Logs format.
func shapedLogFrame(t *testing.T, stream []byte) *data.Frame {
	t.Helper()
	frames, err := frameFromArrowStream(io.NopCloser(bytes.NewReader(stream)), "A", testRowLimit, sqlutil.FormatOptionLogs)
	if err != nil || len(frames) != 1 {
		t.Fatalf("frameFromArrowStream() = %d frames, %v; want 1 frame", len(frames), err)
	}
	return frames[0]
}

func logRow(frame *data.Frame, i int) (body, id, labels string) {
	return frame.Fields[1].At(i).(string), frame.Fields[2].At(i).(string), string(frame.Fields[3].At(i).(json.RawMessage))
}

func TestSQLLogsBodyAndLabels(t *testing.T) {
	ts := tsColumn("ts", `[1700000000000000]`)
	message := strColumn("message", `["m"]`)
	for _, tc := range []struct {
		name    string
		columns []testLogColumn
		body    string
		labels  string
	}{
		{"first string column", []testLogColumn{ts, strColumn("channel", `["c"]`), strColumn("msg", `["m"]`)}, "c", `{"msg":"m"}`},
		{"case is ignored", []testLogColumn{ts, strColumn("MESSAGE", `["m"]`), strColumn("channel", `["c"]`)}, "m", `{"channel":"c"}`},
		{"dictionary-encoded message", []testLogColumn{ts, strColumn("channel", `["c"]`), {name: "message", typ: dictString, values: `["m"]`}}, "m", `{"channel":"c"}`},
		{"non-string message is a label", []testLogColumn{ts, {name: "message", typ: arrow.PrimitiveTypes.Int64, values: `[7]`}, strColumn("text", `["t"]`)}, "t", `{"message":"7"}`},
		{"null body, null map and null map item", []testLogColumn{
			ts,
			strColumn("message", `[null]`),
			mapColumn("args", `[null]`),
			mapColumn("tags", `[[{"key":"a","value":null},{"key":"b","value":"1"}]]`),
		}, "", `{"tags.b":"1"}`},
		{"columns and map entries", []testLogColumn{
			ts,
			message,
			strColumn("channel", `["c1"]`),
			mapColumn("tags", `[[{"key":"host","value":"b"},{"key":"vehicle","value":"v"}]]`),
			mapColumn("args", `[[{"key":"host","value":"a"},{"key":"channel","value":"x"}]]`),
		}, "m", `{"args.channel":"x","channel":"c1","host":"a","tags.host":"b","tags.vehicle":"v"}`},
		{"a column beats a prefixed map entry", []testLogColumn{
			ts,
			message,
			strColumn("tags.host", `["column"]`),
			mapColumn("tags", `[[{"key":"host","value":"map"}]]`),
		}, "m", `{"tags.host":"column"}`},
		{"dictionary-encoded map", []testLogColumn{ts, message, {name: "args", typ: arrow.MapOf(dictString, dictString), values: `[[{"key":"level","value":"error"},{"key":"host","value":null}]]`}}, "m", `{"level":"error"}`},
		{"other maps are prefixed, and a map of nulls adds none", []testLogColumn{
			ts,
			message,
			{name: "nullmap", typ: arrow.MapOf(arrow.BinaryTypes.String, arrow.Null), values: `[[{"key":"k","value":null}]]`},
			mapColumn("tags", `[[{"key":"host","value":"b"}]]`),
		}, "m", `{"tags.host":"b"}`},
		{"types and nulls", []testLogColumn{
			ts,
			message,
			strColumn("missing", `[null]`),
			{name: "n", typ: arrow.PrimitiveTypes.Int64, values: `[42]`},
			{name: "ok", typ: arrow.FixedWidthTypes.Boolean, values: `[true]`},
			tsColumn("other", `[1700000000000000]`),
			{name: "nothing", typ: arrow.Null, values: `[null]`},
		}, "m", `{"n":"42","ok":"true","other":"2023-11-14T22:13:20Z"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := shapedLogFrame(t, logStream(t, tc.columns...))
			if body, _, labels := logRow(frame, 0); body != tc.body || labels != tc.labels {
				t.Errorf("row = %q, %s; want %q, %s", body, labels, tc.body, tc.labels)
			}
		})
	}
}

func TestSQLLogsAcrossBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: testTimestampType},
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "counts", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
	}, nil)
	batch := 0
	stream := sqlArrowStream(t, schema, 2, func(b *array.RecordBuilder) {
		batch++
		for n := batch * 10; n < batch*10+2; n++ {
			b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000000000 + int64(n)*1000000))
			b.Field(1).(*array.StringBuilder).Append("row" + strconv.Itoa(n))
			m := b.Field(2).(*array.MapBuilder)
			m.Append(true)
			m.KeyBuilder().(*array.StringBuilder).Append("n")
			m.ItemBuilder().(*array.Int64Builder).Append(int64(n))
		}
	})
	frame := shapedLogFrame(t, stream)
	// Newest first, so the second batch comes before the first and each batch's rows are reversed.
	for i, want := range []struct{ body, labels string }{
		{"row21", `{"counts.n":"21"}`}, {"row20", `{"counts.n":"20"}`}, {"row11", `{"counts.n":"11"}`}, {"row10", `{"counts.n":"10"}`},
	} {
		if body, _, labels := logRow(frame, i); body != want.body || labels != want.labels {
			t.Errorf("row %d = %q, %s; want %q, %s", i, body, labels, want.body, want.labels)
		}
	}
}

func TestSQLLogsTimeUnits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    arrow.DataType
		values string
		want   time.Time
	}{
		{"microsecond", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, `[1700000000123456]`, time.Unix(1700000000, 123456000)},
		{"nanosecond", &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, `[1700000000123456789]`, time.Unix(1700000000, 123456789)},
		{"date", arrow.FixedWidthTypes.Date32, `[19675]`, time.Date(2023, 11, 14, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := testLogColumn{name: "ts", typ: tc.typ, values: tc.values}
			frame := shapedLogFrame(t, logStream(t, ts, strColumn("message", `["m"]`)))
			if got := frame.Fields[0].At(0).(time.Time); !got.Equal(tc.want) {
				t.Errorf("time = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSQLLogsOrderAndIDs(t *testing.T) {
	page := func(ts, messages, hosts string) *data.Frame {
		return shapedLogFrame(t, logStream(t, tsColumn("ts", ts), strColumn("message", messages), strColumn("host", hosts)))
	}
	// Page 1 ends with one of two twins, so page 2 reads that millisecond again in full.
	pages := []*data.Frame{
		page(`[1700000001000000, 1700000002000000, 1700000001000000]`, `["tie1", "new", "twin"]`, `["a", "a", "b"]`),
		page(`[1700000000000000, 1700000001000000, 1700000001000000, 1700000001000000]`, `["old", "tie1", "twin", "twin"]`, `["a", "a", "b", "b"]`),
	}
	// Grafana shows each id once, so repeated rows must keep their ids and twins must not share one.
	var bodies []string
	shown := map[string]bool{}
	for _, frame := range pages {
		for i := range frame.Rows() {
			if body, id, _ := logRow(frame, i); !shown[id] {
				shown[id] = true
				bodies = append(bodies, body)
			}
		}
	}
	if want := []string{"new", "tie1", "twin", "twin", "old"}; !slices.Equal(bodies, want) {
		t.Errorf("merged bodies = %v, want %v", bodies, want)
	}
	// Each row is read on its own page, since repeats within a page are numbered apart anyway. Rows
	// that differ only by under a millisecond or by a label must not share an id across pages.
	rowID := func(ts, host string) string {
		_, id, _ := logRow(page(ts, `["m"]`, host), 0)
		return id
	}
	id := rowID(`[1700000000000000]`, `["a"]`)
	if later := rowID(`[1700000000000500]`, `["a"]`); later == id {
		t.Errorf("rows 500µs apart share id %q", id)
	}
	if other := rowID(`[1700000000000000]`, `["b"]`); other == id {
		t.Errorf("rows with different labels share id %q", id)
	}
}

func TestSQLLogsShape(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []testLogColumn
	}{
		{"no timestamp", []testLogColumn{strColumn("message", `["m"]`)}},
		{"no string column", []testLogColumn{tsColumn("ts", `[1700000000000000]`), {name: "n", typ: arrow.PrimitiveTypes.Int64, values: `[1]`}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := shapedLogFrame(t, logStream(t, tc.columns...))
			want := "Result is shown as a table because logs need a timestamp column and a text column."
			if frame.Meta.PreferredVisualization != data.VisTypeTable || len(frame.Meta.Notices) != 1 || frame.Meta.Notices[0].Text != want || frame.Meta.Notices[0].Severity != data.NoticeSeverityInfo {
				t.Errorf("meta = %+v, want a table with the info notice", frame.Meta)
			}
		})
	}
}
