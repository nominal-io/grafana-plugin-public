package plugin

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

func parseLogLabels(t *testing.T, raw json.RawMessage) map[string]string {
	t.Helper()
	// json.Unmarshal("null") succeeds with a nil map, so guard against it before
	// accepting labels as a Grafana JSON object.
	if string(raw) == "null" {
		t.Fatalf("labels serialized to null, want a JSON object")
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("not valid JSON object: %v (raw=%q)", err, string(raw))
	}
	return m
}

func logFrameLabelsAt(t *testing.T, frame *data.Frame, row int) map[string]string {
	t.Helper()
	if len(frame.Fields) <= 3 {
		t.Fatalf("expected log frame labels field at index 3, got %d fields", len(frame.Fields))
	}
	labelsField := frame.Fields[3]
	if row < 0 || row >= labelsField.Len() {
		t.Fatalf("expected labels row %d within field length %d", row, labelsField.Len())
	}
	value := labelsField.At(row)
	raw, ok := value.(json.RawMessage)
	if !ok {
		t.Fatalf("labels row %d has type %T, want json.RawMessage", row, value)
	}
	return parseLogLabels(t, raw)
}

func TestMarshalLogArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]string
		channel string
		want    map[string]string
	}{
		{
			name:    "nil args with channel",
			args:    nil,
			channel: "engine.temp",
			want:    map[string]string{"nominal.channel": "engine.temp"},
		},
		{
			name:    "populated args inject channel",
			args:    map[string]string{"host": "srv-1", "level": "error"},
			channel: "engine.temp",
			want:    map[string]string{"host": "srv-1", "level": "error", "nominal.channel": "engine.temp"},
		},
		{
			name:    "existing channel is preserved",
			args:    map[string]string{"host": "srv-1", "nominal.channel": "user-value"},
			channel: "engine.temp",
			want:    map[string]string{"host": "srv-1", "nominal.channel": "user-value"},
		},
		{
			name:    "empty channel does not inject",
			args:    map[string]string{"host": "srv-1"},
			channel: "",
			want:    map[string]string{"host": "srv-1"},
		},
		{
			name:    "empty args with channel",
			args:    map[string]string{},
			channel: "engine.temp",
			want:    map[string]string{"nominal.channel": "engine.temp"},
		},
		{
			name:    "nil args without channel",
			args:    nil,
			channel: "",
			want:    map[string]string{},
		},
		{
			name:    "empty args without channel",
			args:    map[string]string{},
			channel: "",
			want:    map[string]string{},
		},
		{
			name:    "empty keys and values are preserved",
			args:    map[string]string{"": "", "empty-value": ""},
			channel: "engine.temp",
			want:    map[string]string{"": "", "empty-value": "", "nominal.channel": "engine.temp"},
		},
		{
			name:    "quotes backslashes newlines unicode and HTML-sensitive characters",
			args:    map[string]string{"key\"\\\n雪<&>": "value\"\\\n🚀<&>"},
			channel: "channel\"\\\n雪🚀<&>",
			want: map[string]string{
				"key\"\\\n雪<&>":   "value\"\\\n🚀<&>",
				"nominal.channel": "channel\"\\\n雪🚀<&>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sourceBefore map[string]string
			if tt.args != nil {
				sourceBefore = make(map[string]string, len(tt.args))
				for key, value := range tt.args {
					sourceBefore[key] = value
				}
			}

			got := marshalLogArgs(tt.args, tt.channel)
			if !json.Valid(got) {
				t.Fatalf("marshalLogArgs() = %q, want valid JSON", got)
			}
			if parsed := parseLogLabels(t, got); !reflect.DeepEqual(parsed, tt.want) {
				t.Errorf("marshalLogArgs() decoded = %v, want %v", parsed, tt.want)
			}
			if !reflect.DeepEqual(tt.args, sourceBefore) {
				t.Errorf("marshalLogArgs() mutated args to %v, started with %v", tt.args, sourceBefore)
			}
			if repeated := marshalLogArgs(tt.args, tt.channel); !bytes.Equal(repeated, got) {
				t.Errorf("repeated marshalLogArgs() = %q, want byte-identical %q", repeated, got)
			}
		})
	}
}

func TestLogLabelEncoderZeroValue(t *testing.T) {
	encoder := logLabelEncoder{}
	tests := []struct {
		name    string
		args    map[string]string
		want    map[string]string
		wantRaw string
	}{
		{name: "nil args", args: nil, want: map[string]string{}, wantRaw: "{}"},
		{name: "empty args", args: map[string]string{}, want: map[string]string{}, wantRaw: "{}"},
		{
			name: "populated args",
			args: map[string]string{"host": "srv-1", "level": "error"},
			want: map[string]string{"host": "srv-1", "level": "error"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sourceBefore map[string]string
			if tt.args != nil {
				sourceBefore = make(map[string]string, len(tt.args))
				for key, value := range tt.args {
					sourceBefore[key] = value
				}
			}

			got := encoder.encode(tt.args)
			if !json.Valid(got) {
				t.Fatalf("encode() = %q, want valid JSON", got)
			}
			if tt.wantRaw != "" && string(got) != tt.wantRaw {
				t.Errorf("encode() = %q, want %q", got, tt.wantRaw)
			}
			if parsed := parseLogLabels(t, got); !reflect.DeepEqual(parsed, tt.want) {
				t.Errorf("encode() decoded = %v, want %v", parsed, tt.want)
			}
			if !reflect.DeepEqual(tt.args, sourceBefore) {
				t.Errorf("encode() mutated args to %v, started with %v", tt.args, sourceBefore)
			}
			if repeated := encoder.encode(tt.args); !bytes.Equal(repeated, got) {
				t.Errorf("repeated encode() = %q, want byte-identical %q", repeated, got)
			}
		})
	}
}

func TestLogPagedTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("transforms paged log entries into log frame", func(t *testing.T) {
		messages := []string{"error: disk full", "warn: high memory", "info: started"}
		args := []map[string]string{
			{"host": "srv-1", "level": "error"},
			{"host": "srv-2", "level": "warn"},
			{"host": "srv-1", "level": "info"},
		}
		result := createMockPagedLogResult(messages, args, nil)
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if frame.Meta == nil || frame.Meta.Type != data.FrameTypeLogLines {
			t.Errorf("expected FrameTypeLogLines metadata")
		}
		if frame.Meta.PreferredVisualization != data.VisTypeLogs {
			t.Errorf("expected VisTypeLogs, got %v", frame.Meta.PreferredVisualization)
		}

		if len(frame.Fields) != 4 {
			t.Fatalf("expected 4 fields (timestamp, body, id, labels), got %d", len(frame.Fields))
		}

		wantFields := []struct {
			name      string
			fieldType data.FieldType
		}{
			{name: "timestamp", fieldType: data.FieldTypeTime},
			{name: "body", fieldType: data.FieldTypeString},
			{name: "id", fieldType: data.FieldTypeString},
			{name: "labels", fieldType: data.FieldTypeJSON},
		}
		for i, want := range wantFields {
			if got := frame.Fields[i].Name; got != want.name {
				t.Errorf("field %d name = %q, want %q", i, got, want.name)
			}
			if got := frame.Fields[i].Type(); got != want.fieldType {
				t.Errorf("field %q type = %v, want %v", want.name, got, want.fieldType)
			}
			if got := frame.Fields[i].Len(); got != len(messages) {
				t.Fatalf("field %q length = %d, want %d", want.name, got, len(messages))
			}
		}

		for row, sourceIndex := range []int{2, 1, 0} {
			wantTime := time.Unix(1704067200+int64(sourceIndex*60), 0)
			if got := frame.Fields[0].At(row).(time.Time); !got.Equal(wantTime) {
				t.Errorf("row %d timestamp = %v, want %v", row, got, wantTime)
			}
			if got := frame.Fields[1].At(row).(string); got != messages[sourceIndex] {
				t.Errorf("row %d body = %q, want %q", row, got, messages[sourceIndex])
			}
			wantID := (computeapi.LogValue{Id: [16]byte{byte(sourceIndex)}}).Id.String()
			if got := frame.Fields[2].At(row).(string); got != wantID {
				t.Errorf("row %d id = %q, want %q", row, got, wantID)
			}
			wantLabels := map[string]string{
				"host":              args[sourceIndex]["host"],
				"level":             args[sourceIndex]["level"],
				nominalChannelLabel: "app.logs",
			}
			if got := logFrameLabelsAt(t, frame, row); !reflect.DeepEqual(got, wantLabels) {
				t.Errorf("row %d labels = %v, want %v", row, got, wantLabels)
			}
		}
	})

	t.Run("nil Args still yields injected nominal.channel label", func(t *testing.T) {
		messages := []string{"no-args entry"}
		result := createMockPagedLogResult(messages, nil, nil) // nil args
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		parsed := logFrameLabelsAt(t, resp.Frames[0], 0)
		if len(parsed) != 1 || parsed["nominal.channel"] != "app.logs" {
			t.Errorf("expected {nominal.channel: app.logs} for nil Args, got %v", parsed)
		}
	})

	t.Run("sorting preserves source order for equal timestamps", func(t *testing.T) {
		result := createMockPagedLogResult(
			[]string{"newest-a", "oldest", "newest-b"},
			[]map[string]string{{}, {}, {}},
			[]api.Timestamp{
				testTimestamp(1704067320),
				testTimestamp(1704067200),
				testTimestamp(1704067320),
			},
		)
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		bodyField := resp.Frames[0].Fields[1]
		want := []string{"newest-a", "newest-b", "oldest"}
		for i, wantBody := range want {
			if got := bodyField.At(i).(string); got != wantBody {
				t.Fatalf("row %d body = %q, want %q", i, got, wantBody)
			}
		}
	})

	t.Run("empty log response produces frame with correct schema", func(t *testing.T) {
		result := createMockPagedLogResult([]string{}, nil, nil)
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if frame.Meta == nil || frame.Meta.Type != data.FrameTypeLogLines {
			t.Errorf("expected FrameTypeLogLines metadata on empty frame")
		}
		if len(frame.Fields) != 4 {
			t.Fatalf("expected 4 fields even when empty, got %d", len(frame.Fields))
		}
	})
}

func TestCompareLogEntriesNewestFirst(t *testing.T) {
	base := time.Unix(1704067200, 0)
	entry := func(offsetSeconds int64) LogEntry {
		return LogEntry{Time: base.Add(time.Duration(offsetSeconds) * time.Second)}
	}

	tests := []struct {
		name    string
		entries []LogEntry
		want    bool
	}{
		{name: "strict newest first", entries: []LogEntry{entry(3), entry(2), entry(1)}, want: true},
		// Equal timestamps must compare as equal (already sorted) so the sort
		// gate skips them and stable order is preserved.
		{name: "equal timestamps count as sorted", entries: []LogEntry{entry(3), entry(3), entry(2)}, want: true},
		{name: "oldest first", entries: []LogEntry{entry(1), entry(2), entry(3)}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := slices.IsSortedFunc(tt.entries, compareLogEntriesNewestFirst)
			if got != tt.want {
				t.Fatalf("IsSortedFunc(compareLogEntriesNewestFirst) = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogPointTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("transforms single log point", func(t *testing.T) {
		result := createMockLogPointResult("single entry", map[string]string{"host": "srv-1"})
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if frame.Meta == nil || frame.Meta.Type != data.FrameTypeLogLines {
			t.Errorf("expected FrameTypeLogLines metadata")
		}

		bodyField := frame.Fields[1]
		if bodyField.Len() != 1 {
			t.Fatalf("expected 1 entry, got %d", bodyField.Len())
		}
		if v := bodyField.At(0).(string); v != "single entry" {
			t.Errorf("expected %q, got %q", "single entry", v)
		}
	})

	t.Run("nil Args on single log point still yields injected nominal.channel label", func(t *testing.T) {
		result := createMockLogPointResult("no-args", nil)
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		parsed := logFrameLabelsAt(t, resp.Frames[0], 0)
		if len(parsed) != 1 || parsed["nominal.channel"] != "app.logs" {
			t.Errorf("expected {nominal.channel: app.logs} for nil Args, got %v", parsed)
		}
	})
}

func TestLogFramesCarryDistinctChannelLabels(t *testing.T) {
	ds := &Datasource{}

	channelLabel := func(t *testing.T, channel string) string {
		t.Helper()
		result := createMockPagedLogResult([]string{"entry"}, []map[string]string{{"host": "srv-1"}}, nil)
		qm := NominalQueryModel{
			Channel:         channel,
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame for %q, got %d", channel, len(resp.Frames))
		}
		parsed := logFrameLabelsAt(t, resp.Frames[0], 0)
		if parsed["host"] != "srv-1" {
			t.Errorf("expected user arg host preserved for %q, got %v", channel, parsed)
		}
		return parsed["nominal.channel"]
	}

	a := channelLabel(t, "engine.temp")
	b := channelLabel(t, "engine.pressure")
	if a != "engine.temp" || b != "engine.pressure" {
		t.Errorf("expected per-channel labels, got a=%q b=%q", a, b)
	}
	if a == b {
		t.Errorf("expected distinct nominal.channel labels, both were %q", a)
	}
}
