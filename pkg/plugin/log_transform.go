package plugin

import (
	"encoding/json"
	"encoding/json/jsontext"
	"slices"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// Labels is json.RawMessage because Grafana's Logs panel wants a JSON-typed
// column, not a JSON string. It may alias a slice shared across entries, so
// copy before mutating.
type LogEntry struct {
	Time   time.Time
	Body   string
	ID     string
	Labels json.RawMessage
}

const nominalChannelLabel = "nominal.channel"

var emptyLogLabels = json.RawMessage("{}")

type logLabelEncoder struct {
	defaultLabels json.RawMessage
	keys          []string
}

func newLogLabelEncoder(channel string) logLabelEncoder {
	return logLabelEncoder{defaultLabels: defaultLogLabelsForChannel(channel)}
}

// encode writes args as a JSON object with sorted keys, then nominal.channel when the
// encoder has a channel and args lack it, so labels keep a stable order across rows.
func (e *logLabelEncoder) encode(args map[string]string) json.RawMessage {
	defaults := e.defaultLabels
	if len(defaults) == 0 {
		defaults = emptyLogLabels
	}
	if len(args) == 0 {
		// Returns the shared defaults slice; callers must treat it as read-only.
		return defaults
	}
	// Defaults longer than "{}" carry the nominal.channel key.
	_, exists := args[nominalChannelLabel]
	injectChannel := len(defaults) > len(emptyLogLabels) && !exists

	size := 2
	if injectChannel {
		size += len(defaults)
	}
	e.keys = e.keys[:0]
	for k, v := range args {
		e.keys = append(e.keys, k)
		size += len(k) + len(v) + 6
	}
	slices.Sort(e.keys)

	labelsJSON := make([]byte, 0, size)
	labelsJSON = append(labelsJSON, '{')
	for i, k := range e.keys {
		if i > 0 {
			labelsJSON = append(labelsJSON, ',')
		}
		// AppendQuote only errors on invalid UTF-8, which it replaces with U+FFFD.
		labelsJSON, _ = jsontext.AppendQuote(labelsJSON, k)
		labelsJSON = append(labelsJSON, ':')
		labelsJSON, _ = jsontext.AppendQuote(labelsJSON, args[k])
	}
	if injectChannel {
		// defaults is {"nominal.channel":"..."}; drop its '{' and keep its '}'.
		labelsJSON = append(labelsJSON, ',')
		return append(labelsJSON, defaults[1:]...)
	}
	return append(labelsJSON, '}')
}

// marshalLogArgs serializes Args to JSON and adds "nominal.channel" when set and
// not already present, so mixed-channel log panels can tell rows apart. Args is
// not mutated. Empty Args returns the shared read-only default labels.
//
// The "nominal." prefix avoids Grafana hiding underscore-prefixed labels.
func marshalLogArgs(args map[string]string, channel string) json.RawMessage {
	encoder := newLogLabelEncoder(channel)
	return encoder.encode(args)
}

func defaultLogLabelsForChannel(channel string) json.RawMessage {
	if channel == "" {
		return emptyLogLabels
	}
	labels, _ := jsontext.AppendQuote([]byte(`{"`+nominalChannelLabel+`":`), channel)
	return append(labels, '}')
}

// compareLogEntriesNewestFirst orders log entries newest-first; equal timestamps
// compare as equal so a stable sort preserves source order.
func compareLogEntriesNewestFirst(a, b LogEntry) int {
	return b.Time.Compare(a.Time)
}

// logLinesFrame builds a Grafana log-lines frame, newest first. Grafana's infinite scroll takes the
// next time range from the boundary row, so entries are sorted whatever order the source used.
func logLinesFrame(name string, entries []LogEntry) *data.Frame {
	if !slices.IsSortedFunc(entries, compareLogEntriesNewestFirst) {
		slices.SortStableFunc(entries, compareLogEntriesNewestFirst)
	}
	times := make([]time.Time, len(entries))
	bodies := make([]string, len(entries))
	ids := make([]string, len(entries))
	labels := make([]json.RawMessage, len(entries))
	for i, e := range entries {
		times[i], bodies[i], ids[i], labels[i] = e.Time, e.Body, e.ID, e.Labels
	}
	frame := data.NewFrame(name,
		data.NewField("timestamp", nil, times),
		data.NewField("body", nil, bodies),
		data.NewField("id", nil, ids),
		data.NewField("labels", nil, labels),
	)
	frame.Meta = &data.FrameMeta{
		Type: data.FrameTypeLogLines,
		// The log-lines dataplane contract is at v0.0, not time-series-wide's 0.1.
		TypeVersion:            data.FrameTypeVersion{0, 0},
		PreferredVisualization: data.VisTypeLogs,
	}
	return frame
}
