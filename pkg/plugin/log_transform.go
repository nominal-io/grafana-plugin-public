package plugin

import (
	"encoding/json"
	"encoding/json/jsontext"
	"slices"
	"sort"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// logLines holds log entries as the columns of a log-lines frame.
type logLines struct {
	times  []time.Time
	bodies []string
	ids    []string
	// labels is json.RawMessage because Grafana's Logs panel wants a JSON-typed column, not a JSON
	// string. A value may alias a slice shared across lines, so copy before mutating.
	labels []json.RawMessage
	// order holds each line's source position while sorting.
	order []int32
}

// newLogLines returns logLines with room for n lines.
func newLogLines(n int) logLines {
	return logLines{
		times:  make([]time.Time, 0, n),
		bodies: make([]string, 0, n),
		ids:    make([]string, 0, n),
		labels: make([]json.RawMessage, 0, n),
	}
}

func (l *logLines) add(t time.Time, body, id string, labels json.RawMessage) {
	l.times = append(l.times, t)
	l.bodies = append(l.bodies, body)
	l.ids = append(l.ids, id)
	l.labels = append(l.labels, labels)
}

func (l *logLines) Len() int { return len(l.times) }

// Less orders lines newest first, and lines with equal times by source position, so that
// sort.Sort, which is not stable, keeps their source order.
func (l *logLines) Less(i, j int) bool {
	if c := l.times[i].Compare(l.times[j]); c != 0 {
		return c > 0
	}
	return l.order[i] < l.order[j]
}

func (l *logLines) Swap(i, j int) {
	l.times[i], l.times[j] = l.times[j], l.times[i]
	l.bodies[i], l.bodies[j] = l.bodies[j], l.bodies[i]
	l.ids[i], l.ids[j] = l.ids[j], l.ids[i]
	l.labels[i], l.labels[j] = l.labels[j], l.labels[i]
	l.order[i], l.order[j] = l.order[j], l.order[i]
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

// logLinesFrame builds a Grafana log-lines frame, newest first. Grafana's infinite scroll takes the
// next time range from the boundary row, so lines are sorted whatever order the source used. They
// are kept and sorted as columns because data.NewField copies each column; building the columns from
// a sorted slice of entries would hold one more copy of every line.
func logLinesFrame(name string, lines *logLines) *data.Frame {
	if !slices.IsSortedFunc(lines.times, func(a, b time.Time) int { return b.Compare(a) }) {
		lines.order = make([]int32, lines.Len())
		for i := range lines.order {
			lines.order[i] = int32(i)
		}
		sort.Sort(lines)
		lines.order = nil
	}
	frame := data.NewFrame(name,
		data.NewField("timestamp", nil, lines.times),
		data.NewField("body", nil, lines.bodies),
		data.NewField("id", nil, lines.ids),
		data.NewField("labels", nil, lines.labels),
	)
	frame.Meta = &data.FrameMeta{
		Type: data.FrameTypeLogLines,
		// The log-lines dataplane contract is at v0.0, not time-series-wide's 0.1.
		TypeVersion:            data.FrameTypeVersion{0, 0},
		PreferredVisualization: data.VisTypeLogs,
	}
	return frame
}

// logPageRange returns a Logs query's range. Infinite scroll ends the next page at the oldest shown
// line's millisecond, or for oldest-first panels starts it at the newest line's, so the range covers
// the rest of its last millisecond. A bound the frontend places at the line a scroll page continues
// from stops the range at that line instead. Grafana drops lines read again by id.
//
// TODO: SQL scrolling still stops where as many lines as the LIMIT share one exact time, since
// nothing in a row tells those lines apart.
func logPageRange(r backend.TimeRange, startNanos, endNanos *int64) backend.TimeRange {
	toMs := r.To.Truncate(time.Millisecond)
	r.To = toMs.Add(time.Millisecond - time.Nanosecond)
	if inMillisecond(endNanos) {
		r.To = toMs.Add(time.Duration(*endNanos))
	}
	if inMillisecond(startNanos) {
		r.From = r.From.Truncate(time.Millisecond).Add(time.Duration(*startNanos))
	}
	return r
}

func inMillisecond(nanos *int64) bool {
	return nanos != nil && *nanos >= 0 && *nanos < int64(time.Millisecond)
}
