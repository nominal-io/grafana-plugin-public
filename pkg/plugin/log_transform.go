package plugin

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"slices"
	"sort"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
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

// endOfMillisecond returns the last nanosecond of t's millisecond. Infinite scroll ends the next
// page at the oldest row's time cut to the millisecond. Covering the rest of that millisecond keeps
// its older rows; Grafana drops the repeated ones by id.
func endOfMillisecond(t time.Time) time.Time {
	return t.Truncate(time.Millisecond).Add(time.Millisecond - time.Nanosecond)
}

// isLogLines reports whether f was built by logLinesFrame.
func isLogLines(f *data.Frame) bool {
	return f != nil && f.Meta != nil && f.Meta.Type == data.FrameTypeLogLines
}

// maxBoundaryReads caps the extra reads for one log page.
const maxBoundaryReads = 5

// readPastBoundaryMillisecond reads older lines into frame until it reaches a millisecond before
// end's, so Grafana's next page, which ends at the oldest line's millisecond, moves on. readOlder
// returns lines from the given line back, or nil to stop. Repeats are dropped by id. A failed read
// keeps the page, because Grafana discards a page that carries an error.
func readPastBoundaryMillisecond(ctx context.Context, frame *data.Frame, end time.Time, readOlder func(oldest time.Time, id string) (*data.Frame, error)) {
	if !isLogLines(frame) {
		return
	}
	msStart := end.Truncate(time.Millisecond)
	times, ids := frame.Fields[0], frame.Fields[2]
	seen := make(map[string]bool, ids.Len())
	for i := range ids.Len() {
		seen[ids.At(i).(string)] = true
	}
	for range maxBoundaryReads {
		n := frame.Rows()
		if n == 0 || times.At(n-1).(time.Time).Before(msStart) {
			return
		}
		older, err := readOlder(times.At(n-1).(time.Time), ids.At(n-1).(string))
		if err != nil && ctx.Err() == nil {
			log.DefaultLogger.FromContext(ctx).Warn("Failed to read log lines past the page's boundary millisecond", "error", err)
		}
		if err != nil || !isLogLines(older) {
			return
		}
		added := false
		for r := range older.Rows() {
			if id := older.Fields[2].At(r).(string); !seen[id] {
				seen[id] = true
				frame.AppendRow(older.RowCopy(r)...)
				added = true
			}
		}
		if !added {
			return
		}
	}
}
