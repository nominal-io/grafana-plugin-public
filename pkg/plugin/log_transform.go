package plugin

import (
	"encoding/json"
	"time"
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
}

func newLogLabelEncoder(channel string) logLabelEncoder {
	return logLabelEncoder{defaultLabels: defaultLogLabelsForChannel(channel)}
}

func (e logLabelEncoder) encode(args map[string]string) json.RawMessage {
	defaults := e.defaultLabels
	if len(defaults) == 0 {
		defaults = emptyLogLabels
	}
	if len(args) == 0 {
		// Returns the shared defaults slice; callers must treat it as read-only.
		return defaults
	}
	// Defaults longer than "{}" carry the nominal.channel key; deriving this
	// from length keeps the splice below unreachable with an empty default.
	injectChannel := len(defaults) > len(emptyLogLabels)
	if _, exists := args[nominalChannelLabel]; !injectChannel || exists {
		labelsJSON, _ := json.Marshal(args)
		return labelsJSON
	}

	// Both halves are encoding/json objects; defaults is non-empty here.
	labelsJSON, _ := json.Marshal(args)
	labelsJSON[len(labelsJSON)-1] = ','
	return append(labelsJSON, defaults[1:]...)
}

// marshalLogArgs serializes Args to JSON and adds "nominal.channel" when set and
// not already present, so mixed-channel log panels can tell rows apart. Args is
// not mutated. Empty Args returns the shared read-only default labels.
//
// The "nominal." prefix avoids Grafana hiding underscore-prefixed labels.
func marshalLogArgs(args map[string]string, channel string) json.RawMessage {
	return newLogLabelEncoder(channel).encode(args)
}

func defaultLogLabelsForChannel(channel string) json.RawMessage {
	if channel == "" {
		return emptyLogLabels
	}
	labelsJSON, _ := json.Marshal(map[string]string{nominalChannelLabel: channel})
	return labelsJSON
}

// compareLogEntriesNewestFirst orders log entries newest-first; equal timestamps
// compare as equal so a stable sort preserves source order.
func compareLogEntriesNewestFirst(a, b LogEntry) int {
	return b.Time.Compare(a.Time)
}
