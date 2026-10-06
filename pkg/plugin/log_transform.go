package plugin

import (
	"encoding/json"
	"slices"
	"time"
	"unicode/utf8"
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

// encode writes args as a JSON object with sorted keys, so labels keep a stable
// order across rows. Each call returns a new slice except for empty args.
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
		labelsJSON = appendJSONString(labelsJSON, k)
		labelsJSON = append(labelsJSON, ':')
		labelsJSON = appendJSONString(labelsJSON, args[k])
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
	return append(appendJSONString([]byte(`{"`+nominalChannelLabel+`":`), channel), '}')
}

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string literal, escaping only what JSON
// requires. Invalid UTF-8 becomes U+FFFD.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			if b < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
			} else {
				dst = append(dst, '\\', b)
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, "\uFFFD"...)
			start = i + 1
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// compareLogEntriesNewestFirst orders log entries newest-first; equal timestamps
// compare as equal so a stable sort preserves source order.
func compareLogEntriesNewestFirst(a, b LogEntry) int {
	return b.Time.Compare(a.Time)
}
