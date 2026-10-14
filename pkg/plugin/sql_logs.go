package plugin

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// sqlLogColumns returns the first timestamp column and the log text column: the string column named
// message, else body, both ignoring case, else the first string column. named reports whether a name
// matched. An index is -1 when there is no such column.
func sqlLogColumns(fields []arrow.Field) (timeIndex, bodyIndex int, named bool) {
	timeIndex = slices.IndexFunc(fields, func(f arrow.Field) bool { return f.Type.ID() == arrow.TIMESTAMP })
	for _, name := range []string{"message", "body"} {
		isBody := func(f arrow.Field) bool { return isText(f) && strings.EqualFold(f.Name, name) }
		if i := slices.IndexFunc(fields, isBody); i >= 0 {
			return timeIndex, i, true
		}
	}
	return timeIndex, slices.IndexFunc(fields, isText), false
}

// isText reports whether f holds strings. The SQL service can send a low-cardinality string column
// dictionary-encoded.
func isText(f arrow.Field) bool {
	t := f.Type
	if d, ok := t.(*arrow.DictionaryType); ok {
		t = d.ValueType
	}
	return t.ID() == arrow.STRING
}

// sqlLogFrame reads rows into log entries. Labels come from every column except the time, the body
// and map columns. Without a timestamp or string column it returns the result as a table, with a
// notice.
func sqlLogFrame(name string, fields []arrow.Field, batches []arrow.RecordBatch, rows int) (*data.Frame, error) {
	timeIndex, bodyIndex, named := sqlLogColumns(fields)
	if timeIndex < 0 || bodyIndex < 0 {
		frame, err := tableFrame(name, fields, batches, rows)
		if err != nil {
			return nil, err
		}
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityInfo,
			Text:     "Result is shown as a table because logs need a timestamp column and a text column.",
		})
		return frame, nil
	}
	// NULL arrays have no validity bitmap, so IsNull cannot skip them; they are left out here.
	var labelColumns []int
	for c, f := range fields {
		if c != timeIndex && c != bodyIndex && f.Type.ID() != arrow.NULL && f.Type.ID() != arrow.MAP {
			labelColumns = append(labelColumns, c)
		}
	}
	unit := fields[timeIndex].Type.(*arrow.TimestampType).Unit
	entries := make([]LogEntry, 0, rows)
	enc := newLogLabelEncoder("")
	labels := map[string]string{}
	addLabel := func(k, v string) {
		if _, ok := labels[k]; !ok {
			labels[k] = v
		}
	}
	seen := map[uint64]int{}
	for _, batch := range batches {
		times := batch.Column(timeIndex).(*array.Timestamp)
		bodies := batch.Column(bodyIndex)
		for i := range int(batch.NumRows()) {
			if times.IsNull(i) {
				return nil, &nullTimeError{column: fields[timeIndex].Name}
			}
			clear(labels)
			for _, c := range labelColumns {
				if col := batch.Column(c); !col.IsNull(i) {
					addLabel(fields[c].Name, col.ValueStr(i))
				}
			}
			encoded := enc.encode(labels)

			var body string
			if !bodies.IsNull(i) {
				body = bodies.ValueStr(i)
			}
			t := times.Value(i).ToTime(unit)
			// Grafana drops a row whose id it has already shown, so repeats of a row are numbered. The
			// next page reads its boundary millisecond again in full, so the numbers match across pages.
			hash := logEntryHash(t, body, encoded)
			n := seen[hash]
			seen[hash]++
			id := strconv.FormatUint(hash, 16)
			if n > 0 {
				id += "_" + strconv.Itoa(n)
			}
			entries = append(entries, LogEntry{Time: t, Body: body, ID: id, Labels: encoded})
		}
	}
	frame := logLinesFrame(name, entries)
	if !named {
		frame.AppendNotices(data.Notice{
			Severity: data.NoticeSeverityInfo,
			Text:     fmt.Sprintf("No message or body column; showing %s as the log text.", fields[bodyIndex].Name),
		})
	}
	return frame, nil
}

// logEntryHash hashes a row so the same row gets the same id on every page, which Grafana uses to
// drop rows repeated between pages.
func logEntryHash(t time.Time, body string, labels []byte) uint64 {
	h := fnv.New64a()
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(t.UnixNano())))
	h.Write([]byte(body))
	h.Write([]byte{0})
	h.Write(labels)
	return h.Sum64()
}
