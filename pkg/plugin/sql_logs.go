package plugin

import (
	"encoding/binary"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// sqlLogColumns returns the first timestamp or date column and the log text column: the string
// column named message, ignoring case, else the first string column. An index is -1 when there is
// no such column.
func sqlLogColumns(fields []arrow.Field) (timeIndex, bodyIndex int) {
	timeIndex = slices.IndexFunc(fields, func(f arrow.Field) bool { return f.Type.ID() == arrow.TIMESTAMP || f.Type.ID() == arrow.DATE32 })
	isMessage := func(f arrow.Field) bool { return isText(f) && strings.EqualFold(f.Name, "message") }
	if i := slices.IndexFunc(fields, isMessage); i >= 0 {
		return timeIndex, i
	}
	return timeIndex, slices.IndexFunc(fields, isText)
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
	timeIndex, bodyIndex := sqlLogColumns(fields)
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
	lines := newLogLines(rows)
	enc := newLogLabelEncoder("")
	labels := map[string]string{}
	addLabel := func(k, v string) {
		if _, ok := labels[k]; !ok {
			labels[k] = v
		}
	}
	seen := map[uint64]int{}
	for _, batch := range batches {
		times := batch.Column(timeIndex)
		timeAt := timeReader(times)
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
			t := timeAt(i)
			// Grafana drops a row whose id it has already shown, so repeats of a row are numbered. The
			// next page reads its boundary millisecond again in full, so the numbers match across pages.
			hash := logEntryHash(t, body, encoded)
			n := seen[hash]
			seen[hash]++
			id := strconv.FormatUint(hash, 16)
			if n > 0 {
				id += "_" + strconv.Itoa(n)
			}
			lines.add(t, body, id, encoded)
		}
	}
	return logLinesFrame(name, &lines), nil
}

// timeReader reads the times of a timestamp or date32 array. The SQL service sends Date columns as
// date32.
func timeReader(a arrow.Array) func(int) time.Time {
	if d, ok := a.(*array.Date32); ok {
		return func(i int) time.Time { return d.Value(i).ToTime() }
	}
	ts := a.(*array.Timestamp)
	unit := ts.DataType().(*arrow.TimestampType).Unit
	return func(i int) time.Time { return ts.Value(i).ToTime(unit) }
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
