package plugin

import (
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestInterpolateSqlMacros(t *testing.T) {
	q := backend.DataQuery{TimeRange: backend.TimeRange{From: time.Date(2026, 9, 17, 14, 0, 0, 250000000, time.FixedZone("EST", -5*3600)), To: time.Date(2026, 9, 17, 20, 0, 0, 1, time.UTC)}, Interval: 500 * time.Millisecond, JSON: []byte(`{"rawSql":"","format":"timeseries"}`)}
	got, err := interpolateSqlMacros("WHERE $__timeFilter(ts) AND a >= $__timeFrom() AND b < $__timeTo() AND $__timeGroup(ts) = $__timeGroup(ts, 1m) AND $__interval", q)
	if err != nil {
		t.Fatal(err)
	}
	want := "WHERE ts >= TIMESTAMP '2026-09-17 19:00:00' AND ts < TIMESTAMP '2026-09-17 20:00:01' AND a >= TIMESTAMP '2026-09-17 19:00:00' AND b < TIMESTAMP '2026-09-17 20:00:01' AND DATE_BIN(INTERVAL '0.5' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00') = DATE_BIN(INTERVAL '60' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00') AND 500ms"
	if got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
}

func TestInterpolateSqlMacrosErrors(t *testing.T) {
	q := backend.DataQuery{Interval: time.Second, JSON: []byte(`{}`)}
	for _, sql := range []string{"$__timeFilter()", "$__timeGroup()", "$__timeGroup(ts, bad)"} {
		if _, err := interpolateSqlMacros(sql, q); err == nil {
			t.Fatalf("%s: expected error", sql)
		}
	}
	got, err := interpolateSqlMacros("$__timeGroup(ts)", backend.DataQuery{JSON: []byte(`{}`)})
	if err != nil || !strings.Contains(got, "INTERVAL '1' SECOND") {
		t.Fatalf("got %q err %v", got, err)
	}
}
