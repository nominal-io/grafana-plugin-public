package plugin

import (
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

func sqlMacroTestQuery(interval time.Duration) *sqlutil.Query {
	return &sqlutil.Query{
		TimeRange: backend.TimeRange{
			From: time.Date(2026, 9, 17, 14, 0, 0, 250000000, time.FixedZone("EST", -5*3600)),
			To:   time.Date(2026, 9, 17, 20, 0, 0, 1, time.UTC),
		},
		Interval: interval,
	}
}

func TestInterpolateSQLMacros(t *testing.T) {
	const (
		from = "TIMESTAMP '2026-09-17 19:00:00.25'"
		to   = "TIMESTAMP '2026-09-17 20:00:00.000000001'"
	)
	for _, tc := range []struct {
		name     string
		sql      string
		interval time.Duration
		want     string
	}{
		{name: "time filter", sql: "WHERE $__timeFilter(ts)", want: "WHERE ts BETWEEN " + from + " AND " + to},
		{name: "time filter on an expression", sql: "$__timeFilter(CAST(ts AS TIMESTAMP))", want: "CAST(ts AS TIMESTAMP) BETWEEN " + from + " AND " + to},
		{name: "time bounds", sql: "$__timeFrom() AND $__timeTo()", want: from + " AND " + to},
		{name: "time bounds without parentheses", sql: "$__timeFrom AND $__timeTo", want: from + " AND " + to},
		{name: "panel interval", sql: "$__timeGroup(ts)", interval: 500 * time.Millisecond, want: "DATE_BIN(INTERVAL '0.5' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00')"},
		{name: "missing panel interval", sql: "$__timeGroup(ts)", want: "DATE_BIN(INTERVAL '1' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00')"},
		{name: "fixed interval", sql: "$__timeGroup(ts, 1m)", want: "DATE_BIN(INTERVAL '60' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00')"},
		{name: "fractional interval", sql: "$__timeGroup(ts)", interval: 1140 * time.Millisecond, want: "DATE_BIN(INTERVAL '1.14' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00')"},
		{name: "quoted interval", sql: "$__timeGroup(ts, '500ms')", want: "DATE_BIN(INTERVAL '0.5' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00')"},
		{name: "interval variable", sql: "$__timeGroup(ts, $__interval)", interval: 2 * time.Minute, want: "DATE_BIN(INTERVAL '120' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00')"},
		{name: "quoted interval variable", sql: "$__timeGroup(ts, '$__interval')", interval: 2 * time.Minute, want: "DATE_BIN(INTERVAL '120' SECOND, ts, TIMESTAMP '1970-01-01 00:00:00')"},
		{name: "SDK interval", sql: "$__interval", interval: 500 * time.Millisecond, want: "500ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sqlutil.Interpolate(sqlMacroTestQuery(tc.interval).WithSQL(tc.sql), sqlMacros)
			if err != nil || got != tc.want {
				t.Errorf("Interpolate(%q) = %q, %v; want %q", tc.sql, got, err, tc.want)
			}
		})
	}
}

func TestInterpolateSQLMacrosRejectsBadArguments(t *testing.T) {
	for _, sql := range []string{
		"$__timeFilter",
		"$__timeFilter()",
		"$__timeFilter(ts, ts)",
		"$__timeFrom(ts)",
		"$__timeTo(ts)",
		"$__timeGroup()",
		"$__timeGroup(ts, bad)",
		"$__timeGroup(ts, 0s)",
		"$__timeGroup(ts, -1s)",
		"$__timeGroup(ts, 1m, 0)",
	} {
		t.Run(sql, func(t *testing.T) {
			if got, err := sqlutil.Interpolate(sqlMacroTestQuery(time.Second).WithSQL(sql), sqlMacros); err == nil {
				t.Errorf("Interpolate(%q) = %q, want an error", sql, got)
			}
		})
	}
}
