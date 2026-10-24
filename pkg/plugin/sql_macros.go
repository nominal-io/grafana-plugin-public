package plugin

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/gtime"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

const sqlEpochLiteral = "TIMESTAMP '1970-01-01 00:00:00'"

// sqlMacros replace the SDK's default time macros, which emit quoted strings, with Nominal SQL
// timestamp literals. The SDK's $__interval and $__interval_ms macros still apply.
var sqlMacros = sqlutil.Macros{
	"timeFilter": func(q *sqlutil.Query, args []string) (string, error) {
		if len(args) != 1 || args[0] == "" {
			return "", errors.New("$__timeFilter requires a column argument")
		}
		// BETWEEN is inclusive, as in Grafana's SQL data sources, and unlike >= and < it handles
		// fractional-second bounds on every Nominal dataset.
		return fmt.Sprintf("%s BETWEEN %s AND %s", args[0], sqlTimestampLiteral(q.TimeRange.From), sqlTimestampLiteral(q.TimeRange.To)), nil
	},
	"timeFrom": func(q *sqlutil.Query, args []string) (string, error) {
		if !noMacroArguments(args) {
			return "", errors.New("$__timeFrom takes no arguments")
		}
		return sqlTimestampLiteral(q.TimeRange.From.Add(-strictMargin(q))), nil
	},
	"timeTo": func(q *sqlutil.Query, args []string) (string, error) {
		if !noMacroArguments(args) {
			return "", errors.New("$__timeTo takes no arguments")
		}
		return sqlTimestampLiteral(q.TimeRange.To.Add(strictMargin(q))), nil
	},
	"timeGroup": func(q *sqlutil.Query, args []string) (string, error) {
		if len(args) < 1 || len(args) > 2 || args[0] == "" {
			return "", errors.New("$__timeGroup requires a column and an optional interval")
		}
		interval := q.Interval
		if len(args) == 2 {
			if arg := strings.Trim(args[1], `'"`); arg != "$__interval" {
				d, err := gtime.ParseDuration(arg)
				if err != nil || d <= 0 {
					return "", fmt.Errorf("$__timeGroup requires a positive interval, got %q", args[1])
				}
				interval = d
			}
		}
		// Whole microseconds keep the literal within the six fractional digits SQL accepts.
		micros := interval.Microseconds()
		if micros <= 0 {
			micros = time.Second.Microseconds()
		}
		seconds := strconv.FormatFloat(float64(micros)/1e6, 'f', -1, 64)
		return fmt.Sprintf("DATE_BIN(INTERVAL '%s' SECOND, %s, %s)", seconds, args[0], sqlEpochLiteral), nil
	},
}

// strictMargin widens $__timeFrom() and $__timeTo() by 1 ns in Logs format, whose range can stop at
// the exact line a scroll page continues from. A query may compare them with < or >, which would
// otherwise drop lines at that line's time; $__timeFilter's BETWEEN is inclusive and needs no margin.
func strictMargin(q *sqlutil.Query) time.Duration {
	if q.Format == sqlutil.FormatOptionLogs {
		return time.Nanosecond
	}
	return 0
}

// noMacroArguments reports whether a macro was written as $__name or $__name().
func noMacroArguments(args []string) bool {
	return len(args) == 0 || (len(args) == 1 && args[0] == "")
}

// sqlTimestampLiteral keeps subsecond precision, trimming trailing zeros.
func sqlTimestampLiteral(t time.Time) string {
	return "TIMESTAMP '" + t.UTC().Format("2006-01-02 15:04:05.999999999") + "'"
}
