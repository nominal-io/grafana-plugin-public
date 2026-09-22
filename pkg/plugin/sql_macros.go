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
		return sqlTimestampLiteral(q.TimeRange.From), nil
	},
	"timeTo": func(q *sqlutil.Query, args []string) (string, error) {
		if !noMacroArguments(args) {
			return "", errors.New("$__timeTo takes no arguments")
		}
		return sqlTimestampLiteral(q.TimeRange.To), nil
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
		seconds := interval.Seconds()
		if seconds <= 0 {
			seconds = 1
		}
		return fmt.Sprintf("DATE_BIN(INTERVAL '%s' SECOND, %s, %s)", strconv.FormatFloat(seconds, 'f', -1, 64), args[0], sqlEpochLiteral), nil
	},
}

// interpolateSQLMacros expands Grafana's time macros in query.RawSQL.
func interpolateSQLMacros(query *sqlutil.Query) (string, error) {
	return sqlutil.Interpolate(query, sqlMacros)
}

// noMacroArguments reports whether a macro was written as $__name or $__name().
func noMacroArguments(args []string) bool {
	return len(args) == 0 || (len(args) == 1 && args[0] == "")
}

// sqlTimestampLiteral keeps subsecond precision, trimming trailing zeros.
func sqlTimestampLiteral(t time.Time) string {
	return "TIMESTAMP '" + t.UTC().Format("2006-01-02 15:04:05.999999999") + "'"
}
