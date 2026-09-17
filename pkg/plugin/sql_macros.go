package plugin

import (
	"fmt"
	"strconv"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/gtime"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

const sqlEpochLiteral = "TIMESTAMP '1970-01-01 00:00:00'"

// Whole-second TIMESTAMP literals avoid warehouse implicit coercion and fractional-literal issues.
func sqlTimestampLiteral(t time.Time) string {
	return "TIMESTAMP '" + t.UTC().Format("2006-01-02 15:04:05") + "'"
}

func interpolateSqlMacros(rawSQL string, q backend.DataQuery) (string, error) {
	query, err := sqlutil.GetQuery(q)
	if err != nil {
		return "", err
	}
	query.RawSQL = rawSQL
	from := q.TimeRange.From.Truncate(time.Second)
	to := q.TimeRange.To.Truncate(time.Second)
	if !to.Equal(q.TimeRange.To) {
		to = to.Add(time.Second)
	}
	macros := sqlutil.Macros{
		"timeFilter": func(_ *sqlutil.Query, args []string) (string, error) {
			if len(args) != 1 || args[0] == "" {
				return "", fmt.Errorf("$__timeFilter requires a column argument")
			}
			return fmt.Sprintf("%s >= %s AND %s < %s", args[0], sqlTimestampLiteral(from), args[0], sqlTimestampLiteral(to)), nil
		},
		"timeFrom": func(_ *sqlutil.Query, args []string) (string, error) {
			if len(args) != 1 || args[0] != "" {
				return "", fmt.Errorf("$__timeFrom requires no arguments")
			}
			return sqlTimestampLiteral(from), nil
		},
		"timeTo": func(_ *sqlutil.Query, args []string) (string, error) {
			if len(args) != 1 || args[0] != "" {
				return "", fmt.Errorf("$__timeTo requires no arguments")
			}
			return sqlTimestampLiteral(to), nil
		},
		"timeGroup": func(_ *sqlutil.Query, args []string) (string, error) {
			if len(args) < 1 || len(args) > 2 || args[0] == "" {
				return "", fmt.Errorf("$__timeGroup requires a column argument")
			}
			d := q.Interval
			if len(args) == 2 {
				var err error
				d, err = gtime.ParseDuration(args[1])
				if err != nil {
					return "", fmt.Errorf("$__timeGroup invalid period %q: %w", args[1], err)
				}
			}
			seconds := d.Seconds()
			if seconds <= 0 {
				seconds = 1
			}
			return fmt.Sprintf("DATE_BIN(INTERVAL '%s' SECOND, %s, %s)", strconv.FormatFloat(seconds, 'f', -1, 64), args[0], sqlEpochLiteral), nil
		},
	}
	return sqlutil.Interpolate(query, macros)
}
