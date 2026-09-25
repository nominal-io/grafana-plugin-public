# SQL queries

A Nominal data source runs both Compute and SQL queries. Choose the API for each
query in the panel editor with **Query API**; one panel can mix both. Switching
APIs keeps the SQL text and the Compute selections but does not translate
between them. New queries default to Compute.

## Setup

SQL uses the data source's Base URL, API key and Workspace RID. The Base URL must
use `https`, because SQL runs over gRPC on the same host. Queries run in the
configured Workspace RID, or in the API key's default workspace when it is
empty, and the key needs read access to that workspace's data.

**Save & test** checks the API key, access to the Workspace RID when one is set,
and that the SQL service accepts the key and has a workspace to query. A SQL
problem is reported in the result message without failing the check, because
Compute queries still work.

## Writing queries

The editor runs the query when it loses focus, on Ctrl/Cmd+S and on
Ctrl/Cmd+Enter. In an empty editor, **Use time-series example** inserts a
starting query:

```sql
SELECT $__timeGroup(ts) AS "time", channel, AVG(value) AS "value"
FROM points_double
WHERE dataset_rid = '<your-dataset-rid>'
  AND channel IN ('temperature')
  AND $__timeFilter(ts)
GROUP BY 1, 2
ORDER BY 1
```

Queries on `points_double`, `points_int`, `points_string`, `points_struct`,
`logs` and `channels` must filter on `dataset_rid`. `time` and `timestamp` are
reserved words, so quote them as column names, as in `AS "time"`.

**Time series** needs a timestamp column and at least one numeric column. String
and boolean columns become series labels, and each series is returned with only
its own samples, sorted by time. A result without both columns is shown as a
table with a notice, so use a Table, Stat or Bar chart panel for it. **Table**
returns rows and columns as the query produced them.

## Macros

| Macro | Expands to |
| --- | --- |
| `$__timeFilter(ts)` | `ts BETWEEN TIMESTAMP '<from>' AND TIMESTAMP '<to>'` for the panel time range |
| `$__timeFrom()`, `$__timeTo()` | The start or end of the panel time range as a `TIMESTAMP` literal |
| `$__timeGroup(ts)` | `DATE_BIN` buckets at the panel interval |
| `$__timeGroup(ts, 1m)` | `DATE_BIN` buckets of a fixed duration |

Timestamps are UTC. Macros expand in the plugin backend, so alert rules run the
same SQL as panels.

## Dashboard variables

Grafana substitutes dashboard variables before the query runs. Multi-value and
Include All variables become quoted, comma-separated values, so write
`channel IN ($channels)`. Other variables are inserted as they are, with single
quotes doubled, so write `channel = '$channel'`. Grafana's variable formats,
such as `${channel:raw}`, override this. Alert rules cannot use dashboard
variables.

A dashboard variable can also run a SQL query. Return one column to use each
row as both label and value, or return columns named `__text` and `__value`
for a separate label. Rows with a null value are skipped, and numbers become
text.

```sql
SELECT DISTINCT channel
FROM channels
WHERE dataset_rid = '<dataset-rid>'
ORDER BY 1
```

SQL variables can reference other variables and the time macros, like panel
SQL. A variable that uses `$__timeFilter` or another time macro needs its
Refresh setting set to **On time range change** to rerun when the dashboard
time range changes. When the SQL row limit is reached, the dropdown keeps
the rows returned and Grafana shows a warning. Add `LIMIT` to keep the
dropdown short.

Editing a variable saves it as an object instead of a plain string. A plugin
version without SQL variables cannot read that object, so an edited variable
stops loading after a downgrade to an earlier version. A variable you never
edited stays saved as a plain string and keeps working after a downgrade.

## Limits

The SQL service stops a query after 2 minutes. Results stop at Grafana's SQL
row limit, `row_limit` in the `[dataproxy]` section of the Grafana
configuration (1,000,000 rows by default), with a warning. Aggregate with
`$__timeGroup` instead of returning every point over a long time range. The
Nominal API also rate-limits SQL requests.

## Testing

The development setup in the [README](../README.md#development-and-e2e-testing)
applies to SQL. The browser test mocks query results, so it needs a local
Grafana but no Nominal credentials:

```sh
pnpm exec playwright test tests/sqlQueryEditor.spec.ts
```

The live SQL test in the README runs real queries against self-provisioned data.
