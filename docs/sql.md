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
its own samples, sorted by time. A null label becomes an empty string, or
`false` for a boolean column, and shares a series with that value. The first
timestamp column is the series time, and a null value in it fails the query.
When a query returns more than one numeric column, such as `MIN(value)` and
`MAX(value)`, each series also gets a `column` label with the column's name,
because alert rules tell series apart by labels alone. Other columns, such as a
second timestamp, are left out with a notice. A result without both columns is
shown as a table with a notice, so use a Table, Stat or Bar chart panel for it.
**Table** returns rows and columns as the query produced them.

Map columns such as `tags` are shown as `key=value` pairs, for example
`satellite=GOCE-1`. Keys and values that contain `=`, `,`, `"` or a space are
quoted, so different maps never look the same.

## Macros

| Macro | Expands to |
| --- | --- |
| `$__timeFilter(ts)` | `ts BETWEEN TIMESTAMP '<from>' AND TIMESTAMP '<to>'` for the panel time range |
| `$__timeFrom()`, `$__timeTo()` | The start or end of the panel time range as a `TIMESTAMP` literal |
| `$__timeGroup(ts)` | `DATE_BIN` buckets at the panel interval |
| `$__timeGroup(ts, 1m)` | `DATE_BIN` buckets of a fixed duration |

Timestamps are UTC. Macros expand in the plugin backend, so alert rules run the
same SQL as panels. They expand anywhere in the query text, including comments
and quoted strings, so remove a macro rather than commenting it out.

## Dashboard variables

Grafana substitutes dashboard variables before the query runs. Write
`channel IN (${channel:sqlstring})` to use a variable in SQL. It works whether
the variable is single-value, multi-value, or has Include All. A value that
contains a double quote does not match in this form.

Plain `$channel` is quoted only for a multi-value or Include All variable, as
in `'a','b'`. Otherwise it is inserted as it is, with single quotes doubled, so
`channel = '$channel'` works only while Include All is off. Alert rules cannot
use dashboard variables.

A dashboard variable can also run a SQL query. Return one column to use each
row as both label and value, or return columns named `__text` and `__value`
for a separate label. Rows with a null value are skipped. Timestamps become
UTC text that `TIMESTAMP '$time'` accepts. Large numbers lose precision and
dates gain a midnight time, so cast them to text, as in `CAST(id AS VARCHAR)`.

This variable lists assets by name, and its value is the selected asset's RID:

```sql
SELECT title AS __text, asset_rid AS __value
FROM assets
ORDER BY 1
```

A builder query takes `$asset` in its asset field. Tables such as
`points_double` must filter on `dataset_rid`, so a SQL query joins `assets` to
reach the asset's data:

```sql
SELECT $__timeGroup(p.ts) AS "time", p.channel, AVG(p.value) AS "value"
FROM points_double p
JOIN assets a ON p.dataset_rid = a.dataset_rid
WHERE a.asset_rid IN (${asset:sqlstring})
  AND $__timeFilter(p.ts)
GROUP BY 1, 2
ORDER BY 1
```

SQL variables can use other variables and the time macros. A variable that
uses a time macro needs Refresh set to **On time range change**. A query that
reaches the SQL row limit keeps the rows up to the limit, and Grafana shows a
warning that the options are incomplete.

An edited variable is saved in a format that plugin versions without SQL
variables cannot read. A variable you never edit keeps working after a
downgrade.

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
