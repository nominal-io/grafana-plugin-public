# SQL queries

Install a plugin build containing both the SQL backend (#143) and editor (#142).
Deploy the backend first if rolling these changes out separately.

Add a **Nominal** data source, select **Query API → SQL**, and enter the API base
URL, API key and workspace RID. For example, name it **Nominal SQL** and keep your
existing **Nominal Compute** data source for asset/data-scope queries. Changing
the API setting does not convert saved queries. The key needs SQL access to the
selected workspace and data; Save & Test checks API authentication and workspace
access, while running a query verifies SQL access.

## Builder and Code

In a new panel, select the SQL data source. **Builder** lets you search for a
dataset and channels, choose floating-point or integer storage, an aggregation
and a time bucket, and optionally filter by one tag. The default bucket is Auto
(Grafana's panel interval). Choose channels with the selected storage type; the
builder supports numeric telemetry. Click **Run query** and expand **Generated
SQL** to inspect the query. For example, selecting temperature, Mean and 1m
produces a query of this form:

```sql
SELECT $__timeGroup(ts, 1m) AS "time", channel, AVG(value) AS "value"
FROM points_double
WHERE dataset_rid = '<your-dataset-rid>'
  AND channel IN ('temperature')
  AND $__timeFilter(ts)
GROUP BY 1, 2
ORDER BY 1
```

Metadata searches return up to 100 matches; type to narrow the search. You can
also paste a dataset RID or channel name. The API key stays on the Grafana server.

Use **Code** for joins, metadata queries, strings, logs or other custom SQL.
Builder → Code preserves generated SQL. Returning to Builder after editing SQL
asks before replacing it; arbitrary SQL cannot be converted back automatically.
Saved raw SQL opens in Code. Start with `SELECT 1 AS value` and choose **Table**
for a simple connection check. Code commits and runs on blur, Ctrl/Cmd+S or
Ctrl/Cmd+Enter.

**Time series** expects a timestamp column plus numeric values; string columns
become series labels. Sparse series keep missing samples as nulls. **Table**
retains the original rows and columns. Point tables and `channels` require a
`dataset_rid` filter. `$__timeFilter(ts)` uses an inclusive start and exclusive
end, retaining subsecond precision. `$__timeFrom()` and `$__timeTo()` return
UTC timestamps. `$__timeGroup(ts)` uses the panel interval; an explicit positive
duration or `$__interval` is also supported.

Dashboard variables work in Code: use `channel IN ($channels)` for multi-value
variables and `channel = '$channel'` for a single value. SQL query-backed variable
pickers are not included. Grafana alert rules can run persisted SQL and time
macros, but cannot resolve dashboard template variables.

## Test locally

From the repository root, with Go, Mage, pnpm and Docker installed:

```sh
pnpm install
mage -v
pnpm build
pnpm server
```

Open <http://localhost:3000>, add/configure a Nominal SQL data source, and run
`SELECT 1 AS value` with Table format. Then build a numeric query and select a
time range containing data in your dataset. After rebuilding the Go backend,
restart Grafana with `docker compose restart grafana`; refresh the browser after
rebuilding the frontend.

```sh
go test -count=1 ./pkg/...
pnpm typecheck
pnpm test:ci
pnpm lint
pnpm exec playwright install chromium
pnpm exec playwright test tests/sqlQueryEditor.spec.ts
```

The browser test supplies mock metadata/results, checks the generated query,
verifies a rendered value, and tests Code/Builder switching. It needs local
Grafana but no Nominal credentials, and cleans up its datasource/dashboard.
It does not execute SQL against Nominal. For the real API check, export a key
and matching base URL/workspace, then run:

```sh
NOMINAL_LIVE_TESTS=1 \
NOMINAL_BASE_URL=https://api-staging.gov.nominal.io/api \
NOMINAL_WORKSPACE_RID='<workspace-rid>' \
go test -count=1 ./pkg/plugin -run '^TestLiveNominalSqlQueryIntegration$' -v
```

`NOMINAL_API_KEY` must be present in the environment. The live test creates a
temporary dataset and asset, ingests known values, checks exact results within
fractional time bounds and metadata discovery, then archives the resources.
Use the environment matching your key. Production fixture creation additionally
requires `NOMINAL_ALLOW_DEFAULT_LIVE_WRITES=1`.
