# SQL queries

Install a plugin build containing both the SQL backend (#143) and editor (#142).
Deploy the backend first if rolling these changes out separately.

Add a **Nominal** data source and enter the API base URL, API key and workspace
RID. One data source supports both Compute and SQL; choose the API separately
for each query in the panel editor. A panel can contain both kinds of query.
Existing saved queries keep their API, regardless of the old data-source API
setting. New queries default to Compute. Switching APIs retains the SQL text
and Compute selections; it does not translate between them.

The key needs access to the selected workspace and data. Save & Test checks API
authentication and workspace access, while running a query verifies API access.

## SQL editor

In a panel, select your Nominal data source, choose **Query API → SQL** for a
query, and write SQL directly in the editor.
Start with `SELECT 1 AS value` and choose **Table** for a simple connection check.
The editor commits and runs on blur, Ctrl/Cmd+S or Ctrl/Cmd+Enter.
The API key stays on the Grafana server.

For numeric telemetry, choose **Time series** and use **Use time-series example**
in an empty editor, or enter a query like this with your dataset RID and channel:

```sql
SELECT $__timeGroup(ts, 1m) AS "time", channel, AVG(value) AS "value"
FROM points_double
WHERE dataset_rid = '<your-dataset-rid>'
  AND channel IN ('temperature')
  AND $__timeFilter(ts)
GROUP BY 1, 2
ORDER BY 1
```

**Time series** expects a timestamp column plus numeric values; string columns
become series labels. Sparse series keep missing samples as nulls. **Table**
retains the original rows and columns. Point tables and `channels` require a
`dataset_rid` filter. `$__timeFilter(ts)` uses an inclusive start and exclusive
end, retaining subsecond precision. `$__timeFrom()` and `$__timeTo()` return
UTC timestamps. `$__timeGroup(ts)` uses the panel interval; an explicit positive
duration or `$__interval` is also supported.

Dashboard variables work in SQL: use `channel IN ($channels)` for multi-value
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

Open <http://localhost:3000>, configure a Nominal data source, choose SQL in a
panel query, and run
`SELECT 1 AS value` with Table format. Then run the numeric example above and select a
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

The browser test supplies mock results, checks API switching without losing edits,
and verifies that Compute and SQL queries share a data source and panel. It needs
local Grafana but no Nominal credentials, and cleans up its datasource/dashboard.
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
fractional time bounds for raw and aggregated SQL, then archives the resources.
Use the environment matching your key. Production fixture creation additionally
requires `NOMINAL_ALLOW_DEFAULT_LIVE_WRITES=1`.
