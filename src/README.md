# Nominal

Bring Nominal time-series data into Grafana dashboards and Explore, with template-variable support for asset, data scope, and channel selectors. Use it to search Nominal assets, pick data scopes and channels, and visualize numeric, string, and log channels alongside the rest of your Grafana telemetry.

## Requirements

- Grafana 12.1 or later.
- A Nominal API key.
- Access to a Nominal API endpoint, such as `https://api.gov.nominal.io/api`.

## Install and configure

The Base URL, API key, and Workspace RID can all be copied from **Settings > API keys** in the Nominal app (e.g. https://app.gov.nominal.io/settings/org/api-keys).

1. In Grafana, go to **Connections** > **Data sources**.
2. Add the **Nominal** data source.
3. Set **Base URL** to your Nominal API endpoint, including the `/api` path.
4. Create a Nominal API key and paste it in **API Key**.
5. Recommended: set **Workspace RID**. It limits asset search to one workspace and selects the workspace SQL queries run in. Empty searches every workspace the key can access, and SQL uses the key's default workspace.
6. Select **Save & test**.

Grafana stores the API key as an encrypted secret and uses it only from the backend plugin, so it's never exposed to the browser. The health check verifies that Grafana can reach Nominal and authenticate with the configured key. With a Workspace RID set, it also confirms the key can see that workspace and shows its name.

## Query basics

Each query uses either **Compute** or **SQL**, chosen with **Query API** in the query editor. One panel can mix both.

Compute queries follow a three-step pattern:

1. Search for an asset by name, or paste a Nominal resource identifier (RID).
2. Pick a data scope from the asset.
3. Pick a channel from the data scope.

Queries can be used in any Grafana panel or in Explore.

## SQL queries

SQL queries run Nominal SQL in the data source's workspace. They need an `https` Base URL, and telemetry tables such as `points_double` must filter on `dataset_rid`:

```sql
SELECT $__timeGroup(ts) AS "time", channel, AVG(value) AS "value"
FROM points_double
WHERE dataset_rid = '<dataset-rid>'
  AND $__timeFilter(ts)
GROUP BY 1, 2
ORDER BY 1
```

`$__timeFilter(ts)` limits a timestamp column to the panel time range, `$__timeGroup(ts)` buckets it at the panel interval, and `$__timeFrom()` and `$__timeTo()` return the range bounds. Choose **Time series** to plot numeric columns, with string columns as series labels, or **Table** to see the rows as returned. Without a numeric column, a string column named `value` gives one series per label set for a State timeline panel. Choose **Logs** to show each row as a log line. See [Logs](https://github.com/nominal-io/grafana-plugin-public/blob/main/docs/sql.md#logs) for the columns and limits. Write `channel IN (${channel:sqlstring})` to use a variable in SQL. It works whether the variable is single-value, multi-value, or has Include All, but a value that contains a double quote does not match. Plain `$channel` is unquoted for a single value but quoted for multi-value or Include All.

Annotations can also run Nominal SQL. The query needs a `"time"` column and a text column, and the annotation editor's **Use events example** inserts a starting query. See [Annotations](https://github.com/nominal-io/grafana-plugin-public/blob/main/docs/sql.md#annotations) for the columns and more patterns.

## Channel types

### Numeric

Most physical telemetry: temperatures, pressures, currents, vibration amplitudes.

Numeric channels expose an **Aggregation(s)** picker with two groups. Under **Raw points**, **LTTB** (Largest-Triangle-Three-Buckets) returns a subset of the actual samples, chosen to keep the visual shape of the signal. Every returned value is a real sample at its real timestamp. LTTB can't be combined with bucket aggregations: selecting it clears them, and selecting a bucket aggregation clears LTTB.

Under **Bucket aggregations**, each selected aggregation is computed per time bucket and returned as its own series:

- **Mean** — overall shape and trend. The default, and what most line charts want.
- **Min** / **Max** — catch extrema that a mean smooths away. Pair with Mean to render a banded series (min/max envelope around the mean).
- **First** / **Last** — the actual first or last sample value (and its real timestamp) inside each bucket, not a derived statistic. Use when you need a true `(value, time)` pair for sparse signals, or to correlate precisely with external events.
- **Count** — number of samples per bucket. Useful for sanity-checking decimation, spotting gaps, or detecting changes in sample rate.
- **Variance** — within-bucket spread. Flags noisy or unstable intervals without leaving the panel.

String channels always aggregate by **Mode** (the most-frequent value in each bucket); the aggregation picker doesn't apply. Log channels return raw entries without aggregation.

Selecting multiple aggregations at once returns each as its own series, so a single Time series panel can render min, max, and mean as three lines. To turn the same selection into a shaded min/max envelope around the mean, add a Grafana **Fill below to** field override on the min series (filling to max), which produces a banded view without changing the query. For exact values, spot-checks, or embedding numbers inside summary dashboards, swap the visualization to a Table panel.

Numeric channels carry their unit from Nominal, so axes and tooltips render with the correct symbol and scaling (for example, `°C`, `psi`, `m/s`). Symbols outside Grafana's built-in unit registry are appended verbatim as a suffix.

### String

Categorical telemetry: states, modes, fault codes.

A **State Timeline** panel colors each segment by string value over time, making state transitions visible at a glance. It's the right default for any signal that spends meaningful time in each state. For infrequent transitions or audit-style readouts, a **Table** panel reads more cleanly, with one row per value change and its timestamp.

### Log

Event-style records with a message field. Grafana's **Logs** panel renders Nominal log records inline with other telemetry from the same dashboard; pair it with a numeric or string panel for an at-a-glance correlation between signals and logs.

A log channel query loads the newest 1,000 lines in the time range, and scrolling loads older ones. With the log order set to oldest first, scrolling loads only newer lines, so lines older than the first 1,000 do not load. Use newest first, or shorten the time range. Scrolling stops once the lines shown come within one second of the range's start, so if the last page loaded ended within that second, the lines left in it do not show. Start the time range a few seconds earlier to see them.

## Dashboards and template variables

Nominal supports Grafana dashboard variables for assets, data scopes, and channels, so dashboards can be re-pointed at a different asset, data scope, or channel without editing every panel individually.

### Where to define variable queries

1. Open your dashboard and click the gear icon, then **Variables**.
2. Select **New variable**.
3. Set **Type** to **Query**.
4. Set **Data source** to your Nominal data source.
5. Leave **Mode** on **Catalog** and enter one of the strings below (for example, `assets`), or choose **SQL** to write a Nominal SQL query.
6. Click **Run query** to preview, then **Apply**.

Chain variables by referencing earlier ones with `${var}`. For example, define `asset` first, then a `datascope` variable whose query is `datascopes(${asset})`. Grafana re-runs the child query whenever the parent changes.

### Variable queries

- `assets` returns up to 500 assets from the workspace. Use it as the top variable in a smaller asset-driven dashboard built from scratch.
- `assets($__searchFilter)` enables Grafana's dynamic dropdown search. Opening the dropdown loads unfiltered assets; typing in the dropdown re-runs the Nominal asset search with the typed text. Use this for large workspaces where the target asset might not be in the initially loaded result set.
- `assets(engine)` returns assets matching the search text `engine`. Matching is case-insensitive, tolerates minor typos, and looks at the asset's name, description, labels, and properties - so `engine`, `engines`, and `enigne` all find `Engine 1`. Use this to scope a dashboard to a fleet or family when your assets share a naming pattern - `assets(turbine)`, `assets(pack)`, `assets(vehicle)`.
- `datascopes(${asset})` returns every data scope on the selected asset. Chain it directly under an `assets` variable so the data scope dropdown refreshes when the user picks a different asset.
- `channels(${asset})` returns up to 5,000 channel names across all supported data scopes on the selected asset, deduplicated by name. Use it when your asset has a single primary data scope or when panels pin a literal data scope.
- `channels(${asset}, ${datascope})` returns channels filtered to the selected asset and data scope. This is the canonical pattern for production dashboards: pick an asset, pick its data scope, then pick a channel scoped to that pair.
- `channels(${asset}, ${datascope}, $__searchFilter)` enables Grafana's dynamic dropdown search for channels in the selected data scope. Use it for assets with large channel catalogs.
- `channels(${asset}, , $__searchFilter)` searches channels across all supported data scopes on the selected asset. The second argument is intentionally empty.
- `channels(${asset}, $__searchFilter)` is a shortcut for unscoped dynamic channel search, equivalent to `channels(${asset}, , $__searchFilter)`.

The same search slot accepts fixed text: `assets:engine`, `channels(${asset}, ${datascope}, temp)`, and `channels(${asset}, , temp)`. `assets()` is equivalent to `assets`, and `assets:$__searchFilter` is equivalent to `assets($__searchFilter)`.

Scope names containing commas or delimiter-like characters should be referenced through a variable such as `${datascope}` instead of hardcoded inline. Channel arguments are split before Grafana variable interpolation, so `channels(${asset}, ${datascope}, $__searchFilter)` preserves the selected scope name as one value.

### SQL variable queries

Choose **SQL** mode to fill a variable from Nominal SQL. Return one column to use each row as both label and value, or return columns named `__text` and `__value` for a separate label. See [Dashboard variables](https://github.com/nominal-io/grafana-plugin-public/blob/main/docs/sql.md#dashboard-variables) for how values are converted, an example that filters a panel by asset, and what happens after a downgrade.

### Common patterns

**Single-asset multi-channel monitoring.** One named asset, several panels, no template variables. Each panel shows one or more channels from the asset's data scopes. This is the most common layout for a dedicated operations dashboard like "Test stand monitoring", "Motor qualification", or "Vehicle road test".

**Single-asset deep-dive.** Three chained variables (`assets($__searchFilter)`, `datascopes(${asset})`, `channels(${asset}, ${datascope}, $__searchFilter)`) drive every panel. Switching the asset cascades through scopes and channels automatically. Use this when you want one reusable dashboard that works for any asset with the same data scope and channel schema, including assets and channels outside the initially loaded option lists.

**Fleet view.** One filtered asset variable (for example, `assets(engine)` or `assets($__searchFilter)`) set to multi-value, plus a channel variable scoped to the same data scope used by the panels (for example, `channels(${asset}, primary, $__searchFilter)`). Populate the channel picker from one representative asset before expanding the asset variable to multiple values, or use a separate single-value representative asset variable for channel lookup. Each panel shows the same channel as multiple series, one per selected asset. This pattern assumes every asset in the fleet exposes the same channel under the same data scope name; pin that scope as a literal in each panel's **Data scope** field rather than parameterizing it. Use `channels(${asset}, , $__searchFilter)` only when you intentionally want a flat cross-scope channel picker.

**Per-channel panel repeat.** A multi-value channel variable (`channels(${asset}, ${datascope}, $__searchFilter)` set to multi-value) combined with Grafana's panel **Repeat options** clones one panel per selected channel. The dashboard grows or shrinks based on the number of channels selected. The same repeat trick works on a multi-value asset variable when you want one panel per asset instead of one panel with multiple series.

## Troubleshooting

- If **Save & test** fails, confirm that the Base URL includes the `/api` path and that the API key is valid.
- If asset or channel search fails, confirm that the data source can reach Nominal and that the API key has access to the requested data.
- If an expected asset is missing from search, check the **Workspace RID**: only assets in that workspace are listed. Assets from other workspaces that the API key can access still resolve if referenced directly by RID.

## Links

- Nominal: https://www.nominal.io/
- Documentation: https://docs.nominal.io/
- Source repository: https://github.com/nominal-io/grafana-plugin-public
