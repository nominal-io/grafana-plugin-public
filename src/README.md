# Nominal

The Nominal data source for Grafana connects dashboards, Explore, template variables, and alert rules to Nominal time-series data. Use it to search for Nominal assets, choose data scopes and channels, and visualize numeric or string channel values alongside the rest of your Grafana telemetry.

## Requirements

- Grafana 10.4 or later.
- A Nominal API key.
- Access to a Nominal API endpoint, such as `https://api.gov.nominal.io/api`.

## Configure the Data Source

1. In Grafana, go to **Connections** > **Data sources**.
2. Add the **Nominal** data source.
3. Set **Base URL** to your Nominal API endpoint, including the `/api` path.
4. Enter your Nominal API key in **API Key**.
5. Select **Save & test**.

Grafana stores the API key securely and sends it only to the Nominal backend plugin. The health check verifies that Grafana can reach Nominal and authenticate with the configured key.

## Build Queries

The query editor helps you build Nominal queries without writing raw API requests.

- Search for an asset by name or paste a resource identifier directly.
- Select a data scope from the asset.
- Select a channel from the selected data scope.
- Choose the query mode and bucket count for time-series panels.

Queries return Grafana data frames that can be used in dashboards, Explore, and alert rules.

## Dashboard Variables

Nominal supports Grafana dashboard variables for assets, data scopes, and channels.

Example variable queries:

```text
assets
assets(engine)
datascopes(${asset})
channels(${asset})
channels(${asset}, ${datascope})
```

Use these variables to create dashboards that can switch between Nominal assets, scopes, and channels without editing each panel.

## Alerting

The plugin supports Grafana Alerting. After configuring the data source, create an alert rule from a panel that uses a Nominal query, or create a new Grafana-managed alert rule and select the Nominal data source.

Before saving an alert rule, use Grafana's preview or evaluation flow to confirm that the selected query returns the numeric series expected by the alert condition.

## Troubleshooting

- If **Save & test** fails, confirm that the Base URL includes the `/api` path and that the API key is valid.
- If asset or channel search fails, confirm that the data source can reach Nominal and that the API key has access to the requested data.
- If an alert rule does not evaluate, confirm that the query returns a numeric time series for the selected time range.

## Links

- Nominal: https://www.nominal.io/
- Documentation: https://docs.nominal.io/
- Source repository: https://github.com/nominal-io/grafana-plugin-public
- Grafana plugin development documentation: https://grafana.com/developers/plugin-tools
