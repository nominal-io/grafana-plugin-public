# Grafana Reviewer Bootstrap

This file is for Grafana plugin catalog reviewers and internal release operators.

## Quick bootstrap

1. Start Grafana with provisioning:

```sh
pnpm install
mage -v
pnpm run build
pnpm run server
```

2. Login to Grafana at `http://localhost:3000`:
   - Username: `admin` (or `GRAFANA_ADMIN_USER` from your `.env`)
   - Password: `admin` (or `GRAFANA_ADMIN_PASSWORD` from your `.env`)

3. Open `Connections` -> `Data sources` -> `nominal-ds`.

4. In data source settings:
   - Set `Base URL` to staging for reviewer access.
   - Set API key from shared reviewer credentials.
   - Click `Save & test` and expect success.

5. Open dashboard:
   - `Dashboards` -> `Nominal Review` -> `Nominal Plugin Reviewer - Bootstrap Dashboard`

## Known-good query tuple (staging)

Replace values below before submission. These are intentionally non-sensitive and should be committed.

- `assetRid`: `REPLACE_WITH_KNOWN_GOOD_ASSET_RID`
- `dataScopeName`: `REPLACE_WITH_KNOWN_GOOD_DATA_SCOPE`
- `channel`: `REPLACE_WITH_KNOWN_GOOD_CHANNEL`

Panel mapping in provisioned dashboard:

- Panel A: deterministic sanity query (no Nominal data dependency).
- Panel B: real staging query using the tuple above.

## CSV fixture for creating review data

If you need deterministic test data, ingest:

- `review/fixtures/nominal-review-seed.csv`

Suggested channel names from this file:

- `speed_mps`
- `battery_pct`
- `temperature_c`
- `altitude_m`
