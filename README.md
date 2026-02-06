- [Nominal Datasource Grafana Plugin](#nominal-datasource-grafana-plugin)
  - [Plugin Architecture](#plugin-architecture)
  - [Quick Start](#quick-start)
    - [Development and E2E testing](#development-and-e2e-testing)
    - [Verify Plugin Installation](#verify-plugin-installation)
    - [Reviewer bootstrap](#reviewer-bootstrap)
  - [Installing Plugin on Existing Grafana Instances](#installing-plugin-on-existing-grafana-instances)
  - [API Testing](#api-testing)
- [Docs and other references](#docs-and-other-references)

# Nominal Datasource Grafana Plugin

This repository contains **Nominal Grafana plugins** for integrating with the Nominal API.

## Plugin Architecture

This is a **production-ready Grafana data source plugin** with:

- **Go backend**: Secure API key handling and server-side processing
- **TypeScript frontend**: Modern React-based query editor and configuration UI
- **Full-stack integration**: Backend handles API calls, frontend provides intuitive interface
- **Enterprise features**: Authentication, caching, and optimized performance

> **Note**: Alternative implementations (pure TypeScript, panel plugin) are available in the [`archive/`](./archive/) directory for reference and development purposes.

## Quick Start

### Development and E2E testing

(required for Playwright tests)

```sh
pnpm install

# Build and start development environment:
mage -v                    # Build Go backend
pnpm run build            # Build TypeScript frontend
pnpm run server           # Start Docker development environment (with pre-configured datasource)

# Run E2E tests (requires development environment):
pnpm run e2e              # Run Playwright tests
```

> **Note**: Use `pnpm run server` (development Docker) for testing - it includes pre-configured datasources that Playwright tests expect. The production build starts with unconfigured datasources and will cause test failures.

### Verify Plugin Installation

After starting the container, verify the plugin is installed:

```sh
# Check if plugin directory exists
docker exec <container-name> ls -la /var/lib/grafana/plugins/nominaltest-nominalds-datasource/

# Should show:
# - module.js (frontend)
# - gpx_nominal_ds_linux_amd64 (backend binary)
# - plugin.json (metadata)
```

- **Build Output**: The build creates a production-ready Grafana image containing:

  - Built TypeScript frontend
  - Built Go backend
  - Nominal datasource plugin

- Access deployed instance at http://localhost:3000 (credentials from your `.env` file):

  1. Login with your configured credentials
  2. Go to **Configuration > Data sources**
  3. Click **Add data source**
  4. Look for **Nominal Data Source** in the list

### Reviewer bootstrap

For plugin catalog review setup, see:

- [REVIEW.md](./REVIEW.md)

Provisioned resources included in this repo:

- Datasource provisioning: `provisioning/datasources/datasources.yml`
- Dashboard provisioning provider: `provisioning/dashboards/dashboards.yml`
- Reviewer dashboard JSON: `provisioning/dashboards/json/nominal-review-dashboard.json`

> **Note**: Internal deployment tooling (Helm/ECR/production Docker build) is maintained in a private audit repository and intentionally omitted here.

## Installing Plugin on Existing Grafana Instances

If you already have Grafana running (cloud, self-hosted, or on-prem) and just need to install the plugin, download the pre-built plugin ZIP from [GitHub Releases](../../releases).
Releases are cut automatically via release-please when conventional commits land on `main`.

### Quick Install

```bash
# Download the latest release (replace VERSION with actual version, e.g., 0.0.13)
VERSION="0.0.13"
curl -L "https://github.com/nominal-io/grafana-plugin-public/releases/download/${VERSION}/nominaltest-nominalds-datasource-${VERSION}.zip" \
  -o plugin.zip

# Extract to Grafana plugins directory
unzip plugin.zip -d /var/lib/grafana/plugins/

# Restart Grafana to load the plugin
sudo systemctl restart grafana-server
# or for Docker: docker restart <grafana-container>
```

### Configuration

After installation, configure Grafana to allow the unsigned plugin:

```ini
# In grafana.ini or via environment variable
[plugins]
allow_loading_unsigned_plugins = nominaltest-nominalds-datasource

# Or as environment variable:
# GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=nominaltest-nominalds-datasource
```

### Verify Installation

```bash
# Check plugin files exist
ls -la /var/lib/grafana/plugins/nominaltest-nominalds-datasource/

# Expected files:
# - module.js (frontend)
# - gpx_nominal_ds_linux_amd64 (backend binary for Linux)
# - gpx_nominal_ds_darwin_amd64 (backend binary for macOS Intel)
# - gpx_nominal_ds_darwin_arm64 (backend binary for macOS Apple Silicon)
# - plugin.json (metadata)
```

Then in Grafana UI:
1. Go to **Configuration > Data sources**
2. Click **Add data source**
3. Search for **Nominal Data Source**
4. Configure with your Nominal API key and base URL

## API Testing

With Backend Plugin (Go + TypeScript): The backend plugin uses `/resources/` endpoints that route through the Go backend.

- Health Check

  ```sh
  curl "http://localhost:3000/api/datasources/uid/{UID}/resources/test"
  # {"message":"Successfully connected to Nominal API","status":"success"}

  curl "http://localhost:3000/api/datasources/uid/{UID}/health"
  # {"message":"Successfully connected to Nominal API","status":"OK"}
  ```

- Authentication Test

  ```sh
  curl -s "http://localhost:3000/api/datasources/uid/{UID}/resources/api/authentication/v2/my/profile" | jq
  ```

- Compute API Request

  ```sh
  curl -s 'http://localhost:3000/api/datasources/uid/{UID}/resources/api/compute/v2/compute' \
    -H 'Content-Type: application/json' \
    -d@../../tests/bash/payloads/compute-api.json | jq
  ```

- Find Your Datasource

  ```sh
  # List all datasources
  curl -s "http://localhost:3000/api/datasources" | jq '.[] | {id, uid, name, type, url}'

  # Get specific datasource by ID
  curl -s "http://localhost:3000/api/datasources/{ID}" | jq '{id, uid, name, url, jsonData, secureJsonFields}'
  ```

  ```json
  {
    "id": 1,
    "uid": "P1E5984762EB73E39",
    "name": "nominal-ds",
    "url": "",
    "jsonData": {
      "baseUrl": "https://api.gov.nominal.io/api"
    },
    "secureJsonFields": {
      "apiKey": true
    }
  }
  ```

# Docs and other references

- **Plugin dev docs**: https://grafana.com/developers/plugin-tools

- **Datasource**:

  - Overview https://grafana.com/developers/plugin-tools/how-to-guides/data-source-plugins/
  - Basic https://github.com/grafana/grafana-plugin-examples/tree/main/examples/datasource-basic
  - With backend https://github.com/grafana/grafana-plugin-examples/tree/main/examples/datasource-with-backend
  - Add auth for ds plugin https://grafana.com/developers/plugin-tools/how-to-guides/data-source-plugins/add-authentication-for-data-source-plugins

- **Panel type plugin**:

  - Panel plugins https://grafana.com/developers/plugin-tools/how-to-guides/panel-plugins/

- **Viz types**:

  - Visualizations https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/
  - Geomap https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/geomap/

- **Package, Sign, Publish**:

  - Package https://grafana.com/developers/plugin-tools/publish-a-plugin/package-a-plugin
  - Publish a plugin - signing https://grafana.com/developers/plugin-tools/publish-a-plugin/sign-a-plugin
  - Plugin policies https://grafana.com/legal/plugins/#what-are-the-different-classifications-of-plugins
