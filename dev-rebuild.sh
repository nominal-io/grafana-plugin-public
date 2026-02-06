#!/bin/bash

echo "Rebuilding Nominal Datasource Plugin..."

# Check if we need to rebuild backend (if Go files changed)
if [[ -n "$(find pkg/ -name "*.go" -newer pkg/plugin/datasource.go 2>/dev/null)" ]] || [[ "$1" == "--full" ]]; then
    echo "Building backend (Go)..."
    mage -v
else
    echo "Skipping backend build (no Go changes detected)"
fi

echo "Building frontend (TypeScript)..."
pnpm run build

echo "Restarting container..."
docker container restart nominaltest-nominalds-datasource

echo "Rebuild complete! Check logs with: docker logs nominaltest-nominalds-datasource --tail 10" 