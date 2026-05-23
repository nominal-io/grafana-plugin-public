package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	"github.com/palantir/pkg/rid"
)

// assetCacheTTL controls how long fetched asset metadata is cached.
const assetCacheTTL = 5 * time.Minute

// assetCacheEntry holds a cached asset response with its fetch time.
type assetCacheEntry struct {
	asset     *SingleAssetResponse
	fetchedAt time.Time
}

// AssetDataSource represents the data source within an asset's data scope.
type AssetDataSource struct {
	Type       string  `json:"type"`
	Dataset    *string `json:"dataset,omitempty"`
	Connection *string `json:"connection,omitempty"`
	LogSet     *string `json:"logSet,omitempty"`
}

// AssetDataScope represents a single data scope entry on an asset.
type AssetDataScope struct {
	DataScopeName string          `json:"dataScopeName"`
	DataSource    AssetDataSource `json:"dataSource"`
}

// SingleAssetResponse represents a single asset from the batch lookup API.
type SingleAssetResponse struct {
	Rid        string           `json:"rid"`
	Title      string           `json:"title"`
	DataScopes []AssetDataScope `json:"dataScopes"`
}

// AssetSearchResult represents a single asset returned by the search API.
type AssetSearchResult struct {
	Rid         string           `json:"rid"`
	Title       string           `json:"title"`
	Description string           `json:"description"`
	DataScopes  []AssetDataScope `json:"dataScopes"`
}

// AssetResponse represents the API response for asset search.
type AssetResponse struct {
	Results       []AssetSearchResult `json:"results"`
	NextPageToken string              `json:"nextPageToken"`
}

// isSupportedDataSourceType returns true for data source types that support channel queries.
func isSupportedDataSourceType(dsType string) bool {
	return dsType == "dataset" || dsType == "connection" || dsType == "logSet"
}

// dataSourceRidFor returns the RID string for a supported AssetDataSource.
// Returns ("", false) for unsupported types or missing RID pointers.
func dataSourceRidFor(ds AssetDataSource) (string, bool) {
	switch ds.Type {
	case "dataset":
		if ds.Dataset != nil {
			return *ds.Dataset, true
		}
	case "connection":
		if ds.Connection != nil {
			return *ds.Connection, true
		}
	case "logSet":
		if ds.LogSet != nil {
			return *ds.LogSet, true
		}
	}
	return "", false
}

// fetchAssetByRid fetches a single asset by its RID using the batch lookup endpoint
func (d *Datasource) fetchAssetByRid(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	d.assetCacheMu.Lock()
	if d.assetCache == nil {
		d.assetCache = make(map[string]assetCacheEntry)
	}
	if entry, ok := d.assetCache[assetRid]; ok && time.Since(entry.fetchedAt) < assetCacheTTL {
		d.assetCacheMu.Unlock()
		return entry.asset, nil
	}
	d.assetCacheMu.Unlock()

	asset, err := d.fetchAssetByRidUncached(ctx, config, assetRid)
	if err != nil {
		return nil, err
	}

	d.assetCacheMu.Lock()
	d.assetCache[assetRid] = assetCacheEntry{asset: asset, fetchedAt: time.Now()}
	d.assetCacheMu.Unlock()

	return asset, nil
}

// postNominalJSON marshals body as JSON and POSTs it to {config baseURL}+path
// with the standard Authorization and Content-Type headers. On non-200 the
// response body is read, closed, and returned as a typed *apiError. On 200
// the caller owns closing resp.Body.
func (d *Datasource) postNominalJSON(ctx context.Context, config *models.PluginSettings, path string, body any) (*http.Response, error) {
	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+config.Secrets.ApiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.getResourceHTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, newAPIError(resp.StatusCode, errBody)
	}

	return resp, nil
}

func (d *Datasource) fetchAssetByRidUncached(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	resp, err := d.postNominalJSON(ctx, config, "/scout/v1/asset/multiple", []string{assetRid})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var assetMap map[string]SingleAssetResponse
	if err := json.NewDecoder(resp.Body).Decode(&assetMap); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if asset, ok := assetMap[assetRid]; ok {
		return &asset, nil
	}
	return nil, nil
}

// fetchAssetsForVariable fetches assets from the Nominal API using direct HTTP calls
func (d *Datasource) fetchAssetsForVariable(ctx context.Context, config *models.PluginSettings, searchText string, maxResults int) ([]AssetResponse, error) {
	var allResults []AssetResponse
	pageToken := ""
	pageSize := 50
	totalFetched := 0

	for totalFetched < maxResults {
		requestBody := map[string]interface{}{
			"query": map[string]interface{}{
				"searchText": searchText,
				"type":       "searchText",
			},
			"sort": map[string]interface{}{
				"field":        "CREATED_AT",
				"isDescending": false,
			},
			"pageSize": pageSize,
		}
		if pageToken != "" {
			requestBody["nextPageToken"] = pageToken
		}

		resp, err := d.postNominalJSON(ctx, config, "/scout/v1/search-assets", requestBody)
		if err != nil {
			return nil, err
		}

		var assetResp AssetResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&assetResp)
		resp.Body.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("failed to decode response: %w", decodeErr)
		}

		allResults = append(allResults, assetResp)
		totalFetched += len(assetResp.Results)

		if assetResp.NextPageToken == "" || len(assetResp.Results) < pageSize {
			break
		}
		pageToken = assetResp.NextPageToken
	}

	return allResults, nil
}

// getChannelMetadataDescription extracts description from channel metadata
func getChannelMetadataDescription(channel datasourceapi.ChannelMetadata) string {
	if channel.Description != nil {
		return *channel.Description
	}
	return fmt.Sprintf("Channel: %s", string(channel.Name))
}

// getChannelUnit extracts the raw UCUM symbol from channel metadata.
// Returns "" if Unit is nil — treated as "no unit" downstream.
func getChannelUnit(channel datasourceapi.ChannelMetadata) string {
	if channel.Unit == nil {
		return ""
	}
	return strings.TrimSpace(channel.Unit.Symbol)
}

// getChannelDataType normalizes the API's SeriesDataType to "string", "log", or "numeric".
// Returns empty string if the metadata is not available (treated as numeric for backward compatibility).
func getChannelDataType(channel datasourceapi.ChannelMetadata) string {
	if channel.DataType == nil {
		return ""
	}
	switch channel.DataType.Value() {
	case api.SeriesDataType_STRING, api.SeriesDataType_STRING_ARRAY:
		return ChannelDataTypeString
	case api.SeriesDataType_LOG:
		return ChannelDataTypeLog
	default:
		return ChannelDataTypeNumeric
	}
}

// collectDataSourceRidsForScope returns the parsed DataSource RIDs from every
// data scope on the asset matching dataScopeName. Returns nil if no scopes match
// or none have a parseable RID for a supported data source type.
func collectDataSourceRidsForScope(asset *SingleAssetResponse, dataScopeName string) []rids.DataSourceRid {
	var out []rids.DataSourceRid
	for _, scope := range asset.DataScopes {
		if scope.DataScopeName != dataScopeName {
			continue
		}
		ridStr, ok := dataSourceRidFor(scope.DataSource)
		if !ok {
			continue
		}
		parsedRid, err := rid.ParseRID(ridStr)
		if err != nil {
			log.DefaultLogger.Warn("Failed to parse datasource RID for channel metadata inference", "rid", ridStr, "error", err)
			continue
		}
		out = append(out, rids.DataSourceRid(parsedRid))
	}
	return out
}
