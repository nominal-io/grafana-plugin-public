package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

// assetCacheTTL controls how long fetched asset metadata is cached.
const assetCacheTTL = 5 * time.Minute

const maxChannelVariables = 5000

// assetCacheEntry holds a cached asset response with its fetch time.
type assetCacheEntry struct {
	asset     *SingleAssetResponse
	fetchedAt time.Time
}

// channelMetadataCacheEntry holds a cached channel metadata inference result with its fetch time.
type channelMetadataCacheEntry struct {
	channelDataType string // "string", "log", "numeric", or "" for searched-but-not-found / DataType nil
	unit            string // raw Nominal canonical unit symbol; "" if Unit was nil or missing
	fetchedAt       time.Time
}

type NominalCatalog struct {
	resourceHTTPClient *http.Client
	datasourceService  datasourceservice.DataSourceServiceClient

	assetCacheMu sync.Mutex
	assetCache   map[string]assetCacheEntry

	channelMetadataCacheMu sync.Mutex
	channelMetadataCache   map[string]channelMetadataCacheEntry
}

func newNominalCatalog(resourceHTTPClient *http.Client, datasourceService datasourceservice.DataSourceServiceClient) *NominalCatalog {
	return &NominalCatalog{
		resourceHTTPClient:   resourceHTTPClient,
		datasourceService:    datasourceService,
		assetCache:           make(map[string]assetCacheEntry),
		channelMetadataCache: make(map[string]channelMetadataCacheEntry),
	}
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

// clone returns a deep copy so cached entries can never be mutated through a
// returned asset. nil-safe: not-found assets are cached and returned as nil.
func (a *SingleAssetResponse) clone() *SingleAssetResponse {
	if a == nil {
		return nil
	}
	out := *a
	if a.DataScopes != nil {
		out.DataScopes = make([]AssetDataScope, len(a.DataScopes))
		for i, scope := range a.DataScopes {
			scope.DataSource.Dataset = cloneStringPtr(scope.DataSource.Dataset)
			scope.DataSource.Connection = cloneStringPtr(scope.DataSource.Connection)
			scope.DataSource.LogSet = cloneStringPtr(scope.DataSource.LogSet)
			out.DataScopes[i] = scope
		}
	}
	return &out
}

func cloneStringPtr(s *string) *string {
	if s == nil {
		return nil
	}
	v := *s
	return &v
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

func (c *NominalCatalog) HasSupportedDataSource(asset AssetSearchResult) bool {
	for _, scope := range asset.DataScopes {
		if isSupportedDataSourceType(scope.DataSource.Type) {
			return true
		}
	}
	return false
}

// FetchAssetByRid fetches a single asset by its RID using the batch lookup endpoint.
// Results are cached for assetCacheTTL. The returned value is a copy, so callers
// may mutate it without affecting the cache or other callers.
func (c *NominalCatalog) FetchAssetByRid(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	c.assetCacheMu.Lock()
	if c.assetCache == nil {
		c.assetCache = make(map[string]assetCacheEntry)
	}
	if entry, ok := c.assetCache[assetRid]; ok && time.Since(entry.fetchedAt) < assetCacheTTL {
		c.assetCacheMu.Unlock()
		return entry.asset.clone(), nil
	}
	c.assetCacheMu.Unlock()

	asset, err := c.fetchAssetByRidUncached(ctx, config, assetRid)
	if err != nil {
		return nil, err
	}

	c.assetCacheMu.Lock()
	c.assetCache[assetRid] = assetCacheEntry{asset: asset, fetchedAt: time.Now()}
	c.assetCacheMu.Unlock()

	return asset.clone(), nil
}

// postNominalJSON marshals body as JSON and POSTs it to {config baseURL}+path
// with the standard Authorization and Content-Type headers. On non-200 the
// response body is read, closed, and returned as a typed *apiError. On 200
// the caller owns closing resp.Body.
func (c *NominalCatalog) postNominalJSON(ctx context.Context, config *models.PluginSettings, path string, body any) (*http.Response, error) {
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

	if c.resourceHTTPClient == nil {
		return nil, fmt.Errorf("resource HTTP client is not configured")
	}
	resp, err := c.resourceHTTPClient.Do(req)
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

func (c *NominalCatalog) fetchAssetByRidUncached(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	resp, err := c.postNominalJSON(ctx, config, "/scout/v1/asset/multiple", []string{assetRid})
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

// FetchAssetsForVariable fetches assets from the Nominal API using direct HTTP calls.
func (c *NominalCatalog) FetchAssetsForVariable(ctx context.Context, config *models.PluginSettings, searchText string, maxResults int) ([]AssetResponse, error) {
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

		resp, err := c.postNominalJSON(ctx, config, "/scout/v1/search-assets", requestBody)
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

// catalog lazily builds the NominalCatalog, snapshotting resourceHTTPClient and
// datasourceService on first use. Mutate either field before the first call.
func (d *Datasource) catalog() *NominalCatalog {
	if d.nominalCatalog == nil {
		d.nominalCatalog = newNominalCatalog(d.resourceHTTPClient, d.datasourceService)
	}
	return d.nominalCatalog
}

// InferChannelMetadata verifies (or backfills) channel metadata — both data type
// and unit symbol — against the actual ChannelMetadata returned by SearchChannels.
func (c *NominalCatalog) InferChannelMetadata(ctx context.Context, config *models.PluginSettings, qm *NominalQueryModel) {
	if qm == nil || c == nil || c.datasourceService == nil {
		return
	}
	if strings.TrimSpace(qm.AssetRid) == "" || strings.TrimSpace(qm.Channel) == "" || strings.TrimSpace(qm.DataScopeName) == "" {
		return
	}

	cacheKey := qm.AssetRid + "|" + qm.DataScopeName + "|" + qm.Channel

	if entry, hit := c.lookupChannelMetadata(cacheKey); hit {
		applyChannelMetadata(qm, entry)
		return
	}

	asset, err := c.FetchAssetByRid(ctx, config, qm.AssetRid)
	if err != nil {
		log.DefaultLogger.Warn("Failed to fetch asset for channel metadata inference", "assetRid", qm.AssetRid, "error", err)
		return
	}
	if asset == nil {
		return
	}

	dataSourceRids := c.DataSourceRidsForScope(asset, qm.DataScopeName)
	if len(dataSourceRids) == 0 {
		return
	}

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)
	searchRequest := datasourceapi.SearchChannelsRequest{
		ExactMatch:  []string{qm.Channel},
		DataSources: dataSourceRids,
	}
	channelsResponse, err := c.datasourceService.SearchChannels(ctx, bearerToken, searchRequest)
	if err != nil {
		log.DefaultLogger.Warn("Failed to search channels for channel metadata inference", "assetRid", qm.AssetRid, "error", err)
		return
	}

	if entry, ok := channelMetadataEntryForExactMatch(channelsResponse.Results, qm.Channel); ok {
		applyChannelMetadata(qm, entry)
		entry.fetchedAt = time.Now()
		c.storeChannelMetadata(cacheKey, entry)
		return
	}

	c.storeChannelMetadata(cacheKey, channelMetadataCacheEntry{fetchedAt: time.Now()})
}

func (c *NominalCatalog) SearchChannelsForVariables(ctx context.Context, bearerToken bearertoken.Token, dataSourceRids []rids.DataSourceRid) ([]datasourceapi.ChannelMetadata, error) {
	if c == nil || c.datasourceService == nil || len(dataSourceRids) == 0 {
		return nil, nil
	}

	pageSize := 1000
	var allChannelResults []datasourceapi.ChannelMetadata
	var nextPageToken *api.Token

	for {
		searchChannelsRequest := datasourceapi.SearchChannelsRequest{
			FuzzySearchText: "",
			DataSources:     dataSourceRids,
			PageSize:        &pageSize,
			NextPageToken:   nextPageToken,
		}

		channelsResponse, err := c.datasourceService.SearchChannels(ctx, bearerToken, searchChannelsRequest)
		if err != nil {
			return nil, err
		}

		allChannelResults = append(allChannelResults, channelsResponse.Results...)

		if channelsResponse.NextPageToken == nil || len(allChannelResults) >= maxChannelVariables || len(channelsResponse.Results) == 0 {
			break
		}
		nextPageToken = channelsResponse.NextPageToken
	}

	if len(allChannelResults) > maxChannelVariables {
		allChannelResults = allChannelResults[:maxChannelVariables]
	}
	return allChannelResults, nil
}

func channelMetadataEntryForExactMatch(channels []datasourceapi.ChannelMetadata, channelName string) (channelMetadataCacheEntry, bool) {
	// Nominal enforces unique DataScopeName per asset (CreateAssetDataScope conjure
	// doc + DuplicateDataScopeNames error), so SearchChannels-exact-match returns
	// at most one case-exact result. Pick the first match with usable metadata.
	for _, channel := range channels {
		if string(channel.Name) != channelName {
			continue
		}
		entry := channelMetadataCacheEntry{
			channelDataType: getChannelDataType(channel), // "" if ChannelMetadata.DataType is nil
			unit:            getChannelUnit(channel),     // "" if Unit is nil
		}
		if entry.channelDataType == "" && entry.unit == "" {
			continue
		}
		return entry, true
	}
	return channelMetadataCacheEntry{}, false
}

// lookupChannelMetadata returns a cached channel metadata entry if present and
// not yet expired. Caller must apply the entry to its query model on hit.
func (c *NominalCatalog) lookupChannelMetadata(cacheKey string) (channelMetadataCacheEntry, bool) {
	c.channelMetadataCacheMu.Lock()
	defer c.channelMetadataCacheMu.Unlock()
	if c.channelMetadataCache == nil {
		c.channelMetadataCache = make(map[string]channelMetadataCacheEntry)
	}
	entry, ok := c.channelMetadataCache[cacheKey]
	if !ok || time.Since(entry.fetchedAt) >= assetCacheTTL {
		return channelMetadataCacheEntry{}, false
	}
	return entry, true
}

func (c *NominalCatalog) storeChannelMetadata(cacheKey string, entry channelMetadataCacheEntry) {
	c.channelMetadataCacheMu.Lock()
	defer c.channelMetadataCacheMu.Unlock()
	if c.channelMetadataCache == nil {
		c.channelMetadataCache = make(map[string]channelMetadataCacheEntry)
	}
	c.channelMetadataCache[cacheKey] = entry
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

// DataSourceRidsForScope returns the parsed DataSource RIDs from data scopes on
// the asset. An empty dataScopeName includes every supported scope.
func (c *NominalCatalog) DataSourceRidsForScope(asset *SingleAssetResponse, dataScopeName string) []rids.DataSourceRid {
	var out []rids.DataSourceRid
	for _, scope := range asset.DataScopes {
		if dataScopeName != "" && scope.DataScopeName != dataScopeName {
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
