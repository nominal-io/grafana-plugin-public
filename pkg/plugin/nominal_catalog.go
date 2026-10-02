package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-io/grafana-plugin-public/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	scoutrids "github.com/nominal-io/nominal-api-go/scout/rids/api"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	runapi1 "github.com/nominal-io/nominal-api-go/scout/run/api1"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
	"golang.org/x/sync/singleflight"
)

// catalogCacheTTL controls how long fetched assets and channel metadata are cached.
const catalogCacheTTL = 5 * time.Minute

// openRunCacheTTL is shorter so a run that ends is seen as ended soon after.
const openRunCacheTTL = time.Minute

// A cache-miss load runs detached from its caller, so this is the only bound on
// that work. For a metadata lookup it covers the asset fetch plus the channel
// search.
const detachedLookupTimeout = 30 * time.Second

const maxChannelVariables = 5000

// channelMetadataCacheEntry holds a cached channel metadata inference result.
type channelMetadataCacheEntry struct {
	channelDataType string // "string", "log", "numeric", or "" for searched-but-not-found / DataType nil
	unit            string // raw Nominal canonical unit symbol; "" if Unit was nil or missing
}

// ttlCacheEntry pairs a cached value with the time it expires.
type ttlCacheEntry[V any] struct {
	value     V
	expiresAt time.Time
}

// ttlCache is a mutex-guarded cache whose entries expire ttlFor(value) after
// they are stored. Concurrent cache misses for the same key coalesce into one
// detached backend load.
type ttlCache[V any] struct {
	ttlFor func(V) time.Duration

	mu      sync.Mutex
	entries map[string]ttlCacheEntry[V] // guarded by mu
	group   singleflight.Group
}

func newTTLCache[V any](ttl time.Duration) *ttlCache[V] {
	return newTTLCacheFunc(func(V) time.Duration { return ttl })
}

func newTTLCacheFunc[V any](ttlFor func(V) time.Duration) *ttlCache[V] {
	return &ttlCache[V]{
		ttlFor:  ttlFor,
		entries: make(map[string]ttlCacheEntry[V]),
	}
}

// lookup returns the cached value for key if present and not yet expired.
func (c *ttlCache[V]) lookup(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || !time.Now().Before(entry.expiresAt) {
		var zero V
		return zero, false
	}
	return entry.value, true
}

func (c *ttlCache[V]) store(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = ttlCacheEntry[V]{value: value, expiresAt: time.Now().Add(c.ttlFor(value))}
}

// get returns the cached value for key, coalescing concurrent misses for the
// same key into one load, detached from its callers and bounded by
// detachedLookupTimeout. Errors are never cached, so the next miss retries.
func (c *ttlCache[V]) get(ctx context.Context, key string, load func(context.Context) (V, error)) (V, error) {
	var zero V
	if v, hit := c.lookup(key); hit {
		return v, nil
	}

	// Avoid starting detached work for an already-canceled caller.
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	ch := c.group.DoChan(key, func() (any, error) {
		if v, hit := c.lookup(key); hit {
			return v, nil
		}
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedLookupTimeout)
		defer cancel()
		v, err := load(workCtx)
		if err != nil {
			return nil, err
		}
		c.store(key, v)
		return v, nil
	})

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return zero, res.Err
		}
		return res.Val.(V), nil
	}
}

type NominalCatalog struct {
	resourceHTTPClient *http.Client
	datasourceService  datasourceservice.DataSourceServiceClient
	runService         runAPI

	assetCache           *ttlCache[*SingleAssetResponse]
	runCache             *ttlCache[*RunResponse]
	channelMetadataCache *ttlCache[channelMetadataCacheEntry]
}

func newNominalCatalog(resourceHTTPClient *http.Client, datasourceService datasourceservice.DataSourceServiceClient, runService runAPI) *NominalCatalog {
	return &NominalCatalog{
		resourceHTTPClient:   resourceHTTPClient,
		datasourceService:    datasourceService,
		runService:           runService,
		assetCache:           newTTLCache[*SingleAssetResponse](catalogCacheTTL),
		runCache:             newTTLCacheFunc(runCacheTTL),
		channelMetadataCache: newTTLCache[channelMetadataCacheEntry](catalogCacheTTL),
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
// Results are cached for catalogCacheTTL; a not-found asset is cached and returned
// as nil. The returned value is a copy, so callers may mutate it without affecting
// the cache or other callers.
func (c *NominalCatalog) FetchAssetByRid(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	asset, err := c.assetCache.get(ctx, assetRid, func(fetchCtx context.Context) (*SingleAssetResponse, error) {
		return c.fetchAssetByRidUncached(fetchCtx, config, assetRid)
	})
	if err != nil {
		return nil, err
	}
	return asset.clone(), nil
}

// postNominalJSON marshals body as JSON and POSTs it to {config baseURL}+path
// with the standard Authorization and Content-Type headers. It reads and
// closes the response body itself: on non-200 it returns a typed *apiError
// carrying the upstream status and body, otherwise the raw response bytes.
func (c *NominalCatalog) postNominalJSON(ctx context.Context, config *models.PluginSettings, path string, body any) ([]byte, error) {
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

	respBody, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, newAPIError(resp.StatusCode, respBody)
	}
	if readErr != nil {
		return nil, fmt.Errorf("failed to read response body: %w", readErr)
	}

	return respBody, nil
}

func (c *NominalCatalog) fetchAssetByRidUncached(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	body, err := c.postNominalJSON(ctx, config, "/scout/v1/asset/multiple", []string{assetRid})
	if err != nil {
		return nil, err
	}

	var assetMap map[string]SingleAssetResponse
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&assetMap); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if asset, ok := assetMap[assetRid]; ok {
		return &asset, nil
	}
	return nil, nil
}

// withWorkspaceFilter ANDs a search-assets query with a workspace clause; empty RID returns the query unchanged.
func withWorkspaceFilter(query interface{}, workspaceRid string) interface{} {
	if workspaceRid == "" {
		return query
	}
	clause := map[string]interface{}{"type": "workspace", "workspace": workspaceRid}
	if query == nil {
		return clause
	}
	return map[string]interface{}{"type": "and", "and": []interface{}{query, clause}}
}

// FetchAssetsForVariable fetches assets from the Nominal API using direct HTTP calls.
func (c *NominalCatalog) FetchAssetsForVariable(ctx context.Context, config *models.PluginSettings, searchText string, maxResults int) ([]AssetResponse, error) {
	var allResults []AssetResponse
	pageToken := ""
	pageSize := 50
	totalFetched := 0

	query := withWorkspaceFilter(map[string]interface{}{"type": "searchText", "searchText": searchText}, config.WorkspaceRid)

	for totalFetched < maxResults {
		requestBody := map[string]interface{}{
			"query": query,
			"sort": map[string]interface{}{
				"field":        "CREATED_AT",
				"isDescending": false,
			},
			"pageSize": pageSize,
		}
		if pageToken != "" {
			requestBody["nextPageToken"] = pageToken
		}

		body, err := c.postNominalJSON(ctx, config, "/scout/v1/search-assets", requestBody)
		if err != nil {
			return nil, err
		}

		var assetResp AssetResponse
		if err := json.NewDecoder(bytes.NewReader(body)).Decode(&assetResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
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

// maxRunResults caps one run search, which is also the size of one page.
const maxRunResults = 500

// RunResponse holds the run fields the plugin reads.
type RunResponse struct {
	Rid       string
	RunNumber int64
	Title     string
	StartTime time.Time
	EndTime   *time.Time
	Assets    []string
}

func runFromAPI(run runapi1.Run) RunResponse {
	out := RunResponse{
		Rid:       run.Rid.String(),
		RunNumber: int64(run.RunNumber),
		Title:     run.Title,
		StartTime: utcTime(run.StartTime),
		Assets:    make([]string, len(run.Assets)),
	}
	for i, assetRid := range run.Assets {
		out.Assets[i] = assetRid.String()
	}
	if run.EndTime != nil {
		end := utcTime(*run.EndTime)
		out.EndTime = &end
	}
	return out
}

func utcTime(ts runapi.UtcTimestamp) time.Time {
	var nanos int64
	if ts.OffsetNanoseconds != nil {
		nanos = int64(*ts.OffsetNanoseconds)
	}
	return time.Unix(int64(ts.SecondsSinceEpoch), nanos).UTC()
}

func (r *RunResponse) clone() *RunResponse {
	if r == nil {
		return nil
	}
	out := *r
	out.Assets = slices.Clone(r.Assets)
	if r.EndTime != nil {
		end := *r.EndTime
		out.EndTime = &end
	}
	return &out
}

// ridHasType reports whether value parses as a RID of the given type, such as "run" or "asset".
func ridHasType(value, ridType string) bool {
	parsed, err := rid.ParseRID(value)
	return err == nil && parsed.Type == ridType
}

func runCacheTTL(run *RunResponse) time.Duration {
	if run != nil && run.EndTime == nil {
		return openRunCacheTTL
	}
	return catalogCacheTTL
}

// FetchRunByRid fetches one run, cached like assets, or briefly while it has no
// end. A not-found run is cached and returned as nil.
func (c *NominalCatalog) FetchRunByRid(ctx context.Context, config *models.PluginSettings, runRid string) (*RunResponse, error) {
	parsed, err := rid.ParseRID(runRid)
	if err != nil {
		return nil, fmt.Errorf("invalid run RID %q: %w", runRid, err)
	}
	key := runapi.RunRid(parsed)
	run, err := c.runCache.get(ctx, runRid, func(fetchCtx context.Context) (*RunResponse, error) {
		runs, err := c.runService.GetRuns(fetchCtx, bearertoken.Token(config.Secrets.ApiKey), []runapi.RunRid{key})
		if err != nil {
			return nil, err
		}
		found, ok := runs[key]
		if !ok {
			return nil, nil
		}
		out := runFromAPI(found)
		return &out, nil
	})
	if err != nil {
		return nil, err
	}
	return run.clone(), nil
}

// SearchRuns returns unarchived runs, newest first. A non-empty assetRids keeps
// runs on any of those assets.
func (c *NominalCatalog) SearchRuns(ctx context.Context, config *models.PluginSettings, assetRids []string, searchText string) ([]RunResponse, error) {
	clauses := []runapi.SearchQuery{runapi.NewSearchQueryFromArchived(false)}
	if len(assetRids) > 0 {
		anyAsset := make([]runapi.SearchQuery, len(assetRids))
		for i, assetRid := range assetRids {
			parsed, err := rid.ParseRID(assetRid)
			if err != nil {
				return nil, fmt.Errorf("invalid asset RID %q: %w", assetRid, err)
			}
			anyAsset[i] = runapi.NewSearchQueryFromAsset(scoutrids.AssetRid(parsed))
		}
		clauses = append(clauses, runapi.NewSearchQueryFromOr(anyAsset))
	}
	if searchText != "" {
		clauses = append(clauses, runapi.NewSearchQueryFromSearchText(searchText))
	}
	workspaceRid, err := parseWorkspaceRid(config.WorkspaceRid)
	if err != nil {
		return nil, err
	}
	if workspaceRid != nil {
		clauses = append(clauses, runapi.NewSearchQueryFromWorkspace(*workspaceRid))
	}

	sortKey := runapi.NewSortKeyFromField(runapi.New_SortField(runapi.SortField_START_TIME))
	resp, err := c.runService.SearchRuns(ctx, bearertoken.Token(config.Secrets.ApiKey), runapi.SearchRunsRequest{
		Query:    runapi.NewSearchQueryFromAnd(clauses),
		Sort:     runapi.SortOptions{IsDescending: true, SortKey: &sortKey},
		PageSize: maxRunResults,
	})
	if err != nil {
		return nil, err
	}
	runs := make([]RunResponse, len(resp.Results))
	for i, run := range resp.Results {
		runs[i] = runFromAPI(run)
	}
	return runs, nil
}

// InferChannelMetadata verifies (or backfills) channel metadata — both data type
// and unit symbol — against the actual ChannelMetadata returned by SearchChannels.
func (c *NominalCatalog) InferChannelMetadata(ctx context.Context, config *models.PluginSettings, qm *NominalQueryModel) {
	if qm == nil || c.datasourceService == nil {
		return
	}
	if strings.TrimSpace(qm.AssetRid) == "" || strings.TrimSpace(qm.Channel) == "" || strings.TrimSpace(qm.DataScopeName) == "" {
		return
	}

	cacheKey := channelMetadataCacheKey(qm.AssetRid, qm.DataScopeName, qm.Channel)

	assetRid := qm.AssetRid
	dataScopeName := qm.DataScopeName
	channel := qm.Channel
	entry, err := c.channelMetadataCache.get(ctx, cacheKey,
		func(lookupCtx context.Context) (channelMetadataCacheEntry, error) {
			return c.computeChannelMetadata(lookupCtx, config, assetRid, dataScopeName, channel)
		})
	if err != nil {
		// Metadata enrichment is best-effort.
		return
	}
	applyChannelMetadata(qm, entry)
}

// errChannelMetadataUnavailable marks an exit taken before any channel search
// ran. Returning an error keeps it out of the cache: a stale asset can hide a
// scope that appears once the asset cache refreshes, so only a completed search
// is worth caching.
var errChannelMetadataUnavailable = errors.New("channel metadata unavailable")

// computeChannelMetadata performs an uncached lookup. A completed search that
// matches nothing returns an empty entry, which is cached so the search is not
// repeated. The exits before that search return an error and stay uncached.
func (c *NominalCatalog) computeChannelMetadata(ctx context.Context, config *models.PluginSettings, assetRid, dataScopeName, channel string) (channelMetadataCacheEntry, error) {
	asset, err := c.FetchAssetByRid(ctx, config, assetRid)
	if err != nil {
		log.DefaultLogger.Warn("Failed to fetch asset for channel metadata inference", "assetRid", assetRid, "error", err)
		return channelMetadataCacheEntry{}, err
	}
	if asset == nil {
		return channelMetadataCacheEntry{}, errChannelMetadataUnavailable
	}

	dataSourceRids := c.DataSourceRidsForScope(asset, dataScopeName)
	if len(dataSourceRids) == 0 {
		return channelMetadataCacheEntry{}, errChannelMetadataUnavailable
	}

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)
	searchRequest := datasourceapi.SearchChannelsRequest{
		ExactMatch:  []string{channel},
		DataSources: dataSourceRids,
	}
	channelsResponse, err := c.datasourceService.SearchChannels(ctx, bearerToken, searchRequest)
	if err != nil {
		log.DefaultLogger.Warn("Failed to search channels for channel metadata inference", "assetRid", assetRid, "error", err)
		return channelMetadataCacheEntry{}, err
	}

	if entry, ok := channelMetadataEntryForExactMatch(channelsResponse.Results, channel); ok {
		return entry, nil
	}

	return channelMetadataCacheEntry{}, nil
}

func (c *NominalCatalog) SearchChannelsForVariables(ctx context.Context, bearerToken bearertoken.Token, dataSourceRids []rids.DataSourceRid) ([]datasourceapi.ChannelMetadata, error) {
	if c.datasourceService == nil || len(dataSourceRids) == 0 {
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

// Quoted components prevent separator collisions in cache keys.
func channelMetadataCacheKey(assetRid, dataScopeName, channel string) string {
	return fmt.Sprintf("%q|%q|%q", assetRid, dataScopeName, channel)
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
