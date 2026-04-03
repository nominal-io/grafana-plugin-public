package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdkhttpclient "github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
	"github.com/palantir/pkg/safelong"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces - only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ backend.CallResourceHandler   = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// proxyAllowedHeaders is the set of safe request headers forwarded to the
// upstream Nominal API. Sensitive caller context like Cookie and
// Authorization must never be relayed.
var proxyAllowedHeaders = map[string]bool{
	"Content-Type": true,
	"Accept":       true,
}

// maxBatchComputeSubrequests matches the backend subrequest limit.
// See scout ComputeResource.SUBREQUEST_LIMIT.
const maxBatchComputeSubrequests = 300

// defaultAPIBaseURL is the fallback Nominal API base URL when none is configured.
const defaultAPIBaseURL = "https://api.gov.nominal.io/api"

// assetCacheTTL controls how long fetched asset metadata is cached.
const assetCacheTTL = 2 * time.Minute

// assetCacheEntry holds a cached asset response with its fetch time.
type assetCacheEntry struct {
	asset     *SingleAssetResponse
	fetchedAt time.Time
}

// channelTypeCacheEntry holds a cached channel type inference result with its fetch time.
type channelTypeCacheEntry struct {
	channelType string // "string", "numeric", or "" for searched-but-not-found
	fetchedAt   time.Time
}

// NewDatasource creates a new datasource instance.
func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	config, err := models.LoadPluginSettings(settings)
	if err != nil {
		return nil, fmt.Errorf("failed to load plugin settings: %v", err)
	}

	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	// Use the base URL as-is since it should already include the full path
	baseURL = strings.TrimSuffix(baseURL, "/")

	// Use Grafana's SDK-managed HTTP client for direct HTTP requests from the plugin.
	httpClientOpts, err := settings.HTTPClientOptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build HTTP client options: %v", err)
	}

	resourceHTTPClient, err := sdkhttpclient.New(httpClientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource HTTP client: %v", err)
	}
	resourceHTTPClient.Timeout = 30 * time.Second

	// Generated Conjure clients still require their own client type, so keep this
	// wrapper for those service integrations.
	conjureClient, err := conjurehttpclient.NewClient(
		conjurehttpclient.WithBaseURLs([]string{baseURL}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create conjure HTTP client: %v", err)
	}

	ds := &Datasource{
		settings:           settings,
		resourceHTTPClient: resourceHTTPClient,
		authService:        authapi.NewAuthenticationServiceV2Client(conjureClient),
		computeService:     computeapi1.NewComputeServiceClient(conjureClient),
		datasourceService:  datasourceservice.NewDataSourceServiceClient(conjureClient),
		assetCache:         make(map[string]assetCacheEntry),
		channelTypeCache:   make(map[string]channelTypeCacheEntry),
	}

	return ds, nil
}

// interpolateTemplateVariables replaces template variables in strings.
// It supports both ${var} and $var syntax. The ${var} form is processed first
// so that a bare $var replacement cannot accidentally corrupt a ${othervar}
// token that happens to share a prefix (e.g. key "o" must not match inside
// "${othervar}"). The bare $var form uses a word-boundary regex so it only
// matches when the key name ends at a non-word character (or end-of-string).
func interpolateTemplateVariables(input string, variables map[string]interface{}) string {
	if variables == nil {
		return input
	}

	result := input
	for key, value := range variables {
		valueStr := fmt.Sprintf("%v", value)

		// Replace ${var} form first (unambiguous).
		result = strings.ReplaceAll(result, fmt.Sprintf("${%s}", key), valueStr)

		// Replace bare $var form only as a whole token: must not be immediately
		// followed by a word character so that $foo does not match inside $foobar.
		bareRe := regexp.MustCompile(`\$` + regexp.QuoteMeta(key) + `(\W|$)`)
		result = bareRe.ReplaceAllStringFunc(result, func(match string) string {
			// Preserve any trailing non-word character that was part of the match.
			suffix := match[len("$"+key):]
			return valueStr + suffix
		})
	}

	return result
}

// applyTemplateVariables applies template variable interpolation to query fields.
//
// Defense-in-depth: Grafana's SDK resolves dashboard template variables before
// the query JSON reaches the backend in most panel flows, so by the time
// QueryData is called the variables in qm.AssetRid / qm.Channel etc. are
// usually already substituted. However, variables passed explicitly via the
// TemplateVariables field of the query model (populated by the frontend for
// variable-panel queries and programmatic calls) are NOT resolved by the SDK,
// so this server-side pass is still needed for those paths.
func (d *Datasource) applyTemplateVariables(qm *NominalQueryModel) {
	if qm.TemplateVariables == nil {
		return
	}

	qm.AssetRid = interpolateTemplateVariables(qm.AssetRid, qm.TemplateVariables)
	qm.Channel = interpolateTemplateVariables(qm.Channel, qm.TemplateVariables)
	qm.DataScopeName = interpolateTemplateVariables(qm.DataScopeName, qm.TemplateVariables)
	qm.QueryText = interpolateTemplateVariables(qm.QueryText, qm.TemplateVariables)
}

// validateQuery validates query parameters similar to pure-ts implementation
func (d *Datasource) validateQuery(qm NominalQueryModel) error {
	// Check if we have either Nominal-specific fields or legacy fields
	hasNominalQuery := qm.AssetRid != "" && qm.Channel != ""
	hasLegacyQuery := qm.QueryText != ""
	hasConstantQuery := qm.Constant != 0

	if !hasNominalQuery && !hasLegacyQuery && !hasConstantQuery {
		return fmt.Errorf("query must have either asset/channel parameters, query text, or constant value")
	}

	// Validate Nominal query fields
	if hasNominalQuery {
		if strings.TrimSpace(qm.AssetRid) == "" {
			return fmt.Errorf("assetRid cannot be empty")
		}
		if strings.TrimSpace(qm.Channel) == "" {
			return fmt.Errorf("channel cannot be empty")
		}
		// DataScopeName is required — the compute API needs it to locate the channel.
		// The frontend filterQuery also enforces this; this is defense-in-depth.
		if strings.TrimSpace(qm.DataScopeName) == "" {
			return fmt.Errorf("dataScopeName is required for asset/channel queries")
		}
		// Validate bucket count
		if qm.Buckets < 0 {
			return fmt.Errorf("buckets must be non-negative, got %d", qm.Buckets)
		}
		if qm.Buckets > 10000 {
			log.DefaultLogger.Warn("Large bucket count may impact performance", "buckets", qm.Buckets)
		}
	}

	return nil
}

// inferChannelDataType verifies (or backfills) channelDataType against the
// actual channel metadata from the API. The frontend-supplied type may be stale
// when a multi-select template variable expands $channel to a mix of numeric
// and string channels — every expanded query inherits the same saved type.
// The instance-level cache keeps repeated lookups cheap.
func (d *Datasource) inferChannelDataType(ctx context.Context, config *models.PluginSettings, qm *NominalQueryModel) {
	if qm == nil || d.datasourceService == nil {
		return
	}
	if strings.TrimSpace(qm.AssetRid) == "" || strings.TrimSpace(qm.Channel) == "" || strings.TrimSpace(qm.DataScopeName) == "" {
		return
	}

	cacheKey := qm.AssetRid + "|" + qm.DataScopeName + "|" + qm.Channel

	// Check instance-level TTL cache.
	d.channelTypeCacheMu.Lock()
	if d.channelTypeCache == nil {
		d.channelTypeCache = make(map[string]channelTypeCacheEntry)
	}
	if entry, ok := d.channelTypeCache[cacheKey]; ok && time.Since(entry.fetchedAt) < assetCacheTTL {
		d.channelTypeCacheMu.Unlock()
		if entry.channelType != "" {
			qm.ChannelDataType = entry.channelType
		}
		return
	}
	d.channelTypeCacheMu.Unlock()

	asset, err := d.fetchAssetByRid(ctx, config, qm.AssetRid)
	if err != nil {
		log.DefaultLogger.Warn("Failed to fetch asset for channel type inference", "assetRid", qm.AssetRid, "error", err)
		return
	}
	if asset == nil {
		return
	}

	var dataSourceRids []rids.DataSourceRid
	for _, scope := range asset.DataScopes {
		if scope.DataScopeName != qm.DataScopeName {
			continue
		}

		var ridStr string
		switch scope.DataSource.Type {
		case "dataset":
			if scope.DataSource.Dataset != nil {
				ridStr = *scope.DataSource.Dataset
			}
		case "connection":
			if scope.DataSource.Connection != nil {
				ridStr = *scope.DataSource.Connection
			}
		}

		if ridStr == "" {
			continue
		}

		parsedRid, err := rid.ParseRID(ridStr)
		if err != nil {
			log.DefaultLogger.Warn("Failed to parse datasource RID for channel type inference", "rid", ridStr, "error", err)
			continue
		}
		dataSourceRids = append(dataSourceRids, rids.DataSourceRid(parsedRid))
	}
	if len(dataSourceRids) == 0 {
		return
	}

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)
	searchRequest := datasourceapi.SearchChannelsRequest{
		ExactMatch:  []string{qm.Channel},
		DataSources: dataSourceRids,
	}
	channelsResponse, err := d.datasourceService.SearchChannels(ctx, bearerToken, searchRequest)
	if err != nil {
		log.DefaultLogger.Warn("Failed to search channels for channel type inference", "assetRid", qm.AssetRid, "dataScopeName", qm.DataScopeName, "channel", qm.Channel, "error", err)
		return
	}

	for _, channel := range channelsResponse.Results {
		if string(channel.Name) != qm.Channel {
			continue
		}
		if inferredType := getChannelDataType(channel); inferredType != "" {
			qm.ChannelDataType = inferredType
			d.channelTypeCacheMu.Lock()
			d.channelTypeCache[cacheKey] = channelTypeCacheEntry{channelType: inferredType, fetchedAt: time.Now()}
			d.channelTypeCacheMu.Unlock()
			return
		}
	}

	// Cache the miss so we don't re-search for the same combo.
	d.channelTypeCacheMu.Lock()
	d.channelTypeCache[cacheKey] = channelTypeCacheEntry{channelType: "", fetchedAt: time.Now()}
	d.channelTypeCacheMu.Unlock()
}

// Datasource is the Nominal datasource implementation
type Datasource struct {
	settings          backend.DataSourceInstanceSettings
	authService       authapi.AuthenticationServiceV2Client
	computeService    computeapi1.ComputeServiceClient
	datasourceService datasourceservice.DataSourceServiceClient

	// assetCache caches fetchAssetByRid results with a TTL to avoid
	// redundant HTTP calls across queries and dashboard refreshes.
	assetCacheMu sync.Mutex
	assetCache   map[string]assetCacheEntry

	// channelTypeCache caches SearchChannels inference results with a TTL to avoid
	// redundant HTTP calls across queries and dashboard refreshes.
	channelTypeCacheMu sync.Mutex
	channelTypeCache   map[string]channelTypeCacheEntry

	resourceHTTPClient *http.Client
}

func (d *Datasource) getResourceHTTPClient() *http.Client {
	return d.resourceHTTPClient
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	if d.resourceHTTPClient != nil {
		d.resourceHTTPClient.CloseIdleConnections()
	}
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
//
// This implementation batches eligible queries (asset+channel) into a single API call for performance.
// Non-batchable queries (connectionTest, legacy) are handled individually.
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	// Check if DataSourceInstanceSettings is available
	if req.PluginContext.DataSourceInstanceSettings == nil {
		for _, q := range req.Queries {
			response.Responses[q.RefID] = backend.ErrDataResponse(
				backend.StatusBadRequest,
				"DataSource not configured",
			)
		}
		return response, nil
	}

	// Load config once for all queries
	config, err := models.LoadPluginSettings(*req.PluginContext.DataSourceInstanceSettings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load plugin settings", "error", err)
		for _, q := range req.Queries {
			response.Responses[q.RefID] = backend.ErrDataResponse(
				backend.StatusInternal,
				fmt.Sprintf("Failed to load settings: %v", err),
			)
		}
		return response, nil
	}

	// Collect batchable queries
	var batchableQueries []backend.DataQuery
	var batchableModels []NominalQueryModel

	for _, q := range req.Queries {
		// Parse query model
		var qm NominalQueryModel
		if err := json.Unmarshal(q.JSON, &qm); err != nil {
			response.Responses[q.RefID] = backend.ErrDataResponse(
				backend.StatusBadRequest,
				fmt.Sprintf("json unmarshal: %v", err),
			)
			continue
		}

		// Apply template variable interpolation
		d.applyTemplateVariables(&qm)

		// Handle connection test immediately (not batchable)
		if qm.QueryType == "connectionTest" {
			response.Responses[q.RefID] = d.handleConnectionTestQuery(ctx, config)
			continue
		}

		// Validate query
		if err := d.validateQuery(qm); err != nil {
			log.DefaultLogger.Error("Query validation failed", "error", err)
			response.Responses[q.RefID] = backend.ErrDataResponse(
				backend.StatusBadRequest,
				fmt.Sprintf("Query validation failed: %v", err),
			)
			continue
		}

		d.inferChannelDataType(ctx, config, &qm)

		// Default aggregations to ["MEAN"] for backward compat with saved dashboards.
		// For enum channels, aggregations are not used — skip validation.
		qm.ExplicitAggregations = len(qm.Aggregations) > 0
		if qm.ChannelDataType != "string" {
			if !qm.ExplicitAggregations {
				qm.Aggregations = []string{"MEAN"}
			} else if deduped, badAgg := validateAndDedup(qm.Aggregations); badAgg != "" {
				response.Responses[q.RefID] = backend.ErrDataResponse(
					backend.StatusBadRequest,
					fmt.Sprintf("unsupported aggregation %q; valid options are MEAN, MIN, MAX", badAgg),
				)
				continue
			} else {
				qm.Aggregations = deduped
			}
		}

		// Check if this is a batchable query (has asset and channel)
		if qm.AssetRid != "" && qm.Channel != "" {
			batchableQueries = append(batchableQueries, q)
			batchableModels = append(batchableModels, qm)
		} else {
			// Legacy query - handle individually
			response.Responses[q.RefID] = d.handleLegacyQuery(qm, q.TimeRange)
		}
	}

	// Execute batch query for all batchable queries
	if len(batchableQueries) > 0 {
		log.DefaultLogger.Debug("Executing batch query", "count", len(batchableQueries))
		batchResults := d.executeBatchQuery(ctx, config, batchableQueries, batchableModels)
		for refID, res := range batchResults {
			response.Responses[refID] = res
		}
	}

	return response, nil
}

// handleConnectionTestQuery handles the connectionTest query type
func (d *Datasource) handleConnectionTestQuery(ctx context.Context, config *models.PluginSettings) backend.DataResponse {
	var response backend.DataResponse

	log.DefaultLogger.Debug("Processing connectionTest query")

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)
	profile, err := d.authService.GetMyProfile(ctx, bearerToken)
	if err != nil {
		log.DefaultLogger.Error("Connection test failed", "error", err)
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("Connection test failed: %v", err))
	}

	log.DefaultLogger.Debug("Connection test successful", "profileRid", profile.Rid)

	frame := data.NewFrame("connectionTest")
	frame.Fields = append(frame.Fields,
		data.NewField("status", nil, []string{"success"}),
		data.NewField("message", nil, []string{"Successfully connected to Nominal API"}),
	)

	response.Frames = append(response.Frames, frame)
	return response
}

// handleLegacyQuery handles legacy queries that don't have asset/channel
func (d *Datasource) handleLegacyQuery(qm NominalQueryModel, timeRange backend.TimeRange) backend.DataResponse {
	var response backend.DataResponse

	log.DefaultLogger.Debug("Using legacy query support")

	frame := data.NewFrame("response")
	frame.Fields = append(frame.Fields,
		data.NewField("time", nil, []time.Time{timeRange.From, timeRange.To}),
		data.NewField("values", nil, []float64{qm.Constant, qm.Constant + 10}),
	)

	response.Frames = append(response.Frames, frame)
	return response
}

// NominalQueryModel represents a query to the Nominal API
type NominalQueryModel struct {
	// Asset information
	AssetRid        string `json:"assetRid"`
	Channel         string `json:"channel"`
	DataScopeName   string `json:"dataScopeName"`
	ChannelDataType string `json:"channelDataType"`

	// Aggregation functions for numeric channels (e.g. "MEAN", "MIN", "MAX").
	// Empty/missing defaults to ["MEAN"]. Ignored for enum channels.
	Aggregations            []string `json:"aggregations,omitempty"`
	ExplicitAggregations    bool     `json:"-"` // true when aggregations were set by the frontend (not defaulted)

	// Query parameters
	Buckets   int    `json:"buckets"`
	QueryType string `json:"queryType"`

	// Template variables support
	TemplateVariables map[string]interface{} `json:"templateVariables,omitempty"`

	// Legacy support
	QueryText string  `json:"queryText"`
	Constant  float64 `json:"constant"`
}

// validateAndDedup returns the deduplicated aggregation list and the first invalid name (or "").
func validateAndDedup(aggs []string) ([]string, string) {
	seen := make(map[string]bool, len(aggs))
	deduped := make([]string, 0, len(aggs))
	for _, a := range aggs {
		switch a {
		case "MEAN", "MIN", "MAX":
			if !seen[a] {
				seen[a] = true
				deduped = append(deduped, a)
			}
		default:
			return nil, a
		}
	}
	return deduped, ""
}

// buildComputeRequest constructs a ComputeNodeRequest from query model and time range.
// This is extracted to enable reuse for both single and batch compute calls.
// Branches on ChannelDataType: "string" channels produce enum series, all others produce numeric series.
// maxDataPoints from Grafana reflects the panel's pixel width; when positive it overrides qm.Buckets
// so the compute API only returns as many points as the panel can actually display.
func (d *Datasource) buildComputeRequest(qm NominalQueryModel, timeRange backend.TimeRange, maxDataPoints int64) computeapi1.ComputeNodeRequest {
	startSeconds := timeRange.From.Unix()
	endSeconds := timeRange.To.Unix()

	// Build the series node - branch on channel data type
	var series computeapi1.Series
	if qm.ChannelDataType == "string" {
		// Enum path for string channels
		enumTimeShiftSeries := computeapi1.EnumTimeShiftSeries{
			Input: d.buildEnumChannelSeries(qm.AssetRid, qm.Channel, qm.DataScopeName),
			Duration: computeapi1.NewDurationConstantFromLiteral(runapi.Duration{
				Seconds: safelong.SafeLong(0),
				Nanos:   safelong.SafeLong(0),
				Picos:   nil,
			}),
		}
		enumSeries := computeapi1.NewEnumSeriesFromTimeShift(enumTimeShiftSeries)
		series = computeapi1.NewSeriesFromEnum(enumSeries)
	} else {
		// Numeric path for numeric channels (default for empty/unknown ChannelDataType)
		numericTimeShiftSeries := computeapi1.NumericTimeShiftSeries{
			Input: d.buildChannelSeries(qm.AssetRid, qm.Channel, qm.DataScopeName),
			Duration: computeapi1.NewDurationConstantFromLiteral(runapi.Duration{
				Seconds: safelong.SafeLong(0),
				Nanos:   safelong.SafeLong(0),
				Picos:   nil,
			}),
		}
		numericSeries := computeapi1.NewNumericSeriesFromTimeShift(numericTimeShiftSeries)
		series = computeapi1.NewSeriesFromNumeric(numericSeries)
	}

	buckets := int(qm.Buckets)
	if maxDataPoints > 0 && (buckets <= 0 || int(maxDataPoints) < buckets) {
		buckets = int(maxDataPoints)
	}
	var seriesNode computeapi1.SummarizeSeries
	if qm.ChannelDataType == "string" {
		// Enum path: no OutputFormat or NumericOutputFields
		seriesNode = computeapi1.SummarizeSeries{
			Input:   series,
			Buckets: &buckets,
		}
	} else {
		// Numeric path: Arrow format with user-selected aggregation fields.
		arrowFormat := computeapi.New_OutputFormat(computeapi.OutputFormat_ARROW_V3)
		var outputFields []computeapi.NumericOutputField
		for _, agg := range qm.Aggregations {
			outputFields = append(outputFields, computeapi.New_NumericOutputField(
				computeapi.NumericOutputField_Value(agg),
			))
		}
		seriesNode = computeapi1.SummarizeSeries{
			Input:               series,
			Buckets:             &buckets,
			OutputFormat:        &arrowFormat,
			NumericOutputFields: &outputFields,
		}
	}

	// Create computable node
	node := computeapi1.NewComputableNodeFromSeries(seriesNode)

	// Build context with variables
	computeContext := d.buildComputeContext(qm, startSeconds, endSeconds)

	return computeapi1.ComputeNodeRequest{
		Start: api.Timestamp{
			Seconds: safelong.SafeLong(startSeconds),
			Nanos:   safelong.SafeLong(0),
			Picos:   nil,
		},
		End: api.Timestamp{
			Seconds: safelong.SafeLong(endSeconds),
			Nanos:   safelong.SafeLong(0),
			Picos:   nil,
		},
		Node:    node,
		Context: computeContext,
	}
}

// executeBatchQuery executes multiple queries in a single batch API call.
// Returns a map of RefID to DataResponse for each query.
func (d *Datasource) executeBatchQuery(
	ctx context.Context,
	config *models.PluginSettings,
	queries []backend.DataQuery,
	queryModels []NominalQueryModel,
) map[string]backend.DataResponse {
	results := make(map[string]backend.DataResponse)
	bearerToken := bearertoken.Token(config.Secrets.ApiKey)

	if len(queries) != len(queryModels) {
		for _, q := range queries {
			results[q.RefID] = backend.ErrDataResponse(
				backend.StatusInternal,
				"Batch query internal error: query/model count mismatch",
			)
		}
		return results
	}

	for chunkStart := 0; chunkStart < len(queries); chunkStart += maxBatchComputeSubrequests {
		chunkEnd := chunkStart + maxBatchComputeSubrequests
		if chunkEnd > len(queries) {
			chunkEnd = len(queries)
		}

		chunkQueries := queries[chunkStart:chunkEnd]
		chunkModels := queryModels[chunkStart:chunkEnd]
		computeRequests := make([]computeapi1.ComputeNodeRequest, len(chunkModels))
		for i, qm := range chunkModels {
			computeRequests[i] = d.buildComputeRequest(qm, chunkQueries[i].TimeRange, chunkQueries[i].MaxDataPoints)
		}

		batchRequest := computeapi1.BatchComputeWithUnitsRequest{
			Requests: computeRequests,
		}

		log.DefaultLogger.Debug(
			"Making batch compute API call",
			"chunkStart", chunkStart,
			"chunkEnd", chunkEnd,
			"queryCount", len(computeRequests),
		)

		batchResponse, err := d.computeService.BatchComputeWithUnits(ctx, bearerToken, batchRequest)
		if err != nil {
			log.DefaultLogger.Error("Batch compute API call failed", "error", err, "chunkStart", chunkStart, "chunkEnd", chunkEnd)
			for _, q := range chunkQueries {
				results[q.RefID] = backend.ErrDataResponse(
					backend.StatusInternal,
					fmt.Sprintf("Batch compute failed: %v", err),
				)
			}
			continue
		}

		log.DefaultLogger.Debug(
			"Batch compute successful",
			"chunkStart", chunkStart,
			"chunkEnd", chunkEnd,
			"resultCount", len(batchResponse.Results),
		)

		for i, q := range chunkQueries {
			if i >= len(batchResponse.Results) {
				results[q.RefID] = backend.ErrDataResponse(
					backend.StatusInternal,
					"Missing result in batch response",
				)
				continue
			}

			results[q.RefID] = d.transformBatchResult(batchResponse.Results[i], chunkModels[i])
		}
	}

	return results
}

// transformBatchResult converts a single batch result to a Grafana DataResponse.
// Handles both success and error cases from the ComputeNodeResult union type.
func (d *Datasource) transformBatchResult(result computeapi.ComputeWithUnitsResult, qm NominalQueryModel) backend.DataResponse {
	var response backend.DataResponse

	// ComputeNodeResult is a union type - use AcceptFuncs to handle success/error
	err := result.ComputeResult.AcceptFuncs(
		// successFunc - called when compute succeeded
		func(computeResponse computeapi.ComputeNodeResponse) error {
			result, transformErr := d.transformNominalResponseFromClient(computeResponse, qm)
			if transformErr != nil {
				response = backend.ErrDataResponse(
					backend.StatusInternal,
					fmt.Sprintf("Transform failed: %v", transformErr),
				)
				return nil
			}

			if len(result.AggSeries) > 0 {
				// Multi-aggregation Arrow path: one frame per series
				for _, agg := range result.AggSeries {
					frame := data.NewFrame("response")
					displayName := qm.Channel
					if qm.ExplicitAggregations {
						displayName = fmt.Sprintf("%s (%s)", qm.Channel, agg.Name)
					}
					frame.Name = displayName
					if len(agg.TimePoints) > 0 && len(agg.Values) > 0 {
						valueField := data.NewField("value", nil, agg.Values)
						valueField.Config = &data.FieldConfig{DisplayNameFromDS: displayName}
						frame.Fields = append(frame.Fields,
							data.NewField("time", nil, agg.TimePoints),
							valueField,
						)
					} else {
						valueField := data.NewField("value", nil, []*float64{})
						valueField.Config = &data.FieldConfig{DisplayNameFromDS: displayName}
						frame.Fields = append(frame.Fields,
							data.NewField("time", nil, []time.Time{}),
							valueField,
						)
					}
					response.Frames = append(response.Frames, frame)
				}
				log.DefaultLogger.Debug("Successfully processed multi-agg query",
					"series", len(result.AggSeries),
					"dataPoints", func() int {
						if len(result.AggSeries) > 0 {
							return len(result.AggSeries[0].TimePoints)
						}
						return 0
					}())
			} else if result.IsEnum {
				frame := data.NewFrame("response")
				frame.Name = qm.Channel
				// Mark enum frames as table type so panels like Stat can pick up string fields.
				// Time series frames filter to numeric fields only by default.
				frame.Meta = &data.FrameMeta{
					Type:                   data.FrameTypeTable,
					PreferredVisualization: data.VisTypeTable,
				}
				if len(result.TimePoints) > 0 && len(result.StringValues) > 0 {
					valueField := data.NewField("value", nil, result.StringValues)
					valueField.Config = &data.FieldConfig{DisplayNameFromDS: qm.Channel}
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, result.TimePoints),
						valueField,
					)
				} else {
					valueField := data.NewField("value", nil, []string{})
					valueField.Config = &data.FieldConfig{DisplayNameFromDS: qm.Channel}
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, []time.Time{}),
						valueField,
					)
				}
				log.DefaultLogger.Debug("Successfully processed enum query", "dataPoints", len(result.TimePoints))
				response.Frames = append(response.Frames, frame)
			} else {
				// Legacy numeric path (BucketedNumericPlot, NumericPlot)
				frame := data.NewFrame("response")
				frame.Name = qm.Channel
				if len(result.TimePoints) > 0 && len(result.NumericValues) > 0 {
					valueField := data.NewField("value", nil, result.NumericValues)
					valueField.Config = &data.FieldConfig{DisplayNameFromDS: qm.Channel}
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, result.TimePoints),
						valueField,
					)
				} else {
					valueField := data.NewField("value", nil, []*float64{})
					valueField.Config = &data.FieldConfig{DisplayNameFromDS: qm.Channel}
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, []time.Time{}),
						valueField,
					)
				}
				log.DefaultLogger.Debug("Successfully processed query", "dataPoints", len(result.TimePoints))
				response.Frames = append(response.Frames, frame)
			}

			return nil
		},
		// errorFunc - called when compute failed
		func(errorResult computeapi.ErrorResult) error {
			errMsg := fmt.Sprintf("Compute error: %v (code: %v)", errorResult.ErrorType, errorResult.Code)

			// Add a hint for any ChannelHasWrongType error: the query's type setting
			// and the channel's actual type don't agree. Re-selecting the channel in
			// the editor refreshes the stored type metadata.
			errType := string(errorResult.ErrorType)
			if strings.Contains(errType, "ChannelHasWrongType") {
				errMsg += ". Hint: The channel type in this query doesn't match what the API expects. " +
					"Re-select the channel from the dropdown to update its type."
			}

			response = backend.ErrDataResponse(
				backend.StatusInternal,
				errMsg,
			)
			return nil
		},
		// unknownFunc - called for unknown union variants
		func(typeName string) error {
			response = backend.ErrDataResponse(
				backend.StatusInternal,
				fmt.Sprintf("Unknown result type: %s", typeName),
			)
			return nil
		},
	)

	if err != nil {
		return backend.ErrDataResponse(
			backend.StatusInternal,
			fmt.Sprintf("Failed to process result: %v", err),
		)
	}

	return response
}

// buildAssetChannel constructs the shared AssetChannel used by both numeric and enum series builders.
func (d *Datasource) buildAssetChannel(assetRid, channel, dataScopeName string) computeapi.AssetChannel {
	return computeapi.AssetChannel{
		AssetRid:       computeapi.NewStringConstantFromVariable(computeapi.VariableName("assetRid")),
		Channel:        computeapi.NewStringConstantFromLiteral(channel),
		DataScopeName:  computeapi.NewStringConstantFromLiteral(dataScopeName),
		AdditionalTags: map[string]computeapi.StringConstant{},
		TagsToGroupBy:  []string{},
		GroupByTags:    []computeapi.StringConstant{},
	}
}

// buildChannelSeries creates a numeric channel series for the given asset/channel.
func (d *Datasource) buildChannelSeries(assetRid, channel, dataScopeName string) computeapi1.NumericSeries {
	channelSeries := computeapi.NewChannelSeriesFromAsset(d.buildAssetChannel(assetRid, channel, dataScopeName))
	return computeapi1.NewNumericSeriesFromChannel(channelSeries)
}

// buildEnumChannelSeries creates an enum channel series for the given asset/channel.
func (d *Datasource) buildEnumChannelSeries(assetRid, channel, dataScopeName string) computeapi1.EnumSeries {
	channelSeries := computeapi.NewChannelSeriesFromAsset(d.buildAssetChannel(assetRid, channel, dataScopeName))
	return computeapi1.NewEnumSeriesFromChannel(channelSeries)
}

// buildComputeContext creates the context with variables for the compute request
func (d *Datasource) buildComputeContext(qm NominalQueryModel, startSeconds, endSeconds int64) computeapi1.Context {
	variables := map[computeapi.VariableName]computeapi1.VariableValue{
		computeapi.VariableName("assetRid"): computeapi1.NewVariableValueFromString(qm.AssetRid),
	}

	// Add template variables if present
	if qm.TemplateVariables != nil {
		for key, value := range qm.TemplateVariables {
			if strValue, ok := value.(string); ok {
				variables[computeapi.VariableName(key)] = computeapi1.NewVariableValueFromString(strValue)
			}
		}
	}

	return computeapi1.Context{
		Variables:         variables,
		FunctionVariables: nil,
	}
}

// TransformResult holds the output of response transformation.
// Either NumericValues or StringValues is populated, never both.
// IsEnum indicates which field to use when building data frames.
// AggregationSeries holds one aggregation's worth of data (e.g. "mean", "min").
// Each series carries its own timestamps to support FIRST_POINT/LAST_POINT
// in a future version, where the event timestamps differ from bucket boundaries.
type AggregationSeries struct {
	Name       string      // Arrow column name: "mean", "min", "max"
	TimePoints []time.Time
	Values     []*float64
}

type TransformResult struct {
	// Numeric aggregation series (Arrow bucketed path, one entry per requested field)
	AggSeries []AggregationSeries

	// Legacy numeric path (non-Arrow) — single series only
	TimePoints    []time.Time
	NumericValues []*float64

	// Enum path
	StringValues []string
	IsEnum       bool
}

// transformNominalResponseFromClient converts conjure client response to Grafana time series data.
// qm is needed so the Arrow bucketed handler knows which aggregation columns to extract.
func (d *Datasource) transformNominalResponseFromClient(response computeapi.ComputeNodeResponse, qm NominalQueryModel) (TransformResult, error) {
	log.DefaultLogger.Debug("Transforming conjure client response")

	var result TransformResult

	// Use the conjure union visitor pattern to handle different response types
	visitErr := response.AcceptFuncs(
		nil, // rangeFunc
		nil, // rangesSummaryFunc
		nil, // rangeValueFunc
		func(numeric computeapi.NumericPlot) error {
			timePoints, values, err := d.extractNumericDataFromConjure(numeric)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.NumericValues = values
			result.IsEnum = false
			return nil
		},
		func(bucketed computeapi.BucketedNumericPlot) error {
			timePoints, values, err := d.extractBucketedDataFromConjure(bucketed)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.NumericValues = values
			result.IsEnum = false
			return nil
		},
		nil, // numericPointFunc
		nil, // singlePointFunc
		// arrowNumericFunc - Not reachable from SummarizeSeries with Buckets.
		// Returns a clear error rather than speculative parsing of an unverified schema.
		func(arrowNumeric computeapi.ArrowNumericPlot) error {
			return fmt.Errorf("received ArrowNumericPlot unexpectedly; " +
				"this response type is not supported by the plugin")
		},
		// arrowBucketedNumericFunc - Arrow format bucketed numeric response.
		// Extracts one AggregationSeries per requested aggregation field.
		func(arrowBucketed computeapi.ArrowBucketedNumericPlot) error {
			var colNames []string
			for _, agg := range qm.Aggregations {
				colNames = append(colNames, strings.ToLower(agg))
			}
			if len(colNames) == 0 {
				return fmt.Errorf("no aggregation fields requested for ArrowBucketedNumericPlot response")
			}
			series, err := d.extractArrowBucketedNumericSeries(arrowBucketed, colNames)
			if err != nil {
				return err
			}
			result.AggSeries = series
			result.IsEnum = false
			return nil
		},
		// enumFunc - maps integer indices to category strings
		func(enum computeapi.EnumPlot) error {
			timePoints, values, err := d.extractEnumDataFromConjure(enum)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.StringValues = values
			result.IsEnum = true
			return nil
		},
		// enumPointFunc - single-point enum response (value is already a resolved string)
		func(ep *computeapi.EnumPoint) error {
			if ep == nil {
				return nil
			}
			seconds := int64(ep.Timestamp.Seconds)
			nanos := int64(ep.Timestamp.Nanos)
			result.TimePoints = []time.Time{time.Unix(seconds, nanos)}
			result.StringValues = []string{ep.Value}
			result.IsEnum = true
			return nil
		},
		// bucketedEnumFunc - bucketed enum response (returned by SummarizeSeries with buckets)
		func(bucketed computeapi.BucketedEnumPlot) error {
			timePoints, values, err := d.extractBucketedEnumDataFromConjure(bucketed)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.StringValues = values
			result.IsEnum = true
			return nil
		},
		nil, // arrowEnumFunc
		nil, // arrowBucketedEnumFunc
		nil, // pagedLogFunc
		nil, // logPointFunc
		nil, // cartesianFunc
		nil, // bucketedCartesianFunc
		nil, // bucketedCartesian3dFunc
		nil, // frequencyDomainFunc
		nil, // frequencyDomainV2Func
		nil, // bucketedFrequencyDomainFunc
		nil, // numericHistogramFunc
		nil, // enumHistogramFunc
		nil, // curveFitFunc
		nil, // groupedFunc
		nil, // arrowArrayFunc
		nil, // arrowBucketedStructFunc
		nil, // arrowFullResolutionFunc
		func(typeName string) error {
			log.DefaultLogger.Debug("Unhandled response type", "type", typeName)
			return nil
		},
	)

	if visitErr != nil {
		return TransformResult{}, fmt.Errorf("failed to process response: %w", visitErr)
	}

	return result, nil
}

// Helper methods for extracting data from conjure types
func (d *Datasource) extractNumericDataFromConjure(numeric computeapi.NumericPlot) ([]time.Time, []*float64, error) {
	var timePoints []time.Time
	var values []*float64

	// Access the fields directly from the conjure struct
	for i := 0; i < len(numeric.Timestamps) && i < len(numeric.Values); i++ {
		timestamp := numeric.Timestamps[i]
		value := numeric.Values[i]

		// Convert conjure timestamp to Go time
		// SafeLong values need to be cast to int64
		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))
		values = append(values, &value)
	}

	log.DefaultLogger.Debug("Extracted numeric data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

func (d *Datasource) extractBucketedDataFromConjure(bucketed computeapi.BucketedNumericPlot) ([]time.Time, []*float64, error) {
	var timePoints []time.Time
	var values []*float64

	// Access the fields directly from the conjure struct
	for i := 0; i < len(bucketed.Timestamps) && i < len(bucketed.Buckets); i++ {
		timestamp := bucketed.Timestamps[i]
		bucket := bucketed.Buckets[i]

		// Convert conjure timestamp to Go time
		// SafeLong values need to be cast to int64
		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))

		// Use mean value from bucket (it's a direct float64, not pointer)
		mean := bucket.Mean
		values = append(values, &mean)
	}

	log.DefaultLogger.Debug("Extracted bucketed data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

// extractEnumDataFromConjure converts an EnumPlot response to time/string slices.
// Maps integer indices to category strings with bounds checking.
// Out-of-bounds indices produce "unknown(N)" rather than panicking.
func (d *Datasource) extractEnumDataFromConjure(enumPlot computeapi.EnumPlot) ([]time.Time, []string, error) {
	var timePoints []time.Time
	var values []string

	for i := 0; i < len(enumPlot.Timestamps) && i < len(enumPlot.Values); i++ {
		timestamp := enumPlot.Timestamps[i]
		index := enumPlot.Values[i]

		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))

		// Map index to category string with bounds checking
		if index >= 0 && index < len(enumPlot.Categories) {
			values = append(values, enumPlot.Categories[index])
		} else {
			values = append(values, fmt.Sprintf("unknown(%d)", index))
			log.DefaultLogger.Warn("Enum index out of bounds",
				"index", index,
				"categoriesLen", len(enumPlot.Categories),
			)
		}
	}

	log.DefaultLogger.Debug("Extracted enum data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

// extractBucketedEnumDataFromConjure converts a BucketedEnumPlot response to time/string slices.
// Uses the histogram mode (most frequent category) as the representative value for each bucket,
// which is the categorical equivalent of the numeric path's Mean aggregate.
func (d *Datasource) extractBucketedEnumDataFromConjure(bucketed computeapi.BucketedEnumPlot) ([]time.Time, []string, error) {
	var timePoints []time.Time
	var values []string

	for i := 0; i < len(bucketed.Timestamps) && i < len(bucketed.Buckets); i++ {
		timestamp := bucketed.Timestamps[i]
		bucket := bucketed.Buckets[i]

		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))

		// Find the mode (most frequent value) from the histogram.
		// Falls back to FirstPoint if histogram is empty.
		modeIndex := bucket.FirstPoint.Value
		maxCount := safelong.SafeLong(0)
		for idx, count := range bucket.Histogram {
			if count > maxCount {
				maxCount = count
				modeIndex = idx
			}
		}

		if modeIndex >= 0 && modeIndex < len(bucketed.Categories) {
			values = append(values, bucketed.Categories[modeIndex])
		} else {
			values = append(values, fmt.Sprintf("unknown(%d)", modeIndex))
			log.DefaultLogger.Warn("Bucketed enum index out of bounds",
				"index", modeIndex,
				"categoriesLen", len(bucketed.Categories),
			)
		}
	}

	log.DefaultLogger.Debug("Extracted bucketed enum data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

// CheckHealth handles health checks sent from Grafana to the plugin.
func (d *Datasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	log.DefaultLogger.Debug("CheckHealth called")

	// Add timeout to prevent hanging
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	config, err := models.LoadPluginSettings(*req.PluginContext.DataSourceInstanceSettings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load plugin settings", "error", err)
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Unable to load settings",
		}, nil
	}

	// Validate required configuration - fail fast for missing config
	if config.BaseUrl == "" && config.Path == "" {
		log.DefaultLogger.Debug("Health check failed: missing base URL")
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Base URL is required",
		}, nil
	}

	if config.Secrets.ApiKey == "" {
		log.DefaultLogger.Debug("Health check failed: missing API key")
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "API key is required",
		}, nil
	}

	// Test connection using generated client with timeout
	log.DefaultLogger.Debug("Testing connection using nominal-api-go client")

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)
	profile, err := d.authService.GetMyProfile(ctxWithTimeout, bearerToken)
	if err != nil {
		log.DefaultLogger.Error("Health check failed", "error", err)
		// Return a more specific error message based on the error type
		errorMsg := "Failed to connect to Nominal API"
		if strings.Contains(err.Error(), "401") || strings.Contains(err.Error(), "unauthorized") {
			errorMsg = "Invalid API key - authentication failed"
		} else if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "context deadline exceeded") {
			errorMsg = "Connection timeout - unable to reach Nominal API"
		} else if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no such host") {
			errorMsg = "Unable to connect to Nominal API - check base URL"
		}

		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: errorMsg,
		}, nil
	}

	log.DefaultLogger.Debug("Health check successful", "user", profile.DisplayName)
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Successfully connected to Nominal API",
	}, nil
}

// CallResource handles HTTP requests sent to the plugin
// This handles all proxy requests from /api/datasources/proxy/uid/{uid}/...
func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	// Debug logging to see what requests are coming in
	log.DefaultLogger.Debug("=== CallResource called ===")
	log.DefaultLogger.Debug("CallResource called", "path", req.Path, "method", req.Method, "url", req.URL)

	// Handle test endpoint for frontend connection testing
	if req.Path == "test" || req.Path == "/test" {
		log.DefaultLogger.Debug("Handling test connection request")
		return d.handleTestConnection(ctx, req, sender)
	}

	// Handle alternative test endpoint
	if req.Path == "connection-test" {
		log.DefaultLogger.Debug("Handling alternative test connection request")
		return d.handleTestConnection(ctx, req, sender)
	}

	// Handle channels search endpoint
	if req.Path == "channels" || req.Path == "/channels" {
		log.DefaultLogger.Debug("Handling channels search request")
		return d.handleChannelsSearch(ctx, req, sender)
	}

	// Handle assets variable endpoint for Grafana template variables
	if req.Path == "assets" || req.Path == "/assets" {
		log.DefaultLogger.Debug("Handling assets variable request")
		return d.handleAssetsVariable(ctx, req, sender)
	}

	// Handle datascopes variable endpoint for Grafana template variables
	if req.Path == "datascopes" || req.Path == "/datascopes" {
		return d.handleDatascopesVariable(ctx, req, sender)
	}

	// Handle channel variables endpoint for Grafana template variables
	if req.Path == "channelvariables" || req.Path == "/channelvariables" {
		return d.handleChannelVariables(ctx, req, sender)
	}

	// Handle requests with /nominal prefix - strip it for API calls
	if strings.HasPrefix(req.Path, "nominal/") {
		// Remove the /nominal prefix for the actual API call
		req.Path = strings.TrimPrefix(req.Path, "nominal/")
		log.DefaultLogger.Debug("Stripped /nominal prefix", "newPath", req.Path)
	}

	// All other requests are proxied to Nominal API with authentication
	log.DefaultLogger.Debug("Handling proxy request to Nominal API")
	return d.handleNominalProxy(ctx, req, sender)
}

// handleTestConnection handles the test connection endpoint
func (d *Datasource) handleTestConnection(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	// Add timeout to prevent hanging
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Load settings to get API key and base URL
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Test connection: failed to load settings", "error", err)
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Failed to load settings"}`),
		})
	}

	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		log.DefaultLogger.Debug("Test connection: missing base URL")
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Base URL is required"}`),
		})
	}

	if config.Secrets.ApiKey == "" {
		log.DefaultLogger.Debug("Test connection: missing API key")
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "API key is required"}`),
		})
	}

	// Test connection using conjure client with timeout
	bearerToken := bearertoken.Token(config.Secrets.ApiKey)
	profile, err := d.authService.GetMyProfile(ctxWithTimeout, bearerToken)

	if err != nil {
		log.DefaultLogger.Error("Test connection failed", "error", err)
		// Return more specific error messages
		errorMsg := "Failed to connect to Nominal API"
		statusCode := http.StatusServiceUnavailable

		if strings.Contains(err.Error(), "401") || strings.Contains(err.Error(), "unauthorized") {
			errorMsg = "Invalid API key - authentication failed"
			statusCode = http.StatusUnauthorized
		} else if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "context deadline exceeded") {
			errorMsg = "Connection timeout - unable to reach Nominal API"
			statusCode = http.StatusRequestTimeout
		} else if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no such host") {
			errorMsg = "Unable to connect to Nominal API - check base URL"
			statusCode = http.StatusBadGateway
		}

		errBody, _ := json.Marshal(map[string]string{"error": errorMsg})
		return sender.Send(&backend.CallResourceResponse{
			Status: statusCode,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: errBody,
		})
	}

	log.DefaultLogger.Debug("Test connection successful", "profileRid", profile.Rid)

	// Connection successful
	response := map[string]interface{}{
		"status":  "success",
		"message": "Successfully connected to Nominal API and retrieved user profile",
	}
	responseBytes, _ := json.Marshal(response)
	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}

// handleChannelsSearch handles searching for channels in a data source
func (d *Datasource) handleChannelsSearch(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	log.DefaultLogger.Debug("Channels search request", "method", req.Method, "body", string(req.Body))

	if req.Method != "POST" {
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusMethodNotAllowed,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Method not allowed. Use POST."}`),
		})
	}

	// Parse request body
	var searchRequest struct {
		DataSourceRids []string `json:"dataSourceRids"`
		SearchText     string   `json:"searchText"`
	}

	if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
		log.DefaultLogger.Error("Failed to parse channels search request body", "error", err)
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Invalid request body"}`),
		})
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load settings for channels search", "error", err)
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusInternalServerError,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Failed to load settings"}`),
		})
	}

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)

	// Convert string RIDs to proper datasource RID types
	var dataSourceRids []rids.DataSourceRid
	for _, ridStr := range searchRequest.DataSourceRids {
		if parsedRid, err := rid.ParseRID(ridStr); err != nil {
			log.DefaultLogger.Warn("Failed to parse data source RID", "rid", ridStr, "error", err)
			continue
		} else {
			dataSourceRids = append(dataSourceRids, rids.DataSourceRid(parsedRid))
		}
	}

	if len(dataSourceRids) == 0 {
		log.DefaultLogger.Warn("No valid data source RIDs provided")
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "No valid data source RIDs provided"}`),
		})
	}

	// Build the search request with correct field names
	searchChannelsRequest := datasourceapi.SearchChannelsRequest{
		FuzzySearchText: searchRequest.SearchText,
		DataSources:     dataSourceRids,
	}

	log.DefaultLogger.Debug("Making channels search API call", "dataSourceCount", len(dataSourceRids), "searchText", searchRequest.SearchText)

	// Make the API call using the datasource service
	channelsResponse, err := d.datasourceService.SearchChannels(ctx, bearerToken, searchChannelsRequest)
	if err != nil {
		log.DefaultLogger.Error("Channels search API call failed", "error", err)
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusInternalServerError,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Channels search failed"}`),
		})
	}

	// Transform the API response to the expected format
	var channels []map[string]interface{}
	for _, channel := range channelsResponse.Results {
		channelMap := map[string]interface{}{
			"name":        string(channel.Name),
			"dataSource":  channel.DataSource.String(),
			"description": getChannelMetadataDescription(channel),
			"dataType":    getChannelDataType(channel),
		}
		channels = append(channels, channelMap)
	}

	apiResponse := map[string]interface{}{
		"channels": channels,
	}

	// Convert response to JSON
	responseBytes, err := json.Marshal(apiResponse)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal channels search response", "error", err)
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusInternalServerError,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Failed to marshal response"}`),
		})
	}

	log.DefaultLogger.Debug("Channels search successful", "channelCount", len(channels))

	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}

// getChannelMetadataDescription extracts description from channel metadata
func getChannelMetadataDescription(channel datasourceapi.ChannelMetadata) string {
	if channel.Description != nil {
		return *channel.Description
	}
	return fmt.Sprintf("Channel: %s", string(channel.Name))
}

// getChannelDataType normalizes the API's SeriesDataType to a binary string/numeric classification.
// Returns "string" for STRING and STRING_ARRAY types, "numeric" for all other known types,
// or empty string if the metadata is not available (treated as numeric for backward compatibility).
func getChannelDataType(channel datasourceapi.ChannelMetadata) string {
	if channel.DataType == nil {
		return ""
	}
	switch channel.DataType.Value() {
	case api.SeriesDataType_STRING, api.SeriesDataType_STRING_ARRAY:
		return "string"
	default:
		return "numeric"
	}
}

// handleNominalProxy handles proxying requests to Nominal API with secure API key injection
func (d *Datasource) handleNominalProxy(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	// Load settings to get API key and base URL
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		return fmt.Errorf("failed to load settings: %v", err)
	}

	baseURL := config.GetAPIBaseURL()
	if baseURL == "" || config.Secrets.ApiKey == "" {
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Missing base URL or API key configuration"}`),
		})
	}

	// The request path should be the API path (e.g., "api/compute/v2/compute")
	targetPath := req.Path

	// Construct the full target URL
	baseURL = strings.TrimSuffix(baseURL, "/")
	targetURL := baseURL + "/" + targetPath

	log.DefaultLogger.Debug("Proxy request", "fromPath", req.Path, "toURL", targetURL)

	// Parse the target URL to ensure it's valid
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return fmt.Errorf("invalid target URL: %v", err)
	}

	// Create the proxied request
	var body io.Reader
	if req.Body != nil {
		body = bytes.NewReader(req.Body)
	}

	proxyReq, err := http.NewRequestWithContext(ctx, req.Method, parsedURL.String(), body)
	if err != nil {
		return fmt.Errorf("failed to create proxy request: %v", err)
	}

	// Set the Host header explicitly - only if we have a valid host
	if parsedURL.Host != "" {
		proxyReq.Host = parsedURL.Host
	}

	// Forward only the small allowlist of headers the upstream needs.
	for key, values := range req.Headers {
		if !proxyAllowedHeaders[http.CanonicalHeaderKey(key)] {
			continue
		}
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// Use the datasource API key for all proxied upstream requests.
	proxyReq.Header.Set("Authorization", "Bearer "+config.Secrets.ApiKey)

	proxyReq.Header.Set("User-Agent", "grafana-nominal-plugin/1.0.0")
	log.DefaultLogger.Debug("Using API key for proxy request")

	// Ensure Content-Type is set for POST requests
	if req.Method == "POST" && proxyReq.Header.Get("Content-Type") == "" {
		proxyReq.Header.Set("Content-Type", "application/json")
	}

	// Make the request
	resp, err := d.getResourceHTTPClient().Do(proxyReq)
	if err != nil {
		return fmt.Errorf("proxy request failed: %v", err)
	}
	defer resp.Body.Close()

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	// Copy response headers
	responseHeaders := make(map[string][]string)
	for key, values := range resp.Header {
		responseHeaders[key] = values
	}

	// Send the proxied response
	return sender.Send(&backend.CallResourceResponse{
		Status:  resp.StatusCode,
		Headers: responseHeaders,
		Body:    responseBody,
	})
}

// handleAssetsVariable handles the assets endpoint for Grafana template variables
// Returns a list of assets in MetricFindValue format: { text: "Asset Name", value: "ri.scout..." }
func (d *Datasource) handleAssetsVariable(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if req.Method != "POST" {
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusMethodNotAllowed,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte(`{"error": "Method not allowed. Use POST."}`),
		})
	}

	log.DefaultLogger.Debug("Assets variable request")

	// Parse optional request body for search/filter parameters
	var searchRequest struct {
		SearchText string `json:"searchText"`
		MaxResults int    `json:"maxResults"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Error("Failed to parse assets variable request body", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Invalid request body"})
			return sender.Send(&backend.CallResourceResponse{
				Status:  http.StatusBadRequest,
				Headers: map[string][]string{"Content-Type": {"application/json"}},
				Body:    errBody,
			})
		}
	}

	// Set defaults
	if searchRequest.MaxResults == 0 {
		searchRequest.MaxResults = 500
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load settings for assets variable", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Fetch assets with pagination
	assetResponses, err := d.fetchAssetsForVariable(ctx, config, searchRequest.SearchText, searchRequest.MaxResults)
	if err != nil {
		log.DefaultLogger.Error("Failed to fetch assets", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to fetch assets"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Transform to MetricFindValue format: { text: "name", value: "rid" }
	// Filter to assets with supported data sources (dataset or connection)
	result := make([]map[string]string, 0)
outer:
	for _, resp := range assetResponses {
		for _, asset := range resp.Results {
			hasSupported := false
			for _, scope := range asset.DataScopes {
				if scope.DataSource.Type == "dataset" || scope.DataSource.Type == "connection" {
					hasSupported = true
					break
				}
			}
			if hasSupported {
				result = append(result, map[string]string{
					"text":  asset.Title,
					"value": asset.Rid,
				})
				if len(result) >= searchRequest.MaxResults {
					break outer
				}
			}
		}
	}

	responseBytes, err := json.Marshal(result)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal assets variable response", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	log.DefaultLogger.Debug("Assets variable request successful", "assetCount", len(result))

	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}

// handleDatascopesVariable handles the datascopes endpoint for Grafana template variables
// Returns a list of datascopes for a given asset in MetricFindValue format: { text: "scope name", value: "scope name" }
func (d *Datasource) handleDatascopesVariable(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if req.Method != "POST" {
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusMethodNotAllowed,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte(`{"error": "Method not allowed. Use POST."}`),
		})
	}

	log.DefaultLogger.Debug("Datascopes variable request")

	// Parse request body for asset RID
	var searchRequest struct {
		AssetRid string `json:"assetRid"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Error("Failed to parse datascopes request body", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Invalid request body"})
			return sender.Send(&backend.CallResourceResponse{
				Status:  http.StatusBadRequest,
				Headers: map[string][]string{"Content-Type": {"application/json"}},
				Body:    errBody,
			})
		}
	}

	// Validate asset RID is provided
	if searchRequest.AssetRid == "" {
		errBody, _ := json.Marshal(map[string]string{"error": "assetRid is required"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusBadRequest,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Check if asset RID contains unresolved template variable
	if strings.Contains(searchRequest.AssetRid, "$") {
		log.DefaultLogger.Debug("Asset RID contains unresolved template variable", "assetRid", searchRequest.AssetRid)
		// Return empty array - variable not yet resolved
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load settings for datascopes variable", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Fetch asset by RID to get its datascopes
	asset, err := d.fetchAssetByRid(ctx, config, searchRequest.AssetRid)
	if err != nil {
		log.DefaultLogger.Error("Failed to fetch asset", "error", err, "assetRid", searchRequest.AssetRid)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to fetch asset"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	if asset == nil {
		log.DefaultLogger.Debug("Asset not found", "assetRid", searchRequest.AssetRid)
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	// Transform datascopes to MetricFindValue format: { text: "name", value: "name" }
	// Filter to supported data source types (dataset, connection)
	result := make([]map[string]string, 0)
	for _, scope := range asset.DataScopes {
		dsType := scope.DataSource.Type
		if dsType == "dataset" || dsType == "connection" {
			result = append(result, map[string]string{
				"text":  scope.DataScopeName,
				"value": scope.DataScopeName,
			})
		}
	}

	responseBytes, err := json.Marshal(result)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal datascopes response", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	log.DefaultLogger.Debug("Datascopes variable request successful", "datascopeCount", len(result))

	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}

// handleChannelVariables handles the channelvariables endpoint for Grafana template variables
// Returns a list of channel names for a given asset in MetricFindValue format: { text: "channel name", value: "channel name" }
func (d *Datasource) handleChannelVariables(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if req.Method != "POST" {
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusMethodNotAllowed,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte(`{"error": "Method not allowed. Use POST."}`),
		})
	}

	log.DefaultLogger.Debug("Channel variables request")

	// Parse request body for asset RID and optional datascope filter
	var searchRequest struct {
		AssetRid      string `json:"assetRid"`
		DataScopeName string `json:"dataScopeName"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Error("Failed to parse channel variables request body", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Invalid request body"})
			return sender.Send(&backend.CallResourceResponse{
				Status:  http.StatusBadRequest,
				Headers: map[string][]string{"Content-Type": {"application/json"}},
				Body:    errBody,
			})
		}
	}

	// Validate asset RID is provided
	if searchRequest.AssetRid == "" {
		errBody, _ := json.Marshal(map[string]string{"error": "assetRid is required"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusBadRequest,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Check if any parameter contains unresolved template variable
	if strings.Contains(searchRequest.AssetRid, "$") || strings.Contains(searchRequest.DataScopeName, "$") {
		log.DefaultLogger.Debug("Request contains unresolved template variable", "assetRid", searchRequest.AssetRid, "dataScopeName", searchRequest.DataScopeName)
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load settings for channel variables", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Fetch asset by RID to get its datascopes and datasource RIDs
	asset, err := d.fetchAssetByRid(ctx, config, searchRequest.AssetRid)
	if err != nil {
		log.DefaultLogger.Error("Failed to fetch asset", "error", err, "assetRid", searchRequest.AssetRid)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to fetch asset"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	if asset == nil {
		log.DefaultLogger.Debug("Asset not found", "assetRid", searchRequest.AssetRid)
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	// Extract datasource RIDs from the asset's datascopes, optionally filtered by dataScopeName
	var dataSourceRids []rids.DataSourceRid
	for _, scope := range asset.DataScopes {
		dsType := scope.DataSource.Type
		if dsType != "dataset" && dsType != "connection" {
			continue
		}

		// If a dataScopeName filter is provided, only include matching scopes
		if searchRequest.DataScopeName != "" && scope.DataScopeName != searchRequest.DataScopeName {
			continue
		}

		var ridStr string
		if dsType == "dataset" && scope.DataSource.Dataset != nil {
			ridStr = *scope.DataSource.Dataset
		} else if dsType == "connection" && scope.DataSource.Connection != nil {
			ridStr = *scope.DataSource.Connection
		}

		if ridStr != "" {
			if parsedRid, err := rid.ParseRID(ridStr); err == nil {
				dataSourceRids = append(dataSourceRids, rids.DataSourceRid(parsedRid))
			} else {
				log.DefaultLogger.Warn("Failed to parse data source RID", "rid", ridStr, "error", err)
			}
		}
	}

	if len(dataSourceRids) == 0 {
		log.DefaultLogger.Debug("No data source RIDs found for asset", "assetRid", searchRequest.AssetRid)
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)

	// Paginate through all channels. The SearchChannels API caps at 1000 per call,
	// so we loop with NextPageToken to fetch the complete list for template variables.
	const maxChannelVariables = 5000
	pageSize := 1000
	var allChannelResults []datasourceapi.ChannelMetadata
	var nextPageToken *api.Token

	for page := 0; ; page++ {
		searchChannelsRequest := datasourceapi.SearchChannelsRequest{
			FuzzySearchText: "",
			DataSources:     dataSourceRids,
			PageSize:        &pageSize,
			NextPageToken:   nextPageToken,
		}

		channelsResponse, err := d.datasourceService.SearchChannels(ctx, bearerToken, searchChannelsRequest)
		if err != nil {
			log.DefaultLogger.Error("Channels search API call failed", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Channels search failed"})
			return sender.Send(&backend.CallResourceResponse{
				Status:  http.StatusInternalServerError,
				Headers: map[string][]string{"Content-Type": {"application/json"}},
				Body:    errBody,
			})
		}

		allChannelResults = append(allChannelResults, channelsResponse.Results...)

		if channelsResponse.NextPageToken == nil || len(allChannelResults) >= maxChannelVariables || len(channelsResponse.Results) == 0 {
			break
		}
		nextPageToken = channelsResponse.NextPageToken
	}

	// Hard cap: a page append could overshoot if maxChannelVariables is not a
	// multiple of the page size, so truncate any excess here.
	if len(allChannelResults) > maxChannelVariables {
		allChannelResults = allChannelResults[:maxChannelVariables]
	}

	// Deduplicate channel names and return as MetricFindValue format
	seen := make(map[string]bool)
	result := make([]map[string]string, 0)
	for _, channel := range allChannelResults {
		name := string(channel.Name)
		if !seen[name] {
			seen[name] = true
			result = append(result, map[string]string{
				"text":  name,
				"value": name,
			})
		}
	}

	responseBytes, err := json.Marshal(result)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal channel variables response", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	log.DefaultLogger.Debug("Channel variables request successful", "channelCount", len(result))

	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}

// AssetDataSource represents the data source within an asset's data scope.
type AssetDataSource struct {
	Type       string  `json:"type"`
	Dataset    *string `json:"dataset,omitempty"`
	Connection *string `json:"connection,omitempty"`
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

func (d *Datasource) fetchAssetByRidUncached(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	// Use the batch lookup endpoint with a single RID
	bodyBytes, err := json.Marshal([]string{assetRid})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/scout/v1/asset/multiple", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+config.Secrets.ApiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.getResourceHTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(errBody))
	}

	// Response is a map: { "ri.scout...": { rid, title, dataScopes, ... } }
	var assetMap map[string]SingleAssetResponse
	if err := json.NewDecoder(resp.Body).Decode(&assetMap); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Look up the specific asset
	if asset, ok := assetMap[assetRid]; ok {
		return &asset, nil
	}

	return nil, nil
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

// fetchAssetsForVariable fetches assets from the Nominal API using direct HTTP calls
func (d *Datasource) fetchAssetsForVariable(ctx context.Context, config *models.PluginSettings, searchText string, maxResults int) ([]AssetResponse, error) {
	var allResults []AssetResponse
	pageToken := ""
	pageSize := 50
	totalFetched := 0

	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	for totalFetched < maxResults {
		// Build request body matching the format used by QueryEditor
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

		bodyBytes, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}

		// Make HTTP request
		req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/scout/v1/search-assets", bytes.NewReader(bodyBytes))
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
			return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(errBody))
		}

		var assetResp AssetResponse
		if err := json.NewDecoder(resp.Body).Decode(&assetResp); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		allResults = append(allResults, assetResp)
		totalFetched += len(assetResp.Results)

		// Check for more pages
		if assetResp.NextPageToken == "" || len(assetResp.Results) < pageSize {
			break
		}
		pageToken = assetResp.NextPageToken
	}

	return allResults, nil
}

// extractArrowBucketedNumericSeries parses an Arrow IPC stream and extracts
// multiple named numeric columns, returning one AggregationSeries per field.
// All series share the end_bucket_timestamp column as their time axis.
func (d *Datasource) extractArrowBucketedNumericSeries(
	arrowPlot computeapi.ArrowBucketedNumericPlot,
	requestedFields []string,
) ([]AggregationSeries, error) {
	buf := bytes.NewReader(arrowPlot.ArrowBinary)
	reader, err := ipc.NewReader(buf, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer reader.Release()

	schema := reader.Schema()

	// Locate the timestamp column
	tsIdx := schema.FieldIndices("end_bucket_timestamp")
	if len(tsIdx) == 0 {
		return nil, fmt.Errorf("Arrow schema missing end_bucket_timestamp: have %v", schema.Fields())
	}

	// Locate each requested field column
	fieldIndices := make([]int, len(requestedFields))
	for i, name := range requestedFields {
		idx := schema.FieldIndices(name)
		if len(idx) == 0 {
			return nil, fmt.Errorf("Arrow schema missing requested column %q: have %v", name, schema.Fields())
		}
		fieldIndices[i] = idx[0]
	}

	// Initialize result slices — always non-nil so callers don't depend on nil semantics.
	seriesData := make([]AggregationSeries, len(requestedFields))
	for i, name := range requestedFields {
		seriesData[i].Name = name
		seriesData[i].Values = []*float64{}
	}
	sharedTimePoints := []time.Time{}

	for reader.Next() {
		rec := reader.Record()
		nRows := int(rec.NumRows())

		// Extract timestamps
		tsCol, ok := rec.Column(tsIdx[0]).(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("expected Int64 for end_bucket_timestamp, got %T", rec.Column(tsIdx[0]))
		}
		for i := 0; i < nRows; i++ {
			sharedTimePoints = append(sharedTimePoints, time.Unix(0, tsCol.Value(i)))
		}

		// Extract each field's values
		for fi, colIdx := range fieldIndices {
			col, ok := rec.Column(colIdx).(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("expected Float64 for %s, got %T", requestedFields[fi], rec.Column(colIdx))
			}
			for i := 0; i < nRows; i++ {
				if col.IsNull(i) {
					seriesData[fi].Values = append(seriesData[fi].Values, nil)
				} else {
					v := col.Value(i)
					seriesData[fi].Values = append(seriesData[fi].Values, &v)
				}
			}
		}
	}

	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("Arrow IPC read error: %w", err)
	}

	// Share the timestamp slice across all series — do not mutate after assignment.
	for i := range seriesData {
		seriesData[i].TimePoints = sharedTimePoints
	}

	return seriesData, nil
}
