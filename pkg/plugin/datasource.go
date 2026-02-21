package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	"github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
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

// sharedHTTPClient is a reusable HTTP client for direct API calls.
// Reusing a single client enables connection pooling and keep-alive.
var sharedHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

// maxBatchComputeSubrequests matches the backend subrequest limit.
// See scout ComputeResource.SUBREQUEST_LIMIT.
const maxBatchComputeSubrequests = 300


// NewDatasource creates a new datasource instance.
func NewDatasource(_ context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	config, err := models.LoadPluginSettings(settings)
	if err != nil {
		return nil, fmt.Errorf("failed to load plugin settings: %v", err)
	}

	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		baseURL = "https://api.gov.nominal.io/api"
	}
	// Use the base URL as-is since it should already include the full path
	baseURL = strings.TrimSuffix(baseURL, "/")

	// Create HTTP client
	httpClient, err := httpclient.NewClient(
		httpclient.WithBaseURLs([]string{baseURL}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %v", err)
	}

	ds := &Datasource{
		settings:          settings,
		httpClient:        httpClient,
		authService:       authapi.NewAuthenticationServiceV2Client(httpClient),
		computeService:    computeapi.NewComputeServiceClient(httpClient),
		datasourceService: datasourceservice.NewDataSourceServiceClient(httpClient),
	}

	return ds, nil
}

// interpolateTemplateVariables replaces template variables in strings
func interpolateTemplateVariables(input string, variables map[string]interface{}) string {
	if variables == nil {
		return input
	}

	result := input
	for key, value := range variables {
		// Support both ${var} and $var syntax
		patterns := []string{
			fmt.Sprintf("${%s}", key),
			fmt.Sprintf("$%s", key),
		}

		valueStr := fmt.Sprintf("%v", value)
		for _, pattern := range patterns {
			result = strings.ReplaceAll(result, pattern, valueStr)
		}
	}

	return result
}

// applyTemplateVariables applies template variable interpolation to query fields
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
		// DataScopeName is optional but should be validated if provided
		if qm.DataScopeName != "" && strings.TrimSpace(qm.DataScopeName) == "" {
			return fmt.Errorf("dataScopeName cannot be empty when provided")
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

// Datasource is the Nominal datasource implementation
type Datasource struct {
	settings          backend.DataSourceInstanceSettings
	httpClient        httpclient.Client
	authService       authapi.AuthenticationServiceV2Client
	computeService    computeapi.ComputeServiceClient
	datasourceService datasourceservice.DataSourceServiceClient
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	// Clean up datasource instance resources.
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
	AssetRid      string `json:"assetRid"`
	Channel       string `json:"channel"`
	DataScopeName string `json:"dataScopeName"`

	// Query parameters
	Buckets   int    `json:"buckets"`
	QueryType string `json:"queryType"`

	// Template variables support
	TemplateVariables map[string]interface{} `json:"templateVariables,omitempty"`

	// Legacy support
	QueryText string  `json:"queryText"`
	Constant  float64 `json:"constant"`
}

// buildComputeRequest constructs a ComputeNodeRequest from query model and time range.
// This is extracted to enable reuse for both single and batch compute calls.
func (d *Datasource) buildComputeRequest(qm NominalQueryModel, timeRange backend.TimeRange) computeapi.ComputeNodeRequest {
	startSeconds := timeRange.From.Unix()
	endSeconds := timeRange.To.Unix()

	// Build the timeShift series with proper conjure types
	// Use a literal zero duration by default (no shift) unless frontend later adds support
	timeShiftSeries := computeapi.NumericTimeShiftSeries{
		Input: d.buildChannelSeries(qm.AssetRid, qm.Channel, qm.DataScopeName),
		Duration: computeapi.NewDurationConstantFromLiteral(runapi.Duration{
			Seconds: safelong.SafeLong(0),
			Nanos:   safelong.SafeLong(0),
			Picos:   nil,
		}),
	}

	// Create numeric series with timeShift
	numericSeries := computeapi.NewNumericSeriesFromTimeShift(timeShiftSeries)

	// Build the series node
	buckets := int(qm.Buckets)
	seriesNode := computeapi.SummarizeSeries{
		Input:   computeapi.NewSeriesFromNumeric(numericSeries),
		Buckets: &buckets,
	}

	// Create computable node
	node := computeapi.NewComputableNodeFromSeries(seriesNode)

	// Build context with variables
	computeContext := d.buildComputeContext(qm, startSeconds, endSeconds)

	return computeapi.ComputeNodeRequest{
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
		computeRequests := make([]computeapi.ComputeNodeRequest, len(chunkModels))
		for i, qm := range chunkModels {
			computeRequests[i] = d.buildComputeRequest(qm, chunkQueries[i].TimeRange)
		}

		batchRequest := computeapi.BatchComputeWithUnitsRequest{
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
			frame := data.NewFrame("response")
			frame.Name = qm.Channel

			timePoints, values, transformErr := d.transformNominalResponseFromClient(computeResponse)
			if transformErr != nil {
				response = backend.ErrDataResponse(
					backend.StatusInternal,
					fmt.Sprintf("Transform failed: %v", transformErr),
				)
				return nil
			}

			if len(timePoints) > 0 && len(values) > 0 {
				frame.Fields = append(frame.Fields,
					data.NewField("time", nil, timePoints),
					data.NewField("value", nil, values),
				)
				log.DefaultLogger.Debug("Successfully processed query", "dataPoints", len(timePoints))
			} else {
				frame.Fields = append(frame.Fields,
					data.NewField("time", nil, []time.Time{}),
					data.NewField("value", nil, []float64{}),
				)
			}

			response.Frames = append(response.Frames, frame)
			return nil
		},
		// errorFunc - called when compute failed
		func(errorResult computeapi.ErrorResult) error {
			response = backend.ErrDataResponse(
				backend.StatusInternal,
				fmt.Sprintf("Compute error: %v (code: %v)", errorResult.ErrorType, errorResult.Code),
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

// buildChannelSeries creates a channel series for the given asset/channel
func (d *Datasource) buildChannelSeries(assetRid, channel, dataScopeName string) computeapi.NumericSeries {
	// Build asset channel with proper types
	assetChannel := computeapi.AssetChannel{
		AssetRid:       computeapi.NewStringConstantFromVariable(computeapi.VariableName("assetRid")),
		Channel:        computeapi.NewStringConstantFromLiteral(channel),
		DataScopeName:  computeapi.NewStringConstantFromLiteral(dataScopeName),
		AdditionalTags: map[string]computeapi.StringConstant{},
		TagsToGroupBy:  []string{},
		GroupByTags:    []computeapi.StringConstant{},
	}

	// Create channel series from asset
	channelSeries := computeapi.NewChannelSeriesFromAsset(assetChannel)

	return computeapi.NewNumericSeriesFromChannel(channelSeries)
}

// buildComputeContext creates the context with variables for the compute request
func (d *Datasource) buildComputeContext(qm NominalQueryModel, startSeconds, endSeconds int64) computeapi.Context {
	variables := map[computeapi.VariableName]computeapi.VariableValue{
		// Asset RID variable referenced by the series builder
		computeapi.VariableName("assetRid"): computeapi.NewVariableValueFromString(qm.AssetRid),
	}

	// Add template variables if present
	if qm.TemplateVariables != nil {
		for key, value := range qm.TemplateVariables {
			if strValue, ok := value.(string); ok {
				variables[computeapi.VariableName(key)] = computeapi.NewVariableValueFromString(strValue)
			}
		}
	}

	return computeapi.Context{
		Variables:         variables,
		FunctionVariables: nil, // Deprecated field
	}
}

// transformNominalResponseFromClient converts conjure client response to Grafana time series data
func (d *Datasource) transformNominalResponseFromClient(response computeapi.ComputeNodeResponse) ([]time.Time, []float64, error) {
	log.DefaultLogger.Debug("Transforming conjure client response")

	var timePoints []time.Time
	var values []float64
	var err error

	// Use the conjure union visitor pattern to handle different response types
	visitErr := response.AcceptFuncs(
		nil, // range_Func
		nil, // rangesSummaryFunc
		nil, // rangeValueFunc
		// numericFunc
		func(numeric computeapi.NumericPlot) error {
			timePoints, values, err = d.extractNumericDataFromConjure(numeric)
			return err
		},
		// bucketedNumericFunc
		func(bucketed computeapi.BucketedNumericPlot) error {
			timePoints, values, err = d.extractBucketedDataFromConjure(bucketed)
			return err
		},
		nil, // numericPointFunc
		nil, // arrowNumericFunc
		nil, // arrowBucketedNumericFunc
		nil, // enumFunc
		nil, // enumPointFunc
		nil, // bucketedEnumFunc
		nil, // arrowEnumFunc
		nil, // arrowBucketedEnumFunc
		nil, // pagedLogFunc
		nil, // logPointFunc
		nil, // cartesianFunc
		nil, // bucketedCartesianFunc
		nil, // bucketedCartesian3dFunc
		nil, // bucketedGeoFunc
		nil, // frequencyDomainFunc
		nil, // numericHistogramFunc
		nil, // enumHistogramFunc
		nil, // curveFitFunc
		nil, // groupedFunc
		nil, // arrayFunc
		// unknownFunc
		func(typeName string) error {
			log.DefaultLogger.Debug("Unhandled response type", "type", typeName)
			return nil
		},
	)

	if visitErr != nil {
		return nil, nil, fmt.Errorf("failed to process response: %w", visitErr)
	}

	return timePoints, values, err
}

// Helper methods for extracting data from conjure types
func (d *Datasource) extractNumericDataFromConjure(numeric computeapi.NumericPlot) ([]time.Time, []float64, error) {
	var timePoints []time.Time
	var values []float64

	// Access the fields directly from the conjure struct
	for i := 0; i < len(numeric.Timestamps) && i < len(numeric.Values); i++ {
		timestamp := numeric.Timestamps[i]
		value := numeric.Values[i]

		// Convert conjure timestamp to Go time
		// SafeLong values need to be cast to int64
		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))
		values = append(values, value)
	}

	log.DefaultLogger.Debug("Extracted numeric data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

func (d *Datasource) extractBucketedDataFromConjure(bucketed computeapi.BucketedNumericPlot) ([]time.Time, []float64, error) {
	var timePoints []time.Time
	var values []float64

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
		values = append(values, bucket.Mean)
	}

	log.DefaultLogger.Debug("Extracted bucketed data from conjure", "timePoints", len(timePoints), "values", len(values))
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
			Message: "Unable to load settings: " + err.Error(),
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
			Message: errorMsg + ": " + err.Error(),
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
	log.DefaultLogger.Debug("Request headers", "headers", req.Headers)

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
			Body: []byte(`{"error": "Failed to load settings: ` + err.Error() + `"}`),
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

		return sender.Send(&backend.CallResourceResponse{
			Status: statusCode,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "` + errorMsg + `: ` + err.Error() + `"}`),
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
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Invalid request body: ` + err.Error() + `"}`),
		})
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusInternalServerError,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Failed to load settings: ` + err.Error() + `"}`),
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
			Body: []byte(`{"error": "Channels search failed: ` + err.Error() + `"}`),
		})
	}

	// Transform the API response to the expected format
	var channels []map[string]interface{}
	for _, channel := range channelsResponse.Results {
		channelMap := map[string]interface{}{
			"name":        string(channel.Name),
			"dataSource":  channel.DataSource.String(),
			"description": getChannelMetadataDescription(channel),
		}
		channels = append(channels, channelMap)
	}

	apiResponse := map[string]interface{}{
		"channels": channels,
	}

	// Convert response to JSON
	responseBytes, err := json.Marshal(apiResponse)
	if err != nil {
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusInternalServerError,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Failed to marshal response: ` + err.Error() + `"}`),
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

// handleAssetsVariable handles the assets endpoint for Grafana template variables
// Returns a list of assets in MetricFindValue format: { text: "Asset Name", value: "ri.scout..." }
func (d *Datasource) handleAssetsVariable(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	log.DefaultLogger.Debug("Assets variable request", "method", req.Method, "body", string(req.Body))

	// Parse optional request body for search/filter parameters
	var searchRequest struct {
		SearchText string `json:"searchText"`
		MaxResults int    `json:"maxResults"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Debug("Failed to parse request body, using defaults", "error", err)
		}
	}

	// Set defaults
	if searchRequest.MaxResults == 0 {
		searchRequest.MaxResults = 500
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings: " + err.Error()})
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
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to fetch assets: " + err.Error()})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Transform to MetricFindValue format: { text: "name", value: "rid" }
	// Filter to assets with dataset data sources
	result := make([]map[string]string, 0)
	for _, resp := range assetResponses {
		for _, asset := range resp.Results {
			// Check if asset has dataset data sources
			hasDataset := false
			for _, scope := range asset.DataScopes {
				if scope.DataSource.Type == "dataset" {
					hasDataset = true
					break
				}
			}
			if hasDataset {
				result = append(result, map[string]string{
					"text":  asset.Title,
					"value": asset.Rid,
				})
				if len(result) >= searchRequest.MaxResults {
					break
				}
			}
		}
	}

	responseBytes, err := json.Marshal(result)
	if err != nil {
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response: " + err.Error()})
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
	log.DefaultLogger.Debug("Datascopes variable request", "method", req.Method, "body", string(req.Body))

	// Parse request body for asset RID
	var searchRequest struct {
		AssetRid string `json:"assetRid"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Debug("Failed to parse request body", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Invalid request body: " + err.Error()})
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
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings: " + err.Error()})
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
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to fetch asset: " + err.Error()})
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
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response: " + err.Error()})
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
	log.DefaultLogger.Debug("Channel variables request", "method", req.Method, "body", string(req.Body))

	// Parse request body for asset RID and optional datascope filter
	var searchRequest struct {
		AssetRid      string `json:"assetRid"`
		DataScopeName string `json:"dataScopeName"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Debug("Failed to parse request body", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Invalid request body: " + err.Error()})
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
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings: " + err.Error()})
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
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to fetch asset: " + err.Error()})
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

	// Search for channels across all datasource RIDs
	searchChannelsRequest := datasourceapi.SearchChannelsRequest{
		FuzzySearchText: "",
		DataSources:     dataSourceRids,
	}

	channelsResponse, err := d.datasourceService.SearchChannels(ctx, bearerToken, searchChannelsRequest)
	if err != nil {
		log.DefaultLogger.Error("Channels search API call failed", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Channels search failed: " + err.Error()})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Deduplicate channel names and return as MetricFindValue format
	seen := make(map[string]bool)
	result := make([]map[string]string, 0)
	for _, channel := range channelsResponse.Results {
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
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response: " + err.Error()})
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

// SingleAssetResponse represents a single asset from the batch lookup API
type SingleAssetResponse struct {
	Rid        string `json:"rid"`
	Title      string `json:"title"`
	DataScopes []struct {
		DataScopeName string `json:"dataScopeName"`
		DataSource    struct {
			Type       string  `json:"type"`
			Dataset    *string `json:"dataset,omitempty"`
			Connection *string `json:"connection,omitempty"`
		} `json:"dataSource"`
	} `json:"dataScopes"`
}

// fetchAssetByRid fetches a single asset by its RID using the batch lookup endpoint
func (d *Datasource) fetchAssetByRid(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		baseURL = "https://api.gov.nominal.io/api"
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	client := sharedHTTPClient

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

	resp, err := client.Do(req)
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

// AssetResponse represents the API response for asset search
type AssetResponse struct {
	Results []struct {
		Rid         string `json:"rid"`
		Title       string `json:"title"`
		Description string `json:"description"`
		DataScopes  []struct {
			DataScopeName string `json:"dataScopeName"`
			DataSource    struct {
				Type string `json:"type"`
			} `json:"dataSource"`
		} `json:"dataScopes"`
	} `json:"results"`
	NextPageToken string `json:"nextPageToken"`
}

// fetchAssetsForVariable fetches assets from the Nominal API using direct HTTP calls
func (d *Datasource) fetchAssetsForVariable(ctx context.Context, config *models.PluginSettings, searchText string, maxResults int) ([]AssetResponse, error) {
	var allResults []AssetResponse
	pageToken := ""
	pageSize := 50
	totalFetched := 0

	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		baseURL = "https://api.gov.nominal.io/api"
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	client := sharedHTTPClient

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

		resp, err := client.Do(req)
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

	// Copy headers from original request
	for key, values := range req.Headers {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// Add authentication header
	proxyReq.Header.Set("Authorization", "Bearer "+config.Secrets.ApiKey)

	proxyReq.Header.Set("User-Agent", "grafana-nominal-plugin/1.0.0")
	log.DefaultLogger.Debug("Using API key for proxy request")

	// Ensure Content-Type is set for POST requests
	if req.Method == "POST" && proxyReq.Header.Get("Content-Type") == "" {
		proxyReq.Header.Set("Content-Type", "application/json")
	}

	// Make the request
	client := sharedHTTPClient
	resp, err := client.Do(proxyReq)
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
