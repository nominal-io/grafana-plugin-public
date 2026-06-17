package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdkhttpclient "github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
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

// maxBatchComputeSubrequests matches the backend subrequest limit.
// See scout ComputeResource.SUBREQUEST_LIMIT.
const maxBatchComputeSubrequests = 300

// defaultAPIBaseURL is the fallback Nominal API base URL when none is configured.
const defaultAPIBaseURL = "https://api.gov.nominal.io/api"

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
	resourceHTTPClient.Transport = newUserAgentTransport(resourceHTTPClient.Transport)

	// Generated Conjure clients still require their own client type, so keep this
	// wrapper for those service integrations.
	conjureClient, err := conjurehttpclient.NewClient(
		conjurehttpclient.WithBaseURLs([]string{baseURL}),
		conjurehttpclient.WithMiddleware(userAgentMiddleware()),
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
	}
	ds.nominalCatalog = newNominalCatalog(ds.resourceHTTPClient, ds.datasourceService)
	ds.templateVariableCatalog = newTemplateVariableCatalog(ds.nominalCatalog)

	return ds, nil
}

// Datasource is the Nominal datasource implementation
type Datasource struct {
	settings          backend.DataSourceInstanceSettings
	authService       authapi.AuthenticationServiceV2Client
	computeService    computeapi1.ComputeServiceClient
	datasourceService datasourceservice.DataSourceServiceClient

	resourceHTTPClient *http.Client

	nominalCatalog          *NominalCatalog
	templateVariableCatalog *TemplateVariableCatalog
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
// Query execution itself lives behind NominalQueryExecution so Datasource stays
// focused on Grafana setup, settings loading, and plugin lifecycle concerns.
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	// UA components live in ctx so any downstream HTTP picks them up; safe to set
	// before validation because the error short-circuit below performs no I/O.
	ctx = contextWithPluginRequestIdentity(ctx, req.PluginContext)
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

	return newNominalQueryExecution(d, config).Execute(ctx, req.Queries), nil
}

// handleConnectionTestQuery handles the connectionTest query type
func (e *NominalQueryExecution) handleConnectionTestQuery(ctx context.Context) backend.DataResponse {
	var response backend.DataResponse

	log.DefaultLogger.Debug("Processing connectionTest query")

	bearerToken := bearertoken.Token(e.config.Secrets.ApiKey)
	profile, err := e.datasource.authService.GetMyProfile(ctx, bearerToken)
	if err != nil {
		logErrorWithConjureFields("Connection test failed", err)
		message, _ := classifyConnectionError(err)
		return backend.ErrDataResponse(backend.StatusInternal, message)
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
func (e *NominalQueryExecution) handleLegacyQuery(qm NominalQueryModel, timeRange backend.TimeRange) backend.DataResponse {
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

// transformBatchResult converts a single batch result to a Grafana DataResponse.
// Handles both success and error cases from the ComputeNodeResult union type.
func (e *NominalQueryExecution) transformBatchResult(result computeapi.ComputeWithUnitsResult, qm NominalQueryModel) backend.DataResponse {
	var response backend.DataResponse

	// ComputeNodeResult is a union type - use AcceptFuncs to handle success/error
	err := result.ComputeResult.AcceptFuncs(
		// successFunc - called when compute succeeded
		func(computeResponse computeapi.ComputeNodeResponse) error {
			result, transformErr := e.transformNominalResponseFromClient(computeResponse, qm)
			if transformErr != nil {
				response = backend.ErrDataResponse(
					backend.StatusInternal,
					fmt.Sprintf("Transform failed: %v", transformErr),
				)
				return nil
			}

			if result.IsLog {
				// Sort descending (newest first) for Grafana's default log sort order.
				// Grafana's infinite scroll uses the boundary row's timestamp to compute
				// the next time-range query. Don't assume this sort is redundant: the
				// compute API's PageInfo contract specifies selection direction (via sign
				// of PageSize), not response order.
				sort.SliceStable(result.LogEntries, func(a, b int) bool {
					return result.LogEntries[a].Time.After(result.LogEntries[b].Time)
				})

				frame := data.NewFrame(qm.Channel)
				frame.Meta = &data.FrameMeta{
					Type: data.FrameTypeLogLines,
					// log-lines dataplane contract is at v0.0 — don't confuse with time-series-wide's 0.1
					TypeVersion:            data.FrameTypeVersion{0, 0},
					PreferredVisualization: data.VisTypeLogs,
				}

				if len(result.LogEntries) > 0 {
					times := make([]time.Time, len(result.LogEntries))
					bodies := make([]string, len(result.LogEntries))
					ids := make([]string, len(result.LogEntries))
					labels := make([]json.RawMessage, len(result.LogEntries))
					for i, e := range result.LogEntries {
						times[i] = e.Time
						bodies[i] = e.Body
						ids[i] = e.ID
						labels[i] = e.Labels
					}
					frame.Fields = append(frame.Fields,
						data.NewField("timestamp", nil, times),
						data.NewField("body", nil, bodies),
						data.NewField("id", nil, ids),
						data.NewField("labels", nil, labels),
					)
				} else {
					frame.Fields = append(frame.Fields,
						data.NewField("timestamp", nil, []time.Time{}),
						data.NewField("body", nil, []string{}),
						data.NewField("id", nil, []string{}),
						data.NewField("labels", nil, []json.RawMessage{}),
					)
				}

				log.DefaultLogger.Debug("Successfully processed log query",
					"entries", len(result.LogEntries))
				response.Frames = append(response.Frames, frame)
			} else if len(result.AggSeries) > 0 {
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
						valueField.Config = fieldConfigForNumeric(&qm, displayName, agg.CarriesChannelUnit)
						frame.Fields = append(frame.Fields,
							data.NewField("time", nil, agg.TimePoints),
							valueField,
						)
					} else {
						valueField := data.NewField("value", nil, []*float64{})
						valueField.Config = fieldConfigForNumeric(&qm, displayName, agg.CarriesChannelUnit)
						frame.Fields = append(frame.Fields,
							data.NewField("time", nil, []time.Time{}),
							valueField,
						)
					}
					response.Frames = append(response.Frames, frame)
				}
				dataPoints := 0
				if len(result.AggSeries) > 0 {
					dataPoints = len(result.AggSeries[0].TimePoints)
				}
				log.DefaultLogger.Debug("Successfully processed multi-agg query",
					"series", len(result.AggSeries),
					"dataPoints", dataPoints)
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
					valueField.Config = fieldConfigForEnum(&qm)
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, result.TimePoints),
						valueField,
					)
				} else {
					valueField := data.NewField("value", nil, []string{})
					valueField.Config = fieldConfigForEnum(&qm)
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
					valueField.Config = fieldConfigForNumericWithChannelUnit(&qm, qm.Channel)
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, result.TimePoints),
						valueField,
					)
				} else {
					valueField := data.NewField("value", nil, []*float64{})
					valueField.Config = fieldConfigForNumericWithChannelUnit(&qm, qm.Channel)
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
			errMsg := fmt.Sprintf("Compute error: %v (code: %v) [channel=%s, dataType=%s]", errorResult.ErrorType, errorResult.Code, qm.Channel, qm.ChannelDataType)

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

type TransformResult struct {
	// Numeric aggregation series (Arrow bucketed path, one entry per requested field)
	AggSeries []AggregationSeries

	// Legacy numeric path (non-Arrow) — single series only
	TimePoints    []time.Time
	NumericValues []*float64

	// Enum path
	StringValues []string
	IsEnum       bool

	// Log path
	IsLog      bool
	LogEntries []LogEntry
}

// LogEntry represents a single log entry with its timestamp and metadata.
// Labels is json.RawMessage (FieldTypeJSON), not a string — Grafana's Logs panel
// expects a dedicated JSON-typed column for per-entry labels, not a JSON-encoded string.
type LogEntry struct {
	Time   time.Time
	Body   string
	ID     string
	Labels json.RawMessage
}

// marshalLogArgs serializes log Args to JSON and, when available, adds
// "nominal.channel" so mixed-channel log panels can distinguish rows.
//
// The namespaced label avoids Grafana hiding underscore-prefixed labels. Existing
// user-provided "nominal.channel" values are preserved.
//
// Copying is intentional: it preserves caller input and keeps nil Args from
// serializing as JSON null.
func marshalLogArgs(args map[string]string, channel string) json.RawMessage {
	out := make(map[string]string, len(args)+1)
	for k, v := range args {
		out[k] = v
	}
	if channel != "" {
		if _, exists := out["nominal.channel"]; !exists {
			out["nominal.channel"] = channel
		}
	}
	labelsJSON, _ := json.Marshal(out)
	return labelsJSON
}

// transformNominalResponseFromClient converts conjure client response to Grafana time series data.
// qm is needed so the Arrow bucketed handler knows which aggregation columns to extract.
func (e *NominalQueryExecution) transformNominalResponseFromClient(response computeapi.ComputeNodeResponse, qm NominalQueryModel) (TransformResult, error) {
	log.DefaultLogger.Debug("Transforming conjure client response")

	var result TransformResult

	// Use the conjure union visitor pattern to handle different response types
	visitErr := response.AcceptFuncs(
		nil, // rangeFunc
		nil, // rangesSummaryFunc
		nil, // rangeValueFunc
		func(numeric computeapi.NumericPlot) error {
			timePoints, values, err := e.extractNumericDataFromConjure(numeric)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.NumericValues = values
			result.IsEnum = false
			return nil
		},
		func(bucketed computeapi.BucketedNumericPlot) error {
			timePoints, values, err := e.extractBucketedDataFromConjure(bucketed)
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
			var specs []aggColumnSpec
			for _, agg := range qm.Aggregations {
				specs = append(specs, aggColumnSpecFromEnum(agg))
			}
			if len(specs) == 0 {
				return fmt.Errorf("no aggregation fields requested for ArrowBucketedNumericPlot response")
			}
			series, err := extractArrowBucketedNumericSeries(arrowBucketed, specs)
			if err != nil {
				return err
			}
			result.AggSeries = series
			result.IsEnum = false
			return nil
		},
		// enumFunc - maps integer indices to category strings
		func(enum computeapi.EnumPlot) error {
			timePoints, values, err := e.extractEnumDataFromConjure(enum)
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
				result.IsEnum = true
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
			timePoints, values, err := e.extractBucketedEnumDataFromConjure(bucketed)
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
		// pagedLogFunc — paginated log response
		func(paged computeapi.PagedLogPlot) error {
			for i := 0; i < len(paged.Timestamps) && i < len(paged.Values); i++ {
				ts := paged.Timestamps[i]
				val := paged.Values[i]

				result.LogEntries = append(result.LogEntries, LogEntry{
					Time:   time.Unix(int64(ts.Seconds), int64(ts.Nanos)),
					Body:   val.Message,
					ID:     val.Id.String(),
					Labels: marshalLogArgs(val.Args, qm.Channel),
				})
			}
			result.IsLog = true
			log.DefaultLogger.Debug("Extracted paged log data",
				"entries", len(paged.Timestamps))
			return nil
		},
		// logPointFunc — single log point response
		func(lp *computeapi.LogPoint) error {
			if lp == nil {
				result.IsLog = true
				return nil
			}
			result.LogEntries = []LogEntry{{
				Time:   time.Unix(int64(lp.Timestamp.Seconds), int64(lp.Timestamp.Nanos)),
				Body:   lp.Value.Message,
				ID:     lp.Value.Id.String(),
				Labels: marshalLogArgs(lp.Value.Args, qm.Channel),
			}}
			result.IsLog = true
			return nil
		},
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
func (e *NominalQueryExecution) extractNumericDataFromConjure(numeric computeapi.NumericPlot) ([]time.Time, []*float64, error) {
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

func (e *NominalQueryExecution) extractBucketedDataFromConjure(bucketed computeapi.BucketedNumericPlot) ([]time.Time, []*float64, error) {
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
func (e *NominalQueryExecution) extractEnumDataFromConjure(enumPlot computeapi.EnumPlot) ([]time.Time, []string, error) {
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
func (e *NominalQueryExecution) extractBucketedEnumDataFromConjure(bucketed computeapi.BucketedEnumPlot) ([]time.Time, []string, error) {
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
	ctx = contextWithPluginRequestIdentity(ctx, req.PluginContext)
	log.DefaultLogger.Debug("CheckHealth called")

	if req.PluginContext.DataSourceInstanceSettings == nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Data source is not configured",
		}, nil
	}

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
		logErrorWithConjureFields("Health check failed", err)
		message, _ := classifyConnectionError(err)
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: message,
		}, nil
	}

	log.DefaultLogger.Debug("Health check successful", "user", profile.DisplayName)
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Successfully connected to Nominal API",
	}, nil
}

// CallResource handles HTTP requests sent to the plugin.
func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	ctx = contextWithPluginRequestIdentity(ctx, req.PluginContext)
	log.DefaultLogger.Debug("=== CallResource called ===")
	log.DefaultLogger.Debug("CallResource called", "path", req.Path, "method", req.Method, "url", req.URL)
	return newNominalResourceHandler(d).Handle(ctx, req, sender)
}

// carriesChannelUnit = false for COUNT (dimensionless) and VARIANCE (unit²),
// suppressing the channel unit on the resulting frame. Multi-agg call sites
// pass agg.CarriesChannelUnit directly; non-aggregated call sites should use
// fieldConfigForNumericWithChannelUnit instead so the rule is explicit at the
// call site rather than encoded as a literal true.
func fieldConfigForNumeric(qm *NominalQueryModel, displayName string, carriesChannelUnit bool) *data.FieldConfig {
	cfg := &data.FieldConfig{DisplayNameFromDS: displayName}
	if !carriesChannelUnit {
		return cfg
	}
	cfg.Unit = mapToGrafanaUnit(qm.ChannelUnit)
	return cfg
}

// fieldConfigForNumericWithChannelUnit is the call-site-clear wrapper for
// non-aggregated numeric frames, which always carry the channel unit.
func fieldConfigForNumericWithChannelUnit(qm *NominalQueryModel, displayName string) *data.FieldConfig {
	return fieldConfigForNumeric(qm, displayName, true)
}

func fieldConfigForEnum(qm *NominalQueryModel) *data.FieldConfig {
	return &data.FieldConfig{DisplayNameFromDS: qm.Channel}
}
