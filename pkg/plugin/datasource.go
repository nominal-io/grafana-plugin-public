package plugin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdkhttpclient "github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/uuid"
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

	kill killCoalescer
}

func (d *Datasource) getResourceHTTPClient() *http.Client {
	return d.resourceHTTPClient
}

// sendBatchKill sends one best-effort batch without logging sensitive values.
func (d *Datasource) sendBatchKill(ctx context.Context, target killTarget, ids []uuid.UUID) {
	ctx = contextWithUserAgentComponents(ctx, target.ua)
	err := d.computeService.BatchKillRequests(ctx, target.token, computeapi.BatchKillRequestsRequest{RequestIds: ids})
	if err != nil {
		logErrorWithConjureFields("BatchKillRequests failed", err, "count", len(ids))
		return
	}
	log.DefaultLogger.Debug("BatchKillRequests flushed", "count", len(ids))
}

// enqueueKill queues a kill for this datasource's own sender. The coalescer
// takes the sender per call, so binding it here is what keeps a bare
// Datasource literal safe: there is no wiring to forget.
func (d *Datasource) enqueueKill(id uuid.UUID, target killTarget) {
	d.kill.enqueue(d.sendBatchKill, id, target)
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using the NewDatasource factory function.
func (d *Datasource) Dispose() {
	d.nominalCatalog.close()
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
