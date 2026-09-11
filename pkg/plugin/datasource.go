package plugin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdkhttpclient "github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	workspaceapi "github.com/nominal-io/nominal-api-go/security/api/workspace"
	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
	"github.com/palantir/pkg/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
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

	workspaceRid, err := parseWorkspaceRid(config.WorkspaceRid)
	if err != nil {
		return nil, err
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

	// A base URL the SQL transport cannot use (for example plain http) must not break the
	// Conjure-backed features, so the error is surfaced per SQL query instead.
	userAgent := formatUserAgent(userAgentComponentsFromPluginContext(backend.PluginConfigFromContext(ctx)))
	sqlConn, sqlErr := dialSQL(baseURL, userAgent)
	if sqlErr != nil {
		log.DefaultLogger.FromContext(ctx).Warn("SQL queries are unavailable for this data source", "error", sqlErr)
	}

	ds := &Datasource{
		settings:           settings,
		resourceHTTPClient: resourceHTTPClient,
		authService:        authapi.NewAuthenticationServiceV2Client(conjureClient),
		computeService:     computeapi1.NewComputeServiceClient(conjureClient),
		datasourceService:  datasourceservice.NewDataSourceServiceClient(conjureClient),
		workspaceService:   workspaceapi.NewWorkspaceServiceClient(conjureClient),
		workspaceRid:       workspaceRid,
		sqlConn:            sqlConn,
		sqlErr:             sqlErr,
	}
	if sqlConn != nil {
		ds.sqlService = sqlv1.NewSqlServiceClient(sqlConn)
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
	workspaceService  workspaceapi.WorkspaceServiceClient

	workspaceRid *rids.WorkspaceRid
	sqlService   sqlv1.SqlServiceClient
	sqlConn      *grpc.ClientConn
	// sqlErr explains why sqlService is nil, for example a plain http base URL.
	sqlErr error
	// defaultSQLWorkspace caches the API key's default workspace when no Workspace RID is set.
	defaultSQLWorkspace atomic.Pointer[string]

	resourceHTTPClient *http.Client

	// Built once during construction. Every query shares these instances, so
	// their caches are shared too.
	nominalCatalog          *NominalCatalog
	templateVariableCatalog *TemplateVariableCatalog

	kill killCoalescer
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
	if d.resourceHTTPClient != nil {
		d.resourceHTTPClient.CloseIdleConnections()
	}
	if d.sqlConn != nil {
		// Let SQL queries still running on this replaced instance finish.
		time.AfterFunc(sqlServiceTimeLimit, func() { _ = d.sqlConn.Close() })
	}
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
//
// Query execution itself lives behind NominalQueryExecution so Datasource stays
// focused on Grafana setup, settings loading, and plugin lifecycle concerns.
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (response *backend.QueryDataResponse, err error) {
	// Last-resort boundary. The SDK does not recover on this path, so a panic
	// anywhere outside the per-chunk and per-result guards would end the
	// process and every in-flight query on this instance.
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Error("Recovered panic while handling query request",
				"panic", fmt.Sprintf("%v", r),
				"panicType", fmt.Sprintf("%T", r),
				"stack", string(debug.Stack()),
			)
			response = backend.NewQueryDataResponse()
			for _, q := range req.Queries {
				response.Responses[q.RefID] = backend.ErrDataResponse(backend.StatusInternal,
					"Internal error while handling query request")
			}
			err = nil
		}
	}()

	// UA components live in ctx so any downstream HTTP picks them up; safe to set
	// before validation because the error short-circuit below performs no I/O.
	ctx = contextWithPluginRequestIdentity(ctx, req.PluginContext)
	response = backend.NewQueryDataResponse()

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
	config, loadErr := models.LoadPluginSettings(*req.PluginContext.DataSourceInstanceSettings)
	if loadErr != nil {
		log.DefaultLogger.Error("Failed to load plugin settings", "error", loadErr)
		for _, q := range req.Queries {
			response.Responses[q.RefID] = backend.ErrDataResponse(
				backend.StatusInternal,
				fmt.Sprintf("Failed to load settings: %v", loadErr),
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

	message := "Successfully connected to Nominal API"
	if d.workspaceRid != nil {
		name, err := d.workspaceName(ctxWithTimeout, bearerToken, *d.workspaceRid)
		if err != nil {
			return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
		}
		message += ". Workspace: " + name
	}
	if err := d.checkSQL(ctxWithTimeout, bearerToken); err != nil {
		// Compute queries still work, so a SQL problem is reported without failing the check.
		return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: message + ". SQL queries will fail: " + err.Error()}, nil
	}
	if d.workspaceRid == nil {
		message += ". SQL queries use the API key's default workspace"
	}
	return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: message}, nil
}

// checkSQL confirms that the SQL service accepts the API key and that SQL queries have a workspace.
func (d *Datasource) checkSQL(ctx context.Context, token bearertoken.Token) error {
	if d.sqlService == nil {
		return d.sqlErr
	}
	if _, err := d.sqlService.GetSqlCatalog(withBearerToken(ctx, token), &sqlv1.GetSqlCatalogRequest{}); err != nil {
		return errors.New(sqlErrorMessage(status.Convert(err)))
	}
	_, err := d.resolveSQLWorkspace(ctx, token)
	return err
}

// workspaceName resolves a workspace RID to its display name, or the RID when unnamed.
func (d *Datasource) workspaceName(ctx context.Context, token bearertoken.Token, workspaceRid rids.WorkspaceRid) (string, error) {
	workspace, err := d.workspaceService.GetWorkspace(ctx, token, workspaceRid)
	if err != nil {
		logErrorWithConjureFields("Workspace lookup failed", err, "workspaceRid", workspaceRid.String())
		if status := extractErrorDetails(err).Status; status == http.StatusForbidden || status == http.StatusNotFound {
			return "", errors.New(appendInstanceID("Workspace not found or not accessible with this API key", err))
		}
		message, _ := classifyConnectionError(err)
		return "", errors.New(message)
	}
	if workspace.DisplayName != nil && *workspace.DisplayName != "" {
		return *workspace.DisplayName, nil
	}
	return workspaceRid.String(), nil
}

// parseWorkspaceRid returns nil for an empty setting.
func parseWorkspaceRid(s string) (*rids.WorkspaceRid, error) {
	if s == "" {
		return nil, nil
	}
	parsed, err := rid.ParseRID(s)
	if err != nil {
		return nil, fmt.Errorf("Workspace RID %q is not a valid RID", s)
	}
	if parsed.Type != "workspace" {
		return nil, fmt.Errorf("Workspace RID %q has type %q, not workspace", s, parsed.Type)
	}
	return (*rids.WorkspaceRid)(&parsed), nil
}

// CallResource handles HTTP requests sent to the plugin.
func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	ctx = contextWithPluginRequestIdentity(ctx, req.PluginContext)
	log.DefaultLogger.Debug("=== CallResource called ===")
	log.DefaultLogger.Debug("CallResource called", "path", req.Path, "method", req.Method, "url", req.URL)
	return newNominalResourceHandler(d).Handle(ctx, req, sender)
}
