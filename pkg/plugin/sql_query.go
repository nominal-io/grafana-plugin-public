package plugin

import (
	"context"
	"errors"
	"runtime/debug"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	sdkconfig "github.com/grafana/grafana-plugin-sdk-go/config"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	"github.com/palantir/pkg/bearertoken"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// defaultSQLRowLimit matches the default of Grafana's [sql] row_limit, used when Grafana sends none.
	defaultSQLRowLimit = 1_000_000
	// sqlServiceTimeLimit is how long the SQL service lets a query run.
	sqlServiceTimeLimit = 2 * time.Minute
)

var errNoSQLWorkspace = errors.New("set Workspace RID in the data source settings: SQL queries need a workspace and this API key has no default workspace")

// resolveSQLWorkspace returns the configured workspace, or else the API key's default workspace,
// which is cached once a lookup succeeds.
func (d *Datasource) resolveSQLWorkspace(ctx context.Context, token bearertoken.Token) (string, error) {
	if d.workspaceRid != nil {
		return d.workspaceRid.String(), nil
	}
	if rid := d.defaultSQLWorkspace.Load(); rid != nil {
		return *rid, nil
	}
	if d.workspaceService == nil {
		return "", errNoSQLWorkspace
	}
	workspace, err := d.workspaceService.GetDefaultWorkspace(ctx, token)
	if err != nil {
		return "", errors.New(formatUserError("failed to resolve the API key's default workspace", err))
	}
	if workspace == nil {
		return "", errNoSQLWorkspace
	}
	rid := workspace.Rid.String()
	d.defaultSQLWorkspace.Store(&rid)
	return rid, nil
}

// executeSQLQuery expands macros, runs the query and shapes the result for its format.
func (e *NominalQueryExecution) executeSQLQuery(ctx context.Context, prepared preparedQuery) (response backend.DataResponse) {
	defer recoverSQLQuery(ctx, &response)
	if e.datasource.sqlService == nil {
		message := "SQL queries are not configured for this data source"
		if e.datasource.sqlErr != nil {
			message = e.datasource.sqlErr.Error()
		}
		return backend.ErrDataResponse(backend.StatusBadRequest, message)
	}
	query := prepared.SQL
	expanded, err := sqlutil.Interpolate(query, sqlMacros)
	if err != nil {
		return backend.ErrDataResponseWithSource(backend.StatusBadRequest, backend.ErrorSourceDownstream, "Macro expansion failed: "+err.Error())
	}
	frame, err := e.runSQLQuery(ctx, query.RefID, expanded)
	if err != nil {
		failed := sqlErrorResponse(ctx, err)
		failed.Frames = sqlutil.ErrorFrameFromQuery(query.WithSQL(expanded))
		return failed
	}
	frame.RefID = query.RefID
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}
	frame.Meta.ExecutedQueryString = expanded
	frame.Meta.PreferredVisualization = data.VisTypeGraph
	if query.Format == sqlutil.FormatOptionTable {
		frame.Meta.PreferredVisualization = data.VisTypeTable
	}
	shaped, err := shapeSQLFrame(frame, query.Format)
	if err != nil {
		return backend.ErrDataResponseWithSource(backend.StatusBadRequest, backend.ErrorSourceDownstream, err.Error())
	}
	return backend.DataResponse{Frames: data.Frames{shaped}}
}

// runSQLQuery runs sql in the data source's workspace and decodes the Arrow result.
func (e *NominalQueryExecution) runSQLQuery(ctx context.Context, refID, sql string) (*data.Frame, error) {
	token := bearertoken.Token(e.config.Secrets.ApiKey)
	workspace, err := e.datasource.resolveSQLWorkspace(ctx, token)
	if err != nil {
		return nil, err
	}
	// Cancelling ends the stream if the row limit stops reading early.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := e.datasource.sqlService.Query(withBearerToken(ctx, token), &sqlv1.SqlServiceQueryRequest{
		Query:        sql,
		WorkspaceRid: workspace,
		ResultFormat: sqlv1.SqlServiceQueryResultFormat_SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM,
	})
	if err != nil {
		return nil, err
	}
	return frameFromArrowStream(&sqlStreamReader{stream: stream}, refID, sqlRowLimit(ctx))
}

// sqlRowLimit returns the row limit Grafana applies to its own SQL data sources.
func sqlRowLimit(ctx context.Context) int64 {
	if cfg := sdkconfig.GrafanaConfigFromContext(ctx); cfg != nil {
		if sql, err := cfg.SQL(); err == nil && sql.RowLimit > 0 {
			return sql.RowLimit
		}
	}
	return defaultSQLRowLimit
}

// sqlErrorResponse maps a failed SQL query to a response. Errors reported by the SQL service,
// timeouts and cancellations are downstream errors, so Grafana does not count them against the plugin.
func sqlErrorResponse(ctx context.Context, err error) backend.DataResponse {
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return backend.ErrDataResponseWithSource(backend.StatusInternal, backend.ErrorSourceDownstream, "SQL query was cancelled")
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return backend.ErrDataResponseWithSource(backend.StatusTimeout, backend.ErrorSourceDownstream, "SQL query timed out")
	case errors.Is(err, errNoSQLWorkspace):
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}
	logger := log.DefaultLogger.FromContext(ctx)
	// status.FromError would also find a wrapped status, but it replaces the message with the whole
	// error text, including the wrapping.
	var grpcErr interface{ GRPCStatus() *status.Status }
	if errors.As(err, &grpcErr) {
		st := grpcErr.GRPCStatus()
		message := sqlErrorMessage(st)
		logger.Warn("SQL query rejected", "code", st.Code().String(), "error", message)
		return backend.ErrDataResponseWithSource(grafanaStatus(st.Code()), backend.ErrorSourceDownstream, message)
	}
	logger.Error("SQL query failed", "error", err)
	return backend.ErrDataResponse(backend.StatusInternal, err.Error())
}

// sqlErrorMessage prefers the specific detail the SQL service attaches to a status over the
// status's generic message, and appends the service's query ID.
func sqlErrorMessage(st *status.Status) string {
	message, queryID := st.Message(), ""
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			if specific := info.GetMetadata()["detail"]; specific != "" {
				message = specific
			}
			queryID = info.GetMetadata()["sqlQueryId"]
		}
	}
	if message == "" {
		message = "SQL endpoint returned " + st.Code().String()
	}
	if queryID != "" {
		message += " (sqlQueryId: " + queryID + ")"
	}
	return message
}

func grafanaStatus(code codes.Code) backend.Status {
	switch code {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return backend.StatusBadRequest
	case codes.NotFound:
		return backend.StatusNotFound
	case codes.Unauthenticated:
		return backend.StatusUnauthorized
	case codes.PermissionDenied:
		return backend.StatusForbidden
	case codes.ResourceExhausted:
		return backend.StatusTooManyRequests
	case codes.DeadlineExceeded:
		return backend.StatusTimeout
	case codes.Unavailable:
		return backend.StatusBadGateway
	default:
		return backend.StatusInternal
	}
}

// recoverSQLQuery turns a panic into an error response. SQL queries run on their own goroutines,
// outside the SDK's recovery, where a panic would stop the plugin process.
func recoverSQLQuery(ctx context.Context, response *backend.DataResponse) {
	if r := recover(); r != nil {
		log.DefaultLogger.FromContext(ctx).Error("SQL query panicked", "panic", r, "stack", string(debug.Stack()))
		*response = backend.ErrDataResponse(backend.StatusInternal, "SQL query failed unexpectedly")
	}
}

func sqlFormat(format string) sqlutil.FormatQueryOption {
	if format == "table" {
		return sqlutil.FormatOptionTable
	}
	return sqlutil.FormatOptionTimeSeries
}
