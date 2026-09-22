package plugin

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	sdkconfig "github.com/grafana/grafana-plugin-sdk-go/config"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	"github.com/palantir/pkg/bearertoken"
	"golang.org/x/sync/singleflight"
)

const (
	// defaultSQLRowLimit matches the default of Grafana's [sql] row_limit, used when Grafana sends none.
	defaultSQLRowLimit = 1_000_000
	// sqlServiceTimeLimit is how long the SQL service lets a query run.
	sqlServiceTimeLimit    = 2 * time.Minute
	workspaceLookupTimeout = 30 * time.Second
)

var errNoSQLWorkspace = errors.New("set Workspace RID in the data source settings: SQL queries need a workspace and this API key has no default workspace")

// sqlWorkspaceError reports a failed lookup of the API key's default workspace.
type sqlWorkspaceError struct{ err error }

func (e *sqlWorkspaceError) Error() string {
	return formatUserError("failed to resolve the API key's default workspace", e.err)
}

func (e *sqlWorkspaceError) Unwrap() error { return e.err }

// sqlWorkspaceCache remembers the API key's default workspace once a lookup succeeds.
type sqlWorkspaceCache struct {
	mu     sync.Mutex
	rid    string
	lookup singleflight.Group
}

func (c *sqlWorkspaceCache) get() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rid
}

func (c *sqlWorkspaceCache) set(rid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rid = rid
}

// resolveSQLWorkspace returns the configured workspace, or else the API key's default workspace.
// Concurrent queries share one lookup, and a failed lookup is retried by the next query.
func (d *Datasource) resolveSQLWorkspace(ctx context.Context, token bearertoken.Token) (string, error) {
	if d.workspaceRid != nil {
		return d.workspaceRid.String(), nil
	}
	if rid := d.sqlWorkspace.get(); rid != "" {
		return rid, nil
	}
	if d.workspaceService == nil {
		return "", errNoSQLWorkspace
	}
	lookup := d.sqlWorkspace.lookup.DoChan("", func() (any, error) {
		// Other queries may be waiting on this lookup, so one caller's cancellation must not end it.
		lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workspaceLookupTimeout)
		defer cancel()
		workspace, err := d.workspaceService.GetDefaultWorkspace(lookupCtx, token)
		if err != nil {
			return "", &sqlWorkspaceError{err: err}
		}
		if workspace == nil {
			return "", errNoSQLWorkspace
		}
		d.sqlWorkspace.set(workspace.Rid.String())
		return workspace.Rid.String(), nil
	})
	select {
	case result := <-lookup:
		if result.Err != nil {
			return "", result.Err
		}
		return result.Val.(string), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
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
	expanded, err := interpolateSQLMacros(query)
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
		return nil, sqlQueryError(ctx, err)
	}
	return frameFromArrowStream(newSQLStreamReader(ctx, stream), refID, sqlRowLimit(ctx))
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

// sqlErrorResponse maps a failed SQL query to a response. Errors reported by Nominal, timeouts
// and cancellations are downstream errors, so Grafana does not count them against the plugin.
func sqlErrorResponse(ctx context.Context, err error) backend.DataResponse {
	logger := log.DefaultLogger.FromContext(ctx)
	if endpoint, ok := errors.AsType[*sqlEndpointError](err); ok {
		logger.Warn("SQL query rejected", "code", endpoint.Code.String(), "reason", endpoint.Reason, "sqlQueryId", endpoint.QueryID)
		return backend.ErrDataResponseWithSource(endpoint.backendStatus(), backend.ErrorSourceDownstream, endpoint.Error())
	}
	switch {
	case errors.Is(err, context.Canceled):
		return backend.ErrDataResponseWithSource(backend.StatusInternal, backend.ErrorSourceDownstream, "SQL query was cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return backend.ErrDataResponseWithSource(backend.StatusTimeout, backend.ErrorSourceDownstream, "SQL query timed out")
	case errors.Is(err, errNoSQLWorkspace):
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}
	if lookup, ok := errors.AsType[*sqlWorkspaceError](err); ok {
		logErrorWithConjureFields("Default workspace lookup failed", lookup.err)
		_, status := classifyConnectionError(lookup.err)
		return backend.ErrDataResponseWithSource(backend.Status(status), backend.ErrorSourceDownstream, lookup.Error())
	}
	logger.Error("SQL query failed", "error", err)
	return backend.ErrDataResponse(backend.StatusInternal, err.Error())
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
