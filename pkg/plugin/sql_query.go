package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	"github.com/palantir/pkg/bearertoken"
)

const sqlWorkspaceHint = "Set Workspace RID in the data source settings; the SQL endpoint requires a workspace and this API key has no default workspace."
const sqlDisabledMessage = "SQL queries are disabled for this data source. Enable \"SQL queries\" in the data source settings."

type sqlWorkspaceCache struct {
	mu  sync.Mutex
	rid string
}

func (d *Datasource) resolveSqlWorkspace(ctx context.Context, token bearertoken.Token) (string, error) {
	if d.workspaceRid != nil {
		return d.workspaceRid.String(), nil
	}
	d.sqlWorkspace.mu.Lock()
	defer d.sqlWorkspace.mu.Unlock()
	if d.sqlWorkspace.rid != "" {
		return d.sqlWorkspace.rid, nil
	}
	if d.workspaceService == nil {
		return "", errors.New(sqlWorkspaceHint)
	}
	workspace, err := d.workspaceService.GetDefaultWorkspace(ctx, token)
	if err != nil {
		return "", fmt.Errorf("failed to resolve default workspace: %s", formatUserError("", err))
	}
	if workspace == nil {
		return "", errors.New(sqlWorkspaceHint)
	}
	d.sqlWorkspace.rid = workspace.Rid.String()
	return d.sqlWorkspace.rid, nil
}

func sqlFormatOption(format string) sqlutil.FormatQueryOption {
	if format == "table" {
		return sqlutil.FormatOptionTable
	}
	return sqlutil.FormatOptionTimeSeries
}

func (e *NominalQueryExecution) executeSqlQuery(ctx context.Context, prepared preparedQuery) backend.DataResponse {
	if !e.config.EnableSql {
		return backend.ErrDataResponse(backend.StatusBadRequest, sqlDisabledMessage)
	}
	token := bearertoken.Token(e.config.Secrets.ApiKey)
	workspace, err := e.datasource.resolveSqlWorkspace(ctx, token)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}
	expanded, err := interpolateSqlMacros(prepared.Model.RawSql, prepared.Query)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("Macro expansion failed: %v", err))
	}
	if e.datasource.sqlClient == nil {
		return backend.ErrDataResponse(backend.StatusInternal, "SQL client is not configured")
	}
	body, err := e.datasource.sqlClient.Query(ctx, string(token), workspace, expanded)
	if err != nil {
		var endpointErr *sqlEndpointError
		if errors.As(err, &endpointErr) {
			log.DefaultLogger.Warn("SQL query rejected", "status", endpointErr.Status, "errorName", endpointErr.ErrorName, "sqlQueryId", endpointErr.SqlQueryId, "errorInstanceId", endpointErr.InstanceId)
			return backend.ErrDataResponse(endpointErr.backendStatus(), endpointErr.Error())
		}
		if ctx.Err() != nil {
			return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("SQL query cancelled: %v", ctx.Err()))
		}
		log.DefaultLogger.Error("SQL request failed", "error", err)
		return backend.ErrDataResponse(backend.StatusInternal, err.Error())
	}
	defer body.Close()
	frame, err := frameFromArrowStream(body, prepared.Query.RefID)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, err.Error())
	}
	frame.RefID = prepared.Query.RefID
	frame.Meta = &data.FrameMeta{ExecutedQueryString: expanded}
	shaped, err := shapeSqlFrame(frame, sqlFormatOption(prepared.Model.Format))
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}
	return backend.DataResponse{Frames: data.Frames{shaped}}
}
