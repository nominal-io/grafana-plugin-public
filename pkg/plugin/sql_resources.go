package plugin

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/palantir/pkg/bearertoken"
)

// sqlStringLiteral is only for values; identifiers in metadata queries are fixed.
func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// handleSqlOptions powers the SQL builder with workspace-scoped metadata.
// Each request returns at most 100 choices; typing narrows the server-side search.
func (h *NominalResourceHandler) handleSqlOptions(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender, path string) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	config, ok, err := loadResourceSettings(h.datasource.settings, sender, "SQL options: failed to load settings")
	if !ok {
		return err
	}
	if !config.UsesSQL() {
		return jsonErrorResponse(sender, http.StatusBadRequest, sqlDisabledMessage)
	}
	var input struct {
		SearchText string `json:"searchText"`
		DatasetRid string `json:"datasetRid"`
	}
	if ok, err := decodeOptionalResourceJSON(req, sender, &input, "Invalid SQL options request"); !ok {
		return err
	}
	if len(input.SearchText) > 1024 || len(input.DatasetRid) > 1024 {
		return jsonErrorResponse(sender, http.StatusBadRequest, "Search text or dataset RID is too long")
	}
	workspace, err := h.datasource.resolveSqlWorkspace(ctx, bearertoken.Token(config.Secrets.ApiKey))
	if err != nil {
		return jsonErrorResponse(sender, http.StatusBadRequest, err.Error())
	}
	if h.datasource.sqlClient == nil {
		return jsonErrorResponse(sender, http.StatusInternalServerError, "SQL client is not configured")
	}
	if path == "sql/datasets" {
		// The paginated catalog API avoids a full workspace scan through SQL.
		options, err := h.datasource.sqlClient.DatasetOptions(ctx, config.Secrets.ApiKey, workspace, input.SearchText)
		if err != nil {
			return jsonErrorResponse(sender, http.StatusBadGateway, err.Error())
		}
		return jsonMarshalResponse(sender, http.StatusOK, options)
	}
	if strings.TrimSpace(input.DatasetRid) == "" {
		return jsonErrorResponse(sender, http.StatusBadRequest, "Dataset RID is required")
	}
	// POSITION treats %, _ and backslashes literally rather than as LIKE patterns.
	filter := "POSITION(" + sqlStringLiteral(strings.ToLower(input.SearchText)) + " IN LOWER(channel)) > 0"
	query := "SELECT DISTINCT channel, channel AS value FROM channels WHERE dataset_rid = " + sqlStringLiteral(input.DatasetRid) + " AND " + filter + " ORDER BY channel LIMIT 100"
	body, err := h.datasource.sqlClient.Query(ctx, config.Secrets.ApiKey, workspace, query)
	if err != nil {
		var endpointErr *sqlEndpointError
		if errors.As(err, &endpointErr) {
			return jsonErrorResponse(sender, endpointErr.Status, endpointErr.Error())
		}
		return jsonErrorResponse(sender, http.StatusBadGateway, "SQL metadata request failed")
	}
	defer body.Close()
	frame, err := frameFromArrowStream(body, "options")
	if err != nil || len(frame.Fields) != 2 {
		return jsonErrorResponse(sender, http.StatusBadGateway, "Invalid SQL metadata response")
	}
	options := make([]map[string]string, 0, min(frame.Rows(), 100))
	for i := 0; i < min(frame.Rows(), 100); i++ {
		label, labelOK := frame.Fields[0].ConcreteAt(i)
		value, valueOK := frame.Fields[1].ConcreteAt(i)
		labelString, labelIsString := label.(string)
		valueString, valueIsString := value.(string)
		if !labelOK || !valueOK || !labelIsString || !valueIsString {
			return jsonErrorResponse(sender, http.StatusBadGateway, "Invalid SQL metadata option")
		}
		options = append(options, map[string]string{"label": labelString, "value": valueString})
	}
	return jsonMarshalResponse(sender, http.StatusOK, options)
}
