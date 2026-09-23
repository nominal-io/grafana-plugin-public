package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/palantir/pkg/rid"
)

type NominalResourceHandler struct {
	datasource *Datasource
}

func newNominalResourceHandler(datasource *Datasource) *NominalResourceHandler {
	return &NominalResourceHandler{datasource: datasource}
}

func (h *NominalResourceHandler) Handle(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	path := normalizeResourcePath(req.Path)

	switch path {
	case "channels":
		log.DefaultLogger.Debug("Handling channels search request")
		return h.handleChannelsSearch(ctx, req, sender)
	case "assets":
		log.DefaultLogger.Debug("Handling assets variable request")
		return h.handleAssetsVariable(ctx, req, sender)
	case "datascopes":
		return h.handleDatascopesVariable(ctx, req, sender)
	case "channelvariables":
		return h.handleChannelVariables(ctx, req, sender)
	// Old names kept for browser tabs holding a stale bundle.
	case "search-assets", "scout/v1/search-assets":
		return h.handleSearchAssets(ctx, req, sender)
	case "assets-by-rid", "scout/v1/asset/multiple":
		return h.handleAssetsByRid(ctx, req, sender)
	}

	return jsonErrorResponse(sender, http.StatusNotFound, "Unknown resource path")
}

func normalizeResourcePath(path string) string {
	return strings.TrimLeft(path, "/")
}

func jsonBytesResponse(sender backend.CallResourceResponseSender, status int, body []byte) error {
	return sender.Send(&backend.CallResourceResponse{
		Status: status,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: body,
	})
}

func jsonMarshalResponse(sender backend.CallResourceResponseSender, status int, body any) error {
	responseBytes, err := json.Marshal(body)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal resource response", "error", err)
		return jsonBytesResponse(sender, http.StatusInternalServerError, []byte(`{"error":"Failed to marshal response"}`))
	}
	return jsonBytesResponse(sender, status, responseBytes)
}

func jsonErrorResponse(sender backend.CallResourceResponseSender, status int, message string) error {
	return jsonMarshalResponse(sender, status, map[string]string{"error": message})
}

func decodeResourceJSON(body []byte, sender backend.CallResourceResponseSender, target any, logMessage string) (bool, error) {
	if err := json.Unmarshal(body, target); err != nil {
		log.DefaultLogger.Error(logMessage, "error", err)
		return false, jsonErrorResponse(sender, http.StatusBadRequest, "Invalid request body")
	}
	return true, nil
}

func decodeOptionalResourceJSON(req *backend.CallResourceRequest, sender backend.CallResourceResponseSender, target any, logMessage string) (bool, error) {
	if req.Body == nil || len(req.Body) == 0 {
		return true, nil
	}
	return decodeResourceJSON(req.Body, sender, target, logMessage)
}

func loadResourceSettings(settings backend.DataSourceInstanceSettings, sender backend.CallResourceResponseSender, logMessage string) (*models.PluginSettings, bool, error) {
	config, err := models.LoadPluginSettings(settings)
	if err != nil {
		log.DefaultLogger.Error(logMessage, "error", err)
		return nil, false, jsonErrorResponse(sender, http.StatusInternalServerError, "Failed to load settings")
	}
	return config, true, nil
}

func requirePost(req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) (bool, error) {
	if req.Method == http.MethodPost {
		return true, nil
	}
	return false, jsonErrorResponse(sender, http.StatusMethodNotAllowed, "Method not allowed. Use POST.")
}

func (h *NominalResourceHandler) handleSearchAssets(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	var search map[string]any
	if ok, err := decodeResourceJSON(req.Body, sender, &search, "Failed to parse search-assets request body"); !ok {
		return err
	}
	if search == nil {
		return jsonErrorResponse(sender, http.StatusBadRequest, "Invalid request body")
	}
	config, ok, err := loadResourceSettings(h.datasource.settings, sender, "search-assets: failed to load settings")
	if !ok {
		return err
	}
	search["query"] = withWorkspaceFilter(search["query"], config.WorkspaceRid)
	return h.nominalPostResponse(ctx, sender, config, "/scout/v1/search-assets", search)
}

func (h *NominalResourceHandler) handleAssetsByRid(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	var rids []string
	if ok, err := decodeResourceJSON(req.Body, sender, &rids, "Failed to parse assets-by-rid request body"); !ok {
		return err
	}
	if len(rids) == 0 {
		return jsonErrorResponse(sender, http.StatusBadRequest, "Invalid request body")
	}
	for _, r := range rids {
		if _, err := rid.ParseRID(r); err != nil {
			return jsonErrorResponse(sender, http.StatusBadRequest, "Invalid asset RID")
		}
	}
	config, ok, err := loadResourceSettings(h.datasource.settings, sender, "assets-by-rid: failed to load settings")
	if !ok {
		return err
	}
	return h.nominalPostResponse(ctx, sender, config, "/scout/v1/asset/multiple", rids)
}

// nominalPostResponse POSTs body to a fixed upstream path under the datasource API
// key and returns the upstream JSON body unchanged. Upstream error statuses
// are passed through, with the errorInstanceId appended to the message.
func (h *NominalResourceHandler) nominalPostResponse(ctx context.Context, sender backend.CallResourceResponseSender, config *models.PluginSettings, upstreamPath string, body any) error {
	responseBody, err := h.datasource.nominalCatalog.postNominalJSON(ctx, config, upstreamPath, body)
	if err != nil {
		logErrorWithConjureFields("Nominal API request failed", err, "path", upstreamPath)
		status := http.StatusBadGateway
		if d := extractErrorDetails(err); d.Status != 0 {
			status = d.Status
		}
		return jsonErrorResponse(sender, status, appendInstanceID("Nominal API request failed", err))
	}
	return jsonBytesResponse(sender, http.StatusOK, responseBody)
}
