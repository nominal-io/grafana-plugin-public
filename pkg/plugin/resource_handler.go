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
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

// proxyAllowedHeaders is the set of safe request headers forwarded to the
// upstream Nominal API. Sensitive caller context like Cookie and
// Authorization must never be relayed.
var proxyAllowedHeaders = map[string]bool{
	"Content-Type": true,
	"Accept":       true,
}

type NominalResourceHandler struct {
	datasource *Datasource
}

func newNominalResourceHandler(datasource *Datasource) *NominalResourceHandler {
	return &NominalResourceHandler{datasource: datasource}
}

func (h *NominalResourceHandler) Handle(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	path := normalizeResourcePath(req.Path)

	switch path {
	case "test", "connection-test":
		log.DefaultLogger.Debug("Handling test connection request")
		return h.handleTestConnection(ctx, req, sender)
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
	case "scout/v1/search-assets":
		return h.handleSearchAssets(ctx, req, sender)
	case "scout/v1/asset/multiple":
		return h.handleAssetMultiple(ctx, req, sender)
	}

	if strings.HasPrefix(path, "nominal/") {
		targetPath := strings.TrimPrefix(path, "nominal/")
		log.DefaultLogger.Debug("Stripped /nominal prefix", "newPath", targetPath)
		return h.handleNominalProxy(ctx, req, sender, targetPath)
	}

	log.DefaultLogger.Debug("Handling proxy request to Nominal API")
	return h.handleNominalProxy(ctx, req, sender, path)
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

// handleTestConnection handles the test connection endpoint.
func (h *NominalResourceHandler) handleTestConnection(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	// Add timeout to prevent hanging
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Load settings to get API key and base URL
	config, ok, err := loadResourceSettings(d.settings, sender, "Test connection: failed to load settings")
	if !ok {
		return err
	}

	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		log.DefaultLogger.Debug("Test connection: missing base URL")
		return jsonErrorResponse(sender, http.StatusBadRequest, "Base URL is required")
	}

	if config.Secrets.ApiKey == "" {
		log.DefaultLogger.Debug("Test connection: missing API key")
		return jsonErrorResponse(sender, http.StatusBadRequest, "API key is required")
	}

	// Test connection using conjure client with timeout
	bearerToken := bearertoken.Token(config.Secrets.ApiKey)
	profile, err := d.authService.GetMyProfile(ctxWithTimeout, bearerToken)
	if err != nil {
		logErrorWithConjureFields("Test connection failed", err)
		message, statusCode := classifyConnectionError(err)
		return jsonErrorResponse(sender, statusCode, message)
	}

	log.DefaultLogger.Debug("Test connection successful", "profileRid", profile.Rid)

	// Connection successful
	response := map[string]interface{}{
		"status":  "success",
		"message": "Successfully connected to Nominal API and retrieved user profile",
	}
	return jsonMarshalResponse(sender, http.StatusOK, response)
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
	return h.relayScoutPost(ctx, sender, config, "/scout/v1/search-assets", search)
}

func (h *NominalResourceHandler) handleAssetMultiple(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	var rids []string
	if ok, err := decodeResourceJSON(req.Body, sender, &rids, "Failed to parse asset/multiple request body"); !ok {
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
	config, ok, err := loadResourceSettings(h.datasource.settings, sender, "asset/multiple: failed to load settings")
	if !ok {
		return err
	}
	return h.relayScoutPost(ctx, sender, config, "/scout/v1/asset/multiple", rids)
}

// relayScoutPost POSTs body to a fixed upstream path under the datasource API
// key and returns the upstream JSON body unchanged. Upstream error statuses
// are passed through, with the errorInstanceId appended to the message.
func (h *NominalResourceHandler) relayScoutPost(ctx context.Context, sender backend.CallResourceResponseSender, config *models.PluginSettings, upstreamPath string, body any) error {
	responseBody, err := h.datasource.catalog().postNominalJSON(ctx, config, upstreamPath, body)
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

// handleNominalProxy handles proxying requests to Nominal API with secure API key injection.
func (h *NominalResourceHandler) handleNominalProxy(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender, targetPath string) error {
	d := h.datasource

	// Load settings to get API key and base URL
	config, ok, err := loadResourceSettings(d.settings, sender, "Proxy request: failed to load settings")
	if !ok {
		return err
	}

	apiKey := config.Secrets.ApiKey
	baseURL := config.GetAPIBaseURL()
	if baseURL == "" || apiKey == "" {
		return jsonErrorResponse(sender, http.StatusBadRequest, "Missing base URL or API key configuration")
	}

	// Construct the full target URL
	baseURL = strings.TrimSuffix(baseURL, "/")
	targetURL := baseURL + "/" + targetPath

	log.DefaultLogger.Debug("Proxy request", "fromPath", req.Path, "targetPath", targetPath, "toURL", targetURL)

	// Parse the target URL to ensure it's valid
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return fmt.Errorf("invalid target URL: %v", err)
	}

	reqBody := req.Body
	if targetPath == "scout/v1/search-assets" && config.WorkspaceRid != "" {
		var search map[string]interface{}
		if err := json.Unmarshal(reqBody, &search); err != nil || search == nil {
			return jsonErrorResponse(sender, http.StatusBadRequest, "Failed to parse search-assets request body")
		}
		search["query"] = withWorkspaceFilter(search["query"], config.WorkspaceRid)
		if reqBody, err = json.Marshal(search); err != nil {
			return fmt.Errorf("failed to encode search-assets request body: %v", err)
		}
	}

	// Create the proxied request
	var body io.Reader
	if reqBody != nil {
		body = bytes.NewReader(reqBody)
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
	proxyReq.Header.Set("Authorization", "Bearer "+apiKey)

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
