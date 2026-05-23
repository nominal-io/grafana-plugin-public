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
		return h.datasource.handleChannelsSearch(ctx, req, sender)
	case "assets":
		log.DefaultLogger.Debug("Handling assets variable request")
		return h.datasource.handleAssetsVariable(ctx, req, sender)
	case "datascopes":
		return h.datasource.handleDatascopesVariable(ctx, req, sender)
	case "channelvariables":
		return h.datasource.handleChannelVariables(ctx, req, sender)
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

func requirePost(req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) (bool, error) {
	if req.Method == http.MethodPost {
		return true, nil
	}
	return false, jsonBytesResponse(sender, http.StatusMethodNotAllowed, []byte(`{"error": "Method not allowed. Use POST."}`))
}

// handleTestConnection handles the test connection endpoint.
func (h *NominalResourceHandler) handleTestConnection(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

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
			Body: []byte(`{"error": "Failed to load settings"}`),
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
		logErrorWithConjureFields("Test connection failed", err)
		message, statusCode := classifyConnectionError(err)
		errBody, _ := json.Marshal(map[string]string{"error": message})
		return sender.Send(&backend.CallResourceResponse{
			Status: statusCode,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: errBody,
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

// handleNominalProxy handles proxying requests to Nominal API with secure API key injection.
func (h *NominalResourceHandler) handleNominalProxy(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender, targetPath string) error {
	d := h.datasource

	// Load settings to get API key and base URL
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		return fmt.Errorf("failed to load settings: %v", err)
	}

	apiKey := ""
	if config.Secrets != nil {
		apiKey = config.Secrets.ApiKey
	}
	baseURL := config.GetAPIBaseURL()
	if baseURL == "" || apiKey == "" {
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Missing base URL or API key configuration"}`),
		})
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
