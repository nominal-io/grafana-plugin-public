package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

// handleChannelsSearch handles searching for channels in a data source
func (h *NominalResourceHandler) handleChannelsSearch(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	log.DefaultLogger.Debug("Channels search request", "method", req.Method, "bodyBytes", len(req.Body))

	if ok, err := requirePost(req, sender); !ok {
		return err
	}

	// Parse request body
	var searchRequest struct {
		DataSourceRids []string `json:"dataSourceRids"`
		SearchText     string   `json:"searchText"`
	}

	if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
		log.DefaultLogger.Error("Failed to parse channels search request body", "error", err)
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Invalid request body"}`),
		})
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load settings for channels search", "error", err)
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusInternalServerError,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Failed to load settings"}`),
		})
	}

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)

	// Convert string RIDs to proper datasource RID types
	var dataSourceRids []rids.DataSourceRid
	for _, ridStr := range searchRequest.DataSourceRids {
		if parsedRid, err := rid.ParseRID(ridStr); err != nil {
			log.DefaultLogger.Warn("Failed to parse data source RID", "rid", ridStr, "error", err)
			continue
		} else {
			dataSourceRids = append(dataSourceRids, rids.DataSourceRid(parsedRid))
		}
	}

	if len(dataSourceRids) == 0 {
		log.DefaultLogger.Warn("No valid data source RIDs provided")
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusBadRequest,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "No valid data source RIDs provided"}`),
		})
	}

	// Build the search request with correct field names
	searchChannelsRequest := datasourceapi.SearchChannelsRequest{
		FuzzySearchText: searchRequest.SearchText,
		DataSources:     dataSourceRids,
	}

	log.DefaultLogger.Debug("Making channels search API call", "dataSourceCount", len(dataSourceRids), "searchTextLength", len(searchRequest.SearchText))

	// Make the API call using the datasource service
	channelsResponse, err := d.datasourceService.SearchChannels(ctx, bearerToken, searchChannelsRequest)
	if err != nil {
		logErrorWithConjureFields("Channels search API call failed", err)
		errBody, _ := json.Marshal(map[string]string{"error": appendInstanceID("Channels search failed", err)})
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusInternalServerError,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: errBody,
		})
	}

	// Transform the API response to the expected format
	var channels []map[string]interface{}
	for _, channel := range channelsResponse.Results {
		channelMap := map[string]interface{}{
			"name":        string(channel.Name),
			"dataSource":  channel.DataSource.String(),
			"description": getChannelMetadataDescription(channel),
			"dataType":    getChannelDataType(channel),
		}
		channels = append(channels, channelMap)
	}

	apiResponse := map[string]interface{}{
		"channels": channels,
	}

	// Convert response to JSON
	responseBytes, err := json.Marshal(apiResponse)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal channels search response", "error", err)
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusInternalServerError,
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: []byte(`{"error": "Failed to marshal response"}`),
		})
	}

	log.DefaultLogger.Debug("Channels search successful", "channelCount", len(channels))

	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}

// handleAssetsVariable handles the assets endpoint for Grafana template variables
// Returns a list of assets in MetricFindValue format: { text: "Asset Name", value: "ri.scout..." }
func (h *NominalResourceHandler) handleAssetsVariable(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	if ok, err := requirePost(req, sender); !ok {
		return err
	}

	log.DefaultLogger.Debug("Assets variable request")

	// Parse optional request body for search/filter parameters
	var searchRequest struct {
		SearchText string `json:"searchText"`
		MaxResults int    `json:"maxResults"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Error("Failed to parse assets variable request body", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Invalid request body"})
			return sender.Send(&backend.CallResourceResponse{
				Status:  http.StatusBadRequest,
				Headers: map[string][]string{"Content-Type": {"application/json"}},
				Body:    errBody,
			})
		}
	}

	// Set defaults
	if searchRequest.MaxResults == 0 {
		searchRequest.MaxResults = 500
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load settings for assets variable", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Fetch assets with pagination
	assetResponses, err := d.fetchAssetsForVariable(ctx, config, searchRequest.SearchText, searchRequest.MaxResults)
	if err != nil {
		logErrorWithConjureFields("Failed to fetch assets", err)
		errBody, _ := json.Marshal(map[string]string{"error": appendInstanceID("Failed to fetch assets", err)})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Transform to MetricFindValue format: { text: "name", value: "rid" }
	// Filter to assets with supported data sources
	result := make([]map[string]string, 0)
outer:
	for _, resp := range assetResponses {
		for _, asset := range resp.Results {
			hasSupported := false
			for _, scope := range asset.DataScopes {
				if isSupportedDataSourceType(scope.DataSource.Type) {
					hasSupported = true
					break
				}
			}
			if hasSupported {
				result = append(result, map[string]string{
					"text":  asset.Title,
					"value": asset.Rid,
				})
				if len(result) >= searchRequest.MaxResults {
					break outer
				}
			}
		}
	}

	responseBytes, err := json.Marshal(result)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal assets variable response", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	log.DefaultLogger.Debug("Assets variable request successful", "assetCount", len(result))

	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}

// handleDatascopesVariable handles the datascopes endpoint for Grafana template variables
// Returns a list of datascopes for a given asset in MetricFindValue format: { text: "scope name", value: "scope name" }
func (h *NominalResourceHandler) handleDatascopesVariable(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	if ok, err := requirePost(req, sender); !ok {
		return err
	}

	log.DefaultLogger.Debug("Datascopes variable request")

	// Parse request body for asset RID
	var searchRequest struct {
		AssetRid string `json:"assetRid"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Error("Failed to parse datascopes request body", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Invalid request body"})
			return sender.Send(&backend.CallResourceResponse{
				Status:  http.StatusBadRequest,
				Headers: map[string][]string{"Content-Type": {"application/json"}},
				Body:    errBody,
			})
		}
	}

	// Validate asset RID is provided
	if searchRequest.AssetRid == "" {
		errBody, _ := json.Marshal(map[string]string{"error": "assetRid is required"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusBadRequest,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Check if asset RID contains unresolved template variable
	if strings.Contains(searchRequest.AssetRid, "$") {
		log.DefaultLogger.Debug("Asset RID contains unresolved template variable", "assetRid", searchRequest.AssetRid)
		// Return empty array - variable not yet resolved
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load settings for datascopes variable", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Fetch asset by RID to get its datascopes
	asset, err := d.fetchAssetByRid(ctx, config, searchRequest.AssetRid)
	if err != nil {
		logErrorWithConjureFields("Failed to fetch asset", err, "assetRid", searchRequest.AssetRid)
		errBody, _ := json.Marshal(map[string]string{"error": appendInstanceID("Failed to fetch asset", err)})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	if asset == nil {
		log.DefaultLogger.Debug("Asset not found", "assetRid", searchRequest.AssetRid)
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	// Transform datascopes to MetricFindValue format: { text: "name", value: "name" }
	// Filter to supported data source types
	result := make([]map[string]string, 0)
	for _, scope := range asset.DataScopes {
		dsType := scope.DataSource.Type
		if isSupportedDataSourceType(dsType) {
			result = append(result, map[string]string{
				"text":  scope.DataScopeName,
				"value": scope.DataScopeName,
			})
		}
	}

	responseBytes, err := json.Marshal(result)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal datascopes response", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	log.DefaultLogger.Debug("Datascopes variable request successful", "datascopeCount", len(result))

	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}

// handleChannelVariables handles the channelvariables endpoint for Grafana template variables
// Returns a list of channel names for a given asset in MetricFindValue format: { text: "channel name", value: "channel name" }
func (h *NominalResourceHandler) handleChannelVariables(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	if ok, err := requirePost(req, sender); !ok {
		return err
	}

	log.DefaultLogger.Debug("Channel variables request")

	// Parse request body for asset RID and optional datascope filter
	var searchRequest struct {
		AssetRid      string `json:"assetRid"`
		DataScopeName string `json:"dataScopeName"`
	}

	if req.Body != nil && len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &searchRequest); err != nil {
			log.DefaultLogger.Error("Failed to parse channel variables request body", "error", err)
			errBody, _ := json.Marshal(map[string]string{"error": "Invalid request body"})
			return sender.Send(&backend.CallResourceResponse{
				Status:  http.StatusBadRequest,
				Headers: map[string][]string{"Content-Type": {"application/json"}},
				Body:    errBody,
			})
		}
	}

	// Validate asset RID is provided
	if searchRequest.AssetRid == "" {
		errBody, _ := json.Marshal(map[string]string{"error": "assetRid is required"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusBadRequest,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Check if any parameter contains unresolved template variable
	if strings.Contains(searchRequest.AssetRid, "$") || strings.Contains(searchRequest.DataScopeName, "$") {
		log.DefaultLogger.Debug("Request contains unresolved template variable", "assetRid", searchRequest.AssetRid, "dataScopeName", searchRequest.DataScopeName)
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	// Load settings to get API key
	config, err := models.LoadPluginSettings(d.settings)
	if err != nil {
		log.DefaultLogger.Error("Failed to load settings for channel variables", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to load settings"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	// Fetch asset by RID to get its datascopes and datasource RIDs
	asset, err := d.fetchAssetByRid(ctx, config, searchRequest.AssetRid)
	if err != nil {
		logErrorWithConjureFields("Failed to fetch asset", err, "assetRid", searchRequest.AssetRid)
		errBody, _ := json.Marshal(map[string]string{"error": appendInstanceID("Failed to fetch asset", err)})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	if asset == nil {
		log.DefaultLogger.Debug("Asset not found", "assetRid", searchRequest.AssetRid)
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	// Extract datasource RIDs from the asset's datascopes, optionally filtered by dataScopeName
	var dataSourceRids []rids.DataSourceRid
	for _, scope := range asset.DataScopes {
		// If a dataScopeName filter is provided, only include matching scopes
		if searchRequest.DataScopeName != "" && scope.DataScopeName != searchRequest.DataScopeName {
			continue
		}

		ridStr, ok := dataSourceRidFor(scope.DataSource)
		if !ok {
			continue
		}

		if parsedRid, err := rid.ParseRID(ridStr); err == nil {
			dataSourceRids = append(dataSourceRids, rids.DataSourceRid(parsedRid))
		} else {
			log.DefaultLogger.Warn("Failed to parse data source RID", "rid", ridStr, "error", err)
		}
	}

	if len(dataSourceRids) == 0 {
		log.DefaultLogger.Debug("No data source RIDs found for asset", "assetRid", searchRequest.AssetRid)
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusOK,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte("[]"),
		})
	}

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)

	// Paginate through all channels. The SearchChannels API caps at 1000 per call,
	// so we loop with NextPageToken to fetch the complete list for template variables.
	const maxChannelVariables = 5000
	pageSize := 1000
	var allChannelResults []datasourceapi.ChannelMetadata
	var nextPageToken *api.Token

	for page := 0; ; page++ {
		searchChannelsRequest := datasourceapi.SearchChannelsRequest{
			FuzzySearchText: "",
			DataSources:     dataSourceRids,
			PageSize:        &pageSize,
			NextPageToken:   nextPageToken,
		}

		channelsResponse, err := d.datasourceService.SearchChannels(ctx, bearerToken, searchChannelsRequest)
		if err != nil {
			logErrorWithConjureFields("Channels search API call failed", err)
			errBody, _ := json.Marshal(map[string]string{"error": appendInstanceID("Channels search failed", err)})
			return sender.Send(&backend.CallResourceResponse{
				Status:  http.StatusInternalServerError,
				Headers: map[string][]string{"Content-Type": {"application/json"}},
				Body:    errBody,
			})
		}

		allChannelResults = append(allChannelResults, channelsResponse.Results...)

		if channelsResponse.NextPageToken == nil || len(allChannelResults) >= maxChannelVariables || len(channelsResponse.Results) == 0 {
			break
		}
		nextPageToken = channelsResponse.NextPageToken
	}

	// Hard cap: a page append could overshoot if maxChannelVariables is not a
	// multiple of the page size, so truncate any excess here.
	if len(allChannelResults) > maxChannelVariables {
		allChannelResults = allChannelResults[:maxChannelVariables]
	}

	// Deduplicate channel names and return as MetricFindValue format
	seen := make(map[string]bool)
	result := make([]map[string]string, 0)
	for _, channel := range allChannelResults {
		name := string(channel.Name)
		if !seen[name] {
			seen[name] = true
			result = append(result, map[string]string{
				"text":  name,
				"value": name,
			})
		}
	}

	responseBytes, err := json.Marshal(result)
	if err != nil {
		log.DefaultLogger.Error("Failed to marshal channel variables response", "error", err)
		errBody, _ := json.Marshal(map[string]string{"error": "Failed to marshal response"})
		return sender.Send(&backend.CallResourceResponse{
			Status:  http.StatusInternalServerError,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    errBody,
		})
	}

	log.DefaultLogger.Debug("Channel variables request successful", "channelCount", len(result))

	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: responseBytes,
	})
}
