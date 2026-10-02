package plugin

import (
	"context"
	"errors"
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

type channelsSearchRequest struct {
	DataSourceRids []string `json:"dataSourceRids"`
	SearchText     string   `json:"searchText"`
}

type channelSearchResult struct {
	Name        string `json:"name"`
	DataSource  string `json:"dataSource"`
	Description string `json:"description"`
	DataType    string `json:"dataType"`
}

type channelsSearchResponse struct {
	Channels []channelSearchResult `json:"channels"`
}

// handleChannelsSearch handles searching for channels in a data source
func (h *NominalResourceHandler) handleChannelsSearch(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	log.DefaultLogger.Debug("Channels search request", "method", req.Method, "bodyBytes", len(req.Body))

	if ok, err := requirePost(req, sender); !ok {
		return err
	}

	var searchRequest channelsSearchRequest
	if ok, err := decodeResourceJSON(req.Body, sender, &searchRequest, "Failed to parse channels search request body"); !ok {
		return err
	}

	config, ok, err := loadResourceSettings(d.settings, sender, "Failed to load settings for channels search")
	if !ok {
		return err
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
		return jsonErrorResponse(sender, http.StatusBadRequest, "No valid data source RIDs provided")
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
		return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Channels search failed", err))
	}

	channels := make([]channelSearchResult, 0, len(channelsResponse.Results))
	for _, channel := range channelsResponse.Results {
		channels = append(channels, channelSearchResult{
			Name:        string(channel.Name),
			DataSource:  channel.DataSource.String(),
			Description: getChannelMetadataDescription(channel),
			DataType:    getChannelDataType(channel),
		})
	}

	log.DefaultLogger.Debug("Channels search successful", "channelCount", len(channels))
	return jsonMarshalResponse(sender, http.StatusOK, channelsSearchResponse{Channels: channels})
}

// handleAssetsVariable handles the assets endpoint for Grafana template variables
// Returns a list of assets in MetricFindValue format: { text: "Asset Name", value: "ri.scout..." }
func (h *NominalResourceHandler) handleAssetsVariable(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	if ok, err := requirePost(req, sender); !ok {
		return err
	}

	log.DefaultLogger.Debug("Assets variable request")

	var searchRequest assetsVariableRequest

	if ok, err := decodeOptionalResourceJSON(req, sender, &searchRequest, "Failed to parse assets variable request body"); !ok {
		return err
	}

	config, ok, err := loadResourceSettings(d.settings, sender, "Failed to load settings for assets variable")
	if !ok {
		return err
	}

	result, err := d.templateVariableCatalog.Assets(ctx, config, searchRequest)
	if err != nil {
		logErrorWithConjureFields("Failed to fetch assets", err)
		return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Failed to fetch assets", err))
	}

	log.DefaultLogger.Debug("Assets variable request successful", "assetCount", len(result))
	return jsonMarshalResponse(sender, http.StatusOK, result)
}

// handleDatascopesVariable handles the datascopes endpoint for Grafana template variables
// Returns a list of datascopes for a given asset in MetricFindValue format: { text: "scope name", value: "scope name" }
func (h *NominalResourceHandler) handleDatascopesVariable(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	if ok, err := requirePost(req, sender); !ok {
		return err
	}

	log.DefaultLogger.Debug("Datascopes variable request")

	var searchRequest datascopesVariableRequest

	if ok, err := decodeOptionalResourceJSON(req, sender, &searchRequest, "Failed to parse datascopes request body"); !ok {
		return err
	}

	// Validate asset RID is provided
	if searchRequest.AssetRid == "" {
		return jsonErrorResponse(sender, http.StatusBadRequest, "assetRid is required")
	}

	// Must run before loadResourceSettings so unresolved vars return [] even when
	// settings are absent/invalid (the catalog re-checks only to skip the network call).
	if hasUnresolvedTemplateVariable(searchRequest.AssetRid) {
		log.DefaultLogger.Debug("Asset RID contains unresolved template variable", "assetRid", searchRequest.AssetRid)
		return jsonBytesResponse(sender, http.StatusOK, []byte("[]"))
	}

	config, ok, err := loadResourceSettings(d.settings, sender, "Failed to load settings for datascopes variable")
	if !ok {
		return err
	}

	result, err := d.templateVariableCatalog.Datascopes(ctx, config, searchRequest)
	if err != nil {
		logErrorWithConjureFields("Failed to fetch asset", err, "assetRid", searchRequest.AssetRid)
		return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Failed to fetch asset", err))
	}

	log.DefaultLogger.Debug("Datascopes variable request successful", "datascopeCount", len(result))
	return jsonMarshalResponse(sender, http.StatusOK, result)
}

// handleChannelVariables handles the channelvariables endpoint for Grafana template variables
// Returns a list of channel names for a given asset in MetricFindValue format: { text: "channel name", value: "channel name" }
func (h *NominalResourceHandler) handleChannelVariables(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	d := h.datasource

	if ok, err := requirePost(req, sender); !ok {
		return err
	}

	log.DefaultLogger.Debug("Channel variables request")

	var searchRequest channelVariablesRequest

	if ok, err := decodeOptionalResourceJSON(req, sender, &searchRequest, "Failed to parse channel variables request body"); !ok {
		return err
	}

	// Validate asset RID is provided
	if searchRequest.AssetRid == "" {
		return jsonErrorResponse(sender, http.StatusBadRequest, "assetRid is required")
	}

	// Must run before loadResourceSettings so unresolved vars return [] even when
	// settings are absent/invalid (the catalog re-checks only to skip the network call).
	if hasUnresolvedTemplateVariable(searchRequest.AssetRid, searchRequest.DataScopeName) {
		log.DefaultLogger.Debug("Request contains unresolved template variable", "assetRid", searchRequest.AssetRid, "dataScopeName", searchRequest.DataScopeName)
		return jsonBytesResponse(sender, http.StatusOK, []byte("[]"))
	}

	config, ok, err := loadResourceSettings(d.settings, sender, "Failed to load settings for channel variables")
	if !ok {
		return err
	}

	result, err := d.templateVariableCatalog.ChannelVariables(ctx, config, searchRequest)
	if err != nil {
		var catalogErr *templateVariableCatalogError
		if errors.As(err, &catalogErr) && catalogErr.kind == templateVariableAssetFetchError {
			logErrorWithConjureFields("Failed to fetch asset", err, "assetRid", searchRequest.AssetRid)
			return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Failed to fetch asset", err))
		}
		logErrorWithConjureFields("Channels search API call failed", err)
		return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Channels search failed", err))
	}

	log.DefaultLogger.Debug("Channel variables request successful", "channelCount", len(result))
	return jsonMarshalResponse(sender, http.StatusOK, result)
}

type runsRequest struct {
	AssetRids  []string `json:"assetRids"`
	SearchText string   `json:"searchText"`
}

type runRequest struct {
	RunRid string `json:"runRid"`
}

type runItem struct {
	Rid       string   `json:"rid"`
	Title     string   `json:"title"`
	RunNumber int64    `json:"runNumber"`
	StartMs   int64    `json:"startMs"`
	EndMs     *int64   `json:"endMs,omitempty"`
	AssetRids []string `json:"assetRids"`
}

func toRunItem(run RunResponse) runItem {
	item := runItem{
		Rid:       run.Rid,
		Title:     run.Title,
		RunNumber: run.RunNumber,
		StartMs:   run.StartTime.UnixMilli(),
		AssetRids: run.Assets,
	}
	if run.EndTime != nil {
		endMs := run.EndTime.UnixMilli()
		item.EndMs = &endMs
	}
	return item
}

func (h *NominalResourceHandler) handleRuns(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	var body runsRequest
	if ok, err := decodeOptionalResourceJSON(req, sender, &body, "Failed to parse runs request body"); !ok {
		return err
	}
	if hasUnresolvedTemplateVariable(append([]string{body.SearchText}, body.AssetRids...)...) {
		return jsonBytesResponse(sender, http.StatusOK, []byte("[]"))
	}
	config, ok, err := loadResourceSettings(h.datasource.settings, sender, "Failed to load settings for runs")
	if !ok {
		return err
	}
	runs, err := h.datasource.nominalCatalog.SearchRuns(ctx, config, body.AssetRids, body.SearchText)
	if err != nil {
		logErrorWithConjureFields("Failed to search runs", err)
		return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Failed to search runs", err))
	}
	items := make([]runItem, 0, len(runs))
	for _, run := range runs {
		items = append(items, toRunItem(run))
	}
	return jsonMarshalResponse(sender, http.StatusOK, items)
}

func (h *NominalResourceHandler) handleRun(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	var body runRequest
	if ok, err := decodeResourceJSON(req.Body, sender, &body, "Failed to parse run request body"); !ok {
		return err
	}
	if !ridHasType(body.RunRid, "run") {
		return jsonErrorResponse(sender, http.StatusBadRequest, "runRid must be a run RID")
	}
	config, ok, err := loadResourceSettings(h.datasource.settings, sender, "Failed to load settings for run")
	if !ok {
		return err
	}
	run, err := h.datasource.nominalCatalog.FetchRunByRid(ctx, config, body.RunRid)
	if err != nil {
		logErrorWithConjureFields("Failed to fetch run", err, "runRid", body.RunRid)
		return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Failed to fetch run", err))
	}
	if run == nil {
		return jsonErrorResponse(sender, http.StatusNotFound, "Run not found")
	}
	return jsonMarshalResponse(sender, http.StatusOK, toRunItem(*run))
}
