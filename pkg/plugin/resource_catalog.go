package plugin

import (
	"context"
	"net/http"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

type TemplateVariableCatalog struct {
	nominal *NominalCatalog
}

func newTemplateVariableCatalog(nominal *NominalCatalog) *TemplateVariableCatalog {
	return &TemplateVariableCatalog{nominal: nominal}
}

type metricFindValue struct {
	Text  string `json:"text"`
	Value string `json:"value"`
}

type assetsVariableRequest struct {
	SearchText string `json:"searchText"`
	MaxResults int    `json:"maxResults"`
}

type datascopesVariableRequest struct {
	AssetRid string `json:"assetRid"`
}

type channelVariablesRequest struct {
	AssetRid      string `json:"assetRid"`
	DataScopeName string `json:"dataScopeName"`
}

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

type templateVariableCatalogErrorKind int

const (
	templateVariableAssetFetchError templateVariableCatalogErrorKind = iota
	templateVariableChannelSearchError
)

type templateVariableCatalogError struct {
	kind templateVariableCatalogErrorKind
	err  error
}

func (e *templateVariableCatalogError) Error() string {
	return e.err.Error()
}

func (e *templateVariableCatalogError) Unwrap() error {
	return e.err
}

func hasUnresolvedTemplateVariable(values ...string) bool {
	for _, value := range values {
		if strings.Contains(value, "$") {
			return true
		}
	}
	return false
}

func (c *TemplateVariableCatalog) Assets(ctx context.Context, config *models.PluginSettings, req assetsVariableRequest) ([]metricFindValue, error) {
	if req.MaxResults == 0 {
		req.MaxResults = 500
	}

	assetResponses, err := c.nominal.FetchAssetsForVariable(ctx, config, req.SearchText, req.MaxResults)
	if err != nil {
		return nil, err
	}

	result := make([]metricFindValue, 0)
outer:
	for _, resp := range assetResponses {
		for _, asset := range resp.Results {
			if c.nominal.HasSupportedDataSource(asset) {
				result = append(result, metricFindValue{
					Text:  asset.Title,
					Value: asset.Rid,
				})
				if len(result) >= req.MaxResults {
					break outer
				}
			}
		}
	}
	return result, nil
}

func (c *TemplateVariableCatalog) Datascopes(ctx context.Context, config *models.PluginSettings, req datascopesVariableRequest) ([]metricFindValue, error) {
	if hasUnresolvedTemplateVariable(req.AssetRid) {
		return []metricFindValue{}, nil
	}

	asset, err := c.nominal.FetchAssetByRid(ctx, config, req.AssetRid)
	if err != nil {
		return nil, &templateVariableCatalogError{kind: templateVariableAssetFetchError, err: err}
	}
	if asset == nil {
		return []metricFindValue{}, nil
	}

	result := make([]metricFindValue, 0)
	for _, scope := range asset.DataScopes {
		if isSupportedDataSourceType(scope.DataSource.Type) {
			result = append(result, metricFindValue{
				Text:  scope.DataScopeName,
				Value: scope.DataScopeName,
			})
		}
	}
	return result, nil
}

func (c *TemplateVariableCatalog) ChannelVariables(ctx context.Context, config *models.PluginSettings, req channelVariablesRequest) ([]metricFindValue, error) {
	if hasUnresolvedTemplateVariable(req.AssetRid, req.DataScopeName) {
		return []metricFindValue{}, nil
	}

	asset, err := c.nominal.FetchAssetByRid(ctx, config, req.AssetRid)
	if err != nil {
		return nil, &templateVariableCatalogError{kind: templateVariableAssetFetchError, err: err}
	}
	if asset == nil {
		return []metricFindValue{}, nil
	}

	dataSourceRids := c.nominal.DataSourceRidsForScope(asset, req.DataScopeName)
	if len(dataSourceRids) == 0 {
		return []metricFindValue{}, nil
	}

	bearerToken := bearertoken.Token(config.Secrets.ApiKey)
	allChannelResults, err := c.nominal.SearchChannelsForVariables(ctx, bearerToken, dataSourceRids)
	if err != nil {
		return nil, &templateVariableCatalogError{kind: templateVariableChannelSearchError, err: err}
	}

	seen := make(map[string]bool)
	result := make([]metricFindValue, 0)
	for _, channel := range allChannelResults {
		name := string(channel.Name)
		if !seen[name] {
			seen[name] = true
			result = append(result, metricFindValue{
				Text:  name,
				Value: name,
			})
		}
	}
	return result, nil
}

func (d *Datasource) templateCatalog() *TemplateVariableCatalog {
	if d.templateVariableCatalog == nil {
		d.templateVariableCatalog = newTemplateVariableCatalog(d.catalog())
	}
	return d.templateVariableCatalog
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

	result, err := d.templateCatalog().Assets(ctx, config, searchRequest)
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

	result, err := d.templateCatalog().Datascopes(ctx, config, searchRequest)
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

	result, err := d.templateCatalog().ChannelVariables(ctx, config, searchRequest)
	if err != nil {
		if catalogErr, ok := err.(*templateVariableCatalogError); ok && catalogErr.kind == templateVariableAssetFetchError {
			logErrorWithConjureFields("Failed to fetch asset", err, "assetRid", searchRequest.AssetRid)
			return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Failed to fetch asset", err))
		}
		logErrorWithConjureFields("Channels search API call failed", err)
		return jsonErrorResponse(sender, http.StatusInternalServerError, appendInstanceID("Channels search failed", err))
	}

	log.DefaultLogger.Debug("Channel variables request successful", "channelCount", len(result))
	return jsonMarshalResponse(sender, http.StatusOK, result)
}
