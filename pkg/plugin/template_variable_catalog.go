package plugin

import (
	"context"
	"strings"

	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/palantir/pkg/bearertoken"
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

// assetForVariable fetches an asset by RID for a template-variable lookup,
// wrapping any fetch failure as a templateVariableCatalogError. A nil asset
// with a nil error means the asset was not found; callers treat that as an
// empty result.
func (c *TemplateVariableCatalog) assetForVariable(ctx context.Context, config *models.PluginSettings, assetRid string) (*SingleAssetResponse, error) {
	asset, err := c.nominal.FetchAssetByRid(ctx, config, assetRid)
	if err != nil {
		return nil, &templateVariableCatalogError{kind: templateVariableAssetFetchError, err: err}
	}
	return asset, nil
}

func (c *TemplateVariableCatalog) Datascopes(ctx context.Context, config *models.PluginSettings, req datascopesVariableRequest) ([]metricFindValue, error) {
	if hasUnresolvedTemplateVariable(req.AssetRid) {
		return []metricFindValue{}, nil
	}

	asset, err := c.assetForVariable(ctx, config, req.AssetRid)
	if err != nil {
		return nil, err
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

	asset, err := c.assetForVariable(ctx, config, req.AssetRid)
	if err != nil {
		return nil, err
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
