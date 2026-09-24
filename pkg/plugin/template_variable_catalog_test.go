package plugin

import (
	"context"
	"testing"

	"github.com/nominal-io/grafana-plugin-public/pkg/models"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
)

func TestTemplateVariableCatalogAssetsFiltersAndShapesMetricFindValues(t *testing.T) {
	searchResults := []AssetResponse{
		{
			Results: []AssetSearchResult{
				{
					Rid:   "ri.scout.main.asset.1",
					Title: "Asset With Dataset",
					DataScopes: []AssetDataScope{
						{DataScopeName: "scope1", DataSource: AssetDataSource{Type: "dataset"}},
					},
				},
				{
					Rid:   "ri.scout.main.asset.2",
					Title: "Asset With Video Only",
					DataScopes: []AssetDataScope{
						{DataScopeName: "scope2", DataSource: AssetDataSource{Type: "video"}},
					},
				},
			},
		},
	}

	server := newTestAssetServer(t, nil, searchResults)
	defer server.Close()

	nominalCatalog := newNominalCatalog(server.Client(), &mockDatasourceService{})
	templateCatalog := newTemplateVariableCatalog(nominalCatalog)
	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{
			ApiKey: "test-key",
		},
	}

	values, err := templateCatalog.Assets(context.Background(), config, assetsVariableRequest{MaxResults: 10})
	if err != nil {
		t.Fatalf("Assets returned error: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("len(values) = %d, want 1: %v", len(values), values)
	}
	if values[0] != (metricFindValue{Text: "Asset With Dataset", Value: "ri.scout.main.asset.1"}) {
		t.Fatalf("values[0] = %+v, want Asset With Dataset metric value", values[0])
	}
}

func TestTemplateVariableCatalogDatascopesFiltersAndHandlesUnresolvedVariables(t *testing.T) {
	assetRid := "ri.scout.main.asset.1"
	datasetRid := "ri.scout.main.data-source.dataset1"
	videoRid := "ri.scout.main.data-source.video1"
	server := newTestAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "supported", DataSource: AssetDataSource{Type: "dataset", Dataset: &datasetRid}},
				{DataScopeName: "unsupported", DataSource: AssetDataSource{Type: "video", Dataset: &videoRid}},
			},
		},
	}, nil)
	defer server.Close()

	nominalCatalog := newNominalCatalog(server.Client(), &mockDatasourceService{})
	templateCatalog := newTemplateVariableCatalog(nominalCatalog)
	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{
			ApiKey: "test-key",
		},
	}

	values, err := templateCatalog.Datascopes(context.Background(), config, datascopesVariableRequest{AssetRid: assetRid})
	if err != nil {
		t.Fatalf("Datascopes returned error: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("len(values) = %d, want 1: %v", len(values), values)
	}
	if values[0] != (metricFindValue{Text: "supported", Value: "supported"}) {
		t.Fatalf("values[0] = %+v, want supported metric value", values[0])
	}

	unresolved, err := templateCatalog.Datascopes(context.Background(), config, datascopesVariableRequest{AssetRid: "$asset"})
	if err != nil {
		t.Fatalf("unresolved Datascopes returned error: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("unresolved values = %v, want empty", unresolved)
	}
}

func TestTemplateVariableCatalogChannelVariablesDedupesAndHandlesUnresolvedVariables(t *testing.T) {
	assetRid := "ri.scout.main.asset.1"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	server := newTestAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
	defer server.Close()

	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{Name: api.Channel("state")},
				{Name: api.Channel("state")},
				{Name: api.Channel("rpm")},
			},
		},
	}
	nominalCatalog := newNominalCatalog(server.Client(), mockDS)
	templateCatalog := newTemplateVariableCatalog(nominalCatalog)
	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{
			ApiKey: "test-key",
		},
	}

	values, err := templateCatalog.ChannelVariables(context.Background(), config, channelVariablesRequest{AssetRid: assetRid, DataScopeName: "scope-a"})
	if err != nil {
		t.Fatalf("ChannelVariables returned error: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("len(values) = %d, want 2: %v", len(values), values)
	}
	if values[0] != (metricFindValue{Text: "state", Value: "state"}) || values[1] != (metricFindValue{Text: "rpm", Value: "rpm"}) {
		t.Fatalf("values = %+v, want state/rpm metric values", values)
	}
	if got := mockDS.searchChannelsCallCount(); got != 1 {
		t.Fatalf("SearchChannels calls = %d, want 1", got)
	}

	unresolved, err := templateCatalog.ChannelVariables(context.Background(), config, channelVariablesRequest{AssetRid: assetRid, DataScopeName: "$scope"})
	if err != nil {
		t.Fatalf("unresolved ChannelVariables returned error: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("unresolved values = %v, want empty", unresolved)
	}
}
