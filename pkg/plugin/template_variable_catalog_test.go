package plugin

import (
	"context"
	"reflect"
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

	nominalCatalog := newNominalCatalog(server.Client(), &mockDatasourceService{}, nil)
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
	otherRid := "ri.scout.main.asset.2"
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

	runService := newMockRunService(
		testRun("ri.scout.main.run.1", assetRid),
		testRun("ri.scout.main.run.2", assetRid, otherRid),
	)
	nominalCatalog := newNominalCatalog(server.Client(), &mockDatasourceService{}, runService)
	templateCatalog := newTemplateVariableCatalog(nominalCatalog)
	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{
			ApiKey: "test-key",
		},
	}

	supported := []metricFindValue{{Text: "supported", Value: "supported"}}
	tests := []struct {
		name string
		rid  string
		want []metricFindValue
	}{
		{"asset RID", assetRid, supported},
		{"single-asset run", "ri.scout.main.run.1", supported},
		{"multi-asset run", "ri.scout.main.run.2", []metricFindValue{}},
		{"unknown run", "ri.scout.main.run.9", []metricFindValue{}},
		{"unresolved variable", "$asset", []metricFindValue{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, err := templateCatalog.Datascopes(context.Background(), config, datascopesVariableRequest{AssetRid: tt.rid})
			if err != nil {
				t.Fatalf("Datascopes returned error: %v", err)
			}
			if !reflect.DeepEqual(values, tt.want) {
				t.Fatalf("values = %+v, want %+v", values, tt.want)
			}
		})
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
	nominalCatalog := newNominalCatalog(server.Client(), mockDS, newMockRunService(testRun("ri.scout.main.run.1", assetRid)))
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

	fromRun, err := templateCatalog.ChannelVariables(context.Background(), config, channelVariablesRequest{AssetRid: "ri.scout.main.run.1", DataScopeName: "scope-a"})
	if err != nil {
		t.Fatalf("run ChannelVariables returned error: %v", err)
	}
	if !reflect.DeepEqual(fromRun, values) {
		t.Fatalf("run values = %+v, want %+v", fromRun, values)
	}

	unresolved, err := templateCatalog.ChannelVariables(context.Background(), config, channelVariablesRequest{AssetRid: assetRid, DataScopeName: "$scope"})
	if err != nil {
		t.Fatalf("unresolved ChannelVariables returned error: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("unresolved values = %v, want empty", unresolved)
	}
}
