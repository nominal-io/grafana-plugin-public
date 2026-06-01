package plugin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	"github.com/palantir/pkg/rid"
)

func TestIsSupportedDataSourceType(t *testing.T) {
	tests := []struct {
		dsType string
		want   bool
	}{
		{dsType: "dataset", want: true},
		{dsType: "connection", want: true},
		{dsType: "logSet", want: true},
		{dsType: "video", want: false},
		{dsType: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.dsType, func(t *testing.T) {
			if got := isSupportedDataSourceType(tt.dsType); got != tt.want {
				t.Fatalf("isSupportedDataSourceType(%q) = %v, want %v", tt.dsType, got, tt.want)
			}
		})
	}
}

func TestDataSourceRidFor(t *testing.T) {
	datasetRid := "ri.scout.main.data-source.dataset1"
	connectionRid := "ri.scout.main.data-source.connection1"
	logSetRid := "ri.scout.main.data-source.logset1"

	tests := []struct {
		name string
		ds   AssetDataSource
		want string
		ok   bool
	}{
		{
			name: "dataset returns dataset rid",
			ds:   AssetDataSource{Type: "dataset", Dataset: &datasetRid},
			want: datasetRid,
			ok:   true,
		},
		{
			name: "connection returns connection rid",
			ds:   AssetDataSource{Type: "connection", Connection: &connectionRid},
			want: connectionRid,
			ok:   true,
		},
		{
			name: "logSet returns logSet rid",
			ds:   AssetDataSource{Type: "logSet", LogSet: &logSetRid},
			want: logSetRid,
			ok:   true,
		},
		{
			name: "dataset missing rid returns false",
			ds:   AssetDataSource{Type: "dataset"},
			ok:   false,
		},
		{
			name: "connection missing rid returns false",
			ds:   AssetDataSource{Type: "connection"},
			ok:   false,
		},
		{
			name: "logSet missing rid returns false",
			ds:   AssetDataSource{Type: "logSet"},
			ok:   false,
		},
		{
			name: "unsupported type returns false",
			ds:   AssetDataSource{Type: "video"},
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := dataSourceRidFor(tt.ds)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("rid = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNominalCatalogDataSourceRidsForScopeFiltersExactScope(t *testing.T) {
	datasetRid := "ri.scout.main.data-source.dataset1"
	connectionRid := "ri.scout.main.data-source.connection1"
	logSetRid := "ri.scout.main.data-source.logset1"
	malformedRid := "not-a-rid"
	unsupportedRid := "ri.scout.main.data-source.video1"
	otherRid := "ri.scout.main.data-source.other"

	asset := &SingleAssetResponse{
		Rid:   "ri.scout.main.asset.asset1",
		Title: "Test Asset",
		DataScopes: []AssetDataScope{
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &datasetRid}},
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "connection", Connection: &connectionRid}},
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "logSet", LogSet: &logSetRid}},
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &malformedRid}},
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "video", Dataset: &unsupportedRid}},
			{DataScopeName: "scope-b", DataSource: AssetDataSource{Type: "dataset", Dataset: &otherRid}},
		},
	}

	catalog := newNominalCatalog(nil, nil)
	got := catalog.DataSourceRidsForScope(asset, "scope-a")
	if len(got) != 3 {
		t.Fatalf("len(DataSourceRidsForScope) = %d, want 3; got %v", len(got), got)
	}

	want := []string{datasetRid, connectionRid, logSetRid}
	for i, rid := range want {
		if got[i].String() != rid {
			t.Fatalf("rid[%d] = %q, want %q", i, got[i].String(), rid)
		}
	}

	if got := catalog.DataSourceRidsForScope(asset, "missing"); len(got) != 0 {
		t.Fatalf("missing scope returned %d RIDs, want 0: %v", len(got), got)
	}
}

func TestNominalCatalogDataSourceRidsForScopeSupportsExactAndAllScopes(t *testing.T) {
	datasetRid := "ri.scout.main.data-source.dataset1"
	connectionRid := "ri.scout.main.data-source.connection1"
	logSetRid := "ri.scout.main.data-source.logset1"
	otherRid := "ri.scout.main.data-source.other"
	malformedRid := "not-a-rid"
	catalog := newNominalCatalog(nil, nil)

	asset := &SingleAssetResponse{
		Rid:   "ri.scout.main.asset.asset1",
		Title: "Test Asset",
		DataScopes: []AssetDataScope{
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &datasetRid}},
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "connection", Connection: &connectionRid}},
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &malformedRid}},
			{DataScopeName: "scope-b", DataSource: AssetDataSource{Type: "logSet", LogSet: &logSetRid}},
			{DataScopeName: "scope-c", DataSource: AssetDataSource{Type: "video", Dataset: &otherRid}},
		},
	}

	exact := catalog.DataSourceRidsForScope(asset, "scope-a")
	if len(exact) != 2 {
		t.Fatalf("exact scope RID count = %d, want 2; got %v", len(exact), exact)
	}
	if exact[0].String() != datasetRid || exact[1].String() != connectionRid {
		t.Fatalf("exact scope RIDs = %v, want [%s %s]", exact, datasetRid, connectionRid)
	}

	all := catalog.DataSourceRidsForScope(asset, "")
	if len(all) != 3 {
		t.Fatalf("all scope RID count = %d, want 3; got %v", len(all), all)
	}
	if all[0].String() != datasetRid || all[1].String() != connectionRid || all[2].String() != logSetRid {
		t.Fatalf("all scope RIDs = %v, want [%s %s %s]", all, datasetRid, connectionRid, logSetRid)
	}
}

func TestNominalCatalogHasSupportedDataSource(t *testing.T) {
	datasetRid := "ri.scout.main.data-source.dataset1"
	catalog := newNominalCatalog(nil, nil)

	supported := AssetSearchResult{
		Rid:   "ri.scout.main.asset.supported",
		Title: "Supported",
		DataScopes: []AssetDataScope{
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &datasetRid}},
		},
	}
	if !catalog.HasSupportedDataSource(supported) {
		t.Fatal("HasSupportedDataSource(supported) = false, want true")
	}

	unsupported := AssetSearchResult{
		Rid:   "ri.scout.main.asset.unsupported",
		Title: "Unsupported",
		DataScopes: []AssetDataScope{
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "video"}},
		},
	}
	if catalog.HasSupportedDataSource(unsupported) {
		t.Fatal("HasSupportedDataSource(unsupported) = true, want false")
	}
}

func TestNominalCatalogFetchAssetByRidUsesOwnCache(t *testing.T) {
	assetRid := "ri.scout.main.asset.cached"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	var fetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Cached Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, &fetchCount)
	defer server.Close()

	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{
			ApiKey: "test-key",
		},
	}
	catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})

	first, err := catalog.FetchAssetByRid(context.Background(), config, assetRid)
	if err != nil {
		t.Fatalf("first FetchAssetByRid returned error: %v", err)
	}
	second, err := catalog.FetchAssetByRid(context.Background(), config, assetRid)
	if err != nil {
		t.Fatalf("second FetchAssetByRid returned error: %v", err)
	}

	if first == nil || second == nil {
		t.Fatalf("expected cached asset on both calls, got first=%v second=%v", first, second)
	}
	if first.Title != "Cached Asset" || second.Title != "Cached Asset" {
		t.Fatalf("cached titles = %q/%q, want Cached Asset", first.Title, second.Title)
	}
	if fetchCount != 1 {
		t.Fatalf("asset fetch count = %d, want 1", fetchCount)
	}
}

func TestNominalCatalogFetchAssetByRidSurfacesHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf(`{"error":"bad path %s"}`, r.URL.Path), http.StatusTeapot)
	}))
	defer server.Close()

	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{
			ApiKey: "test-key",
		},
	}
	catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})

	if _, err := catalog.FetchAssetByRid(context.Background(), config, "ri.scout.main.asset.missing"); err == nil {
		t.Fatal("FetchAssetByRid error = nil, want non-nil")
	}
}

func TestNominalCatalogInferChannelMetadataUsesOwnCache(t *testing.T) {
	assetRid := "ri.scout.main.asset.metadata"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	var fetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Metadata Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, &fetchCount)
	defer server.Close()

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("state"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "dataset1")),
					DataType:   &stringType,
				},
			},
		},
	}
	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{
			ApiKey: "test-key",
		},
	}
	catalog := newNominalCatalog(server.Client(), mockDS)

	first := NominalQueryModel{AssetRid: assetRid, DataScopeName: "scope-a", Channel: "state", ChannelDataType: ChannelDataTypeNumeric}
	catalog.InferChannelMetadata(context.Background(), config, &first)
	if first.ChannelDataType != ChannelDataTypeString {
		t.Fatalf("first ChannelDataType = %q, want %q", first.ChannelDataType, ChannelDataTypeString)
	}

	second := NominalQueryModel{AssetRid: assetRid, DataScopeName: "scope-a", Channel: "state", ChannelDataType: ChannelDataTypeNumeric}
	catalog.InferChannelMetadata(context.Background(), config, &second)
	if second.ChannelDataType != ChannelDataTypeString {
		t.Fatalf("second ChannelDataType = %q, want %q", second.ChannelDataType, ChannelDataTypeString)
	}
	if fetchCount != 1 {
		t.Fatalf("asset fetch count = %d, want 1", fetchCount)
	}
	if mockDS.searchChannelsCalls != 1 {
		t.Fatalf("SearchChannels calls = %d, want 1", mockDS.searchChannelsCalls)
	}
}
