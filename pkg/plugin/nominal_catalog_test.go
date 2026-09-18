package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	"github.com/palantir/pkg/bearertoken"
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
	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Cached Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
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
	if int(assetFetches.Load()) != 1 {
		t.Fatalf("asset fetch count = %d, want 1", int(assetFetches.Load()))
	}
}

func TestNominalCatalogFetchAssetByRidReturnsCopy(t *testing.T) {
	assetRid := "ri.scout.main.asset.copied"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Copied Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
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
	if first == nil || len(first.DataScopes) != 1 || first.DataScopes[0].DataSource.Dataset == nil {
		t.Fatalf("unexpected first asset shape: %+v", first)
	}

	// Mutate everything reachable from the returned value; the cache must not see it.
	first.Title = "mutated"
	first.DataScopes[0].DataScopeName = "mutated-scope"
	*first.DataScopes[0].DataSource.Dataset = "ri.mutated"

	second, err := catalog.FetchAssetByRid(context.Background(), config, assetRid)
	if err != nil {
		t.Fatalf("second FetchAssetByRid returned error: %v", err)
	}
	if second == nil || len(second.DataScopes) != 1 || second.DataScopes[0].DataSource.Dataset == nil {
		t.Fatalf("unexpected second asset shape: %+v", second)
	}
	if second.Title != "Copied Asset" {
		t.Fatalf("cached title = %q, want Copied Asset (mutation leaked into cache)", second.Title)
	}
	if second.DataScopes[0].DataScopeName != "scope-a" {
		t.Fatalf("cached scope name = %q, want scope-a (mutation leaked into cache)", second.DataScopes[0].DataScopeName)
	}
	if got := *second.DataScopes[0].DataSource.Dataset; got != dataSourceRid {
		t.Fatalf("cached dataset RID = %q, want %q (mutation leaked into cache)", got, dataSourceRid)
	}
	if int(assetFetches.Load()) != 1 {
		t.Fatalf("asset fetch count = %d, want 1 (second call should still be served from cache)", int(assetFetches.Load()))
	}
}

// A not-found asset (a dashboard saved against a since-deleted one) caches as
// nil, which round-trips through singleflight's any value uncheckable by the compiler.
func TestNominalCatalogFetchAssetByRidCachesNotFound(t *testing.T) {
	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{}, nil)
	defer server.Close()

	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
	}
	catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})

	first, err := catalog.FetchAssetByRid(context.Background(), config, "ri.scout.main.asset.missing")
	if err != nil {
		t.Fatalf("first FetchAssetByRid returned error: %v", err)
	}
	if first != nil {
		t.Fatalf("first FetchAssetByRid = %+v, want nil for a missing asset", first)
	}

	second, err := catalog.FetchAssetByRid(context.Background(), config, "ri.scout.main.asset.missing")
	if err != nil {
		t.Fatalf("second FetchAssetByRid returned error: %v", err)
	}
	if second != nil {
		t.Fatalf("second FetchAssetByRid = %+v, want nil for a missing asset", second)
	}
	if int(assetFetches.Load()) != 1 {
		t.Fatalf("asset fetch count = %d, want 1 (the not-found result should be cached)", int(assetFetches.Load()))
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

func TestNominalCatalogFetchAssetByRidRequiresResourceHTTPClient(t *testing.T) {
	assetRid := "ri.scout.main.asset.requires-client"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Unexpected Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
	defer server.Close()

	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{
			ApiKey: "test-key",
		},
	}
	catalog := newNominalCatalog(nil, &mockDatasourceService{})

	if _, err := catalog.FetchAssetByRid(context.Background(), config, assetRid); err == nil || !strings.Contains(err.Error(), "resource HTTP client is not configured") {
		t.Fatalf("FetchAssetByRid error = %v, want missing resource HTTP client error", err)
	}
	if int(assetFetches.Load()) != 0 {
		t.Fatalf("asset fetch count = %d, want 0", int(assetFetches.Load()))
	}
}

func TestTTLCacheExpiredEntryReloads(t *testing.T) {
	cache := newTTLCache[string](catalogCacheTTL, "test")
	cache.mu.Lock()
	cache.entries["key"] = ttlCacheEntry[string]{value: "stale", fetchedAt: time.Now().Add(-catalogCacheTTL)}
	cache.mu.Unlock()

	loads := 0
	load := func(context.Context) (string, error) {
		loads++
		return "fresh", nil
	}

	if got, err := cache.get(context.Background(), "key", load); err != nil || got != "fresh" {
		t.Fatalf("get after expiry = (%q, %v), want (fresh, nil)", got, err)
	}
	if got, err := cache.get(context.Background(), "key", load); err != nil || got != "fresh" {
		t.Fatalf("get after reload = (%q, %v), want (fresh, nil)", got, err)
	}
	if loads != 1 {
		t.Fatalf("loads = %d, want 1: the expired entry must reload once and then serve from cache", loads)
	}
}

func TestNominalCatalogInferChannelMetadataUsesOwnCache(t *testing.T) {
	assetRid := "ri.scout.main.asset.metadata"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Metadata Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
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
	if int(assetFetches.Load()) != 1 {
		t.Fatalf("asset fetch count = %d, want 1", int(assetFetches.Load()))
	}
	if got := mockDS.searchChannelsCallCount(); got != 1 {
		t.Fatalf("SearchChannels calls = %d, want 1", got)
	}
}

func TestChannelMetadataEntryForExactMatch(t *testing.T) {
	numericType := api.New_SeriesDataType(api.SeriesDataType_DOUBLE)

	tests := []struct {
		name        string
		channels    []datasourceapi.ChannelMetadata
		channelName string
		wantEntry   channelMetadataCacheEntry
		wantOK      bool
	}{
		{
			name: "exact match returns normalized type and trimmed unit",
			channels: []datasourceapi.ChannelMetadata{{
				Name:     api.Channel("engine_temp"),
				DataType: &numericType,
				Unit:     &runapi.Unit{Symbol: " Cel "},
			}},
			channelName: "engine_temp",
			wantEntry: channelMetadataCacheEntry{
				channelDataType: "numeric",
				unit:            "Cel",
			},
			wantOK: true,
		},
		{
			name: "case mismatch is ignored",
			channels: []datasourceapi.ChannelMetadata{{
				Name:     api.Channel("Engine_Temp"),
				DataType: &numericType,
				Unit:     &runapi.Unit{Symbol: "Cel"},
			}},
			channelName: "engine_temp",
			wantOK:      false,
		},
		{
			name: "exact match with no usable metadata is ignored",
			channels: []datasourceapi.ChannelMetadata{{
				Name: api.Channel("engine_temp"),
			}},
			channelName: "engine_temp",
			wantOK:      false,
		},
		{
			name: "unit-only exact match returns entry",
			channels: []datasourceapi.ChannelMetadata{{
				Name: api.Channel("engine_temp"),
				Unit: &runapi.Unit{Symbol: "psia"},
			}},
			channelName: "engine_temp",
			wantEntry: channelMetadataCacheEntry{
				unit: "psia",
			},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := channelMetadataEntryForExactMatch(tt.channels, tt.channelName)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got.channelDataType != tt.wantEntry.channelDataType {
				t.Errorf("channelDataType = %q, want %q", got.channelDataType, tt.wantEntry.channelDataType)
			}
			if got.unit != tt.wantEntry.unit {
				t.Errorf("unit = %q, want %q", got.unit, tt.wantEntry.unit)
			}
		})
	}
}

func TestInferChannelTypeDeduplicatesWithinRequest(t *testing.T) {
	assetRid := "ri.scout.main.asset.dedup1"
	dataSourceRid := "ri.scout.main.data-source.ds1"

	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Test Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
	defer server.Close()

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("temperature"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1")),
					DataType:   &stringType,
				},
			},
		},
	}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockEnumComputeResult([]string{"a"}, []int{0}),
				createMockEnumComputeResult([]string{"b"}, []int{1}),
				createMockEnumComputeResult([]string{"c"}, []int{2}),
			},
		},
	}

	ds := withCatalog(&Datasource{
		computeService:     mockCompute,
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	})

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// 3 queries for the same asset+scope+channel should make 1 asset fetch
	// and 1 SearchChannels call.
	req := newQueryRequestForURL(server.URL, []backend.DataQuery{
		{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
		{RefID: "B", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
		{RefID: "C", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
	})

	_, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if int(assetFetches.Load()) != 1 {
		t.Errorf("expected 1 asset fetch call (cached), got %d", int(assetFetches.Load()))
	}
	if got := mockDS.searchChannelsCallCount(); got != 1 {
		t.Errorf("expected 1 SearchChannels call (deduplicated), got %d", got)
	}
}

func TestAssetCacheTTLReusedAcrossRequests(t *testing.T) {
	assetRid := "ri.scout.main.asset.ttl1"
	dataSourceRid := "ri.scout.main.data-source.ds1"

	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Test Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
	defer server.Close()

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("temperature"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1")),
					DataType:   &stringType,
				},
			},
		},
	}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockEnumComputeResult([]string{"a"}, []int{0}),
			},
		},
	}

	ds := withCatalog(&Datasource{
		computeService:     mockCompute,
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	})

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	makeReq := func() *backend.QueryDataRequest {
		return newQueryRequestForURL(server.URL, []backend.DataQuery{
			{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
		})
	}

	// Two separate QueryData calls should reuse the cached asset.
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if int(assetFetches.Load()) != 1 {
		t.Errorf("expected 1 asset fetch across 2 QueryData calls (TTL cache), got %d", int(assetFetches.Load()))
	}
}

func TestChannelTypeCacheTTLReusedAcrossRequests(t *testing.T) {
	assetRid := "ri.scout.main.asset.ttl2"
	dataSourceRid := "ri.scout.main.data-source.ds2"

	server := newTestAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Test Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
	defer server.Close()

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("temperature"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds2")),
					DataType:   &stringType,
				},
			},
		},
	}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockEnumComputeResult([]string{"a"}, []int{0}),
			},
		},
	}

	ds := withCatalog(&Datasource{
		computeService:     mockCompute,
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	})

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	makeReq := func() *backend.QueryDataRequest {
		return newQueryRequestForURL(server.URL, []backend.DataQuery{
			{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
		})
	}

	// Two separate QueryData calls should reuse the cached channel type.
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if got := mockDS.searchChannelsCallCount(); got != 1 {
		t.Errorf("expected 1 SearchChannels call across 2 QueryData calls (TTL cache), got %d", got)
	}
}

func TestGetChannelDataType(t *testing.T) {
	tests := []struct {
		name     string
		dataType *api.SeriesDataType
		expected string
	}{
		{"nil dataType returns empty", nil, ""},
		{"STRING returns string", ptrSeriesDataType(api.SeriesDataType_STRING), "string"},
		{"STRING_ARRAY returns string", ptrSeriesDataType(api.SeriesDataType_STRING_ARRAY), "string"},
		{"LOG returns log", ptrSeriesDataType(api.SeriesDataType_LOG), "log"},
		{"DOUBLE returns numeric", ptrSeriesDataType(api.SeriesDataType_DOUBLE), "numeric"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := datasourceapi.ChannelMetadata{DataType: tt.dataType}
			got := getChannelDataType(ch)
			if got != tt.expected {
				t.Errorf("getChannelDataType() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func ptrSeriesDataType(v api.SeriesDataType_Value) *api.SeriesDataType {
	dt := api.New_SeriesDataType(v)
	return &dt
}

const catalogTestTimeout = 2 * time.Second

type blockingLookup struct {
	arrived     chan struct{}
	release     chan struct{}
	arrivedOnce sync.Once
	releaseOnce sync.Once
	calls       atomic.Int32
}

func newBlockingLookup() *blockingLookup {
	return &blockingLookup{arrived: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingLookup) block() {
	b.calls.Add(1)
	b.arrivedOnce.Do(func() { close(b.arrived) })
	<-b.release
}

func (b *blockingLookup) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func waitForTestSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(catalogTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type flightWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func newFlightWaitContext(ctx context.Context) (*flightWaitContext, <-chan struct{}) {
	waiting := make(chan struct{})
	return &flightWaitContext{Context: ctx, waiting: waiting}, waiting
}

func (c *flightWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type assetLookupResult struct {
	asset *SingleAssetResponse
	err   error
}

// runSurvivingCallerScenario blocks a first caller on the backend, joins a
// second to the same flight, cancels the first while the backend is still
// blocked, then releases the backend for the survivor.
func runSurvivingCallerScenario(t *testing.T, blocker *blockingLookup, canceledCall, survivorCall func(ctx context.Context)) {
	t.Helper()
	cancelBase, cancel := context.WithCancel(context.Background())
	cancelCtx, cancelWaiting := newFlightWaitContext(cancelBase)
	survivorCtx, survivorWaiting := newFlightWaitContext(context.Background())
	canceledDone := make(chan struct{})
	survivorDone := make(chan struct{})

	go func() {
		canceledCall(cancelCtx)
		close(canceledDone)
	}()
	waitForTestSignal(t, blocker.arrived, "the blocked backend request")
	waitForTestSignal(t, cancelWaiting, "the initiating caller to wait on the flight")

	go func() {
		survivorCall(survivorCtx)
		close(survivorDone)
	}()
	waitForTestSignal(t, survivorWaiting, "the surviving caller to join the flight")

	cancel()
	waitForTestSignal(t, canceledDone, "the canceled caller to return while the backend is blocked")
	blocker.unblock()
	waitForTestSignal(t, survivorDone, "the surviving caller to receive the shared result")
}

func TestNominalCatalogFetchAssetByRidSharesFlightWithSurvivingCaller(t *testing.T) {
	const assetRid = "ri.scout.main.asset.detach"
	blocker := newBlockingLookup()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		blocker.block()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]SingleAssetResponse{
			assetRid: {Rid: assetRid, Title: "Detached"},
		})
	}))
	t.Cleanup(func() {
		blocker.unblock()
		server.Close()
	})

	config := &models.PluginSettings{BaseUrl: server.URL, Secrets: &models.SecretPluginSettings{ApiKey: "test-key"}}
	catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})
	var canceled, survivor assetLookupResult
	runSurvivingCallerScenario(t, blocker,
		func(ctx context.Context) {
			canceled.asset, canceled.err = catalog.FetchAssetByRid(ctx, config, assetRid)
		},
		func(ctx context.Context) {
			survivor.asset, survivor.err = catalog.FetchAssetByRid(ctx, config, assetRid)
		},
	)

	if !errors.Is(canceled.err, context.Canceled) || canceled.asset != nil {
		t.Fatalf("canceled lookup = (%+v, %v), want (nil, context.Canceled)", canceled.asset, canceled.err)
	}
	if survivor.err != nil || survivor.asset == nil || survivor.asset.Title != "Detached" {
		t.Fatalf("surviving lookup = (%+v, %v), want Detached asset", survivor.asset, survivor.err)
	}
	if got := blocker.calls.Load(); got != 1 {
		t.Fatalf("asset backend calls = %d, want 1", got)
	}
}

func TestNominalCatalogInferChannelMetadataSharesFlightWithSurvivingCaller(t *testing.T) {
	const assetRid = "ri.scout.main.asset.infercancel"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid: assetRid,
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, nil)
	t.Cleanup(server.Close)

	blocker := newBlockingLookup()
	t.Cleanup(blocker.unblock)
	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsFunc: func(_ context.Context, _ bearertoken.Token, _ datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
			blocker.block()
			return datasourceapi.SearchChannelsResponse{Results: []datasourceapi.ChannelMetadata{{
				Name:       api.Channel("state"),
				DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "dataset1")),
				DataType:   &stringType,
			}}}, nil
		},
	}
	config := &models.PluginSettings{BaseUrl: server.URL, Secrets: &models.SecretPluginSettings{ApiKey: "test-key"}}
	catalog := newNominalCatalog(server.Client(), mockDS)
	canceledModel := NominalQueryModel{AssetRid: assetRid, DataScopeName: "scope-a", Channel: "state", ChannelDataType: ChannelDataTypeNumeric}
	survivorModel := canceledModel
	runSurvivingCallerScenario(t, blocker,
		func(ctx context.Context) { catalog.InferChannelMetadata(ctx, config, &canceledModel) },
		func(ctx context.Context) { catalog.InferChannelMetadata(ctx, config, &survivorModel) },
	)

	if canceledModel.ChannelDataType != ChannelDataTypeNumeric {
		t.Fatalf("canceled caller ChannelDataType = %q, want %q", canceledModel.ChannelDataType, ChannelDataTypeNumeric)
	}
	if survivorModel.ChannelDataType != ChannelDataTypeString {
		t.Fatalf("surviving caller ChannelDataType = %q, want %q", survivorModel.ChannelDataType, ChannelDataTypeString)
	}
	if got := blocker.calls.Load(); got != 1 {
		t.Fatalf("SearchChannels calls = %d, want 1", got)
	}
	if int(assetFetches.Load()) != 1 {
		t.Fatalf("asset backend calls = %d, want 1", int(assetFetches.Load()))
	}
}

func TestNominalCatalogFetchAssetByRidSharesFailureAndRetriesAfter(t *testing.T) {
	const assetRid = "ri.scout.main.asset.failshare"
	const callers = 4

	blocker := newBlockingLookup()
	var fetches atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fetches.Add(1) == 1 {
			blocker.block()
			http.Error(w, `{"error":"upstream down"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]SingleAssetResponse{assetRid: {Rid: assetRid, Title: "Recovered"}})
	}))
	t.Cleanup(func() {
		blocker.unblock()
		server.Close()
	})

	config := &models.PluginSettings{BaseUrl: server.URL, Secrets: &models.SecretPluginSettings{ApiKey: "test-key"}}
	catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})

	results := make([]assetLookupResult, callers)
	var wg sync.WaitGroup
	for i := range callers {
		ctx, waiting := newFlightWaitContext(context.Background())
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].asset, results[i].err = catalog.FetchAssetByRid(ctx, config, assetRid)
		}()
		if i == 0 {
			waitForTestSignal(t, blocker.arrived, "the blocked backend request")
		}
		waitForTestSignal(t, waiting, "a caller to join the flight")
	}

	allDone := make(chan struct{})
	go func() { wg.Wait(); close(allDone) }()
	blocker.unblock()
	waitForTestSignal(t, allDone, "every caller to receive the failed shared result")

	for i, got := range results {
		if got.err == nil || got.asset != nil {
			t.Fatalf("caller %d = (%+v, %v), want (nil, error)", i, got.asset, got.err)
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("asset backend calls during failed flight = %d, want 1", got)
	}

	asset, err := catalog.FetchAssetByRid(context.Background(), config, assetRid)
	if err != nil || asset == nil || asset.Title != "Recovered" {
		t.Fatalf("retry after failure = (%+v, %v), want Recovered asset", asset, err)
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("asset backend calls after retry = %d, want 2 (the failure must not be cached)", got)
	}
}

func TestNominalCatalogDoesNotDispatchPreCanceledMiss(t *testing.T) {
	tests := []struct {
		name string
		call func(t *testing.T, ctx context.Context, catalog *NominalCatalog, config *models.PluginSettings)
	}{
		{
			name: "asset fetch",
			call: func(t *testing.T, ctx context.Context, catalog *NominalCatalog, config *models.PluginSettings) {
				asset, err := catalog.FetchAssetByRid(ctx, config, "ri.scout.main.asset.canceled")
				if !errors.Is(err, context.Canceled) || asset != nil {
					t.Fatalf("pre-canceled lookup = (%+v, %v), want (nil, context.Canceled)", asset, err)
				}
			},
		},
		{
			name: "channel metadata inference",
			call: func(t *testing.T, ctx context.Context, catalog *NominalCatalog, config *models.PluginSettings) {
				qm := NominalQueryModel{
					AssetRid:        "ri.scout.main.asset.canceled",
					DataScopeName:   "scope-a",
					Channel:         "state",
					ChannelDataType: ChannelDataTypeNumeric,
				}
				catalog.InferChannelMetadata(ctx, config, &qm)
				if qm.ChannelDataType != ChannelDataTypeNumeric {
					t.Fatalf("pre-canceled inference mutated ChannelDataType to %q", qm.ChannelDataType)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blocker := newBlockingLookup()
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				blocker.block()
			}))
			t.Cleanup(func() {
				blocker.unblock()
				server.Close()
			})
			mockDS := &mockDatasourceService{}
			config := &models.PluginSettings{BaseUrl: server.URL, Secrets: &models.SecretPluginSettings{ApiKey: "test-key"}}
			catalog := newNominalCatalog(server.Client(), mockDS)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			tt.call(t, ctx, catalog, config)

			select {
			case <-blocker.arrived:
				t.Fatal("pre-canceled cache miss dispatched a backend request")
			case <-time.After(100 * time.Millisecond):
			}
			if got := mockDS.searchChannelsCallCount(); got != 0 {
				t.Fatalf("SearchChannels calls = %d, want 0", got)
			}
		})
	}
}

func TestChannelMetadataCacheKeyKeepsDelimiterNamesDistinct(t *testing.T) {
	a := channelMetadataCacheKey("asset", "scope|x", "chan")
	b := channelMetadataCacheKey("asset", "scope", "x|chan")
	if a == b {
		t.Fatalf("cache keys collide across the separator: %q", a)
	}
}
