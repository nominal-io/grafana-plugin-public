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

func TestNominalCatalogAssetCacheSweepOnStore(t *testing.T) {
	const assetRid = "ri.scout.main.asset.sweepwire"
	server := newTestAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {Rid: assetRid, Title: "Sweep Wire"},
	}, nil)
	t.Cleanup(server.Close)

	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
	}

	tests := []struct {
		name               string
		lastSweep          time.Time
		wantExpiredPresent bool
	}{
		{name: "elapsed interval sweeps", lastSweep: time.Now().Add(-2 * sweepInterval)},
		{name: "recent sweep gates eviction", lastSweep: time.Now(), wantExpiredPresent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})
			catalog.assetCache["expired"] = assetCacheEntry{
				asset:     &SingleAssetResponse{Rid: "expired"},
				fetchedAt: time.Now().Add(-2 * assetCacheTTL),
			}
			catalog.assetCache["fresh"] = assetCacheEntry{
				asset:     &SingleAssetResponse{Rid: "fresh"},
				fetchedAt: time.Now(),
			}
			catalog.assetCacheLastSweep = tt.lastSweep

			if _, err := catalog.FetchAssetByRid(context.Background(), config, assetRid); err != nil {
				t.Fatalf("FetchAssetByRid error: %v", err)
			}

			catalog.assetCacheMu.Lock()
			_, expiredPresent := catalog.assetCache["expired"]
			_, freshPresent := catalog.assetCache["fresh"]
			_, storedPresent := catalog.assetCache[assetRid]
			catalog.assetCacheMu.Unlock()
			if expiredPresent != tt.wantExpiredPresent {
				t.Fatalf("expired entry present = %v, want %v", expiredPresent, tt.wantExpiredPresent)
			}
			if !freshPresent {
				t.Fatal("fresh entry was swept")
			}
			if !storedPresent {
				t.Fatal("fetched asset was not stored")
			}
		})
	}
}

func TestNominalCatalogChannelCacheSweepOnStore(t *testing.T) {
	tests := []struct {
		name               string
		lastSweep          time.Time
		wantExpiredPresent bool
	}{
		{name: "elapsed interval sweeps", lastSweep: time.Now().Add(-2 * sweepInterval)},
		{name: "recent sweep gates eviction", lastSweep: time.Now(), wantExpiredPresent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := newNominalCatalog(nil, nil)
			catalog.channelMetadataCache["expired"] = channelMetadataCacheEntry{
				channelDataType: "numeric",
				fetchedAt:       time.Now().Add(-2 * assetCacheTTL),
			}
			catalog.channelMetadataCache["fresh"] = channelMetadataCacheEntry{
				channelDataType: "numeric",
				fetchedAt:       time.Now(),
			}
			catalog.channelMetadataCacheLastSweep = tt.lastSweep

			catalog.storeChannelMetadata("stored", channelMetadataCacheEntry{
				channelDataType: "string",
				fetchedAt:       time.Now(),
			})

			catalog.channelMetadataCacheMu.Lock()
			_, expiredPresent := catalog.channelMetadataCache["expired"]
			_, freshPresent := catalog.channelMetadataCache["fresh"]
			_, storedPresent := catalog.channelMetadataCache["stored"]
			catalog.channelMetadataCacheMu.Unlock()
			if expiredPresent != tt.wantExpiredPresent {
				t.Fatalf("expired entry present = %v, want %v", expiredPresent, tt.wantExpiredPresent)
			}
			if !freshPresent {
				t.Fatal("fresh entry was swept")
			}
			if !storedPresent {
				t.Fatal("new entry was not stored")
			}
		})
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
	if mockDS.searchChannelsCalls != 1 {
		t.Fatalf("SearchChannels calls = %d, want 1", mockDS.searchChannelsCalls)
	}
}

func TestNominalCatalogFetchAssetByRidCoalescesConcurrentCalls(t *testing.T) {
	assetRid := "ri.scout.main.asset.coalesce"
	dataSourceRid := "ri.scout.main.data-source.ds1"
	var calls int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/scout/v1/asset/multiple" {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		atomic.AddInt32(&calls, 1)
		<-release // block so concurrent callers coalesce onto this in-flight request
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]SingleAssetResponse{
			assetRid: {
				Rid:   assetRid,
				Title: "Coalesced",
				DataScopes: []AssetDataScope{
					{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
				},
			},
		})
	}))
	defer server.Close()

	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
	}
	catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})

	const n = 8
	var wg sync.WaitGroup
	results := make([]*SingleAssetResponse, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = catalog.FetchAssetByRid(context.Background(), config, assetRid)
		}(i)
	}
	// Best-effort concurrency inducement, not a correctness barrier: a late
	// goroutine hits the cache or the in-flight re-check and issues no HTTP
	// call, so the calls == 1 assertion cannot flake.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("asset fetch HTTP calls = %d, want 1 (concurrent calls should coalesce)", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d error: %v", i, errs[i])
		}
		if results[i] == nil || results[i].Title != "Coalesced" {
			t.Fatalf("goroutine %d result = %+v, want Coalesced asset", i, results[i])
		}
	}
}

func TestNominalCatalogFetchAssetByRidCanceledCallerReturnsPromptly(t *testing.T) {
	assetRid := "ri.scout.main.asset.detach"
	var calls int32
	var arrivedOnce sync.Once
	arrived := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/scout/v1/asset/multiple" {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		atomic.AddInt32(&calls, 1)
		arrivedOnce.Do(func() { close(arrived) })
		<-release
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]SingleAssetResponse{
			assetRid: {Rid: assetRid, Title: "Detached"},
		})
	}))
	defer server.Close()

	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
	}
	catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		asset *SingleAssetResponse
		err   error
	}
	done := make(chan result, 1)
	go func() {
		a, err := catalog.FetchAssetByRid(ctx, config, assetRid)
		done <- result{a, err}
	}()

	<-arrived // the shared fetch is in flight and blocked on the server
	cancel()

	// The canceled caller must return promptly while the server is still
	// blocked, without waiting for the shared flight.
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("FetchAssetByRid error = %v, want context.Canceled", got.err)
		}
		if got.asset != nil {
			t.Fatalf("asset = %+v, want nil for the canceled caller", got.asset)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled caller did not return while the shared fetch was still in flight")
	}

	// The detached flight must survive the cancellation and populate the cache.
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		catalog.assetCacheMu.Lock()
		_, stored := catalog.assetCache[assetRid]
		catalog.assetCacheMu.Unlock()
		if stored {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("detached fetch never populated the cache")
		}
		time.Sleep(10 * time.Millisecond)
	}

	asset, err := catalog.FetchAssetByRid(context.Background(), config, assetRid)
	if err != nil || asset == nil || asset.Title != "Detached" {
		t.Fatalf("follow-up fetch = (%+v, %v), want the Detached asset from cache", asset, err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("HTTP calls = %d, want 1 (the detached flight's result should serve the follow-up)", got)
	}
}
