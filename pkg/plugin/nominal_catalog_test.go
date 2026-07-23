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
	cancelBase, cancel := context.WithCancel(context.Background())
	cancelCtx, cancelWaiting := newFlightWaitContext(cancelBase)
	survivorCtx, survivorWaiting := newFlightWaitContext(context.Background())
	canceledDone := make(chan assetLookupResult, 1)
	survivorDone := make(chan assetLookupResult, 1)

	go func() {
		asset, err := catalog.FetchAssetByRid(cancelCtx, config, assetRid)
		canceledDone <- assetLookupResult{asset: asset, err: err}
	}()
	waitForTestSignal(t, blocker.arrived, "the asset backend request")
	waitForTestSignal(t, cancelWaiting, "the initiating caller to wait on the flight")

	go func() {
		asset, err := catalog.FetchAssetByRid(survivorCtx, config, assetRid)
		survivorDone <- assetLookupResult{asset: asset, err: err}
	}()
	waitForTestSignal(t, survivorWaiting, "the surviving caller to join the flight")
	if got := blocker.calls.Load(); got != 1 {
		t.Fatalf("asset backend calls before release = %d, want 1", got)
	}

	cancel()
	select {
	case got := <-canceledDone:
		if !errors.Is(got.err, context.Canceled) || got.asset != nil {
			t.Fatalf("canceled lookup = (%+v, %v), want (nil, context.Canceled)", got.asset, got.err)
		}
	case <-time.After(catalogTestTimeout):
		t.Fatal("canceled caller did not return while the shared fetch was blocked")
	}
	select {
	case got := <-survivorDone:
		t.Fatalf("surviving caller returned before backend release: (%+v, %v)", got.asset, got.err)
	default:
	}

	blocker.unblock()
	select {
	case got := <-survivorDone:
		if got.err != nil || got.asset == nil || got.asset.Title != "Detached" {
			t.Fatalf("surviving lookup = (%+v, %v), want Detached asset", got.asset, got.err)
		}
	case <-time.After(catalogTestTimeout):
		t.Fatal("surviving caller did not receive the shared result")
	}
	if got := blocker.calls.Load(); got != 1 {
		t.Fatalf("asset backend calls = %d, want 1", got)
	}
}

func TestNominalCatalogFetchAssetByRidDoesNotDispatchPreCanceledMiss(t *testing.T) {
	blocker := newBlockingLookup()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		blocker.block()
	}))
	t.Cleanup(func() {
		blocker.unblock()
		server.Close()
	})
	config := &models.PluginSettings{BaseUrl: server.URL, Secrets: &models.SecretPluginSettings{ApiKey: "test-key"}}
	catalog := newNominalCatalog(server.Client(), &mockDatasourceService{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := range 20 {
		assetRid := fmt.Sprintf("ri.scout.main.asset.canceled-%d", i)
		asset, err := catalog.FetchAssetByRid(ctx, config, assetRid)
		if !errors.Is(err, context.Canceled) || asset != nil {
			t.Fatalf("pre-canceled lookup %d = (%+v, %v), want (nil, context.Canceled)", i, asset, err)
		}
	}
	select {
	case <-blocker.arrived:
		t.Fatal("pre-canceled cache miss dispatched an asset backend request")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestNominalCatalogInferChannelMetadataSharesFlightWithSurvivingCaller(t *testing.T) {
	const assetRid = "ri.scout.main.asset.infercancel"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	var assetFetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid: assetRid,
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, &assetFetchCount)
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
	cancelBase, cancel := context.WithCancel(context.Background())
	cancelCtx, cancelWaiting := newFlightWaitContext(cancelBase)
	survivorCtx, survivorWaiting := newFlightWaitContext(context.Background())
	canceledDone := make(chan struct{})
	survivorDone := make(chan struct{})

	go func() {
		catalog.InferChannelMetadata(cancelCtx, config, &canceledModel)
		close(canceledDone)
	}()
	waitForTestSignal(t, blocker.arrived, "the channel backend request")
	waitForTestSignal(t, cancelWaiting, "the initiating caller to wait on the flight")

	go func() {
		catalog.InferChannelMetadata(survivorCtx, config, &survivorModel)
		close(survivorDone)
	}()
	waitForTestSignal(t, survivorWaiting, "the surviving caller to join the flight")
	if got := blocker.calls.Load(); got != 1 {
		t.Fatalf("SearchChannels calls before release = %d, want 1", got)
	}

	cancel()
	waitForTestSignal(t, canceledDone, "the canceled channel caller to return")
	if canceledModel.ChannelDataType != ChannelDataTypeNumeric {
		t.Fatalf("canceled caller ChannelDataType = %q, want %q", canceledModel.ChannelDataType, ChannelDataTypeNumeric)
	}
	select {
	case <-survivorDone:
		t.Fatal("surviving channel caller returned before backend release")
	default:
	}

	blocker.unblock()
	waitForTestSignal(t, survivorDone, "the surviving channel caller to receive the shared result")
	if survivorModel.ChannelDataType != ChannelDataTypeString {
		t.Fatalf("surviving caller ChannelDataType = %q, want %q", survivorModel.ChannelDataType, ChannelDataTypeString)
	}
	if got := blocker.calls.Load(); got != 1 {
		t.Fatalf("SearchChannels calls = %d, want 1", got)
	}
	if assetFetchCount != 1 {
		t.Fatalf("asset backend calls = %d, want 1", assetFetchCount)
	}
}

func TestNominalCatalogInferChannelMetadataDoesNotDispatchPreCanceledMiss(t *testing.T) {
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
	for i := range 20 {
		qm := NominalQueryModel{
			AssetRid:        fmt.Sprintf("ri.scout.main.asset.canceled-inference-%d", i),
			DataScopeName:   "scope-a",
			Channel:         "state",
			ChannelDataType: ChannelDataTypeNumeric,
		}
		catalog.InferChannelMetadata(ctx, config, &qm)
	}

	select {
	case <-blocker.arrived:
		t.Fatal("pre-canceled cache miss dispatched a channel metadata lookup")
	case <-time.After(100 * time.Millisecond):
	}
	if mockDS.searchChannelsCallCount() != 0 {
		t.Fatalf("SearchChannels calls = %d, want 0", mockDS.searchChannelsCallCount())
	}
}

func TestNominalCatalogInferChannelMetadataKeepsDelimiterNamesDistinct(t *testing.T) {
	const assetRid = "ri.scout.main.asset.delimiters"
	dataset1 := "ri.scout.main.data-source.dataset1"
	dataset2 := "ri.scout.main.data-source.dataset2"
	datasetRids := map[string]string{
		"chan":   dataset1,
		"x|chan": dataset2,
	}
	var fetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid: assetRid,
			DataScopes: []AssetDataScope{
				{DataScopeName: "scope|x", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataset1}},
				{DataScopeName: "scope", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataset2}},
			},
		},
	}, &fetchCount)
	t.Cleanup(server.Close)

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	logType := api.New_SeriesDataType(api.SeriesDataType_LOG)
	mockDS := &mockDatasourceService{
		searchChannelsFunc: func(_ context.Context, _ bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
			if len(req.ExactMatch) != 1 || len(req.DataSources) != 1 {
				return datasourceapi.SearchChannelsResponse{}, fmt.Errorf("unexpected request: %+v", req)
			}
			channel := req.ExactMatch[0]
			wantDataSource, ok := datasetRids[channel]
			if !ok || req.DataSources[0].String() != wantDataSource {
				return datasourceapi.SearchChannelsResponse{}, fmt.Errorf("channel %q data sources = %v, want %q", channel, req.DataSources, wantDataSource)
			}
			dataType := stringType
			if channel == "x|chan" {
				dataType = logType
			}
			return datasourceapi.SearchChannelsResponse{Results: []datasourceapi.ChannelMetadata{{
				Name:       api.Channel(channel),
				DataSource: req.DataSources[0],
				DataType:   &dataType,
			}}}, nil
		},
	}
	config := &models.PluginSettings{BaseUrl: server.URL, Secrets: &models.SecretPluginSettings{ApiKey: "test-key"}}
	catalog := newNominalCatalog(server.Client(), mockDS)
	tests := []struct {
		scope, channel, wantType string
	}{
		{scope: "scope|x", channel: "chan", wantType: ChannelDataTypeString},
		{scope: "scope", channel: "x|chan", wantType: ChannelDataTypeLog},
	}
	for _, tt := range tests {
		qm := NominalQueryModel{AssetRid: assetRid, DataScopeName: tt.scope, Channel: tt.channel, ChannelDataType: ChannelDataTypeNumeric}
		catalog.InferChannelMetadata(context.Background(), config, &qm)
		if qm.ChannelDataType != tt.wantType {
			t.Fatalf("scope %q channel %q ChannelDataType = %q, want %q", tt.scope, tt.channel, qm.ChannelDataType, tt.wantType)
		}
	}
	if mockDS.searchChannelsCallCount() != len(tests) {
		t.Fatalf("SearchChannels calls = %d, want %d distinct lookups", mockDS.searchChannelsCallCount(), len(tests))
	}
	if fetchCount != 1 {
		t.Fatalf("asset backend calls = %d, want 1", fetchCount)
	}
}
