package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

// ============================================================================
// TemplateVariableCatalog tests
// ============================================================================

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
	if mockDS.searchChannelsCalls != 1 {
		t.Fatalf("SearchChannels calls = %d, want 1", mockDS.searchChannelsCalls)
	}

	unresolved, err := templateCatalog.ChannelVariables(context.Background(), config, channelVariablesRequest{AssetRid: assetRid, DataScopeName: "$scope"})
	if err != nil {
		t.Fatalf("unresolved ChannelVariables returned error: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("unresolved values = %v, want empty", unresolved)
	}
}

// ============================================================================
// CallResource handler tests
// ============================================================================

func TestHandleAssetsVariable(t *testing.T) {
	t.Run("returns assets with dataset or connection data sources in text/value format", func(t *testing.T) {
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
						Title: "Asset With Connection",
						DataScopes: []AssetDataScope{
							{DataScopeName: "scope2", DataSource: AssetDataSource{Type: "connection"}},
						},
					},
					{
						Rid:   "ri.scout.main.asset.3",
						Title: "Asset With Video Only",
						DataScopes: []AssetDataScope{
							{DataScopeName: "scope3", DataSource: AssetDataSource{Type: "video"}},
						},
					},
				},
			},
		}

		server := newTestAssetServer(t, nil, searchResults)
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		req := &backend.CallResourceRequest{Path: "assets", Method: "POST", Body: []byte(`{}`)}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Dataset and connection assets should be included, video-only excluded
		if len(result) != 2 {
			t.Fatalf("expected 2 assets, got %d: %v", len(result), result)
		}
		resultsByText := map[string]map[string]string{}
		for _, r := range result {
			resultsByText[r["text"]] = r
		}
		if r, ok := resultsByText["Asset With Dataset"]; !ok {
			t.Error("expected Asset With Dataset in results")
		} else if r["value"] != "ri.scout.main.asset.1" {
			t.Errorf("Asset With Dataset value = %q, want %q", r["value"], "ri.scout.main.asset.1")
		}
		if r, ok := resultsByText["Asset With Connection"]; !ok {
			t.Error("expected Asset With Connection in results")
		} else if r["value"] != "ri.scout.main.asset.2" {
			t.Errorf("Asset With Connection value = %q, want %q", r["value"], "ri.scout.main.asset.2")
		}
		if _, ok := resultsByText["Asset With Video Only"]; ok {
			t.Error("video-only asset should be excluded")
		}
	})

	t.Run("respects maxResults", func(t *testing.T) {
		assets := make([]AssetSearchResult, 5)
		for i := range assets {
			assets[i] = AssetSearchResult{
				Rid:   fmt.Sprintf("ri.scout.main.asset.%d", i),
				Title: fmt.Sprintf("Asset %d", i),
				DataScopes: []AssetDataScope{
					{DataScopeName: "ds", DataSource: AssetDataSource{Type: "dataset"}},
				},
			}
		}
		searchResults := []AssetResponse{{Results: assets}}

		server := newTestAssetServer(t, nil, searchResults)
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]interface{}{"maxResults": 2})
		req := &backend.CallResourceRequest{Path: "assets", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if len(result) != 2 {
			t.Errorf("expected 2 results (maxResults), got %d", len(result))
		}
	})

	t.Run("handles empty body with defaults", func(t *testing.T) {
		server := newTestAssetServer(t, nil, []AssetResponse{{Results: nil}})
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		req := &backend.CallResourceRequest{Path: "assets", Method: "POST"}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 results, got %d", len(result))
		}
	})
}

func TestHandleAssetsVariablePagination(t *testing.T) {
	t.Run("fetches multiple pages and respects maxResults across pages", func(t *testing.T) {
		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			if r.URL.Path != "/scout/v1/search-assets" {
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}

			var reqBody map[string]interface{}
			json.NewDecoder(r.Body).Decode(&reqBody)
			callCount++

			// Page 1: return 50 assets with a next page token
			if reqBody["nextPageToken"] == nil || reqBody["nextPageToken"] == "" {
				results := make([]AssetSearchResult, 50)
				for i := range results {
					results[i] = AssetSearchResult{
						Rid:   fmt.Sprintf("ri.scout.main.asset.page1-%d", i),
						Title: fmt.Sprintf("Page1 Asset %d", i),
						DataScopes: []AssetDataScope{
							{DataScopeName: "ds", DataSource: AssetDataSource{Type: "dataset"}},
						},
					}
				}
				json.NewEncoder(w).Encode(AssetResponse{Results: results, NextPageToken: "page2token"})
				return
			}

			// Page 2: return 10 more assets with no next page token
			results := make([]AssetSearchResult, 10)
			for i := range results {
				results[i] = AssetSearchResult{
					Rid:   fmt.Sprintf("ri.scout.main.asset.page2-%d", i),
					Title: fmt.Sprintf("Page2 Asset %d", i),
					DataScopes: []AssetDataScope{
						{DataScopeName: "ds", DataSource: AssetDataSource{Type: "dataset"}},
					},
				}
			}
			json.NewEncoder(w).Encode(AssetResponse{Results: results})
		}))
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]interface{}{"maxResults": 100})
		req := &backend.CallResourceRequest{Path: "assets", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Should have fetched both pages: 50 + 10 = 60
		if len(result) != 60 {
			t.Errorf("expected 60 assets across 2 pages, got %d", len(result))
		}
		if callCount != 2 {
			t.Errorf("expected 2 API calls (2 pages), got %d", callCount)
		}
	})

	t.Run("stops pagination when maxResults is reached", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			// Always return a full page with a next token
			results := make([]AssetSearchResult, 50)
			for i := range results {
				results[i] = AssetSearchResult{
					Rid:   fmt.Sprintf("ri.scout.main.asset.%d", i),
					Title: fmt.Sprintf("Asset %d", i),
					DataScopes: []AssetDataScope{
						{DataScopeName: "ds", DataSource: AssetDataSource{Type: "dataset"}},
					},
				}
			}
			json.NewEncoder(w).Encode(AssetResponse{Results: results, NextPageToken: "next"})
		}))
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		// maxResults = 3 should stop after the first page and return only 3
		body, _ := json.Marshal(map[string]interface{}{"maxResults": 3})
		req := &backend.CallResourceRequest{Path: "assets", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		if len(result) != 3 {
			t.Errorf("expected 3 results (maxResults cap), got %d", len(result))
		}
	})
}

// --- handleDatascopesVariable tests ---

func TestHandleDatascopesVariable(t *testing.T) {
	assetRid := "ri.scout.main.asset.abc123"
	datasetRid := "ri.scout.main.data-source.ds1"
	connectionRid := "ri.scout.main.data-source.conn1"

	makeAsset := func() map[string]SingleAssetResponse {
		return map[string]SingleAssetResponse{
			assetRid: {
				Rid:   assetRid,
				Title: "Test Asset",
				DataScopes: []AssetDataScope{
					{DataScopeName: "dataset-scope", DataSource: AssetDataSource{Type: "dataset", Dataset: &datasetRid}},
					{DataScopeName: "connection-scope", DataSource: AssetDataSource{Type: "connection", Connection: &connectionRid}},
					{DataScopeName: "video-scope", DataSource: AssetDataSource{Type: "video"}},
				},
			},
		}
	}

	t.Run("returns datascopes for asset in text/value format", func(t *testing.T) {
		server := newTestAssetServer(t, makeAsset(), nil)
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "datascopes", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Should include dataset and connection scopes, but not video
		if len(result) != 2 {
			t.Fatalf("expected 2 datascopes, got %d: %v", len(result), result)
		}

		names := map[string]bool{}
		for _, r := range result {
			names[r["text"]] = true
			if r["text"] != r["value"] {
				t.Errorf("text and value should match: text=%q, value=%q", r["text"], r["value"])
			}
		}
		if !names["dataset-scope"] {
			t.Error("expected dataset-scope in results")
		}
		if !names["connection-scope"] {
			t.Error("expected connection-scope in results")
		}
	})

	t.Run("missing assetRid returns 400", func(t *testing.T) {
		ds := newTestDatasource("https://api.test.com", &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{})
		req := &backend.CallResourceRequest{Path: "datascopes", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusBadRequest {
			t.Errorf("status = %d, want 400; body = %s", resp.Status, string(resp.Body))
		}
	})

	t.Run("unresolved template variable returns empty array 200", func(t *testing.T) {
		ds := newTestDatasource("https://api.test.com", &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{"assetRid": "$asset"})
		req := &backend.CallResourceRequest{Path: "datascopes", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}
		if string(resp.Body) != "[]" {
			t.Errorf("body = %q, want %q", string(resp.Body), "[]")
		}
	})

	t.Run("asset not found returns empty array 200", func(t *testing.T) {
		// Server with empty asset map — asset won't be found
		server := newTestAssetServer(t, map[string]SingleAssetResponse{}, nil)
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{"assetRid": "ri.scout.main.asset.nonexistent"})
		req := &backend.CallResourceRequest{Path: "datascopes", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}
		if string(resp.Body) != "[]" {
			t.Errorf("body = %q, want %q", string(resp.Body), "[]")
		}
	})
}

// --- handleChannelVariables tests ---

func TestHandleChannelVariables(t *testing.T) {
	assetRid := "ri.scout.main.asset.ch123"
	datasetRid := "ri.scout.main.data-source.ds1"

	makeAssetWithDS := func() map[string]SingleAssetResponse {
		return map[string]SingleAssetResponse{
			assetRid: {
				Rid:   assetRid,
				Title: "Test Asset",
				DataScopes: []AssetDataScope{
					{DataScopeName: "scope1", DataSource: AssetDataSource{Type: "dataset", Dataset: &datasetRid}},
				},
			},
		}
	}

	t.Run("returns deduplicated channel names", func(t *testing.T) {
		server := newTestAssetServer(t, makeAssetWithDS(), nil)
		defer server.Close()

		mockDS := &mockDatasourceService{
			searchChannelsResponse: datasourceapi.SearchChannelsResponse{
				Results: []datasourceapi.ChannelMetadata{
					{Name: api.Channel("temperature"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
					{Name: api.Channel("pressure"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
					{Name: api.Channel("temperature"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))}, // duplicate
				},
			},
		}

		ds := newTestDatasource(server.URL, &mockAuthService{}, mockDS)

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 deduplicated channels, got %d: %v", len(result), result)
		}

		names := map[string]bool{}
		for _, r := range result {
			names[r["text"]] = true
			if r["text"] != r["value"] {
				t.Errorf("text and value should match: text=%q, value=%q", r["text"], r["value"])
			}
		}
		if !names["temperature"] || !names["pressure"] {
			t.Errorf("expected temperature and pressure, got %v", names)
		}
	})

	t.Run("filters by dataScopeName", func(t *testing.T) {
		twoScopeAsset := map[string]SingleAssetResponse{
			assetRid: {
				Rid:   assetRid,
				Title: "Test Asset",
				DataScopes: []AssetDataScope{
					{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &datasetRid}},
					{DataScopeName: "scope-b", DataSource: AssetDataSource{Type: "dataset", Dataset: strPtr("ri.scout.main.data-source.ds2")}},
				},
			},
		}

		server := newTestAssetServer(t, twoScopeAsset, nil)
		defer server.Close()

		mockDS := &mockDatasourceService{
			searchChannelsResponse: datasourceapi.SearchChannelsResponse{
				Results: []datasourceapi.ChannelMetadata{
					{Name: api.Channel("ch1"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
				},
			},
		}

		ds := newTestDatasource(server.URL, &mockAuthService{}, mockDS)

		body, _ := json.Marshal(map[string]interface{}{"assetRid": assetRid, "dataScopeName": "scope-a"})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		// Verify only scope-a's datasource RID was sent
		if len(mockDS.searchChannelsRequest.DataSources) != 1 {
			t.Errorf("expected 1 datasource RID (filtered by scope-a), got %d", len(mockDS.searchChannelsRequest.DataSources))
		}
	})

	t.Run("uses connection datasource RID when type is connection", func(t *testing.T) {
		connectionRid := "ri.scout.main.data-source.conn1"
		connAsset := map[string]SingleAssetResponse{
			assetRid: {
				Rid:   assetRid,
				Title: "Connection Asset",
				DataScopes: []AssetDataScope{
					{DataScopeName: "conn-scope", DataSource: AssetDataSource{Type: "connection", Connection: &connectionRid}},
				},
			},
		}

		server := newTestAssetServer(t, connAsset, nil)
		defer server.Close()

		mockDS := &mockDatasourceService{
			searchChannelsResponse: datasourceapi.SearchChannelsResponse{
				Results: []datasourceapi.ChannelMetadata{
					{Name: api.Channel("voltage"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "conn1"))},
				},
			},
		}

		ds := newTestDatasource(server.URL, &mockAuthService{}, mockDS)

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		// Verify the connection RID was sent to SearchChannels
		if len(mockDS.searchChannelsRequest.DataSources) != 1 {
			t.Fatalf("expected 1 datasource RID, got %d", len(mockDS.searchChannelsRequest.DataSources))
		}
		gotRid := mockDS.searchChannelsRequest.DataSources[0].String()
		if gotRid != connectionRid {
			t.Errorf("datasource RID = %q, want %q", gotRid, connectionRid)
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if len(result) != 1 || result[0]["text"] != "voltage" {
			t.Errorf("expected [{text:voltage, value:voltage}], got %v", result)
		}
	})

	t.Run("missing assetRid returns 400", func(t *testing.T) {
		ds := newTestDatasource("https://api.test.com", &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", resp.Status)
		}
	})

	t.Run("unresolved template variable in assetRid returns empty 200", func(t *testing.T) {
		ds := newTestDatasource("https://api.test.com", &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{"assetRid": "${asset}"})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.Status)
		}
		if string(resp.Body) != "[]" {
			t.Errorf("body = %q, want %q", string(resp.Body), "[]")
		}
	})

	t.Run("unresolved template variable in dataScopeName returns empty 200", func(t *testing.T) {
		ds := newTestDatasource("https://api.test.com", &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid, "dataScopeName": "$scope"})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.Status)
		}
		if string(resp.Body) != "[]" {
			t.Errorf("body = %q, want %q", string(resp.Body), "[]")
		}
	})

	t.Run("no datasource RIDs returns empty 200", func(t *testing.T) {
		videoAsset := map[string]SingleAssetResponse{
			assetRid: {
				Rid:   assetRid,
				Title: "Video Asset",
				DataScopes: []AssetDataScope{
					{DataScopeName: "video-scope", DataSource: AssetDataSource{Type: "video"}},
				},
			},
		}

		server := newTestAssetServer(t, videoAsset, nil)
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}
		if string(resp.Body) != "[]" {
			t.Errorf("body = %q, want %q", string(resp.Body), "[]")
		}
	})

	t.Run("paginates across multiple pages", func(t *testing.T) {
		server := newTestAssetServer(t, makeAssetWithDS(), nil)
		defer server.Close()

		pageToken := api.Token("page2")
		callCount := 0
		mockDS := &mockDatasourceService{
			searchChannelsFunc: func(ctx context.Context, authHeader bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
				callCount++
				if callCount == 1 {
					return datasourceapi.SearchChannelsResponse{
						Results: []datasourceapi.ChannelMetadata{
							{Name: api.Channel("ch1"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
							{Name: api.Channel("ch2"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
							{Name: api.Channel("ch3"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
						},
						NextPageToken: &pageToken,
					}, nil
				}
				// Second call: verify token was passed, return final page
				if req.NextPageToken == nil || *req.NextPageToken != pageToken {
					return datasourceapi.SearchChannelsResponse{}, fmt.Errorf("expected page token %q on second call", pageToken)
				}
				return datasourceapi.SearchChannelsResponse{
					Results: []datasourceapi.ChannelMetadata{
						{Name: api.Channel("ch4"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
						{Name: api.Channel("ch5"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
					},
				}, nil
			},
		}

		ds := newTestDatasource(server.URL, &mockAuthService{}, mockDS)

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if len(result) != 5 {
			t.Fatalf("expected 5 channels across 2 pages, got %d: %v", len(result), result)
		}
		if callCount != 2 {
			t.Errorf("expected 2 SearchChannels calls, got %d", callCount)
		}
	})

	t.Run("stops at safety cap", func(t *testing.T) {
		server := newTestAssetServer(t, makeAssetWithDS(), nil)
		defer server.Close()

		pageToken := api.Token("next")
		callCount := 0
		mockDS := &mockDatasourceService{
			searchChannelsFunc: func(ctx context.Context, authHeader bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
				callCount++
				// Always return 1000 channels with a next page token
				channels := make([]datasourceapi.ChannelMetadata, 1000)
				for i := range channels {
					channels[i] = datasourceapi.ChannelMetadata{
						Name:       api.Channel(fmt.Sprintf("ch-%d-%d", callCount, i)),
						DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1")),
					}
				}
				return datasourceapi.SearchChannelsResponse{
					Results:       channels,
					NextPageToken: &pageToken,
				}, nil
			},
		}

		ds := newTestDatasource(server.URL, &mockAuthService{}, mockDS)

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		// 5 pages of 1000 = 5000, which hits the maxChannelVariables cap
		if len(result) != 5000 {
			t.Fatalf("expected 5000 channels (safety cap), got %d", len(result))
		}
		if callCount != 5 {
			t.Errorf("expected 5 SearchChannels calls before cap, got %d", callCount)
		}
	})

	t.Run("deduplicates across pages", func(t *testing.T) {
		server := newTestAssetServer(t, makeAssetWithDS(), nil)
		defer server.Close()

		pageToken := api.Token("page2")
		mockDS := &mockDatasourceService{
			searchChannelsFunc: func(ctx context.Context, authHeader bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
				if req.NextPageToken == nil {
					return datasourceapi.SearchChannelsResponse{
						Results: []datasourceapi.ChannelMetadata{
							{Name: api.Channel("a"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
							{Name: api.Channel("b"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
						},
						NextPageToken: &pageToken,
					}, nil
				}
				return datasourceapi.SearchChannelsResponse{
					Results: []datasourceapi.ChannelMetadata{
						{Name: api.Channel("b"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
						{Name: api.Channel("c"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
					},
				}, nil
			},
		}

		ds := newTestDatasource(server.URL, &mockAuthService{}, mockDS)

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
		}

		var result []map[string]string
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if len(result) != 3 {
			t.Fatalf("expected 3 unique channels after cross-page dedup, got %d: %v", len(result), result)
		}
	})

	t.Run("error on second page returns error", func(t *testing.T) {
		server := newTestAssetServer(t, makeAssetWithDS(), nil)
		defer server.Close()

		pageToken := api.Token("page2")
		mockDS := &mockDatasourceService{
			searchChannelsFunc: func(ctx context.Context, authHeader bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
				if req.NextPageToken == nil {
					return datasourceapi.SearchChannelsResponse{
						Results: []datasourceapi.ChannelMetadata{
							{Name: api.Channel("ch1"), DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))},
						},
						NextPageToken: &pageToken,
					}, nil
				}
				return datasourceapi.SearchChannelsResponse{}, fmt.Errorf("network error on page 2")
			},
		}

		ds := newTestDatasource(server.URL, &mockAuthService{}, mockDS)

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body = %s", resp.Status, string(resp.Body))
		}
	})

	t.Run("asset fetch error keeps asset error message", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/scout/v1/asset/multiple" {
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			http.Error(w, `{"error":"asset lookup failed"}`, http.StatusInternalServerError)
		}))
		defer server.Close()

		ds := newTestDatasource(server.URL, &mockAuthService{}, &mockDatasourceService{})

		body, _ := json.Marshal(map[string]string{"assetRid": assetRid})
		req := &backend.CallResourceRequest{Path: "channelvariables", Method: "POST", Body: body}
		resp := callResourceAndCapture(t, ds, req)
		if resp.Status != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body = %s", resp.Status, string(resp.Body))
		}
		if !strings.Contains(string(resp.Body), "Failed to fetch asset") {
			t.Fatalf("body = %s, want Failed to fetch asset", string(resp.Body))
		}
	})
}
