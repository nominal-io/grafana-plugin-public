package plugin

import (
	"context"
	"encoding/json"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSqlMetadataOptions(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "name", Type: arrow.BinaryTypes.String}, {Name: "rid", Type: arrow.BinaryTypes.String}}, nil)
	stream := sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) {
		b.Field(0).(*array.StringBuilder).Append("Engine")
		b.Field(1).(*array.StringBuilder).Append("dataset-1")
	})
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" || payload["workspace_rid"] != testWorkspaceRid {
			t.Error("missing configured credentials/workspace")
		}
		query = payload["query"]
		_, _ = w.Write(stream)
	}))
	defer srv.Close()
	workspace := sqlTestWorkspaceRid(t)
	ds := &Datasource{settings: backend.DataSourceInstanceSettings{JSONData: []byte(`{"queryApi":"sql"}`), DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"}}, workspaceRid: &workspace, sqlClient: newSqlClient(srv.URL, nil)}
	for _, path := range []string{"sql/channels"} {
		response := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Method: "POST", Path: path, Body: []byte(`{"searchText":"Engine's%","datasetRid":"dataset'1"}`)})
		if response.Status != 200 || string(response.Body) != `[{"label":"Engine","value":"dataset-1"}]` {
			t.Fatalf("%d %s", response.Status, response.Body)
		}
		if !strings.Contains(query, "'engine''s%'") || !strings.HasSuffix(query, "LIMIT 100") {
			t.Fatal(query)
		}
		if path == "sql/channels" && !strings.Contains(query, "dataset_rid = 'dataset''1'") {
			t.Fatal(query)
		}
	}
	for _, req := range []*backend.CallResourceRequest{
		{Method: "GET", Path: "sql/datasets"}, {Method: "POST", Path: "sql/channels", Body: []byte(`{}`)},
		{Method: "POST", Path: "sql/datasets", Body: []byte(`bad json`)},
	} {
		query = ""
		response := callResourceAndCapture(t, ds, req)
		if response.Status < 400 || query != "" {
			t.Fatalf("invalid request sent upstream: %+v %s", response, query)
		}
	}
	ds.settings.JSONData = []byte(`{"queryApi":"compute"}`)
	response := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Method: "POST", Path: "sql/datasets"})
	if response.Status != 400 {
		t.Fatalf("Compute mode metadata: %+v", response)
	}
}

func TestSqlMetadataEndpointError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		_, _ = w.Write([]byte(`{"parameters":{"detail":"No SQL access","sqlQueryId":"query-1"}}`))
	}))
	defer srv.Close()
	workspace := sqlTestWorkspaceRid(t)
	ds := &Datasource{settings: backend.DataSourceInstanceSettings{JSONData: []byte(`{"queryApi":"sql"}`)}, workspaceRid: &workspace, sqlClient: newSqlClient(srv.URL, nil)}
	response := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Method: "POST", Path: "sql/channels", Body: []byte(`{"datasetRid":"dataset-1"}`)})
	if response.Status != 403 || !strings.Contains(string(response.Body), "No SQL access (sqlQueryId: query-1)") {
		t.Fatalf("%+v", response)
	}
}

func TestSqlDatasetSearchUsesWorkspaceAndPagination(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/catalog/v1/search-datasets-v2" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		var request struct {
			Query    json.RawMessage `json:"query"`
			PageSize int             `json:"pageSize"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.PageSize != 100 || !strings.Contains(string(request.Query), testWorkspaceRid) || !strings.Contains(string(request.Query), "engine") || !strings.Contains(string(request.Query), "archiveStatus") {
			t.Errorf("unexpected request %+v", request)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("missing configured key")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"rid":"ri.catalog.main.dataset.dataset-1","name":"Engine"}]}`))
	}))
	defer srv.Close()
	settings := backend.DataSourceInstanceSettings{JSONData: mustMarshal(map[string]string{"queryApi": "sql", "baseUrl": srv.URL, "workspaceRid": testWorkspaceRid}), DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"}}
	instance, err := NewDatasource(context.Background(), settings)
	if err != nil {
		t.Fatal(err)
	}
	ds := instance.(*Datasource)
	defer ds.Dispose()
	response := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Method: "POST", Path: "sql/datasets", Body: []byte(`{"searchText":"engine"}`)})
	if response.Status != 200 || string(response.Body) != `[{"label":"Engine","value":"ri.catalog.main.dataset.dataset-1"}]` {
		t.Fatalf("%d %s", response.Status, response.Body)
	}
}
