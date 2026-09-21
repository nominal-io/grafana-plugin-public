package plugin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	workspaceapi "github.com/nominal-io/nominal-api-go/security/api/workspace"
	"github.com/palantir/pkg/bearertoken"
)

func TestPrepareQuerySql(t *testing.T) {
	e := newTestQueryExecution(&Datasource{}, nil)
	prepared, response := e.prepareQuery(context.Background(), backend.DataQuery{RefID: "A", JSON: []byte(`{"queryType":"sql","rawSql":"select 1"}`)})
	if response != nil || prepared.Kind != preparedQuerySql {
		t.Fatalf("%v %#v", response, prepared)
	}
	_, response = e.prepareQuery(context.Background(), backend.DataQuery{JSON: []byte(`{"queryType":"sql","rawSql":" "}`)})
	if response == nil || response.Error.Error() != "SQL query is empty" {
		t.Fatalf("%v", response)
	}
}

func TestResolveSqlWorkspaceCachesDefault(t *testing.T) {
	workspace := &workspaceapi.Workspace{}
	workspace.Rid = sqlTestWorkspaceRid(t)
	service := &mockWorkspaceService{defaultWorkspace: workspace}
	ds := &Datasource{workspaceService: service}
	for range 2 {
		got, err := ds.resolveSqlWorkspace(context.Background(), bearertoken.Token("k"))
		if err != nil || got != testWorkspaceRid {
			t.Fatalf("%q %v", got, err)
		}
	}
	if service.defaultCalls != 1 {
		t.Fatal(service.defaultCalls)
	}
	_, err := (&Datasource{workspaceService: &mockWorkspaceService{}}).resolveSqlWorkspace(context.Background(), bearertoken.Token("k"))
	if err == nil || err.Error() != sqlWorkspaceHint {
		t.Fatal(err)
	}
}

func TestResolveSqlWorkspaceRetriesErrors(t *testing.T) {
	workspace := &workspaceapi.Workspace{Rid: sqlTestWorkspaceRid(t)}
	service := &mockWorkspaceService{}
	service.defaultFunc = func() (*workspaceapi.Workspace, error) {
		if service.defaultCalls == 1 {
			return nil, context.Canceled
		}
		return workspace, nil
	}
	ds := &Datasource{workspaceService: service}
	if _, err := ds.resolveSqlWorkspace(context.Background(), bearertoken.Token("k")); err == nil {
		t.Fatal("expected initial error")
	}
	if got, err := ds.resolveSqlWorkspace(context.Background(), bearertoken.Token("k")); err != nil || got != testWorkspaceRid {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := ds.resolveSqlWorkspace(context.Background(), bearertoken.Token("k")); err != nil || service.defaultCalls != 2 {
		t.Fatalf("calls=%d err=%v", service.defaultCalls, err)
	}
}

func TestExecuteSqlQuery(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64}}, nil)
	stream := sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) { b.Field(0).(*array.Float64Builder).Append(42) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(stream) }))
	defer srv.Close()
	workspace := sqlTestWorkspaceRid(t)
	ds := &Datasource{workspaceRid: &workspace, sqlClient: newSqlClient(srv.URL, http.DefaultTransport)}
	e := newNominalQueryExecution(ds, &models.PluginSettings{Secrets: &models.SecretPluginSettings{ApiKey: "k"}})
	query := backend.DataQuery{RefID: "A", JSON: []byte(`{"queryType":"sql","rawSql":"SELECT 42","format":"table"}`)}
	prepared, _ := e.prepareQuery(context.Background(), query)
	response := e.executeSqlQuery(context.Background(), prepared)
	if response.Error != nil || len(response.Frames) != 1 || response.Frames[0].Meta.ExecutedQueryString != "SELECT 42" {
		t.Fatalf("%+v", response)
	}
}

func TestExecuteSqlQuerySurfacesEndpointError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"errorName":"bad","parameters":{"detail":"bad query","sqlQueryId":"q-1"}}`))
	}))
	defer srv.Close()
	workspace := sqlTestWorkspaceRid(t)
	e := newNominalQueryExecution(&Datasource{workspaceRid: &workspace, sqlClient: newSqlClient(srv.URL, http.DefaultTransport)}, &models.PluginSettings{Secrets: &models.SecretPluginSettings{ApiKey: "k"}})
	prepared, _ := e.prepareQuery(context.Background(), backend.DataQuery{RefID: "A", JSON: []byte(`{"queryType":"sql","rawSql":"SELECT x"}`)})
	response := e.executeSqlQuery(context.Background(), prepared)
	if response.Status != backend.StatusBadRequest || response.Error == nil || response.Error.Error() != "bad query (sqlQueryId: q-1)" {
		t.Fatalf("%+v", response)
	}
}

func TestQueryDataMixesSqlAndCompute(t *testing.T) {
	for _, settings := range []string{`{}`, `{"queryApi":"sql"}`, `{"queryApi":"compute"}`, `{"enableSql":true}`} {
		t.Run(settings, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64}}, nil)
			stream := sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) { b.Field(0).(*array.Float64Builder).Append(42) })
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = w.Write(stream)
			}))
			defer srv.Close()
			workspace := sqlTestWorkspaceRid(t)
			compute := &mockComputeService{batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
				Results: []computeapi.ComputeWithUnitsResult{createMockArrowComputeResult([]float64{7})},
			}}
			ds := &Datasource{workspaceRid: &workspace, sqlClient: newSqlClient(srv.URL, http.DefaultTransport), computeService: compute}
			req := newQueryRequest([]backend.DataQuery{
				{RefID: "SQL", JSON: []byte(`{"queryType":"sql","rawSql":"SELECT 42","format":"table"}`)},
				{RefID: "Compute", JSON: []byte(`{"queryType":"timeShift","assetRid":"ri.nominal.asset.1","channel":"temp","dataScopeName":"default","buckets":100}`), TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(3600, 0)}},
				{RefID: "InvalidSQL", JSON: []byte(`{"queryType":"sql","rawSql":" "}`)},
			})
			req.PluginContext.DataSourceInstanceSettings.JSONData = []byte(settings)
			response, err := ds.QueryData(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			for _, ref := range []string{"SQL", "Compute"} {
				result, ok := response.Responses[ref]
				if !ok || result.Error != nil || len(result.Frames) != 1 || result.Frames[0].Rows() != 1 {
					t.Fatalf("%s: %+v", ref, result)
				}
			}
			if response.Responses["InvalidSQL"].Error == nil || calls.Load() != 1 || compute.batchComputeCalls != 1 || len(compute.lastBatchRequest.Requests) != 1 {
				t.Fatalf("SQL calls=%d Compute calls=%d responses=%+v", calls.Load(), compute.batchComputeCalls, response.Responses)
			}
		})
	}
}

func sqlTestWorkspaceRid(t *testing.T) rids.WorkspaceRid {
	t.Helper()
	workspace, err := parseWorkspaceRid(testWorkspaceRid)
	if err != nil {
		t.Fatal(err)
	}
	return *workspace
}

func TestSqlQueriesBoundParallelRequests(t *testing.T) {
	var active, peak, total atomic.Int32
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	stream := sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) { b.Field(0).(*array.Int64Builder).Append(1) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		total.Add(1)
		for old := peak.Load(); current > old; old = peak.Load() {
			if peak.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write(stream)
	}))
	defer srv.Close()
	workspace := sqlTestWorkspaceRid(t)
	e := newNominalQueryExecution(&Datasource{workspaceRid: &workspace, sqlClient: newSqlClient(srv.URL, nil)}, &models.PluginSettings{Secrets: &models.SecretPluginSettings{ApiKey: "k"}})
	queries := make([]backend.DataQuery, 20)
	for i := range queries {
		queries[i] = backend.DataQuery{RefID: fmt.Sprint(i), JSON: []byte(`{"queryType":"sql","rawSql":"SELECT 1","format":"table"}`)}
	}
	response := e.Execute(context.Background(), queries)
	if peak.Load() > 8 || total.Load() != 20 || len(response.Responses) != 20 {
		t.Fatalf("peak=%d total=%d responses=%d", peak.Load(), total.Load(), len(response.Responses))
	}
	for ref, res := range response.Responses {
		if res.Error != nil {
			t.Fatalf("%s: %v", ref, res.Error)
		}
	}
}
