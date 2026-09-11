package plugin

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/uuid"
)

func TestQueryDataWithNilDataSourceInstanceSettings(t *testing.T) {
	ds := &Datasource{}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: nil, // Not configured
		},
		Queries: []backend.DataQuery{
			{RefID: "A"},
			{RefID: "B"},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return error responses for all queries
	if len(resp.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(resp.Responses))
	}

	for refID, response := range resp.Responses {
		if response.Error == nil {
			t.Errorf("expected error for query %s, got nil", refID)
		}
		if response.Status != backend.StatusBadRequest {
			t.Errorf("expected StatusBadRequest for query %s, got %v", refID, response.Status)
		}
	}
}

func TestCheckHealthWithNilDataSourceInstanceSettings(t *testing.T) {
	ds := &Datasource{}

	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: nil,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != backend.HealthStatusError {
		t.Fatalf("expected error health status, got %v", result.Status)
	}
	if result.Message != "Data source is not configured" {
		t.Fatalf("expected not configured message, got %q", result.Message)
	}
}

func TestQueryDataRecoversPanicOutsideBatchExecution(t *testing.T) {
	// Channel metadata inference runs during query preparation, before the
	// per-chunk and per-result recovery boundaries exist.
	assetRid := "ri.scout.main.asset.abc123"
	dataSourceRid := "ri.scout.main.data-source.ds1"
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

	mockDS := &mockDatasourceService{
		searchChannelsFunc: func(context.Context, bearertoken.Token, datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
			panic("metadata lookup exploded")
		},
	}
	ds := &Datasource{
		computeService:     &mockComputeService{},
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	}
	defer ds.Dispose()

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temp", DataScopeName: "default", ChannelDataType: "numeric"})},
			{RefID: "B", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "pressure", DataScopeName: "default", ChannelDataType: "numeric"})},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockDS.searchChannelsCalls == 0 {
		t.Fatal("test did not reach the panicking metadata lookup")
	}
	for _, refID := range []string{"A", "B"} {
		r, ok := resp.Responses[refID]
		if !ok || r.Error == nil {
			t.Fatalf("expected an error response for %s, got %+v", refID, r)
		}
		if got := r.Error.Error(); got != "Internal error while handling query request" {
			t.Fatalf("expected the request-level containment message for %s, got %q", refID, got)
		}
	}
}

func TestQueryDataWithInvalidJSON(t *testing.T) {
	ds := &Datasource{
		settings: testDatasourceSettings(),
	}

	req := newQueryRequest([]backend.DataQuery{
		{
			RefID: "A",
			JSON:  []byte(`{invalid json`),
		},
	})

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(resp.Responses))
	}

	response := resp.Responses["A"]
	if response.Error == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
	if response.Status != backend.StatusBadRequest {
		t.Errorf("expected StatusBadRequest, got %v", response.Status)
	}
}

func TestKillDeliverySurvivesDispose(t *testing.T) {
	// Dispose also runs for instances that never queried, so it must not panic.
	(&Datasource{}).Dispose()

	mockService := &mockComputeService{}
	ds := &Datasource{computeService: mockService}
	target := killTarget{token: bearertoken.Token("t1")}

	ds.enqueueKill(uuid.NewUUID(), target)
	ds.Dispose()
	waitForCondition(t, 2*time.Second, func() bool { return len(mockService.killCallsSnapshot()) == 1 })

	// An enqueue that lands after Dispose still flushes on the normal interval:
	// the SDK disposes a replaced instance without draining its in-flight
	// requests, and those are exactly the kills this exists to deliver.
	ds.enqueueKill(uuid.NewUUID(), target)
	waitForCondition(t, 2*time.Second, func() bool { return len(mockService.killCallsSnapshot()) == 2 })
}
