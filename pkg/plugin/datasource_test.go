package plugin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/uuid"
)

func TestQueryDataWithNilDataSourceInstanceSettings(t *testing.T) {
	ds := withCatalog(&Datasource{})

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
	ds := withCatalog(&Datasource{})

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

func TestQueryDataWithInvalidJSON(t *testing.T) {
	ds := withCatalog(&Datasource{
		settings: testDatasourceSettings(),
	})

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
	(withCatalog(&Datasource{})).Dispose()

	mockService := &mockComputeService{}
	ds := withCatalog(&Datasource{computeService: mockService})
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

// queryHandler serves only QueryData, so a test can run it behind SDK middlewares.
type queryHandler struct {
	backend.BaseHandler
	ds *Datasource
}

func (h queryHandler) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	return h.ds.QueryData(ctx, req)
}

// Grafana cancels a query when a dashboard refresh replaces it and discards the reply, so the
// SDK request logger records it as cancelled instead of logging failed queries.
func TestCancelledQueryIsNotLoggedAsError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cancelled  bool
		wantErrors bool
	}{
		{"cancelled query", true, false},
		{"failed query", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := countErrorLogs(t)
			ds := newTestDatasource("https://api.test", &mockAuthService{}, nil)
			ds.computeService = &mockComputeService{batchComputeError: errors.New("compute failed")}
			handler, err := backend.HandlerFromMiddlewares(queryHandler{ds: ds}, backend.NewLoggerMiddleware(logger, nil))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			query := NominalQueryModel{AssetRid: "ri.scout.main.asset.a", DataScopeName: "default", Channel: "temp", ChannelDataType: "numeric"}
			_, _ = handler.QueryData(ctx, &backend.QueryDataRequest{
				PluginContext: backend.PluginContext{DataSourceInstanceSettings: &ds.settings},
				Queries:       []backend.DataQuery{{RefID: "A", JSON: mustMarshal(query), TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(3600, 0)}}},
			})
			if got := logger.errors.Load() > 0; got != tc.wantErrors {
				t.Errorf("logged errors = %v, want %v (%d error logs)", got, tc.wantErrors, logger.errors.Load())
			}
		})
	}
}
