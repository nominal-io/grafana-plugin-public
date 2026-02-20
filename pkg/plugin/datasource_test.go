package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/safelong"
)

func TestBuildComputeContext(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name         string
		qm           NominalQueryModel
		startSeconds int64
		endSeconds   int64
		expectedVars int
	}{
		{
			name: "basic context with assetRid",
			qm: NominalQueryModel{
				AssetRid: "ri.nominal.asset.12345",
				Channel:  "temperature",
			},
			startSeconds: 1000,
			endSeconds:   2000,
			expectedVars: 1, // Just assetRid
		},
		{
			name: "context with template variables",
			qm: NominalQueryModel{
				AssetRid: "ri.nominal.asset.12345",
				Channel:  "temperature",
				TemplateVariables: map[string]interface{}{
					"env":    "prod",
					"region": "us-east",
				},
			},
			startSeconds: 1000,
			endSeconds:   2000,
			expectedVars: 3, // assetRid + 2 template vars
		},
		{
			name: "context with non-string template variables ignored",
			qm: NominalQueryModel{
				AssetRid: "ri.nominal.asset.12345",
				Channel:  "temperature",
				TemplateVariables: map[string]interface{}{
					"strVar": "value",
					"intVar": 123, // non-string, should be ignored
				},
			},
			startSeconds: 1000,
			endSeconds:   2000,
			expectedVars: 2, // assetRid + 1 string template var
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := ds.buildComputeContext(tt.qm, tt.startSeconds, tt.endSeconds)

			if len(ctx.Variables) != tt.expectedVars {
				t.Errorf("expected %d variables, got %d", tt.expectedVars, len(ctx.Variables))
			}

			// Verify assetRid is always present
			if _, ok := ctx.Variables["assetRid"]; !ok {
				t.Error("expected assetRid variable to be present")
			}

			// Verify non-string template variables are excluded
			if tt.qm.TemplateVariables != nil {
				for key, value := range tt.qm.TemplateVariables {
					_, isString := value.(string)
					_, inContext := ctx.Variables[computeapi.VariableName(key)]
					if !isString && inContext {
						t.Errorf("non-string variable %q should be excluded from context", key)
					}
					if isString && !inContext {
						t.Errorf("string variable %q should be included in context", key)
					}
				}
			}
		})
	}
}

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

func TestQueryDataWithInvalidJSON(t *testing.T) {
	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{
				RefID: "A",
				JSON:  []byte(`{invalid json`),
			},
		},
	}

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

func TestQueryDataRoutesQueriesByType(t *testing.T) {
	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name           string
		queries        []backend.DataQuery
		expectedRefIDs []string
		checkFrame     func(t *testing.T, refID string, response backend.DataResponse)
	}{
		{
			name: "routes legacy constant query correctly",
			queries: []backend.DataQuery{
				{
					RefID:     "LegacyConstant",
					JSON:      mustMarshal(NominalQueryModel{Constant: 42.0}),
					TimeRange: timeRange,
				},
			},
			expectedRefIDs: []string{"LegacyConstant"},
			checkFrame: func(t *testing.T, refID string, response backend.DataResponse) {
				if response.Error != nil {
					t.Errorf("unexpected error for %s: %v", refID, response.Error)
				}
				if len(response.Frames) != 1 {
					t.Fatalf("expected 1 frame for %s, got %d", refID, len(response.Frames))
				}
				if response.Frames[0].Name != "response" {
					t.Errorf("expected frame name 'response' for %s, got %q", refID, response.Frames[0].Name)
				}
			},
		},
		{
			name: "routes legacy query text query correctly",
			queries: []backend.DataQuery{
				{
					RefID:     "LegacyQueryText",
					JSON:      mustMarshal(NominalQueryModel{QueryText: "SELECT * FROM data"}),
					TimeRange: timeRange,
				},
			},
			expectedRefIDs: []string{"LegacyQueryText"},
			checkFrame: func(t *testing.T, refID string, response backend.DataResponse) {
				if response.Error != nil {
					t.Errorf("unexpected error for %s: %v", refID, response.Error)
				}
				if len(response.Frames) != 1 {
					t.Fatalf("expected 1 frame for %s, got %d", refID, len(response.Frames))
				}
			},
		},
		{
			name: "handles multiple legacy queries",
			queries: []backend.DataQuery{
				{
					RefID:     "A",
					JSON:      mustMarshal(NominalQueryModel{Constant: 10.0}),
					TimeRange: timeRange,
				},
				{
					RefID:     "B",
					JSON:      mustMarshal(NominalQueryModel{Constant: 20.0}),
					TimeRange: timeRange,
				},
			},
			expectedRefIDs: []string{"A", "B"},
			checkFrame: func(t *testing.T, refID string, response backend.DataResponse) {
				if response.Error != nil {
					t.Errorf("unexpected error for %s: %v", refID, response.Error)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &backend.QueryDataRequest{
				PluginContext: backend.PluginContext{
					DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
						JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
						DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
					},
				},
				Queries: tt.queries,
			}

			resp, err := ds.QueryData(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(resp.Responses) != len(tt.expectedRefIDs) {
				t.Fatalf("expected %d responses, got %d", len(tt.expectedRefIDs), len(resp.Responses))
			}

			for _, refID := range tt.expectedRefIDs {
				response, ok := resp.Responses[refID]
				if !ok {
					t.Errorf("expected response for %q", refID)
					continue
				}
				if tt.checkFrame != nil {
					tt.checkFrame(t, refID, response)
				}
			}
		})
	}
}

// mustMarshal is a test helper that panics on marshal failure
func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func makeBatchableQueries(count int, timeRange backend.TimeRange) []backend.DataQuery {
	queries := make([]backend.DataQuery, count)
	for i := 0; i < count; i++ {
		queries[i] = backend.DataQuery{
			RefID:     fmt.Sprintf("Q%03d", i),
			JSON:      mustMarshal(NominalQueryModel{AssetRid: fmt.Sprintf("ri.nominal.asset.%d", i+1), Channel: fmt.Sprintf("temp%d", i+1), Buckets: 100}),
			TimeRange: timeRange,
		}
	}
	return queries
}

func makeBatchComputeWithUnitsResponse(count int) computeapi.BatchComputeWithUnitsResponse {
	results := make([]computeapi.ComputeWithUnitsResult, count)
	for i := 0; i < count; i++ {
		results[i] = createMockComputeResult([]float64{float64(i + 1)})
	}
	return computeapi.BatchComputeWithUnitsResponse{Results: results}
}

func TestBuildComputeRequest(t *testing.T) {
	ds := &Datasource{}

	qm := NominalQueryModel{
		AssetRid:      "ri.nominal.asset.12345",
		Channel:       "temperature",
		DataScopeName: "default",
		Buckets:       100,
	}

	timeRange := backend.TimeRange{
		From: time.Unix(1704067200, 0), // 2024-01-01 00:00:00 UTC
		To:   time.Unix(1704153600, 0), // 2024-01-02 00:00:00 UTC
	}

	req := ds.buildComputeRequest(qm, timeRange)

	// Verify start and end times
	if int64(req.Start.Seconds) != 1704067200 {
		t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, 1704067200)
	}
	if int64(req.End.Seconds) != 1704153600 {
		t.Errorf("End.Seconds = %d, want %d", req.End.Seconds, 1704153600)
	}
}

func TestBuildChannelSeries(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name          string
		assetRid      string
		channel       string
		dataScopeName string
	}{
		{
			name:          "builds series with all parameters",
			assetRid:      "ri.nominal.asset.123",
			channel:       "temperature",
			dataScopeName: "default",
		},
		{
			name:          "builds series with empty dataScopeName",
			assetRid:      "ri.nominal.asset.456",
			channel:       "pressure",
			dataScopeName: "",
		},
		{
			name:          "builds series with special characters in channel",
			assetRid:      "ri.nominal.asset.789",
			channel:       "sensor/value-1",
			dataScopeName: "scope_test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ds.buildChannelSeries(tt.assetRid, tt.channel, tt.dataScopeName)

			// Serialize to JSON to inspect the structure
			// This is more maintainable than using the visitor pattern with 35+ nil arguments
			jsonBytes, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("failed to marshal NumericSeries: %v", err)
			}

			jsonStr := string(jsonBytes)

			// Verify the series is a channel type (timeShift is added by buildComputeRequest, not here)
			if !strings.Contains(jsonStr, `"type":"channel"`) {
				t.Errorf("expected channel series type in JSON, got: %s", jsonStr)
			}

			// Verify it's an asset-based channel
			if !strings.Contains(jsonStr, `"type":"asset"`) {
				t.Errorf("expected asset channel type in JSON, got: %s", jsonStr)
			}

			// Verify the channel name is present as a literal
			expectedChannelLiteral := fmt.Sprintf(`"channel":{"type":"literal","literal":"%s"}`, tt.channel)
			if !strings.Contains(jsonStr, expectedChannelLiteral) {
				t.Errorf("expected channel literal %q in JSON, got: %s", tt.channel, jsonStr)
			}

			// Verify the dataScopeName is present as a literal
			expectedDataScopeLiteral := fmt.Sprintf(`"dataScopeName":{"type":"literal","literal":"%s"}`, tt.dataScopeName)
			if !strings.Contains(jsonStr, expectedDataScopeLiteral) {
				t.Errorf("expected dataScopeName literal %q in JSON, got: %s", tt.dataScopeName, jsonStr)
			}

			// Verify the assetRid is a variable reference (not a literal)
			// The actual assetRid value is passed via context variables
			expectedAssetRidVar := `"assetRid":{"type":"variable","variable":"assetRid"}`
			if !strings.Contains(jsonStr, expectedAssetRidVar) {
				t.Errorf("expected assetRid to be variable reference in JSON, got: %s", jsonStr)
			}
		})
	}
}

// mockComputeService implements computeapi.ComputeServiceClient for testing
type mockComputeService struct {
	mu                    sync.Mutex
	batchComputeCalls     int
	lastBatchRequest      computeapi.BatchComputeWithUnitsRequest
	batchRequests         []computeapi.BatchComputeWithUnitsRequest
	batchComputeResponse  computeapi.BatchComputeWithUnitsResponse
	batchComputeResponses []computeapi.BatchComputeWithUnitsResponse
	batchComputeError     error
	batchComputeErrors    []error
	singleComputeCalls    int
}

func (m *mockComputeService) Compute(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi.ComputeNodeRequest) (computeapi.ComputeNodeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.singleComputeCalls++
	return computeapi.ComputeNodeResponse{}, nil
}

func (m *mockComputeService) ParameterizedCompute(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi.ParameterizedComputeNodeRequest) (computeapi.ParameterizedComputeNodeResponse, error) {
	return computeapi.ParameterizedComputeNodeResponse{}, nil
}

func (m *mockComputeService) ComputeUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi.ComputeUnitsRequest) (computeapi.ComputeUnitResult, error) {
	return computeapi.ComputeUnitResult{}, nil
}

func (m *mockComputeService) BatchComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batchComputeCalls++
	m.lastBatchRequest = requestArg
	m.batchRequests = append(m.batchRequests, requestArg)

	callIndex := m.batchComputeCalls - 1
	if callIndex < len(m.batchComputeErrors) && m.batchComputeErrors[callIndex] != nil {
		return computeapi.BatchComputeWithUnitsResponse{}, m.batchComputeErrors[callIndex]
	}
	if callIndex < len(m.batchComputeResponses) {
		return m.batchComputeResponses[callIndex], nil
	}
	if m.batchComputeError != nil {
		return computeapi.BatchComputeWithUnitsResponse{}, m.batchComputeError
	}
	return m.batchComputeResponse, nil
}

func (m *mockComputeService) BatchComputeUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi.BatchComputeUnitsRequest) (computeapi.BatchComputeUnitResult, error) {
	return computeapi.BatchComputeUnitResult{}, nil
}

func (m *mockComputeService) ComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi.ComputeWithUnitsRequest) (computeapi.ComputeWithUnitsResponse, error) {
	return computeapi.ComputeWithUnitsResponse{}, nil
}

func TestBatchQueryExecution(t *testing.T) {
	// Create mock compute service
	mockService := &mockComputeService{}

	// Create mock response with success results for each query
	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockComputeResult([]float64{1.0, 2.0, 3.0}),
			createMockComputeResult([]float64{4.0, 5.0, 6.0}),
			createMockComputeResult([]float64{7.0, 8.0, 9.0}),
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// Create 3 batchable queries (asset + channel)
	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify batch was called exactly once (not 3 times)
	if mockService.batchComputeCalls != 1 {
		t.Errorf("expected 1 batch compute call, got %d", mockService.batchComputeCalls)
	}

	// Verify single compute was never called
	if mockService.singleComputeCalls != 0 {
		t.Errorf("expected 0 single compute calls, got %d", mockService.singleComputeCalls)
	}

	// Verify the batch request contained all 3 queries
	if len(mockService.lastBatchRequest.Requests) != 3 {
		t.Errorf("expected 3 requests in batch, got %d", len(mockService.lastBatchRequest.Requests))
	}

	// Verify we got responses for all queries
	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	for _, refID := range []string{"A", "B", "C"} {
		response, ok := resp.Responses[refID]
		if !ok {
			t.Errorf("expected response for %q", refID)
			continue
		}
		if response.Error != nil {
			t.Errorf("unexpected error for %s: %v", refID, response.Error)
		}
	}
}

func TestBatchQueryChunksAtSubrequestLimit(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponses: []computeapi.BatchComputeWithUnitsResponse{
			makeBatchComputeWithUnitsResponse(maxBatchComputeSubrequests),
			makeBatchComputeWithUnitsResponse(1),
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	queries := makeBatchableQueries(maxBatchComputeSubrequests+1, timeRange)

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockService.batchComputeCalls != 2 {
		t.Fatalf("expected 2 batch compute calls, got %d", mockService.batchComputeCalls)
	}
	if len(mockService.batchRequests) != 2 {
		t.Fatalf("expected 2 recorded batch requests, got %d", len(mockService.batchRequests))
	}
	if len(mockService.batchRequests[0].Requests) != maxBatchComputeSubrequests {
		t.Fatalf("expected first chunk size %d, got %d", maxBatchComputeSubrequests, len(mockService.batchRequests[0].Requests))
	}
	if len(mockService.batchRequests[1].Requests) != 1 {
		t.Fatalf("expected second chunk size 1, got %d", len(mockService.batchRequests[1].Requests))
	}
	if len(resp.Responses) != len(queries) {
		t.Fatalf("expected %d responses, got %d", len(queries), len(resp.Responses))
	}

	for _, q := range queries {
		response := resp.Responses[q.RefID]
		if response.Error != nil {
			t.Fatalf("expected no error for %s, got %v", q.RefID, response.Error)
		}
	}
}

func TestBatchQueryChunkTransportErrorOnlyFailsThatChunk(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponses: []computeapi.BatchComputeWithUnitsResponse{
			makeBatchComputeWithUnitsResponse(maxBatchComputeSubrequests),
		},
		batchComputeErrors: []error{
			nil,
			fmt.Errorf("API error: service unavailable"),
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	queries := makeBatchableQueries(maxBatchComputeSubrequests+1, timeRange)

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockService.batchComputeCalls != 2 {
		t.Fatalf("expected 2 batch compute calls, got %d", mockService.batchComputeCalls)
	}

	for i := 0; i < maxBatchComputeSubrequests; i++ {
		refID := fmt.Sprintf("Q%03d", i)
		response := resp.Responses[refID]
		if response.Error != nil {
			t.Fatalf("expected success for %s, got %v", refID, response.Error)
		}
	}

	failedChunkRefID := fmt.Sprintf("Q%03d", maxBatchComputeSubrequests)
	failedChunkResponse := resp.Responses[failedChunkRefID]
	if failedChunkResponse.Error == nil {
		t.Fatalf("expected error for %s, got nil", failedChunkRefID)
	}
	if !strings.Contains(failedChunkResponse.Error.Error(), "Batch compute failed") {
		t.Fatalf("expected batch failure message for %s, got %v", failedChunkRefID, failedChunkResponse.Error)
	}
}

func TestBatchQueryMixedWithLegacy(t *testing.T) {
	// Create mock compute service
	mockService := &mockComputeService{}

	// Create mock response for the 2 batchable queries
	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockComputeResult([]float64{1.0, 2.0}),
			createMockComputeResult([]float64{3.0, 4.0}),
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// Mix of batchable and legacy queries
	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{Constant: 42.0}), // Legacy - not batched
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify batch was called once for the 2 batchable queries
	if mockService.batchComputeCalls != 1 {
		t.Errorf("expected 1 batch compute call, got %d", mockService.batchComputeCalls)
	}

	// Verify the batch request contained only the 2 batchable queries
	if len(mockService.lastBatchRequest.Requests) != 2 {
		t.Errorf("expected 2 requests in batch, got %d", len(mockService.lastBatchRequest.Requests))
	}

	// Verify we got responses for all 3 queries
	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	// Legacy query B should have been handled separately
	respB, ok := resp.Responses["B"]
	if !ok {
		t.Error("expected response for legacy query B")
	} else if respB.Error != nil {
		t.Errorf("unexpected error for B: %v", respB.Error)
	} else if len(respB.Frames) != 1 || respB.Frames[0].Name != "response" {
		t.Error("legacy query B should have frame named 'response'")
	}
}

func TestBatchQueryError(t *testing.T) {
	// Create mock compute service that returns an error
	mockService := &mockComputeService{
		batchComputeError: fmt.Errorf("API error: service unavailable"),
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify we got error responses for all queries when batch fails
	for _, refID := range []string{"A", "B"} {
		response, ok := resp.Responses[refID]
		if !ok {
			t.Errorf("expected response for %q", refID)
			continue
		}
		if response.Error == nil {
			t.Errorf("expected error for %s, got nil", refID)
		}
		if !strings.Contains(response.Error.Error(), "Batch compute failed") {
			t.Errorf("expected batch error message for %s, got: %v", refID, response.Error)
		}
	}
}

func TestBatchQueryWithPartialErrors(t *testing.T) {
	// Create mock compute service
	mockService := &mockComputeService{}

	// Create mock response with mix of success and error results
	// This simulates a batch where one query fails (e.g., channel not found)
	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockComputeResult([]float64{1.0, 2.0, 3.0}), // Query A: Success
			createMockErrorResult(404, "CHANNEL_NOT_FOUND"),   // Query B: Error
			createMockComputeResult([]float64{7.0, 8.0, 9.0}), // Query C: Success
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "nonexistent", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify we got responses for all queries
	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	// Query A should succeed
	respA := resp.Responses["A"]
	if respA.Error != nil {
		t.Errorf("expected no error for A, got: %v", respA.Error)
	}
	if len(respA.Frames) != 1 {
		t.Errorf("expected 1 frame for A, got %d", len(respA.Frames))
	}

	// Query B should have an error from the API
	respB := resp.Responses["B"]
	if respB.Error == nil {
		t.Error("expected error for B, got nil")
	} else {
		if !strings.Contains(respB.Error.Error(), "Compute error") {
			t.Errorf("expected 'Compute error' in message for B, got: %v", respB.Error)
		}
		// Error message format: "Compute error: %v (code: %v)" with ErrorType and ErrorCode
		if !strings.Contains(respB.Error.Error(), "CHANNEL_NOT_FOUND") {
			t.Errorf("expected error type 'CHANNEL_NOT_FOUND' in message for B, got: %v", respB.Error)
		}
		if !strings.Contains(respB.Error.Error(), "404") {
			t.Errorf("expected error code '404' in message for B, got: %v", respB.Error)
		}
	}

	// Query C should succeed despite B failing
	respC := resp.Responses["C"]
	if respC.Error != nil {
		t.Errorf("expected no error for C, got: %v", respC.Error)
	}
	if len(respC.Frames) != 1 {
		t.Errorf("expected 1 frame for C, got %d", len(respC.Frames))
	}
}

func TestBatchQueryWithMissingResults(t *testing.T) {
	// Create mock compute service that returns fewer results than queries
	mockService := &mockComputeService{}

	// Only return 2 results for 3 queries - simulates API bug or truncation
	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockComputeResult([]float64{1.0, 2.0, 3.0}),
			createMockComputeResult([]float64{4.0, 5.0, 6.0}),
			// Missing third result
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// Create 3 batchable queries
	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify we got responses for all 3 queries
	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	// Query A should succeed (has result at index 0)
	respA := resp.Responses["A"]
	if respA.Error != nil {
		t.Errorf("expected no error for A, got: %v", respA.Error)
	}
	if len(respA.Frames) != 1 {
		t.Errorf("expected 1 frame for A, got %d", len(respA.Frames))
	}

	// Query B should succeed (has result at index 1)
	respB := resp.Responses["B"]
	if respB.Error != nil {
		t.Errorf("expected no error for B, got: %v", respB.Error)
	}
	if len(respB.Frames) != 1 {
		t.Errorf("expected 1 frame for B, got %d", len(respB.Frames))
	}

	// Query C should have an error (no result at index 2)
	respC := resp.Responses["C"]
	if respC.Error == nil {
		t.Error("expected error for C due to missing result, got nil")
	} else if !strings.Contains(respC.Error.Error(), "Missing result in batch response") {
		t.Errorf("expected 'Missing result in batch response' error for C, got: %v", respC.Error)
	}
}

// createMockErrorResult creates a mock ComputeWithUnitsResult with an error
func createMockErrorResult(code int, errorType string) computeapi.ComputeWithUnitsResult {
	errorResult := computeapi.ErrorResult{
		Code:      computeapi.ErrorCode(code),
		ErrorType: computeapi.ErrorType(errorType),
	}

	computeResult := computeapi.NewComputeNodeResultFromError(errorResult)

	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

// createMockComputeResult creates a mock ComputeWithUnitsResult with numeric data
func createMockComputeResult(values []float64) computeapi.ComputeWithUnitsResult {
	timestamps := make([]api.Timestamp, len(values))
	baseTime := int64(1704067200) // 2024-01-01 00:00:00 UTC
	for i := range timestamps {
		timestamps[i] = api.Timestamp{
			Seconds: safelong.SafeLong(baseTime + int64(i*60)),
			Nanos:   safelong.SafeLong(0),
		}
	}

	numericPlot := computeapi.NumericPlot{
		Timestamps: timestamps,
		Values:     values,
	}

	computeResponse := computeapi.NewComputeNodeResponseFromNumeric(numericPlot)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)

	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

// Benchmark for new batch-related function
func BenchmarkBuildComputeContext(b *testing.B) {
	ds := &Datasource{}
	qm := NominalQueryModel{
		AssetRid: "ri.nominal.asset.12345",
		Channel:  "temperature",
		TemplateVariables: map[string]interface{}{
			"env":    "prod",
			"region": "us-east",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ds.buildComputeContext(qm, 1704067200, 1704153600)
	}
}
