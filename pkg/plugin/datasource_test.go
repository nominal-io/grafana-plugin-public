package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-io/nominal-api-go/api/rids"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
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
			JSON:      mustMarshal(NominalQueryModel{AssetRid: fmt.Sprintf("ri.nominal.asset.%d", i+1), Channel: fmt.Sprintf("temp%d", i+1), DataScopeName: "ds1", Buckets: 100}),
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

	req := ds.buildComputeRequest(qm, timeRange, 0)

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

// mockComputeService implements computeapi1.ComputeServiceClient for testing
type mockComputeService struct {
	mu                    sync.Mutex
	batchComputeCalls     int
	lastBatchRequest      computeapi1.BatchComputeWithUnitsRequest
	batchRequests         []computeapi1.BatchComputeWithUnitsRequest
	batchComputeResponse  computeapi.BatchComputeWithUnitsResponse
	batchComputeResponses []computeapi.BatchComputeWithUnitsResponse
	batchComputeError     error
	batchComputeErrors    []error
	singleComputeCalls    int
}

func (m *mockComputeService) Compute(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ComputeNodeRequest) (computeapi.ComputeNodeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.singleComputeCalls++
	return computeapi.ComputeNodeResponse{}, nil
}

func (m *mockComputeService) ParameterizedCompute(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ParameterizedComputeNodeRequest) (computeapi.ParameterizedComputeNodeResponse, error) {
	return computeapi.ParameterizedComputeNodeResponse{}, nil
}

func (m *mockComputeService) ComputeUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ComputeUnitsRequest) (computeapi.ComputeUnitResult, error) {
	return computeapi.ComputeUnitResult{}, nil
}

func (m *mockComputeService) BatchComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
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

func (m *mockComputeService) BatchComputeUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.BatchComputeUnitsRequest) (computeapi.BatchComputeUnitResult, error) {
	return computeapi.BatchComputeUnitResult{}, nil
}

func (m *mockComputeService) ComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ComputeWithUnitsRequest) (computeapi.ComputeWithUnitsResponse, error) {
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
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
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

func TestQueryDataInfersMissingStringChannelType(t *testing.T) {
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

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("state"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1")),
					DataType:   &stringType,
				},
			},
		},
	}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockEnumComputeResult([]string{"idle", "active"}, []int{0, 1}),
			},
		},
	}

	ds := &Datasource{
		computeService:    mockCompute,
		datasourceService: mockDS,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{
				RefID: "A",
				JSON: mustMarshal(NominalQueryModel{
					AssetRid:      assetRid,
					Channel:       "state",
					DataScopeName: "default",
					Buckets:       100,
				}),
				TimeRange: timeRange,
			},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockCompute.batchComputeCalls != 1 {
		t.Fatalf("expected 1 batch compute call, got %d", mockCompute.batchComputeCalls)
	}
	if len(mockCompute.lastBatchRequest.Requests) != 1 {
		t.Fatalf("expected 1 compute request, got %d", len(mockCompute.lastBatchRequest.Requests))
	}
	if len(mockDS.searchChannelsRequest.ExactMatch) != 1 || mockDS.searchChannelsRequest.ExactMatch[0] != "state" {
		t.Fatalf("expected exact-match channel lookup for state, got %v", mockDS.searchChannelsRequest.ExactMatch)
	}

	response := resp.Responses["A"]
	if response.Error != nil {
		t.Fatalf("unexpected response error: %v", response.Error)
	}

	jsonBytes, err := json.Marshal(mockCompute.lastBatchRequest.Requests[0].Node)
	if err != nil {
		t.Fatalf("failed to marshal compute node: %v", err)
	}
	if !strings.Contains(string(jsonBytes), `"type":"enum"`) {
		t.Fatalf("expected enum compute request after inferring string type, got: %s", string(jsonBytes))
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
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{Constant: 42.0}), // Legacy - not batched
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
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
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
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
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "nonexistent", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
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
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
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

func TestErrorMessageFormatPreservation(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric channel error retains original format without hint", func(t *testing.T) {
		result := createMockErrorResult(404, "CHANNEL_NOT_FOUND")
		qm := NominalQueryModel{
			Channel:  "temperature",
			AssetRid: "ri.nominal.asset.test",
		}
		resp := ds.transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		// Original format must be preserved exactly
		if !strings.Contains(errMsg, "Compute error: CHANNEL_NOT_FOUND (code: 404)") {
			t.Errorf("expected original error format, got: %s", errMsg)
		}
		// Numeric errors must NOT have hints
		if strings.Contains(errMsg, "Hint:") {
			t.Errorf("numeric channel errors should not have hints, got: %s", errMsg)
		}
	})

	t.Run("generic compute error retains original format without hint", func(t *testing.T) {
		result := createMockErrorResult(500, "Compute:InternalError")
		qm := NominalQueryModel{
			Channel:  "pressure",
			AssetRid: "ri.nominal.asset.test",
		}
		resp := ds.transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Compute error: Compute:InternalError (code: 500)") {
			t.Errorf("expected original error format, got: %s", errMsg)
		}
		if strings.Contains(errMsg, "Hint:") {
			t.Errorf("generic errors should not have hints, got: %s", errMsg)
		}
	})

	t.Run("ChannelHasWrongType always includes hint regardless of ChannelDataType", func(t *testing.T) {
		// The hint fires for any ChannelHasWrongType error — the stored type and the
		// API's actual type disagree regardless of whether ChannelDataType is set.
		result := createMockErrorResult(400, "Compute:ChannelHasWrongType")
		qm := NominalQueryModel{
			Channel:         "status",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "string",
		}
		resp := ds.transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Compute error: Compute:ChannelHasWrongType (code: 400)") {
			t.Errorf("expected raw error in message, got: %s", errMsg)
		}
		if !strings.Contains(errMsg, "Hint:") {
			t.Errorf("expected hint for ChannelHasWrongType even when ChannelDataType is populated, got: %s", errMsg)
		}
	})

	t.Run("ChannelHasWrongType with empty metadata includes hint", func(t *testing.T) {
		result := createMockErrorResult(400, "Compute:ChannelHasWrongType")
		qm := NominalQueryModel{
			Channel:         "status",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "",
		}
		resp := ds.transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Hint:") {
			t.Errorf("expected hint when ChannelDataType is empty, got: %s", errMsg)
		}
	})

	t.Run("bare ChannelHasWrongType also gets hint", func(t *testing.T) {
		result := createMockErrorResult(400, "ChannelHasWrongType")
		qm := NominalQueryModel{
			Channel:         "mode",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "",
		}
		resp := ds.transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Hint:") {
			t.Errorf("expected hint for bare ChannelHasWrongType, got: %s", errMsg)
		}
	})
}

func TestNumericRegressionAfterPhase3(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric batch result produces float64 value fields", func(t *testing.T) {
		values := []float64{1.5, 2.5, 3.5, 4.5}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "temperature",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		// Must have time + value fields
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}
		// Value field must be float64
		valueField := frame.Fields[1]
		if valueField.Len() != len(values) {
			t.Fatalf("expected %d values, got %d", len(values), valueField.Len())
		}
		for i := 0; i < valueField.Len(); i++ {
			v, ok := valueField.At(i).(float64)
			if !ok {
				t.Errorf("value at index %d is not float64, got %T", i, valueField.At(i))
			}
			if v != values[i] {
				t.Errorf("value at index %d: expected %f, got %f", i, values[i], v)
			}
		}
	})

	t.Run("numeric frame does not have table type metadata", func(t *testing.T) {
		values := []float64{10.0, 20.0}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "pressure",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		// Numeric frames should NOT have FrameTypeTable metadata (that's for enum frames)
		if frame.Meta != nil && frame.Meta.Type == data.FrameTypeTable {
			t.Error("numeric frame should not have FrameTypeTable metadata")
		}
	})

	t.Run("numeric frame name includes channel name", func(t *testing.T) {
		values := []float64{5.0}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "velocity",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		if !strings.Contains(frame.Name, "velocity") {
			t.Errorf("expected frame name to contain 'velocity', got: %s", frame.Name)
		}
	})
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

// createMockEnumComputeResult creates a mock ComputeWithUnitsResult with enum data
func createMockEnumComputeResult(categories []string, indices []int) computeapi.ComputeWithUnitsResult {
	timestamps := make([]api.Timestamp, len(indices))
	baseTime := int64(1704067200) // 2024-01-01 00:00:00 UTC
	for i := range timestamps {
		timestamps[i] = api.Timestamp{
			Seconds: safelong.SafeLong(baseTime + int64(i*60)),
			Nanos:   safelong.SafeLong(0),
		}
	}

	enumPlot := computeapi.EnumPlot{
		Timestamps: timestamps,
		Values:     indices,
		Categories: categories,
	}

	computeResponse := computeapi.NewComputeNodeResponseFromEnum(enumPlot)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)

	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

// createMockEnumPointComputeResult creates a mock ComputeWithUnitsResult with a single enum point
func createMockEnumPointComputeResult(value string) computeapi.ComputeWithUnitsResult {
	enumPoint := computeapi.EnumPoint{
		Timestamp: api.Timestamp{
			Seconds: safelong.SafeLong(1704067200),
			Nanos:   safelong.SafeLong(0),
		},
		Value: value,
	}

	computeResponse := computeapi.NewComputeNodeResponseFromEnumPoint(&enumPoint)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)

	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

func TestEnumPlotTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("maps indices to category strings", func(t *testing.T) {
		categories := []string{"on", "off", "standby"}
		indices := []int{0, 2, 1, 0}

		result := createMockEnumComputeResult(categories, indices)
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}

		// Verify value field is string type
		valueField := frame.Fields[1]
		if valueField.Name != "value" {
			t.Errorf("expected field name 'value', got %q", valueField.Name)
		}

		// Check resolved string values
		if valueField.Len() != 4 {
			t.Fatalf("expected 4 values, got %d", valueField.Len())
		}
		expectedValues := []string{"on", "standby", "off", "on"}
		for i, expected := range expectedValues {
			actual, ok := valueField.At(i).(string)
			if !ok {
				t.Fatalf("value at index %d is not a string", i)
			}
			if actual != expected {
				t.Errorf("value at index %d: expected %q, got %q", i, expected, actual)
			}
		}
	})

	t.Run("handles out-of-bounds indices gracefully", func(t *testing.T) {
		categories := []string{"on", "off"}
		indices := []int{0, 5, 1} // index 5 is out of bounds

		result := createMockEnumComputeResult(categories, indices)
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		valueField := frame.Fields[1]
		if valueField.Len() != 3 {
			t.Fatalf("expected 3 values, got %d", valueField.Len())
		}
		if v := valueField.At(0).(string); v != "on" {
			t.Errorf("index 0: expected %q, got %q", "on", v)
		}
		if v := valueField.At(1).(string); v != "unknown(5)" {
			t.Errorf("index 1: expected %q, got %q", "unknown(5)", v)
		}
		if v := valueField.At(2).(string); v != "off" {
			t.Errorf("index 2: expected %q, got %q", "off", v)
		}
	})

	t.Run("handles empty enum response", func(t *testing.T) {
		categories := []string{"on", "off"}
		indices := []int{}

		result := createMockEnumComputeResult(categories, indices)
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}

		// Empty enum should have []string{} not []float64{}
		valueField := frame.Fields[1]
		if valueField.Len() != 0 {
			t.Errorf("expected 0 values for empty enum, got %d", valueField.Len())
		}
	})
}

func TestEnumPointTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("passes through resolved string value directly", func(t *testing.T) {
		result := createMockEnumPointComputeResult("active")
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}

		valueField := frame.Fields[1]
		if valueField.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", valueField.Len())
		}
		if v := valueField.At(0).(string); v != "active" {
			t.Errorf("expected %q, got %q", "active", v)
		}
	})
}

func TestNumericPathUnchangedAfterRefactor(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric results still produce float64 frames", func(t *testing.T) {
		result := createMockComputeResult([]float64{1.5, 2.5, 3.5})
		qm := NominalQueryModel{
			Channel:  "temperature",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}

		valueField := frame.Fields[1]
		if valueField.Name != "value" {
			t.Errorf("expected field name 'value', got %q", valueField.Name)
		}
		if valueField.Len() != 3 {
			t.Fatalf("expected 3 values, got %d", valueField.Len())
		}
		expectedValues := []float64{1.5, 2.5, 3.5}
		for i, expected := range expectedValues {
			actual, ok := valueField.At(i).(float64)
			if !ok {
				t.Fatalf("value at index %d is not a float64", i)
			}
			if actual != expected {
				t.Errorf("value at index %d: expected %v, got %v", i, expected, actual)
			}
		}
	})
}

func TestDisplayNameFromDS(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric path with data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		values := []float64{1.0, 2.0}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "temperature",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}
		frame := resp.Frames[0]
		// Fields: [time, value]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}
		valueField := frame.Fields[1]
		if valueField.Config == nil {
			t.Fatal("expected non-nil Config on value field")
		}
		if valueField.Config.DisplayNameFromDS != "temperature" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "temperature")
		}
	})

	t.Run("numeric path with empty data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		result := createMockComputeResult([]float64{})
		qm := NominalQueryModel{
			Channel:  "pressure",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		valueField := frame.Fields[1]
		if valueField.Config == nil {
			t.Fatal("expected non-nil Config on value field")
		}
		if valueField.Config.DisplayNameFromDS != "pressure" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "pressure")
		}
	})

	t.Run("enum path with data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		categories := []string{"on", "off"}
		indices := []int{0, 1}
		result := createMockEnumComputeResult(categories, indices)
		qm := NominalQueryModel{
			Channel:  "mode",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}
		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}
		valueField := frame.Fields[1]
		if valueField.Config == nil {
			t.Fatal("expected non-nil Config on value field")
		}
		if valueField.Config.DisplayNameFromDS != "mode" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "mode")
		}
	})

	t.Run("enum path with empty data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		result := createMockEnumComputeResult([]string{"on", "off"}, []int{})
		qm := NominalQueryModel{
			Channel:  "state",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := ds.transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		valueField := frame.Fields[1]
		if valueField.Config == nil {
			t.Fatal("expected non-nil Config on value field")
		}
		if valueField.Config.DisplayNameFromDS != "state" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "state")
		}
	})
}

func TestBuildComputeRequestBranching(t *testing.T) {
	ds := &Datasource{}

	baseQM := NominalQueryModel{
		AssetRid:      "ri.nominal.asset.test",
		Channel:       "temperature",
		DataScopeName: "default",
		Buckets:       1000,
	}

	baseTimeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	t.Run("string ChannelDataType produces enum series", func(t *testing.T) {
		qm := baseQM
		qm.ChannelDataType = "string"
		req := ds.buildComputeRequest(qm, baseTimeRange, 0)

		// The request should have a valid node
		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for enum request")
		}
		// Verify the timestamps are set
		if int64(req.Start.Seconds) != baseTimeRange.From.Unix() {
			t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, baseTimeRange.From.Unix())
		}
		if int64(req.End.Seconds) != baseTimeRange.To.Unix() {
			t.Errorf("End.Seconds = %d, want %d", req.End.Seconds, baseTimeRange.To.Unix())
		}

		// Serialize the node to JSON and verify it contains enum-specific types
		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal ComputableNode: %v", err)
		}
		jsonStr := string(jsonBytes)

		// Enum path should contain "enum" type markers in the serialized structure
		if !strings.Contains(jsonStr, `"type":"enum"`) {
			t.Errorf("expected enum series type in JSON for string ChannelDataType, got: %s", jsonStr)
		}
		// Should NOT contain numeric series type at the top level
		if strings.Contains(jsonStr, `"type":"numeric"`) {
			t.Errorf("expected no numeric series type in JSON for string ChannelDataType, got: %s", jsonStr)
		}
	})

	t.Run("numeric ChannelDataType produces numeric series", func(t *testing.T) {
		qm := baseQM
		qm.ChannelDataType = "numeric"
		req := ds.buildComputeRequest(qm, baseTimeRange, 0)

		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for numeric request")
		}
		if int64(req.Start.Seconds) != baseTimeRange.From.Unix() {
			t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, baseTimeRange.From.Unix())
		}

		// Serialize and verify it contains numeric-specific types
		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal ComputableNode: %v", err)
		}
		jsonStr := string(jsonBytes)

		if !strings.Contains(jsonStr, `"type":"numeric"`) {
			t.Errorf("expected numeric series type in JSON for numeric ChannelDataType, got: %s", jsonStr)
		}
	})

	t.Run("empty ChannelDataType defaults to numeric series", func(t *testing.T) {
		qm := baseQM
		qm.ChannelDataType = ""
		req := ds.buildComputeRequest(qm, baseTimeRange, 0)

		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for default request")
		}
		if int64(req.Start.Seconds) != baseTimeRange.From.Unix() {
			t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, baseTimeRange.From.Unix())
		}

		// Default should be numeric
		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal ComputableNode: %v", err)
		}
		jsonStr := string(jsonBytes)

		if !strings.Contains(jsonStr, `"type":"numeric"`) {
			t.Errorf("expected numeric series type in JSON for empty ChannelDataType, got: %s", jsonStr)
		}
		if strings.Contains(jsonStr, `"type":"enum"`) {
			t.Errorf("expected no enum series type in JSON for empty ChannelDataType, got: %s", jsonStr)
		}
	})

	t.Run("missing ChannelDataType defaults to numeric series", func(t *testing.T) {
		qm := baseQM
		// ChannelDataType is zero-value ""
		req := ds.buildComputeRequest(qm, baseTimeRange, 0)

		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for missing ChannelDataType request")
		}
	})

	t.Run("string and numeric produce structurally different requests", func(t *testing.T) {
		stringQM := baseQM
		stringQM.ChannelDataType = "string"
		stringReq := ds.buildComputeRequest(stringQM, baseTimeRange, 0)

		numericQM := baseQM
		numericQM.ChannelDataType = "numeric"
		numericReq := ds.buildComputeRequest(numericQM, baseTimeRange, 0)

		stringJSON, _ := json.Marshal(stringReq.Node)
		numericJSON, _ := json.Marshal(numericReq.Node)

		if string(stringJSON) == string(numericJSON) {
			t.Error("expected different JSON for string vs numeric ChannelDataType, but they are identical")
		}
	})
}

func TestBuildComputeRequestMaxDataPoints(t *testing.T) {
	ds := &Datasource{}

	baseTimeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	extractBuckets := func(t *testing.T, req computeapi1.ComputeNodeRequest) int {
		t.Helper()
		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal node: %v", err)
		}
		var node struct {
			Series struct {
				Buckets *int `json:"buckets"`
			} `json:"series"`
		}
		if err := json.Unmarshal(jsonBytes, &node); err != nil {
			t.Fatalf("failed to unmarshal node: %v", err)
		}
		if node.Series.Buckets == nil {
			return 0
		}
		return *node.Series.Buckets
	}

	t.Run("maxDataPoints caps buckets when smaller", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       1000,
		}
		req := ds.buildComputeRequest(qm, baseTimeRange, 500)
		if got := extractBuckets(t, req); got != 500 {
			t.Errorf("buckets = %d, want 500", got)
		}
	})

	t.Run("maxDataPoints does not increase buckets", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       500,
		}
		req := ds.buildComputeRequest(qm, baseTimeRange, 1000)
		if got := extractBuckets(t, req); got != 500 {
			t.Errorf("buckets = %d, want 500", got)
		}
	})

	t.Run("maxDataPoints used when buckets is zero", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       0,
		}
		req := ds.buildComputeRequest(qm, baseTimeRange, 800)
		if got := extractBuckets(t, req); got != 800 {
			t.Errorf("buckets = %d, want 800", got)
		}
	})

	t.Run("zero maxDataPoints uses saved buckets", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       1000,
		}
		req := ds.buildComputeRequest(qm, baseTimeRange, 0)
		if got := extractBuckets(t, req); got != 1000 {
			t.Errorf("buckets = %d, want 1000", got)
		}
	})
}

func TestBuildEnumChannelSeries(t *testing.T) {
	ds := &Datasource{}

	t.Run("returns non-nil enum series", func(t *testing.T) {
		enumSeries := ds.buildEnumChannelSeries("ri.nominal.asset.test", "status", "default")
		// Serialize to JSON to verify the structure
		jsonBytes, err := json.Marshal(enumSeries)
		if err != nil {
			t.Fatalf("failed to marshal EnumSeries: %v", err)
		}
		jsonStr := string(jsonBytes)

		// Should contain channel type (same as numeric path)
		if !strings.Contains(jsonStr, `"type":"channel"`) {
			t.Errorf("expected channel series type in JSON, got: %s", jsonStr)
		}
		if !strings.Contains(jsonStr, `"type":"asset"`) {
			t.Errorf("expected asset channel type in JSON, got: %s", jsonStr)
		}
		// Verify channel name is present
		if !strings.Contains(jsonStr, `"channel":{"type":"literal","literal":"status"}`) {
			t.Errorf("expected channel literal 'status' in JSON, got: %s", jsonStr)
		}
	})

	t.Run("mirrors buildChannelSeries asset channel structure", func(t *testing.T) {
		// Both builders should produce the same AssetChannel structure;
		// verify by checking the channel and dataScopeName appear identically.
		enumSeries := ds.buildEnumChannelSeries("ri.nominal.asset.test", "sensor1", "scope1")
		numericSeries := ds.buildChannelSeries("ri.nominal.asset.test", "sensor1", "scope1")

		enumJSON, _ := json.Marshal(enumSeries)
		numericJSON, _ := json.Marshal(numericSeries)

		// Both should contain the same channel literal
		expectedChannel := `"channel":{"type":"literal","literal":"sensor1"}`
		if !strings.Contains(string(enumJSON), expectedChannel) {
			t.Errorf("enum series missing expected channel literal, got: %s", string(enumJSON))
		}
		if !strings.Contains(string(numericJSON), expectedChannel) {
			t.Errorf("numeric series missing expected channel literal, got: %s", string(numericJSON))
		}

		// Both should contain the same dataScopeName literal
		expectedScope := `"dataScopeName":{"type":"literal","literal":"scope1"}`
		if !strings.Contains(string(enumJSON), expectedScope) {
			t.Errorf("enum series missing expected dataScopeName literal, got: %s", string(enumJSON))
		}
		if !strings.Contains(string(numericJSON), expectedScope) {
			t.Errorf("numeric series missing expected dataScopeName literal, got: %s", string(numericJSON))
		}
	})
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

// ============================================================================
// Mock services for CallResource handler tests
// ============================================================================

// mockAuthService implements authapi.AuthenticationServiceV2Client for testing
type mockAuthService struct {
	getMyProfileResponse authapi.UserV2
	getMyProfileError    error
}

func (m *mockAuthService) GetMyProfile(ctx context.Context, authHeader bearertoken.Token) (authapi.UserV2, error) {
	return m.getMyProfileResponse, m.getMyProfileError
}

func (m *mockAuthService) UpdateMyProfile(ctx context.Context, authHeader bearertoken.Token, req authapi.UpdateMyProfileRequest) (authapi.UserV2, error) {
	return authapi.UserV2{}, nil
}

func (m *mockAuthService) GetMySettings(ctx context.Context, authHeader bearertoken.Token) (authapi.UserSettings, error) {
	return authapi.UserSettings{}, nil
}

func (m *mockAuthService) UpdateMySettings(ctx context.Context, authHeader bearertoken.Token, settings authapi.UserSettings) (authapi.UserSettings, error) {
	return authapi.UserSettings{}, nil
}

func (m *mockAuthService) GetMyOrgSettings(ctx context.Context, authHeader bearertoken.Token) (authapi.OrgSettings, error) {
	return authapi.OrgSettings{}, nil
}

func (m *mockAuthService) UpdateMyOrgSettings(ctx context.Context, authHeader bearertoken.Token, settings authapi.OrgSettings) (authapi.OrgSettings, error) {
	return authapi.OrgSettings{}, nil
}

func (m *mockAuthService) SearchUsersV2(ctx context.Context, authHeader bearertoken.Token, req authapi.SearchUsersRequest) (authapi.SearchUsersResponseV2, error) {
	return authapi.SearchUsersResponseV2{}, nil
}

func (m *mockAuthService) GetUsers(ctx context.Context, authHeader bearertoken.Token, userRids []authapi.UserRid) ([]authapi.UserV2, error) {
	return nil, nil
}

func (m *mockAuthService) GetUser(ctx context.Context, authHeader bearertoken.Token, userRid authapi.UserRid) (authapi.UserV2, error) {
	return authapi.UserV2{}, nil
}

func (m *mockAuthService) DismissMyCoachmark(ctx context.Context, authHeader bearertoken.Token, requestArg authapi.DismissCoachmarkRequest) (authapi.CoachmarkDismissal, error) {
	return authapi.CoachmarkDismissal{}, nil
}

func (m *mockAuthService) IsMyCoachmarkDismissed(ctx context.Context, authHeader bearertoken.Token, coachmarkIdArg string) (bool, error) {
	return false, nil
}

func (m *mockAuthService) GetJwks(ctx context.Context) (authapi.Jwks, error) {
	return authapi.Jwks{}, nil
}

func (m *mockAuthService) GenerateMediaMtxToken(ctx context.Context, authHeader bearertoken.Token, requestArg authapi.GenerateMediaMtxTokenRequest) (authapi.GenerateMediaMtxTokenResponse, error) {
	return authapi.GenerateMediaMtxTokenResponse{}, nil
}

func (m *mockAuthService) GetMyCoachmarkDismissals(ctx context.Context, authHeader bearertoken.Token, requestArg authapi.GetCoachmarkDismissalsRequest) (authapi.GetCoachmarkDismissalsResponse, error) {
	return authapi.GetCoachmarkDismissalsResponse{}, nil
}

func (m *mockAuthService) ResetMyCoachmarkDismissal(ctx context.Context, authHeader bearertoken.Token, coachmarkIdArg string) error {
	return nil
}

// mockDatasourceService implements datasourceservice.DataSourceServiceClient for testing
type mockDatasourceService struct {
	searchChannelsResponse datasourceapi.SearchChannelsResponse
	searchChannelsError    error
	searchChannelsRequest  datasourceapi.SearchChannelsRequest
	searchChannelsCalls    int
	// searchChannelsFunc, when non-nil, overrides searchChannelsResponse/searchChannelsError.
	// This allows tests to return different responses on successive calls (e.g. pagination).
	searchChannelsFunc func(ctx context.Context, authHeader bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error)
}

func (m *mockDatasourceService) SearchChannels(ctx context.Context, authHeader bearertoken.Token, queryArg datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
	m.searchChannelsCalls++
	m.searchChannelsRequest = queryArg
	if m.searchChannelsFunc != nil {
		return m.searchChannelsFunc(ctx, authHeader, queryArg)
	}
	return m.searchChannelsResponse, m.searchChannelsError
}

func (m *mockDatasourceService) SearchFilteredChannels(ctx context.Context, authHeader bearertoken.Token, queryArg datasourceapi.SearchFilteredChannelsRequest) (datasourceapi.SearchFilteredChannelsResponse, error) {
	return datasourceapi.SearchFilteredChannelsResponse{}, nil
}

func (m *mockDatasourceService) SearchHierarchicalChannels(ctx context.Context, authHeader bearertoken.Token, queryArg datasourceapi.SearchHierarchicalChannelsRequest) (datasourceapi.SearchHierarchicalChannelsResponse, error) {
	return datasourceapi.SearchHierarchicalChannelsResponse{}, nil
}

func (m *mockDatasourceService) IndexChannelPrefixTree(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.IndexChannelPrefixTreeRequest) (datasourceapi.ChannelPrefixTree, error) {
	return datasourceapi.ChannelPrefixTree{}, nil
}

func (m *mockDatasourceService) BatchGetChannelPrefixTrees(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.BatchGetChannelPrefixTreeRequest) (datasourceapi.BatchGetChannelPrefixTreeResponse, error) {
	return datasourceapi.BatchGetChannelPrefixTreeResponse{}, nil
}

func (m *mockDatasourceService) GetAvailableTagsForChannel(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.GetAvailableTagsForChannelRequest) (datasourceapi.GetAvailableTagsForChannelResponse, error) {
	return datasourceapi.GetAvailableTagsForChannelResponse{}, nil
}

func (m *mockDatasourceService) GetDataScopeBounds(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.BatchGetDataScopeBoundsRequest) (datasourceapi.BatchGetDataScopeBoundsResponse, error) {
	return datasourceapi.BatchGetDataScopeBoundsResponse{}, nil
}

func (m *mockDatasourceService) GetTagValuesForDataSource(ctx context.Context, authHeader bearertoken.Token, dataSourceRidArg rids.DataSourceRid, requestArg datasourceapi.GetTagValuesForDataSourceRequest) (map[api.TagName][]api.TagValue, error) {
	return nil, nil
}

func (m *mockDatasourceService) GetAvailableTagKeys(ctx context.Context, authHeader bearertoken.Token, dataSourceRidArg rids.DataSourceRid, requestArg datasourceapi.GetAvailableTagKeysRequest) (datasourceapi.GetAvailableTagKeysResponse, error) {
	return datasourceapi.GetAvailableTagKeysResponse{}, nil
}

func (m *mockDatasourceService) GetAvailableTagValues(ctx context.Context, authHeader bearertoken.Token, dataSourceRidArg rids.DataSourceRid, requestArg datasourceapi.GetAvailableTagValuesRequest) (datasourceapi.GetAvailableTagValuesResponse, error) {
	return datasourceapi.GetAvailableTagValuesResponse{}, nil
}

// Verify mock types implement their interfaces at compile time
var _ authapi.AuthenticationServiceV2Client = (*mockAuthService)(nil)
var _ datasourceservice.DataSourceServiceClient = (*mockDatasourceService)(nil)

// callResourceAndCapture is a test helper that calls CallResource and captures the response
func callResourceAndCapture(t *testing.T, ds *Datasource, req *backend.CallResourceRequest) *backend.CallResourceResponse {
	t.Helper()
	var captured *backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(resp *backend.CallResourceResponse) error {
		captured = resp
		return nil
	})
	err := ds.CallResource(context.Background(), req, sender)
	if err != nil {
		t.Fatalf("CallResource returned error: %v", err)
	}
	if captured == nil {
		t.Fatal("CallResource did not send a response")
	}
	return captured
}

// newTestAssetServer creates an httptest server that handles asset-related API endpoints.
// It returns the server (caller must defer Close) and configures:
//   - POST /scout/v1/asset/multiple — batch asset lookup by RID
//   - POST /scout/v1/search-assets — paginated asset search
func newTestAssetServer(t *testing.T, assets map[string]SingleAssetResponse, searchResults []AssetResponse) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/scout/v1/asset/multiple":
			var rids []string
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &rids); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			result := make(map[string]SingleAssetResponse)
			for _, rid := range rids {
				if asset, ok := assets[rid]; ok {
					result[rid] = asset
				}
			}
			json.NewEncoder(w).Encode(result)

		case "/scout/v1/search-assets":
			if len(searchResults) > 0 {
				json.NewEncoder(w).Encode(searchResults[0])
			} else {
				json.NewEncoder(w).Encode(AssetResponse{})
			}

		default:
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		}
	}))
}

// newTestDatasource creates a Datasource for testing CallResource handlers.
func newTestDatasource(baseURL string, authSvc authapi.AuthenticationServiceV2Client, dsSvc datasourceservice.DataSourceServiceClient) *Datasource {
	return &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData:                []byte(fmt.Sprintf(`{"baseUrl": "%s"}`, baseURL)),
			DecryptedSecureJSONData: map[string]string{"apiKey": "test-api-key"},
		},
		authService:       authSvc,
		datasourceService: dsSvc,
	}
}

// ============================================================================
// CallResource routing tests (new routes only)
// ============================================================================

func TestCallResourceRouting(t *testing.T) {
	mockAuth := &mockAuthService{
		getMyProfileResponse: authapi.UserV2{
			Rid:         authapi.UserRid(rid.MustNew("user", "test", "user", "user123")),
			DisplayName: "Test User",
		},
	}

	// Create a test server that acts as the Nominal API proxy target
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"proxied": "true", "path": r.URL.Path})
	}))
	defer proxyServer.Close()

	ds := newTestDatasource(proxyServer.URL, mockAuth, &mockDatasourceService{})

	tests := []struct {
		name           string
		path           string
		method         string
		body           []byte
		expectStatus   int
		expectContains string
	}{
		{
			name:         "routes /assets",
			path:         "assets",
			method:       "POST",
			body:         []byte(`{}`),
			expectStatus: http.StatusOK,
		},
		{
			name:         "routes /datascopes without assetRid",
			path:         "datascopes",
			method:       "POST",
			body:         []byte(`{}`),
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "routes /channelvariables without assetRid",
			path:         "channelvariables",
			method:       "POST",
			body:         []byte(`{}`),
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "GET /assets returns 405",
			path:         "assets",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /datascopes returns 405",
			path:         "datascopes",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /channelvariables returns 405",
			path:         "channelvariables",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "POST /assets with invalid body returns 400",
			path:         "assets",
			method:       "POST",
			body:         []byte(`not json`),
			expectStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &backend.CallResourceRequest{
				Path:   tt.path,
				Method: tt.method,
				Body:   tt.body,
			}
			resp := callResourceAndCapture(t, ds, req)
			if resp.Status != tt.expectStatus {
				t.Errorf("status = %d, want %d; body = %s", resp.Status, tt.expectStatus, string(resp.Body))
			}
			if tt.expectContains != "" && !strings.Contains(string(resp.Body), tt.expectContains) {
				t.Errorf("body %q does not contain %q", string(resp.Body), tt.expectContains)
			}
		})
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
}

// newCountingAssetServer is like newTestAssetServer but also counts requests
// to the /scout/v1/asset/multiple endpoint.
func newCountingAssetServer(t *testing.T, assets map[string]SingleAssetResponse, fetchCount *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/scout/v1/asset/multiple" {
			*fetchCount++
			var rids []string
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &rids); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			result := make(map[string]SingleAssetResponse)
			for _, rid := range rids {
				if asset, ok := assets[rid]; ok {
					result[rid] = asset
				}
			}
			json.NewEncoder(w).Encode(result)
		} else {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		}
	}))
}

func TestInferChannelTypeDeduplicatesWithinRequest(t *testing.T) {
	assetRid := "ri.scout.main.asset.dedup1"
	dataSourceRid := "ri.scout.main.data-source.ds1"

	var assetFetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Test Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, &assetFetchCount)
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

	ds := &Datasource{
		computeService:    mockCompute,
		datasourceService: mockDS,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// 3 queries for the same asset+scope+channel — should only make 1 asset
	// fetch and 1 SearchChannels call.
	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
			{RefID: "B", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
			{RefID: "C", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
		},
	}

	_, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assetFetchCount != 1 {
		t.Errorf("expected 1 asset fetch call (cached), got %d", assetFetchCount)
	}
	if mockDS.searchChannelsCalls != 1 {
		t.Errorf("expected 1 SearchChannels call (deduplicated), got %d", mockDS.searchChannelsCalls)
	}
}

func TestAssetCacheTTLReusedAcrossRequests(t *testing.T) {
	assetRid := "ri.scout.main.asset.ttl1"
	dataSourceRid := "ri.scout.main.data-source.ds1"

	var assetFetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Test Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, &assetFetchCount)
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

	ds := &Datasource{
		computeService:    mockCompute,
		datasourceService: mockDS,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	makeReq := func() *backend.QueryDataRequest {
		return &backend.QueryDataRequest{
			PluginContext: backend.PluginContext{
				DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
					JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
					DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
				},
			},
			Queries: []backend.DataQuery{
				{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
			},
		}
	}

	// Two separate QueryData calls should reuse the cached asset.
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if assetFetchCount != 1 {
		t.Errorf("expected 1 asset fetch across 2 QueryData calls (TTL cache), got %d", assetFetchCount)
	}
}

// strPtr is a helper to create a *string
func strPtr(s string) *string {
	return &s
}
