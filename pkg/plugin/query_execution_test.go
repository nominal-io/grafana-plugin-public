package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
	"github.com/palantir/pkg/uuid"
)

func TestPartitionPreparedQueriesKeepsQueryModelPairs(t *testing.T) {
	prepared := []preparedQuery{
		{
			Query: backend.DataQuery{RefID: "numeric"},
			Model: NominalQueryModel{Channel: "temperature", ChannelDataType: "numeric"},
			Kind:  preparedQueryBatchable,
		},
		{
			Query: backend.DataQuery{RefID: "logs"},
			Model: NominalQueryModel{Channel: "app.logs", ChannelDataType: "log"},
			Kind:  preparedQueryBatchable,
		},
		{
			Query: backend.DataQuery{RefID: "string"},
			Model: NominalQueryModel{Channel: "state", ChannelDataType: "string"},
			Kind:  preparedQueryBatchable,
		},
	}

	logBatch, otherBatch := partitionPreparedQueries(prepared)

	if len(logBatch.queries) != 1 || len(logBatch.models) != 1 {
		t.Fatalf("expected one log query/model pair, got %d queries and %d models", len(logBatch.queries), len(logBatch.models))
	}
	if logBatch.queries[0].RefID != "logs" || logBatch.models[0].Channel != "app.logs" {
		t.Fatalf("log pair was not preserved: query=%v model=%v", logBatch.queries[0].RefID, logBatch.models[0].Channel)
	}

	if len(otherBatch.queries) != 2 || len(otherBatch.models) != 2 {
		t.Fatalf("expected two non-log query/model pairs, got %d queries and %d models", len(otherBatch.queries), len(otherBatch.models))
	}
	for i := range otherBatch.queries {
		if otherBatch.queries[i].RefID == "numeric" && otherBatch.models[i].Channel != "temperature" {
			t.Fatalf("numeric query/model pair was not preserved: model=%v", otherBatch.models[i].Channel)
		}
		if otherBatch.queries[i].RefID == "string" && otherBatch.models[i].Channel != "state" {
			t.Fatalf("string query/model pair was not preserved: model=%v", otherBatch.models[i].Channel)
		}
	}
}

func TestQueryDataRoutesQueriesByType(t *testing.T) {
	ds := &Datasource{
		settings: testDatasourceSettings(),
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
			req := newQueryRequest(tt.queries)

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
		results[i] = createMockArrowComputeResult([]float64{float64(i + 1)})
	}
	return computeapi.BatchComputeWithUnitsResponse{Results: results}
}

func TestBatchQueryExecution(t *testing.T) {
	mockService := &mockComputeService{}

	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0, 3.0}),
			createMockArrowComputeResult([]float64{4.0, 5.0, 6.0}),
			createMockArrowComputeResult([]float64{7.0, 8.0, 9.0}),
		},
	}

	ds := &Datasource{
		settings:       testDatasourceSettings(),
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
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := newQueryRequest(queries)

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All queries leave ChannelDataType unset, so they land in one non-log
	// partition and one batch call. Adding a log query here makes it two.
	// See TestMixedLogNumericParallelBatch for the partitioned case.
	if mockService.batchComputeCalls != 1 {
		t.Errorf("expected 1 batch compute call, got %d", mockService.batchComputeCalls)
	}

	if mockService.singleComputeCalls != 0 {
		t.Errorf("expected 0 single compute calls, got %d", mockService.singleComputeCalls)
	}

	if len(mockService.lastBatchRequest.Requests) != 3 {
		t.Errorf("expected 3 requests in batch, got %d", len(mockService.lastBatchRequest.Requests))
	}

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
		settings:       testDatasourceSettings(),
		computeService: mockService,
	}

	req := newBatchQueryRequest(maxBatchComputeSubrequests + 1)

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
	if len(resp.Responses) != len(req.Queries) {
		t.Fatalf("expected %d responses, got %d", len(req.Queries), len(resp.Responses))
	}

	for _, q := range req.Queries {
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

	ds := withCatalog(&Datasource{
		computeService:     mockCompute,
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	})

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	req := newQueryRequestForURL(server.URL, []backend.DataQuery{
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
	})

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

	series := summarizeSeriesFromNode(t, mockCompute.lastBatchRequest.Requests[0].Node)
	if kind := seriesKind(t, series.Input); kind != "enum" {
		t.Fatalf("expected enum compute request after inferring string type, got series kind %q", kind)
	}
}

// A saved numeric query with explicit aggregations expands into a string and a
// numeric channel. inferChannelMetadata must override the saved type per query
// so the string channel gets an enum request, not Arrow numeric.
func TestMixedTypeTemplateVariableWithExplicitAggregations(t *testing.T) {
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
	numericType := api.New_SeriesDataType(api.SeriesDataType_DOUBLE)
	mockDS := &mockDatasourceService{
		searchChannelsFunc: func(_ context.Context, _ bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
			if len(req.ExactMatch) == 0 {
				return datasourceapi.SearchChannelsResponse{}, nil
			}
			chName := req.ExactMatch[0]
			dsRid := rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))
			switch chName {
			case "state":
				return datasourceapi.SearchChannelsResponse{
					Results: []datasourceapi.ChannelMetadata{
						{Name: api.Channel("state"), DataSource: dsRid, DataType: &stringType},
					},
				}, nil
			case "temperature":
				return datasourceapi.SearchChannelsResponse{
					Results: []datasourceapi.ChannelMetadata{
						{Name: api.Channel("temperature"), DataSource: dsRid, DataType: &numericType},
					},
				}, nil
			default:
				return datasourceapi.SearchChannelsResponse{}, nil
			}
		},
	}

	// Both queries are batched into a single API call. First result is Arrow
	// bucketed numeric (with mean+min columns), second is enum for the string channel.
	arrowBytes := createTestArrowMultiAgg(
		[]int64{1000000000000, 2000000000000},
		map[string][]float64{"mean": {10.0, 20.0}, "min": {5.0, 15.0}},
	)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				{ComputeResult: computeapi.NewComputeNodeResultFromSuccess(
					computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot),
				)},
				createMockEnumComputeResult([]string{"idle", "active"}, []int{0, 1}),
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

	// Template variable expansion: same asset, explicit aggregations, one numeric
	// and one string channel. Both inherit channelDataType "numeric" from the saved query.
	req := newQueryRequestForURL(server.URL, []backend.DataQuery{
		{
			RefID: "A",
			JSON: mustMarshal(NominalQueryModel{
				AssetRid:        assetRid,
				Channel:         "temperature",
				DataScopeName:   "default",
				ChannelDataType: "numeric",
				Aggregations:    []string{"MEAN", "MIN"},
				Buckets:         100,
			}),
			TimeRange: timeRange,
		},
		{
			RefID: "B",
			JSON: mustMarshal(NominalQueryModel{
				AssetRid:        assetRid,
				Channel:         "state",
				DataScopeName:   "default",
				ChannelDataType: "numeric", // saved as numeric, but actually string
				Aggregations:    []string{"MEAN", "MIN"},
				Buckets:         100,
			}),
			TimeRange: timeRange,
		},
	})

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Responses["A"].Error != nil {
		t.Fatalf("query A error: %v", resp.Responses["A"].Error)
	}
	if resp.Responses["B"].Error != nil {
		t.Fatalf("query B error: %v", resp.Responses["B"].Error)
	}

	if mockCompute.batchComputeCalls != 1 {
		t.Fatalf("expected 1 batch compute call, got %d", mockCompute.batchComputeCalls)
	}
	if len(mockCompute.lastBatchRequest.Requests) != 2 {
		t.Fatalf("expected 2 requests in batch, got %d", len(mockCompute.lastBatchRequest.Requests))
	}

	// The numeric query (temperature) gets an Arrow request with output fields.
	numericSeries := summarizeSeriesFromNode(t, mockCompute.lastBatchRequest.Requests[0].Node)
	if kind := seriesKind(t, numericSeries.Input); kind != "numeric" {
		t.Errorf("expected numeric series, got kind %q", kind)
	}
	if !isArrowV3(numericSeries.OutputFormat) {
		t.Errorf("expected numeric request with ARROW_V3 output format, got %v", numericSeries.OutputFormat)
	}

	// The string query (state) gets an enum request with no output format.
	enumSeries := summarizeSeriesFromNode(t, mockCompute.lastBatchRequest.Requests[1].Node)
	if kind := seriesKind(t, enumSeries.Input); kind != "enum" {
		t.Errorf("expected enum series, got kind %q", kind)
	}
	if enumSeries.OutputFormat != nil {
		t.Errorf("expected enum request without output format, got %v", enumSeries.OutputFormat)
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
		settings:       testDatasourceSettings(),
		computeService: mockService,
	}

	req := newBatchQueryRequest(maxBatchComputeSubrequests + 1)

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
	mockService := &mockComputeService{}

	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0}),
			createMockArrowComputeResult([]float64{3.0, 4.0}),
		},
	}

	ds := &Datasource{
		settings:       testDatasourceSettings(),
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
			JSON:      mustMarshal(NominalQueryModel{Constant: 42.0}), // Legacy - not batched
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := newQueryRequest(queries)

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockService.batchComputeCalls != 1 {
		t.Errorf("expected 1 batch compute call, got %d", mockService.batchComputeCalls)
	}

	if len(mockService.lastBatchRequest.Requests) != 2 {
		t.Errorf("expected 2 requests in batch, got %d", len(mockService.lastBatchRequest.Requests))
	}

	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

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
	mockService := &mockComputeService{
		batchComputeError: fmt.Errorf("API error: service unavailable"),
	}

	ds := &Datasource{
		settings:       testDatasourceSettings(),
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

	req := newQueryRequest(queries)

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
	mockService := &mockComputeService{}

	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0, 3.0}), // Query A: Success
			createMockErrorResult(404, "CHANNEL_NOT_FOUND"),        // Query B: Error
			createMockArrowComputeResult([]float64{7.0, 8.0, 9.0}), // Query C: Success
		},
	}

	ds := &Datasource{
		settings:       testDatasourceSettings(),
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

	req := newQueryRequest(queries)

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	respA := resp.Responses["A"]
	if respA.Error != nil {
		t.Errorf("expected no error for A, got: %v", respA.Error)
	}
	if len(respA.Frames) != 1 {
		t.Errorf("expected 1 frame for A, got %d", len(respA.Frames))
	}

	respB := resp.Responses["B"]
	if respB.Error == nil {
		t.Error("expected error for B, got nil")
	} else {
		if !strings.Contains(respB.Error.Error(), "Compute error") {
			t.Errorf("expected 'Compute error' in message for B, got: %v", respB.Error)
		}
		if !strings.Contains(respB.Error.Error(), "CHANNEL_NOT_FOUND") {
			t.Errorf("expected error type 'CHANNEL_NOT_FOUND' in message for B, got: %v", respB.Error)
		}
		if !strings.Contains(respB.Error.Error(), "404") {
			t.Errorf("expected error code '404' in message for B, got: %v", respB.Error)
		}
	}

	respC := resp.Responses["C"]
	if respC.Error != nil {
		t.Errorf("expected no error for C, got: %v", respC.Error)
	}
	if len(respC.Frames) != 1 {
		t.Errorf("expected 1 frame for C, got %d", len(respC.Frames))
	}
}

func TestBatchQueryWithMissingResults(t *testing.T) {
	mockService := &mockComputeService{}

	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0, 3.0}),
			createMockArrowComputeResult([]float64{4.0, 5.0, 6.0}),
			// Missing third result
		},
	}

	ds := &Datasource{
		settings:       testDatasourceSettings(),
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
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := newQueryRequest(queries)

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	respA := resp.Responses["A"]
	if respA.Error != nil {
		t.Errorf("expected no error for A, got: %v", respA.Error)
	}
	if len(respA.Frames) != 1 {
		t.Errorf("expected 1 frame for A, got %d", len(respA.Frames))
	}

	respB := resp.Responses["B"]
	if respB.Error != nil {
		t.Errorf("expected no error for B, got: %v", respB.Error)
	}
	if len(respB.Frames) != 1 {
		t.Errorf("expected 1 frame for B, got %d", len(respB.Frames))
	}

	respC := resp.Responses["C"]
	if respC.Error == nil {
		t.Error("expected error for C due to missing result, got nil")
	} else if !strings.Contains(respC.Error.Error(), "Missing result in batch response") {
		t.Errorf("expected 'Missing result in batch response' error for C, got: %v", respC.Error)
	}
}

func TestBatchQueryWithExtraResultsIgnoresExtras(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockArrowComputeResult([]float64{1.0, 2.0, 3.0}),
				createMockArrowComputeResult([]float64{4.0, 5.0, 6.0}),
				createMockArrowComputeResult([]float64{7.0, 8.0, 9.0}),
				createMockArrowComputeResult([]float64{10.0, 11.0, 12.0}),
			},
		},
	}

	ds := &Datasource{
		settings:       testDatasourceSettings(),
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
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := newQueryRequest(queries)

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockService.lastBatchRequest.Requests) != len(queries) {
		t.Fatalf("expected %d compute requests, got %d", len(queries), len(mockService.lastBatchRequest.Requests))
	}
	if len(resp.Responses) != len(queries) {
		t.Fatalf("expected %d responses, got %d", len(queries), len(resp.Responses))
	}
	expectedFirst := map[string]float64{"A": 1.0, "B": 4.0, "C": 7.0}
	for _, q := range queries {
		response := resp.Responses[q.RefID]
		if response.Error != nil {
			t.Fatalf("expected no error for %s, got %v", q.RefID, response.Error)
		}
		if len(response.Frames) != 1 {
			t.Fatalf("expected 1 frame for %s, got %d", q.RefID, len(response.Frames))
		}
		v, ok := response.Frames[0].Fields[1].At(0).(*float64)
		if !ok || v == nil {
			t.Fatalf("expected %s first value %v, got %v", q.RefID, expectedFirst[q.RefID], v)
		}
		if *v != expectedFirst[q.RefID] {
			t.Fatalf("expected %s first value %v, got %v", q.RefID, expectedFirst[q.RefID], *v)
		}
	}
}

func TestMixedLogNumericParallelBatch(t *testing.T) {
	// Call order is nondeterministic under parallel batches, so match each
	// request to its response by inspecting it.
	logResponse := computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockPagedLogResult([]string{"log entry"}, []map[string]string{{"k": "v"}}, nil),
		},
	}
	numericResponse := computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0}),
		},
	}
	mockService := &mockComputeService{
		batchComputeFunc: func(req computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
			// Log requests carry the "log" series type, numeric ones "numeric".
			reqJSON, _ := json.Marshal(req)
			if strings.Contains(string(reqJSON), `"type":"log"`) {
				return logResponse, nil
			}
			return numericResponse, nil
		},
	}

	ds := &Datasource{
		settings:       testDatasourceSettings(),
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	req := newQueryRequest([]backend.DataQuery{
		{
			RefID:     "LOG",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "app.logs", DataScopeName: "ds1", ChannelDataType: "log", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "NUM",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temperature", DataScopeName: "ds1", ChannelDataType: "numeric", Buckets: 100, Aggregations: []string{"MEAN"}}),
			TimeRange: timeRange,
		},
	})

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockService.batchComputeCalls != 2 {
		t.Errorf("expected 2 batch compute calls for mixed log/numeric, got %d", mockService.batchComputeCalls)
	}

	for i, req := range mockService.batchRequests {
		if len(req.Requests) != 1 {
			t.Errorf("batch call %d: expected 1 request, got %d", i, len(req.Requests))
		}
	}

	if len(resp.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(resp.Responses))
	}

	logResp, ok := resp.Responses["LOG"]
	if !ok {
		t.Fatal("expected response for LOG refID")
	}
	numResp, ok := resp.Responses["NUM"]
	if !ok {
		t.Fatal("expected response for NUM refID")
	}

	if logResp.Error != nil {
		t.Errorf("unexpected error for LOG: %v", logResp.Error)
	}
	if numResp.Error != nil {
		t.Errorf("unexpected error for NUM: %v", numResp.Error)
	}
	if len(logResp.Frames) == 0 {
		t.Error("expected frames for LOG response")
	}
	if len(numResp.Frames) == 0 {
		t.Error("expected frames for NUM response")
	}
}

// Distinguishable from the "unknown" fallback a missing identity would produce.
const batchRequestPluginVersion = "9.9.9-test"

func newBatchQueryRequest(queryCount int) *backend.QueryDataRequest {
	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	req := newQueryRequest(makeBatchableQueries(queryCount, timeRange))
	req.PluginContext.PluginVersion = batchRequestPluginVersion
	return req
}

func TestBatchComputeStampsSharedRequestID(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponse: makeBatchComputeWithUnitsResponse(3),
	}
	ds := &Datasource{computeService: mockService}
	defer ds.Dispose()

	req := newBatchQueryRequest(3)

	if _, err := ds.QueryData(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	reqs := mockService.lastBatchRequest.Requests
	if len(reqs) != 3 {
		t.Fatalf("expected 3 subrequests, got %d", len(reqs))
	}
	first := reqs[0].RequestId
	if first == nil {
		t.Fatal("expected RequestId to be stamped on subrequests")
	}
	for i, r := range reqs {
		if r.RequestId == nil || *r.RequestId != *first {
			t.Fatalf("subrequest %d does not share the batch RequestId", i)
		}
	}
}

func TestBatchQueryKillPolicy(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		cancelMidFlight bool
		wantKill        bool
	}{
		{
			name:     "transport error",
			err:      fmt.Errorf("connection reset"),
			wantKill: true,
		},
		{
			name:     "confirmed success",
			wantKill: false,
		},
		{
			name:     "error with HTTP status",
			err:      &apiError{Status: http.StatusInternalServerError},
			wantKill: false,
		},
		{
			name:            "context canceled with HTTP status error",
			err:             &apiError{Status: http.StatusBadRequest},
			cancelMidFlight: true,
			wantKill:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mockService := &mockComputeService{
				batchComputeFunc: func(request computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
					if tt.cancelMidFlight {
						cancel()
					}
					if tt.err != nil {
						return computeapi.BatchComputeWithUnitsResponse{}, tt.err
					}
					return makeBatchComputeWithUnitsResponse(len(request.Requests)), nil
				},
			}
			ds := &Datasource{computeService: mockService}

			if _, err := ds.QueryData(ctx, newBatchQueryRequest(1)); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantKill {
				waitForCondition(t, 2*time.Second, func() bool { return len(mockService.killCallsSnapshot()) >= 1 })
			}
			ds.Dispose()

			if !tt.wantKill {
				// The final flush is async, so a spurious kill needs time to
				// land before a no-kill assertion means anything.
				time.Sleep(3 * killFlushInterval)
				if kills := mockService.killCallsSnapshot(); len(kills) != 0 {
					t.Fatalf("expected no kill, got %v", kills)
				}
				return
			}

			kills := mockService.killCallsSnapshot()
			stamped := mockService.lastBatchRequest.Requests[0].RequestId
			if stamped == nil {
				t.Fatal("expected RequestId to be stamped on the batch")
			}
			if len(kills) != 1 || !slices.Equal(kills[0].ids, []uuid.UUID{*stamped}) {
				t.Fatalf("expected one kill for stamped RequestId %v, got %v", *stamped, kills)
			}
			// The kill is dispatched off the request goroutine, so identity has to
			// ride the enqueue rather than be read from the ambient context later.
			if got := kills[0].ua.PluginVersion; got != batchRequestPluginVersion {
				t.Errorf("kill carried PluginVersion %q, want %q", got, batchRequestPluginVersion)
			}
		})
	}
}

func TestBatchQueryStopsChunkingAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockService := &mockComputeService{}
	mockService.batchComputeFunc = func(requestArg computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
		cancel() // cancelled mid-flight during the first chunk
		return makeBatchComputeWithUnitsResponse(len(requestArg.Requests)), nil
	}
	ds := &Datasource{computeService: mockService}

	resp, err := ds.QueryData(ctx, newBatchQueryRequest(maxBatchComputeSubrequests+1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := mockService.batchComputeCalls; got != 1 {
		t.Fatalf("expected chunking to stop after cancellation, got %d batch calls", got)
	}

	// The skipped chunk's queries still answer, with a cancellation error.
	cancelled := 0
	for _, r := range resp.Responses {
		if r.Error != nil && strings.Contains(r.Error.Error(), "cancelled") {
			cancelled++
		}
	}
	if cancelled != 1 {
		t.Fatalf("expected 1 cancelled response for the never-sent chunk, got %d", cancelled)
	}

	// Only the in-flight chunk's requestID gets a kill; the never-sent chunk
	// must not enqueue phantom ids.
	waitForCondition(t, 2*time.Second, func() bool { return len(mockService.killCallsSnapshot()) >= 1 })
	total := 0
	for _, kc := range mockService.killCallsSnapshot() {
		total += len(kc.ids)
	}
	if total != 1 {
		t.Fatalf("expected exactly 1 killed requestID, got %d", total)
	}
}
