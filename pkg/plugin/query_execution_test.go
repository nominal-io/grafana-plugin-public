package plugin

import (
	"context"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
)

func preparedNumericQueries(refIDs ...string) []preparedQuery {
	prepared := make([]preparedQuery, len(refIDs))
	for i, refID := range refIDs {
		prepared[i] = preparedQuery{
			Query: backend.DataQuery{RefID: refID},
			Model: NominalQueryModel{Channel: "chan-" + refID, ChannelDataType: ChannelDataTypeNumeric},
		}
	}
	return prepared
}

func TestUnsupportedResponseAffectsOnlyItsQuery(t *testing.T) {
	mock := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				{ComputeResult: computeapi.NewComputeNodeResultFromSuccess(
					computeapi.NewComputeNodeResponseFromGrouped(computeapi.GroupedComputeNodeResponses{}),
				)},
				createMockComputeResult(nil),
			},
		},
	}
	ds := &Datasource{computeService: mock}
	e := newTestQueryExecution(ds, nil)

	results := e.executePreparedBatches(context.Background(), preparedNumericQueries("grouped", "numeric"))

	groupedResp, ok := results["grouped"]
	if !ok || groupedResp.Error == nil {
		t.Fatalf("expected an error response for the grouped query, got %+v", groupedResp)
	}
	numericResp, ok := results["numeric"]
	if !ok {
		t.Fatal("missing response for the numeric query")
	}
	if numericResp.Error != nil {
		t.Fatalf("numeric query must not be poisoned by its sibling, got error: %v", numericResp.Error)
	}
	if len(numericResp.Frames) != 1 {
		t.Fatalf("numeric query should render one frame, got %d", len(numericResp.Frames))
	}
}

func TestPanicInOneResultTransformAffectsOnlyItsQuery(t *testing.T) {
	corrupt := createTestArrowBucketedNumeric(
		[]int64{1773975408000000000, 1773975414000000000},
		[]float64{0.71, -0.40}, nil)
	// This offset corrupts the Arrow body so conversion panics in arrow-go v18.
	corrupt[415] ^= 0xFF

	mock := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				{ComputeResult: computeapi.NewComputeNodeResultFromSuccess(
					computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(
						computeapi.ArrowBucketedNumericPlot{ArrowBinary: corrupt}),
				)},
				createMockComputeResult(nil),
			},
		},
	}
	prepared := preparedNumericQueries("panics", "healthy")
	prepared[0].Model.Aggregations = []string{AggMean}
	e := newTestQueryExecution(&Datasource{computeService: mock}, nil)

	results := e.executePreparedBatches(context.Background(), prepared)

	panicResp, ok := results["panics"]
	if !ok || panicResp.Error == nil {
		t.Fatalf("expected an error response for the panicking query, got %+v", panicResp)
	}
	if got := panicResp.Error.Error(); got != "Internal error while processing query result" {
		t.Fatalf("expected the per-result panic containment message, got: %q", got)
	}
	healthyResp, ok := results["healthy"]
	if !ok {
		t.Fatal("missing response for the healthy query")
	}
	if healthyResp.Error != nil {
		t.Fatalf("healthy query must not be poisoned by its panicking sibling, got error: %v", healthyResp.Error)
	}
	if len(healthyResp.Frames) != 1 {
		t.Fatalf("healthy query should render one frame, got %d", len(healthyResp.Frames))
	}
}

func TestExecutePreparedBatchesSurvivesPanicInBatch(t *testing.T) {
	mock := &mockComputeService{
		batchComputeFunc: func(computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
			panic("sentinel-batch-panic-detail-must-not-surface")
		},
	}
	ds := &Datasource{computeService: mock}
	e := newTestQueryExecution(ds, nil)

	results := e.executePreparedBatches(context.Background(), preparedNumericQueries("A", "B"))

	if len(results) != 2 {
		t.Fatalf("expected error responses for both queries, got %d responses", len(results))
	}
	for _, refID := range []string{"A", "B"} {
		resp, ok := results[refID]
		if !ok {
			t.Fatalf("missing response for %s", refID)
		}
		if resp.Error == nil {
			t.Fatalf("expected an error response for %s after batch panic, got nil error", refID)
		}
		if got := resp.Error.Error(); got != "Internal error while executing query batch" {
			t.Fatalf("expected the exact generic internal-error message for %s, got: %q", refID, got)
		}
	}
}
