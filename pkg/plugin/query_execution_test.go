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
