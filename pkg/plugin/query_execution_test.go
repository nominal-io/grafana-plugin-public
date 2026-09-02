package plugin

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	"github.com/palantir/pkg/uuid"
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
	// Byte 415 is in the record-batch metadata; corrupting it zeroes a buffer
	// length so arrow-go v18 panics counting nulls.
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

func TestExecutePreparedBatchesSurvivesPanicInChunk(t *testing.T) {
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
		if got := resp.Error.Error(); got != "Internal error while executing query chunk" {
			t.Fatalf("expected the exact generic internal-error message for %s, got: %q", refID, got)
		}
	}
}

func TestChunkPanicPreservesEarlierResultsAndEnqueuesKill(t *testing.T) {
	var calls atomic.Int32
	var panickedRequestID uuid.UUID
	mock := &mockComputeService{
		batchComputeFunc: func(request computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
			call := calls.Add(1)
			if call == 3 {
				panickedRequestID = *request.Requests[0].RequestId
				panic("sentinel third-chunk panic")
			}
			return makeBatchComputeWithUnitsResponse(len(request.Requests)), nil
		},
	}
	ds := &Datasource{computeService: mock, kill: killCoalescer{interval: time.Millisecond}}
	e := newTestQueryExecution(ds, nil)
	refIDs := make([]string, 700)
	for i := range refIDs {
		refIDs[i] = fmt.Sprintf("Q%03d", i)
	}
	prepared := preparedNumericQueries(refIDs...)
	for i := range prepared {
		prepared[i].Model.Aggregations = []string{AggMean}
	}

	results := e.executePreparedBatches(context.Background(), prepared)

	if len(results) != len(refIDs) {
		t.Fatalf("expected %d responses, got %d", len(refIDs), len(results))
	}
	for _, refID := range refIDs[:2*maxBatchComputeSubrequests] {
		if err := results[refID].Error; err != nil {
			t.Fatalf("earlier successful result %s was discarded: %v", refID, err)
		}
	}
	for _, refID := range refIDs[2*maxBatchComputeSubrequests:] {
		if err := results[refID].Error; err == nil || err.Error() != "Internal error while executing query chunk" {
			t.Fatalf("panicked chunk result %s should have the generic chunk error, got %v", refID, err)
		}
	}

	waitForCondition(t, 2*time.Second, func() bool { return len(mock.killCallsSnapshot()) == 1 })
	kills := mock.killCallsSnapshot()
	if len(kills[0].ids) != 1 || kills[0].ids[0] != panickedRequestID {
		t.Fatalf("expected kill for panicked request %v, got %v", panickedRequestID, kills[0].ids)
	}
}
