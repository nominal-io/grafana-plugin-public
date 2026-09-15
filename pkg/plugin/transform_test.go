package plugin

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	"github.com/palantir/pkg/safelong"
)

func TestTransformBatchResultLegacyNumeric(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric batch result produces float64 value fields", func(t *testing.T) {
		values := []float64{1.5, 2.5, 3.5, 4.5}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "temperature",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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
		// Value field must be *float64 (nullable)
		valueField := frame.Fields[1]
		if valueField.Len() != len(values) {
			t.Fatalf("expected %d values, got %d", len(values), valueField.Len())
		}
		for i := 0; i < valueField.Len(); i++ {
			v, ok := valueField.At(i).(*float64)
			if !ok {
				t.Errorf("value at index %d is not *float64, got %T", i, valueField.At(i))
			}
			if v == nil || *v != values[i] {
				t.Errorf("value at index %d: expected %f, got %v", i, values[i], v)
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

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		if !strings.Contains(frame.Name, "velocity") {
			t.Errorf("expected frame name to contain 'velocity', got: %s", frame.Name)
		}
	})
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

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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

func TestExtractionTruncatesToShortestInput(t *testing.T) {
	exec := newTestQueryExecution(&Datasource{}, nil)

	t.Run("enum plot", func(t *testing.T) {
		plot := computeapi.EnumPlot{
			Timestamps: []api.Timestamp{
				testTimestamp(1704067200),
				testTimestamp(1704067260),
				testTimestamp(1704067320),
			},
			Values:     []int{0},
			Categories: []string{"idle"},
		}

		times, values, err := exec.extractEnumDataFromConjure(plot)
		if err != nil {
			t.Fatalf("extract enum data: %v", err)
		}
		if len(times) != 1 || len(values) != 1 {
			t.Fatalf("got %d times and %d values, want 1 each", len(times), len(values))
		}
		if values[0] != "idle" {
			t.Fatalf("value = %q, want %q", values[0], "idle")
		}
	})

	t.Run("bucketed enum plot", func(t *testing.T) {
		plot := computeapi.BucketedEnumPlot{
			Timestamps: []api.Timestamp{
				testTimestamp(1704067200),
				testTimestamp(1704067260),
			},
			Buckets: []computeapi.EnumBucket{{
				Histogram: map[int]safelong.SafeLong{0: safelong.SafeLong(3)},
				FirstPoint: computeapi.CompactEnumPoint{
					Timestamp: testTimestamp(1704067200),
					Value:     0,
				},
			}},
			Categories: []string{"idle"},
		}

		times, values, err := exec.extractBucketedEnumDataFromConjure(plot)
		if err != nil {
			t.Fatalf("extract bucketed enum data: %v", err)
		}
		if len(times) != 1 || len(values) != 1 {
			t.Fatalf("got %d times and %d values, want 1 each", len(times), len(values))
		}
		if values[0] != "idle" {
			t.Fatalf("value = %q, want %q", values[0], "idle")
		}
	})

	t.Run("paged log plot", func(t *testing.T) {
		result := createMockPagedLogResult(
			[]string{"only value"},
			[]map[string]string{{}},
			[]api.Timestamp{
				testTimestamp(1704067200),
				testTimestamp(1704067260),
			},
		)
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := exec.transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}
		bodyField := resp.Frames[0].Fields[1]
		if bodyField.Len() != 1 {
			t.Fatalf("expected 1 log entry after truncation, got %d", bodyField.Len())
		}
		if got := bodyField.At(0).(string); got != "only value" {
			t.Fatalf("body = %q, want %q", got, "only value")
		}
	})
}

func TestBucketedEnumModeBreaksTiesByLowestIndex(t *testing.T) {
	exec := newTestQueryExecution(&Datasource{}, nil)
	plot := computeapi.BucketedEnumPlot{
		Timestamps: []api.Timestamp{
			testTimestamp(1704067200),
		},
		Buckets: []computeapi.EnumBucket{{
			Histogram: map[int]safelong.SafeLong{
				2: safelong.SafeLong(3),
				0: safelong.SafeLong(3),
				1: safelong.SafeLong(1),
			},
			FirstPoint: computeapi.CompactEnumPoint{
				Timestamp: testTimestamp(1704067200),
				Value:     1,
			},
		}},
		Categories: []string{"idle", "ready", "running"},
	}

	// Run repeatedly: Go randomizes map iteration order, so a tie broken by
	// iteration order would flip between "idle" and "running" across runs.
	for i := 0; i < 50; i++ {
		_, values, err := exec.extractBucketedEnumDataFromConjure(plot)
		if err != nil {
			t.Fatalf("extract bucketed enum data: %v", err)
		}
		if len(values) != 1 || values[0] != "idle" {
			t.Fatalf("iteration %d: values = %v, want [idle]", i, values)
		}
	}
}

func TestEnumPointTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("passes through resolved string value directly", func(t *testing.T) {
		result := createMockEnumPointComputeResult("active")
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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

	t.Run("nil enum point produces empty enum frame not numeric", func(t *testing.T) {
		computeResponse := computeapi.NewComputeNodeResponseFromEnumPoint(nil)
		computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)
		result := computeapi.ComputeWithUnitsResult{ComputeResult: computeResult}
		qm := NominalQueryModel{
			Channel:         "status",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "string",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}
		frame := resp.Frames[0]
		if frame.Meta == nil {
			t.Fatal("expected frame metadata on empty enum response")
		}
		if frame.Meta.Type != data.FrameTypeTable {
			t.Errorf("expected FrameTypeTable (enum path), got %v — nil enum points must not fall through to numeric", frame.Meta.Type)
		}
		if frame.Meta.PreferredVisualization != data.VisTypeTable {
			t.Errorf("expected VisTypeTable, got %v", frame.Meta.PreferredVisualization)
		}
	})
}

func TestTransformArrowBucketedNumericResponse(t *testing.T) {
	arrowBytes := createTestArrowBucketedNumeric(
		[]int64{1000000000000, 2000000000000, 3000000000000},
		[]float64{1.5, 2.5, 3.5},
		nil,
	)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	response := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)

	ds := &Datasource{}
	qm := NominalQueryModel{Aggregations: []string{"MEAN"}}
	result, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, qm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsEnum {
		t.Error("expected IsEnum=false for Arrow bucketed numeric")
	}
	if len(result.AggSeries) != 1 {
		t.Fatalf("expected 1 AggSeries, got %d", len(result.AggSeries))
	}
	series := result.AggSeries[0]
	if series.Name != "mean" {
		t.Errorf("AggSeries[0].Name = %q, want %q", series.Name, "mean")
	}
	if len(series.TimePoints) != 3 {
		t.Fatalf("expected 3 time points, got %d", len(series.TimePoints))
	}
	if len(series.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(series.Values))
	}
	if series.Values[0] == nil || *series.Values[0] != 1.5 {
		t.Errorf("Values[0] = %v, want 1.5", series.Values[0])
	}
	if series.Values[2] == nil || *series.Values[2] != 3.5 {
		t.Errorf("Values[2] = %v, want 3.5", series.Values[2])
	}
}

func TestTransformArrowMultiAggregation(t *testing.T) {
	ts := []int64{1000000000000, 2000000000000, 3000000000000}
	columns := map[string][]float64{
		"mean": {1.5, 2.5, 3.5},
		"min":  {1.0, 2.0, 3.0},
		"max":  {2.0, 3.0, 4.0},
	}
	arrowBytes := createTestArrowMultiAgg(ts, columns)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	response := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)

	ds := &Datasource{}
	qm := NominalQueryModel{Aggregations: []string{"MEAN", "MIN", "MAX"}}
	result, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, qm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.AggSeries) != 3 {
		t.Fatalf("expected 3 AggSeries, got %d", len(result.AggSeries))
	}

	expected := []struct {
		name  string
		first float64
		last  float64
	}{
		{"mean", 1.5, 3.5},
		{"min", 1.0, 3.0},
		{"max", 2.0, 4.0},
	}
	for i, exp := range expected {
		s := result.AggSeries[i]
		if s.Name != exp.name {
			t.Errorf("AggSeries[%d].Name = %q, want %q", i, s.Name, exp.name)
		}
		if len(s.TimePoints) != 3 {
			t.Errorf("AggSeries[%d] has %d time points, want 3", i, len(s.TimePoints))
		}
		if len(s.Values) != 3 {
			t.Errorf("AggSeries[%d] has %d values, want 3", i, len(s.Values))
		}
		if s.Values[0] == nil || *s.Values[0] != exp.first {
			t.Errorf("AggSeries[%d].Values[0] = %v, want %v", i, s.Values[0], exp.first)
		}
		if s.Values[2] == nil || *s.Values[2] != exp.last {
			t.Errorf("AggSeries[%d].Values[2] = %v, want %v", i, s.Values[2], exp.last)
		}
	}
}

// createTestArrowFirstLast builds an Arrow IPC buffer matching the API schema for
// FIRST_POINT/LAST_POINT: first_value, first_timestamp, last_value, last_timestamp,
// plus the shared end_bucket_timestamp.
func createTestArrowFirstLast(
	tb testing.TB,
	endBucketTs []int64,
	firstValues []float64, firstTimestamps []int64,
	lastValues []float64, lastTimestamps []int64,
) []byte {
	tb.Helper()
	return buildFirstLastArrow(tb, endBucketTs, firstValues, nullableInt64Values{
		values: firstTimestamps,
	}, lastValues, nullableInt64Values{
		values: lastTimestamps,
	})
}

func TestTransformArrowFirstLastPoint(t *testing.T) {
	endBucketTs := []int64{1000000000000, 2000000000000, 3000000000000}
	firstValues := []float64{10.0, 20.0, 30.0}
	firstTimestamps := []int64{900000000000, 1900000000000, 2900000000000}
	lastValues := []float64{15.0, 25.0, 35.0}
	lastTimestamps := []int64{999000000000, 1999000000000, 2999000000000}

	arrowBytes := createTestArrowFirstLast(t, endBucketTs, firstValues, firstTimestamps, lastValues, lastTimestamps)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	response := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)

	ds := &Datasource{}
	qm := NominalQueryModel{Aggregations: []string{"FIRST_POINT", "LAST_POINT"}}
	result, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, qm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.AggSeries) != 2 {
		t.Fatalf("expected 2 AggSeries, got %d", len(result.AggSeries))
	}

	// FIRST_POINT series
	first := result.AggSeries[0]
	if first.Name != "first" {
		t.Errorf("AggSeries[0].Name = %q, want %q", first.Name, "first")
	}
	if len(first.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(first.Values))
	}
	if first.Values[0] == nil || *first.Values[0] != 10.0 {
		t.Errorf("first.Values[0] = %v, want 10.0", first.Values[0])
	}
	// Verify FIRST_POINT uses its own timestamps, not end_bucket_timestamp
	if first.TimePoints[0] != time.Unix(0, 900000000000) {
		t.Errorf("first.TimePoints[0] = %v, want %v", first.TimePoints[0], time.Unix(0, 900000000000))
	}

	// LAST_POINT series
	last := result.AggSeries[1]
	if last.Name != "last" {
		t.Errorf("AggSeries[1].Name = %q, want %q", last.Name, "last")
	}
	if last.Values[2] == nil || *last.Values[2] != 35.0 {
		t.Errorf("last.Values[2] = %v, want 35.0", last.Values[2])
	}
	// Verify LAST_POINT uses its own timestamps
	if last.TimePoints[2] != time.Unix(0, 2999000000000) {
		t.Errorf("last.TimePoints[2] = %v, want %v", last.TimePoints[2], time.Unix(0, 2999000000000))
	}

	// Verify first and last have DIFFERENT time axes
	if first.TimePoints[0] == last.TimePoints[0] {
		t.Errorf("first and last should have different timestamps, both got %v", first.TimePoints[0])
	}
}

// TestTransformArrowMixedAggWithFirstPoint tests a query with both standard aggregations
// (which share end_bucket_timestamp) and FIRST_POINT (which has its own timestamp column).
func TestTransformArrowMixedAggWithFirstPoint(t *testing.T) {
	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "mean", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "first_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "first_timestamp", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	endTs := []int64{1000000000000, 2000000000000}
	meanVals := []float64{5.0, 6.0}
	firstVals := []float64{4.0, 5.5}
	firstTs := []int64{900000000000, 1900000000000}

	tsBuilder := array.NewInt64Builder(pool)
	for _, v := range endTs {
		tsBuilder.Append(v)
	}
	tsArr := tsBuilder.NewArray()
	defer tsArr.Release()
	tsBuilder.Release()

	meanBuilder := array.NewFloat64Builder(pool)
	for _, v := range meanVals {
		meanBuilder.Append(v)
	}
	meanArr := meanBuilder.NewArray()
	defer meanArr.Release()
	meanBuilder.Release()

	firstValBuilder := array.NewFloat64Builder(pool)
	for _, v := range firstVals {
		firstValBuilder.Append(v)
	}
	firstValArr := firstValBuilder.NewArray()
	defer firstValArr.Release()
	firstValBuilder.Release()

	firstTsBuilder := array.NewInt64Builder(pool)
	for _, v := range firstTs {
		firstTsBuilder.Append(v)
	}
	firstTsArr := firstTsBuilder.NewArray()
	defer firstTsArr.Release()
	firstTsBuilder.Release()

	rec := array.NewRecord(schema, []arrow.Array{tsArr, meanArr, firstValArr, firstTsArr}, 2)
	defer rec.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		panic(err)
	}
	writer.Close()

	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
	response := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)

	ds := &Datasource{}
	qm := NominalQueryModel{Aggregations: []string{"MEAN", "FIRST_POINT"}}
	result, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, qm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.AggSeries) != 2 {
		t.Fatalf("expected 2 AggSeries, got %d", len(result.AggSeries))
	}

	// MEAN uses shared end_bucket_timestamp
	meanSeries := result.AggSeries[0]
	if meanSeries.Name != "mean" {
		t.Errorf("AggSeries[0].Name = %q, want %q", meanSeries.Name, "mean")
	}
	if meanSeries.TimePoints[0] != time.Unix(0, 1000000000000) {
		t.Errorf("mean.TimePoints[0] = %v, want %v", meanSeries.TimePoints[0], time.Unix(0, 1000000000000))
	}

	// FIRST_POINT uses its own first_timestamp
	firstSeries := result.AggSeries[1]
	if firstSeries.Name != "first" {
		t.Errorf("AggSeries[1].Name = %q, want %q", firstSeries.Name, "first")
	}
	if firstSeries.TimePoints[0] != time.Unix(0, 900000000000) {
		t.Errorf("first.TimePoints[0] = %v, want %v (should use first_timestamp, not end_bucket_timestamp)",
			firstSeries.TimePoints[0], time.Unix(0, 900000000000))
	}
}

func TestTransformArrowNumericPlotReturnsError(t *testing.T) {
	arrowPlot := computeapi.ArrowNumericPlot{ArrowBinary: []byte{}}
	response := computeapi.NewComputeNodeResponseFromArrowNumeric(arrowPlot)

	ds := &Datasource{}
	_, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, NominalQueryModel{})
	if err == nil {
		t.Fatal("expected error for ArrowNumericPlot, got nil")
	}
	if !strings.Contains(err.Error(), "ArrowNumericPlot unexpectedly") {
		t.Errorf("error should mention ArrowNumericPlot, got: %v", err)
	}
}
