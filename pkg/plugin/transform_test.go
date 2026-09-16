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
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}
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

		valueField := frame.Fields[1]
		if valueField.Name != "value" {
			t.Errorf("expected field name 'value', got %q", valueField.Name)
		}

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
	// FIRST_POINT uses its own timestamps, not end_bucket_timestamp.
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
	if last.TimePoints[2] != time.Unix(0, 2999000000000) {
		t.Errorf("last.TimePoints[2] = %v, want %v", last.TimePoints[2], time.Unix(0, 2999000000000))
	}

	// First and last have different time axes.
	if first.TimePoints[0] == last.TimePoints[0] {
		t.Errorf("first and last should have different timestamps, both got %v", first.TimePoints[0])
	}
}

// Standard aggregations share end_bucket_timestamp. FIRST_POINT has its own
// timestamp column.
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

func TestDisplayNameFromDS(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric path with data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		values := []float64{1.0, 2.0}
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

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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

// Checks FieldConfig.Unit wiring through transformBatchResult's three branches:
// multi-agg, enum, and legacy single-numeric. TestFieldConfigForNumeric and
// TestFieldConfigForEnum cover the builders in isolation.
func TestFieldConfigUnit(t *testing.T) {
	ds := &Datasource{}

	// Grafana ignores Unit on time fields today; this guards against a builder
	// applying FieldConfig to the wrong field.
	assertTimeFieldUnitFree := func(t *testing.T, frame *data.Frame) {
		t.Helper()
		if cfg := frame.Fields[0].Config; cfg != nil && cfg.Unit != "" {
			t.Errorf("time field must have no Unit, got %q", cfg.Unit)
		}
	}

	t.Run("legacy numeric path on Cel channel sets FieldConfig.Unit=celsius", func(t *testing.T) {
		// Legacy single-numeric branch, data present.
		result := createMockComputeResult([]float64{1.0, 2.0})
		qm := NominalQueryModel{
			Channel:     "engine_temp",
			AssetRid:    "ri.nominal.asset.test",
			ChannelUnit: "Cel",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "celsius" {
			t.Errorf("Unit = %q, want %q", valueField.Config.Unit, "celsius")
		}
		if valueField.Config.DisplayNameFromDS != "engine_temp" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "engine_temp")
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("legacy numeric path with empty data still sets Unit=celsius", func(t *testing.T) {
		// Legacy single-numeric branch, no data.
		result := createMockComputeResult([]float64{})
		qm := NominalQueryModel{
			Channel:     "engine_temp",
			AssetRid:    "ri.nominal.asset.test",
			ChannelUnit: "Cel",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "celsius" {
			t.Errorf("Unit = %q, want %q", valueField.Config.Unit, "celsius")
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("legacy numeric path with empty ChannelUnit leaves Unit empty", func(t *testing.T) {
		result := createMockComputeResult([]float64{1.0})
		qm := NominalQueryModel{
			Channel:  "engine_temp",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "" {
			t.Errorf("Unit = %q, want empty", valueField.Config.Unit)
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("enum path on Cel-tagged channel does NOT set Unit", func(t *testing.T) {
		// Enum branch, data present. Even with ChannelUnit set, enum/string
		// frames must not carry a unit; numeric formatting is meaningless.
		result := createMockEnumComputeResult([]string{"on", "off"}, []int{0, 1})
		qm := NominalQueryModel{
			Channel:     "engine_state",
			AssetRid:    "ri.nominal.asset.test",
			ChannelUnit: "Cel", // would be wrong on an enum; builder must ignore it
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "" {
			t.Errorf("Unit = %q, want empty (enum frames carry no unit)", valueField.Config.Unit)
		}
		if valueField.Config.DisplayNameFromDS != "engine_state" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "engine_state")
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("enum path with empty data does NOT set Unit", func(t *testing.T) {
		// Enum branch, no data.
		result := createMockEnumComputeResult([]string{"on", "off"}, []int{})
		qm := NominalQueryModel{
			Channel:     "engine_state",
			AssetRid:    "ri.nominal.asset.test",
			ChannelUnit: "Cel",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "" {
			t.Errorf("Unit = %q, want empty", valueField.Config.Unit)
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("multi-agg MEAN+COUNT+VARIANCE on Cel channel: MEAN has unit, COUNT/VARIANCE do not", func(t *testing.T) {
		// Multi-agg branch, data present.
		ts := []int64{1000000000000, 2000000000000}
		columns := map[string][]float64{
			"mean":     {10.0, 20.0},
			"count":    {5, 5},
			"variance": {1.5, 2.5},
		}
		arrowBytes := createTestArrowMultiAgg(ts, columns)
		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
		result := computeapi.ComputeWithUnitsResult{
			ComputeResult: computeapi.NewComputeNodeResultFromSuccess(
				computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot),
			),
		}
		qm := NominalQueryModel{
			Channel:              "engine_temp",
			AssetRid:             "ri.nominal.asset.test",
			ChannelUnit:          "Cel",
			Aggregations:         []string{AggMean, AggCount, AggVariance},
			ExplicitAggregations: true,
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if len(resp.Frames) != 3 {
			t.Fatalf("expected 3 frames (mean, count, variance), got %d", len(resp.Frames))
		}

		// Frame order matches qm.Aggregations: [MEAN, COUNT, VARIANCE].
		expected := []struct {
			displayName string
			wantUnit    string
		}{
			{"engine_temp (mean)", "celsius"},
			{"engine_temp (count)", ""},
			{"engine_temp (variance)", ""},
		}
		for i, exp := range expected {
			valueField := resp.Frames[i].Fields[1]
			if valueField.Config == nil {
				t.Fatalf("frame[%d]: nil Config", i)
			}
			if valueField.Config.Unit != exp.wantUnit {
				t.Errorf("frame[%d] (%s).Unit = %q, want %q", i, exp.displayName, valueField.Config.Unit, exp.wantUnit)
			}
			if valueField.Config.DisplayNameFromDS != exp.displayName {
				t.Errorf("frame[%d].DisplayNameFromDS = %q, want %q", i, valueField.Config.DisplayNameFromDS, exp.displayName)
			}
			assertTimeFieldUnitFree(t, resp.Frames[i])
		}
	})
}

func TestFieldConfigForNumeric(t *testing.T) {
	// Only the helper's own behavior: mapped unit applied, unit suppressed when
	// the aggregation does not carry it, and suffix fallthrough for symbols not
	// in unitSymbolToGrafanaID. TestFieldConfigUnit covers the frame paths.
	tests := []struct {
		name               string
		channelUnit        string
		displayName        string
		carriesChannelUnit bool
		wantUnit           string
		wantDispName       string
	}{
		{
			name:               "applies mapped Grafana unit",
			channelUnit:        "Cel",
			displayName:        "engine_temp (mean)",
			carriesChannelUnit: true,
			wantUnit:           "celsius",
			wantDispName:       "engine_temp (mean)",
		},
		{
			name:               "suppresses unit when aggregation does not carry channel unit",
			channelUnit:        "Cel",
			displayName:        "engine_temp (count)",
			carriesChannelUnit: false,
			wantUnit:           "",
			wantDispName:       "engine_temp (count)",
		},
		{
			name:               "falls through to explicit suffix for unmapped symbol",
			channelUnit:        "asdfsdfs",
			displayName:        "weird_channel",
			carriesChannelUnit: true,
			wantUnit:           "suffix:asdfsdfs",
			wantDispName:       "weird_channel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := &NominalQueryModel{ChannelUnit: tt.channelUnit, Channel: "engine_temp"}
			got := fieldConfigForNumeric(qm, tt.displayName, tt.carriesChannelUnit)
			if got.Unit != tt.wantUnit {
				t.Errorf("Unit = %q, want %q", got.Unit, tt.wantUnit)
			}
			if got.DisplayNameFromDS != tt.wantDispName {
				t.Errorf("DisplayNameFromDS = %q, want %q", got.DisplayNameFromDS, tt.wantDispName)
			}
		})
	}
}

func TestFieldConfigForEnum(t *testing.T) {
	// Enum frames never carry a unit, regardless of what ChannelUnit holds.
	qm := &NominalQueryModel{Channel: "engine_state", ChannelUnit: "Cel"}
	got := fieldConfigForEnum(qm)
	if got.Unit != "" {
		t.Errorf("fieldConfigForEnum must not set Unit, got %q", got.Unit)
	}
	if got.DisplayNameFromDS != "engine_state" {
		t.Errorf("DisplayNameFromDS = %q, want %q", got.DisplayNameFromDS, "engine_state")
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
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Compute error: CHANNEL_NOT_FOUND (code: 404)") {
			t.Errorf("expected original error format, got: %s", errMsg)
		}
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
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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
		// The hint fires for any ChannelHasWrongType error. The stored type and the
		// API's actual type disagree regardless of whether ChannelDataType is set.
		result := createMockErrorResult(400, "Compute:ChannelHasWrongType")
		qm := NominalQueryModel{
			Channel:         "status",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "string",
		}
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
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
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Hint:") {
			t.Errorf("expected hint for bare ChannelHasWrongType, got: %s", errMsg)
		}
	})
}
