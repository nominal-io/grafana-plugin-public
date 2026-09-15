package plugin

import (
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

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

// TestFieldConfigUnit verifies FieldConfig.Unit wiring through the real
// transformBatchResult frame-construction path across its three branches:
// multi-agg, enum, and legacy single-numeric.
//
// Complements field_config_test.go which covers the builders in isolation —
// these tests guard the wire-up.
func TestFieldConfigUnit(t *testing.T) {
	ds := &Datasource{}

	// assertTimeFieldUnitFree confirms the unit lands only on the value field,
	// never on the time axis. Grafana ignores Unit on time fields today, but the
	// negative assertion guards against a future bug where the builder applies
	// FieldConfig to the wrong field.
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
		// frames must not carry a unit — numeric formatting is meaningless.
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
	// End-to-end frame-building paths are covered by TestFieldConfigUnit above.
	// This test exercises only the helper's unique behavior: mapped unit applied,
	// unit suppressed when the aggregation does not carry it, and suffix fallthrough
	// for symbols not in unitSymbolToGrafanaID.
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
