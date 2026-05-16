package plugin

import "testing"

func TestFieldConfigForNumeric(t *testing.T) {
	tests := []struct {
		name         string
		channelUnit  string
		displayName  string
		unitless     bool
		wantUnit     string
		wantDispName string
	}{
		{
			name:         "multi-agg MEAN on Cel channel applies unit",
			channelUnit:  "Cel",
			displayName:  "engine_temp (mean)",
			unitless:     false,
			wantUnit:     "celsius",
			wantDispName: "engine_temp (mean)",
		},
		{
			name:         "multi-agg COUNT on Cel channel suppresses unit (Unitless)",
			channelUnit:  "Cel",
			displayName:  "engine_temp (count)",
			unitless:     true,
			wantUnit:     "",
			wantDispName: "engine_temp (count)",
		},
		{
			name:         "multi-agg VARIANCE on Cel channel suppresses unit (Unitless)",
			channelUnit:  "Cel",
			displayName:  "engine_temp (variance)",
			unitless:     true,
			wantUnit:     "",
			wantDispName: "engine_temp (variance)",
		},
		{
			name:         "multi-agg FIRST on Cel channel applies unit (unit-bearing)",
			channelUnit:  "Cel",
			displayName:  "engine_temp (first)",
			unitless:     false,
			wantUnit:     "celsius",
			wantDispName: "engine_temp (first)",
		},
		{
			name:         "legacy path (unitless=false) on Cel channel applies unit",
			channelUnit:  "Cel",
			displayName:  "engine_temp",
			unitless:     false,
			wantUnit:     "celsius",
			wantDispName: "engine_temp",
		},
		{
			name:         "legacy path on channel with empty unit produces empty unit",
			channelUnit:  "",
			displayName:  "engine_temp",
			unitless:     false,
			wantUnit:     "",
			wantDispName: "engine_temp",
		},
		{
			name:         "legacy path on display-only unit falls through verbatim",
			channelUnit:  "asdfsdfs",
			displayName:  "weird_channel",
			unitless:     false,
			wantUnit:     "asdfsdfs",
			wantDispName: "weird_channel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := &NominalQueryModel{ChannelUnit: tt.channelUnit, Channel: "engine_temp"}
			got := fieldConfigForNumeric(qm, tt.displayName, tt.unitless)
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
	// Enum frames never carry a unit, regardless of what ChannelUnit holds —
	// non-numeric values have no use for it.
	qm := &NominalQueryModel{Channel: "engine_state", ChannelUnit: "Cel"}
	got := fieldConfigForEnum(qm)
	if got.Unit != "" {
		t.Errorf("fieldConfigForEnum must not set Unit, got %q", got.Unit)
	}
	if got.DisplayNameFromDS != "engine_state" {
		t.Errorf("DisplayNameFromDS = %q, want %q", got.DisplayNameFromDS, "engine_state")
	}
}
