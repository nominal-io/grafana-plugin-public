package plugin

import "github.com/grafana/grafana-plugin-sdk-go/data"

// fieldConfigForNumeric builds the FieldConfig for a numeric value field.
// unitless suppresses FieldConfig.Unit (for COUNT/VARIANCE). The legacy
// single-aggregation path passes false; multi-agg path passes the spec flag.
func fieldConfigForNumeric(qm *NominalQueryModel, displayName string, unitless bool) *data.FieldConfig {
	cfg := &data.FieldConfig{DisplayNameFromDS: displayName}
	if unitless {
		return cfg
	}
	cfg.Unit = mapToGrafanaUnit(qm.ChannelUnit)
	return cfg
}

// fieldConfigForEnum builds the FieldConfig for an enum/string value field.
// No unit is applied — non-numeric frames have no use for it.
func fieldConfigForEnum(qm *NominalQueryModel) *data.FieldConfig {
	return &data.FieldConfig{DisplayNameFromDS: qm.Channel}
}
