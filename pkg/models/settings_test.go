package models

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"testing"
)

func TestQueryAPISettings(t *testing.T) {
	for _, tc := range []struct {
		json string
		sql  bool
	}{
		{`{}`, false}, {`{"queryApi":"compute"}`, false}, {`{"queryApi":"sql"}`, true},
		{`{"enableSql":true}`, true}, {`{"queryApi":"compute","enableSql":true}`, false},
	} {
		settings, err := LoadPluginSettings(backend.DataSourceInstanceSettings{JSONData: []byte(tc.json)})
		if err != nil || settings.UsesSQL() != tc.sql {
			t.Fatalf("%s: %+v %v", tc.json, settings, err)
		}
	}
	if _, err := LoadPluginSettings(backend.DataSourceInstanceSettings{JSONData: []byte(`{"queryApi":"other"}`)}); err == nil {
		t.Fatal("expected invalid API error")
	}
}
