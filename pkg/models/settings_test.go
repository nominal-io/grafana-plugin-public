package models

import (
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestLoadSettingsIgnoresRetiredQueryAPIOptions(t *testing.T) {
	for _, legacy := range []string{`"queryApi":"sql"`, `"queryApi":"compute"`, `"enableSql":true`} {
		settings, err := LoadPluginSettings(backend.DataSourceInstanceSettings{
			JSONData:                []byte(`{"baseUrl":"https://example.com/api","workspaceRid":" workspace ",` + legacy + `}`),
			DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if settings.BaseUrl != "https://example.com/api" || settings.WorkspaceRid != "workspace" || settings.Secrets.ApiKey != "test-key" {
			t.Fatal("connection settings were not preserved")
		}
	}
}
