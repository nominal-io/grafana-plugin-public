package models

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

type PluginSettings struct {
	BaseUrl string `json:"baseUrl"`
	Path    string `json:"path"` // Legacy field
	// WorkspaceRid limits asset search to one workspace; empty means all.
	WorkspaceRid string `json:"workspaceRid"`
	QueryAPI     string `json:"queryApi,omitempty"`
	// EnableSql is retained for settings saved by the SQL preview.
	EnableSql bool                  `json:"enableSql"`
	Secrets   *SecretPluginSettings `json:"-"`
}

// UsesSQL selects the API for this datasource. Explicit configuration wins over legacy settings.
func (ps *PluginSettings) UsesSQL() bool {
	return ps.QueryAPI == "sql" || (ps.QueryAPI == "" && ps.EnableSql)
}

// GetAPIBaseURL returns the API base URL, preferring baseUrl over legacy path
func (ps *PluginSettings) GetAPIBaseURL() string {
	if ps.BaseUrl != "" {
		return ps.BaseUrl
	}
	// Fallback to legacy path field
	if ps.Path != "" {
		return ps.Path
	}
	return ""
}

type SecretPluginSettings struct {
	ApiKey string `json:"apiKey"`
}

func LoadPluginSettings(source backend.DataSourceInstanceSettings) (*PluginSettings, error) {
	settings := PluginSettings{}
	err := json.Unmarshal(source.JSONData, &settings)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal PluginSettings json: %w", err)
	}

	if settings.QueryAPI != "" && settings.QueryAPI != "compute" && settings.QueryAPI != "sql" {
		return nil, fmt.Errorf("unknown query API %q; use compute or sql", settings.QueryAPI)
	}
	settings.WorkspaceRid = strings.TrimSpace(settings.WorkspaceRid)
	settings.Secrets = loadSecretPluginSettings(source.DecryptedSecureJSONData)

	return &settings, nil
}

func loadSecretPluginSettings(source map[string]string) *SecretPluginSettings {
	return &SecretPluginSettings{
		ApiKey: source["apiKey"],
	}
}
