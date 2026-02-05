package models

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

type PluginSettings struct {
	BaseUrl string                `json:"baseUrl"`
	Path    string                `json:"path"` // Legacy field
	Secrets *SecretPluginSettings `json:"-"`
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

	settings.Secrets = loadSecretPluginSettings(source.DecryptedSecureJSONData)

	return &settings, nil
}

func loadSecretPluginSettings(source map[string]string) *SecretPluginSettings {
	return &SecretPluginSettings{
		ApiKey: source["apiKey"],
	}
}
