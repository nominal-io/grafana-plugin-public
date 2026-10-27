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
	// WorkspaceRid is the workspace SQL queries run in and asset search is limited to. When empty,
	// SQL uses the API key's default workspace and asset search covers every workspace.
	WorkspaceRid  string                `json:"workspaceRid"`
	AuthMode      string                `json:"authMode"`
	OAuthClientID string                `json:"oauthClientId"`
	Secrets       *SecretPluginSettings `json:"-"`
}

func (ps *PluginSettings) UsesOAuth() bool {
	return ps.AuthMode == "oauth"
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
	// AuthToken is the bearer token sent to Nominal: the API key, or in OAuth mode the signed-in user's token.
	AuthToken         string `json:"apiKey"`
	OAuthClientSecret string `json:"oauthClientSecret"`
}

func LoadPluginSettings(source backend.DataSourceInstanceSettings) (*PluginSettings, error) {
	settings := PluginSettings{}
	err := json.Unmarshal(source.JSONData, &settings)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal PluginSettings json: %w", err)
	}

	settings.WorkspaceRid = strings.TrimSpace(settings.WorkspaceRid)
	settings.Secrets = loadSecretPluginSettings(source.DecryptedSecureJSONData)
	if settings.UsesOAuth() {
		// An API key saved before switching to OAuth is never sent.
		settings.Secrets.AuthToken = ""
	}

	return &settings, nil
}

func loadSecretPluginSettings(source map[string]string) *SecretPluginSettings {
	return &SecretPluginSettings{
		AuthToken:         source["apiKey"],
		OAuthClientSecret: source["oauthClientSecret"],
	}
}
