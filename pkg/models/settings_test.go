package models

import (
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestLoadPluginSettings_TrimsWhitespace(t *testing.T) {
	cases := []struct {
		name       string
		jsonData   string
		secure     map[string]string
		wantBase   string
		wantPath   string
		wantApiKey string
	}{
		{
			name:       "trailing newline on api key",
			jsonData:   `{"baseUrl":"https://api.gov.nominal.io/api"}`,
			secure:     map[string]string{"apiKey": "abc123\n"},
			wantBase:   "https://api.gov.nominal.io/api",
			wantApiKey: "abc123",
		},
		{
			name:       "surrounding whitespace on all fields",
			jsonData:   `{"baseUrl":"  https://api.gov.nominal.io/api  ","path":"\thttps://legacy/api\t"}`,
			secure:     map[string]string{"apiKey": "  secret-key  "},
			wantBase:   "https://api.gov.nominal.io/api",
			wantPath:   "https://legacy/api",
			wantApiKey: "secret-key",
		},
		{
			name:       "clean values unchanged",
			jsonData:   `{"baseUrl":"https://api.gov.nominal.io/api"}`,
			secure:     map[string]string{"apiKey": "clean-key"},
			wantBase:   "https://api.gov.nominal.io/api",
			wantApiKey: "clean-key",
		},
		{
			name:       "empty api key stays empty",
			jsonData:   `{"baseUrl":"https://api.gov.nominal.io/api"}`,
			secure:     map[string]string{},
			wantBase:   "https://api.gov.nominal.io/api",
			wantApiKey: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LoadPluginSettings(backend.DataSourceInstanceSettings{
				JSONData:                []byte(tc.jsonData),
				DecryptedSecureJSONData: tc.secure,
			})
			if err != nil {
				t.Fatalf("LoadPluginSettings returned error: %v", err)
			}
			if got.BaseUrl != tc.wantBase {
				t.Errorf("BaseUrl: got %q, want %q", got.BaseUrl, tc.wantBase)
			}
			if got.Path != tc.wantPath {
				t.Errorf("Path: got %q, want %q", got.Path, tc.wantPath)
			}
			if got.Secrets == nil {
				t.Fatal("Secrets is nil")
			}
			if got.Secrets.ApiKey != tc.wantApiKey {
				t.Errorf("ApiKey: got %q, want %q", got.Secrets.ApiKey, tc.wantApiKey)
			}
		})
	}
}
