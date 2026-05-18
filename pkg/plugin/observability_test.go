package plugin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
)

const uaWant = "nominal-grafana/0.11.3 (linux-amd64) go/1.25.7 grafana/12.1.0"

func TestUserAgentComponentsFromPluginContext_MissingFieldsUseUnknown(t *testing.T) {
	got := userAgentComponentsFromPluginContext(backend.PluginContext{})

	if got.PluginVersion != unknownComponent {
		t.Errorf("PluginVersion = %q, want %q", got.PluginVersion, unknownComponent)
	}
	if got.GrafanaVersion != unknownComponent {
		t.Errorf("GrafanaVersion = %q, want %q", got.GrafanaVersion, unknownComponent)
	}
}

func TestUserAgentMiddleware_SetsHeader(t *testing.T) {
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("User-Agent")
	}))
	defer srv.Close()

	c := userAgentComponents{
		PluginVersion: "0.11.3", GoOS: "linux", GoArch: "amd64",
		GoVersion: "go1.25.7", GrafanaVersion: "12.1.0",
	}
	ctx := contextWithUserAgentComponents(context.Background(), c)

	client := &http.Client{Transport: newUserAgentTransport(http.DefaultTransport)}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if seen != uaWant {
		t.Errorf("server saw UA = %q, want %q", seen, uaWant)
	}
}

func TestUserAgentMiddleware_FallsBackWhenContextMissing(t *testing.T) {
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("User-Agent")
	}))
	defer srv.Close()

	client := &http.Client{Transport: newUserAgentTransport(http.DefaultTransport)}
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if seen == "" || !strings.HasPrefix(seen, "nominal-grafana/") {
		t.Errorf("fallback UA = %q, want nominal-grafana/... prefix", seen)
	}
}

func keyValueSliceToMap(kv []any) map[string]any {
	out := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		k, _ := kv[i].(string)
		out[k] = kv[i+1]
	}
	return out
}

func conjureErrorBody(instanceID string) string {
	return `{
		"errorCode": "INTERNAL",
		"errorName": "Default:Internal",
		"errorInstanceId": "` + instanceID + `",
		"parameters": {}
	}`
}

func TestConjureError_InstanceIDFlowsThroughHelpers(t *testing.T) {
	const failingInstanceID = "11111111-2222-3333-4444-555555555555"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(conjureErrorBody(failingInstanceID)))
	}))
	defer srv.Close()

	conjureClient, err := conjurehttpclient.NewClient(
		conjurehttpclient.WithBaseURLs([]string{srv.URL}),
		conjurehttpclient.WithMiddleware(userAgentMiddleware()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	auth := authapi.NewAuthenticationServiceV2Client(conjureClient)
	_, callErr := auth.GetMyProfile(context.Background(), bearertoken.Token("x"))
	if callErr == nil {
		t.Fatal("expected error from failing server, got nil")
	}

	asMap := keyValueSliceToMap(errorFieldsFromConjure(callErr))
	if got := asMap["error_instance_id"]; got != failingInstanceID {
		t.Errorf("error_instance_id = %v, want %s", got, failingInstanceID)
	}
}

func TestConnectionTestQuery_SurfacesInstanceID(t *testing.T) {
	const failingInstanceID = "22222222-3333-4444-5555-666666666666"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(conjureErrorBody(failingInstanceID)))
	}))
	defer srv.Close()

	conjureClient, err := conjurehttpclient.NewClient(
		conjurehttpclient.WithBaseURLs([]string{srv.URL}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ds := &Datasource{
		authService: authapi.NewAuthenticationServiceV2Client(conjureClient),
	}
	exec := &NominalQueryExecution{
		datasource: ds,
		config:     &models.PluginSettings{Secrets: &models.SecretPluginSettings{ApiKey: "x"}},
	}

	resp := exec.handleConnectionTestQuery(context.Background())
	if resp.Error == nil {
		t.Fatal("expected DataResponse.Error, got nil")
	}
	wantLabel := "errorInstanceId: " + failingInstanceID
	if !strings.Contains(resp.Error.Error(), wantLabel) {
		t.Errorf("DataResponse.Error = %q, missing labeled instance id %q",
			resp.Error.Error(), wantLabel)
	}
}

func TestCheckHealth_SurfacesInstanceID(t *testing.T) {
	const failingInstanceID = "33333333-4444-5555-6666-777777777777"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(conjureErrorBody(failingInstanceID)))
	}))
	defer srv.Close()

	conjureClient, err := conjurehttpclient.NewClient(
		conjurehttpclient.WithBaseURLs([]string{srv.URL}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ds := &Datasource{
		authService: authapi.NewAuthenticationServiceV2Client(conjureClient),
	}

	settings := backend.DataSourceInstanceSettings{
		JSONData: []byte(`{"baseUrl": "` + srv.URL + `"}`),
		DecryptedSecureJSONData: map[string]string{
			"apiKey": "x",
		},
	}
	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &settings,
		},
	})
	if err != nil {
		t.Fatalf("CheckHealth returned err: %v", err)
	}
	if result.Status != backend.HealthStatusError {
		t.Fatalf("Status = %v, want HealthStatusError", result.Status)
	}
	wantLabel := "errorInstanceId: " + failingInstanceID
	if !strings.Contains(result.Message, wantLabel) {
		t.Errorf("Message = %q, missing labeled instance id %q", result.Message, wantLabel)
	}
}
