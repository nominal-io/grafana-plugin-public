package plugin

import (
	"context"
	"errors"
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

func TestInstanceIDFromError_ReadsFromAPIError(t *testing.T) {
	const id = "abcd1234-5678-90ab-cdef-000000000001"
	apiErr := newAPIError(http.StatusInternalServerError, []byte(conjureErrorBody(id)))
	if got := instanceIDFromError(apiErr); got != id {
		t.Errorf("instanceIDFromError(*apiError) = %q, want %q", got, id)
	}
	if got := instanceIDFromError(errors.New("no instance id here")); got != "" {
		t.Errorf("instanceIDFromError(plain) = %q, want empty", got)
	}
	if got := instanceIDFromError(nil); got != "" {
		t.Errorf("instanceIDFromError(nil) = %q, want empty", got)
	}
}

func TestErrorFieldsFromConjure_FromAPIError(t *testing.T) {
	const id = "55555555-6666-7777-8888-999999999999"
	apiErr := newAPIError(http.StatusInternalServerError, []byte(conjureErrorBody(id)))
	asMap := keyValueSliceToMap(errorFieldsFromConjure(apiErr))

	if got := asMap["error_instance_id"]; got != id {
		t.Errorf("error_instance_id = %v, want %s", got, id)
	}
	if got := asMap["error_code"]; got != "INTERNAL" {
		t.Errorf("error_code = %v, want INTERNAL", got)
	}
	if got := asMap["error_name"]; got != "Default:Internal" {
		t.Errorf("error_name = %v, want Default:Internal", got)
	}
}

// TestAPIError_NoBodyLeak guards the leak-fix invariant: even when the upstream
// Conjure body carries `parameters` with user-supplied values, *apiError must
// never render them via Error(). Logging callers do `"error", err`, so anything
// in Error() lands in logs — keeping `parameters` out of Error() is what makes
// the structured-fields path safe to share with the signing-reviewer constraint.
func TestAPIError_NoBodyLeak(t *testing.T) {
	const id = "deadbeef-1111-2222-3333-444444444444"
	const userValue = "secret-user-input-do-not-log"
	body := `{
		"errorCode": "INVALID_ARGUMENT",
		"errorName": "Default:InvalidArgument",
		"errorInstanceId": "` + id + `",
		"parameters": {"value": "` + userValue + `"}
	}`
	apiErr := newAPIError(http.StatusBadRequest, []byte(body))

	if strings.Contains(apiErr.Error(), userValue) {
		t.Errorf("apiError.Error() leaked user-supplied parameter value: %q", apiErr.Error())
	}
	if !strings.Contains(apiErr.Error(), id) {
		t.Errorf("apiError.Error() should include instance ID, got %q", apiErr.Error())
	}
}

func TestAPIError_UnstructuredBodyDroppedFromError(t *testing.T) {
	// Non-Conjure response — body should not surface via Error() at all.
	apiErr := newAPIError(http.StatusBadGateway, []byte("<html>upstream proxy says no</html>"))
	if strings.Contains(apiErr.Error(), "html") || strings.Contains(apiErr.Error(), "proxy") {
		t.Errorf("apiError.Error() leaked non-Conjure body: %q", apiErr.Error())
	}
	if got := apiErr.Error(); got != "API returned status 502" {
		t.Errorf("apiError.Error() = %q, want %q", got, "API returned status 502")
	}
}

func TestClassifyConnectionError(t *testing.T) {
	const id = "11111111-1111-1111-1111-111111111111"
	apiErrWithStatus := func(status int, code, name string) error {
		return newAPIError(status, []byte(`{
			"errorCode": "`+code+`",
			"errorName": "`+name+`",
			"errorInstanceId": "`+id+`"
		}`))
	}

	cases := []struct {
		name        string
		err         error
		wantMessage string
		wantStatus  int
	}{
		{
			name:        "apiError status 401 -> auth message",
			err:         apiErrWithStatus(http.StatusUnauthorized, "UNAUTHORIZED", "Default:Unauthorized"),
			wantMessage: "Invalid API key - authentication failed (errorInstanceId: " + id + ")",
			wantStatus:  http.StatusUnauthorized,
		},
		{
			name:        "apiError non-401 status falls to generic",
			err:         apiErrWithStatus(http.StatusInternalServerError, "INTERNAL", "Default:Internal"),
			wantMessage: "Failed to connect to Nominal API (errorInstanceId: " + id + ")",
			wantStatus:  http.StatusServiceUnavailable,
		},
		{
			name:        "context deadline exceeded",
			err:         errors.New("Get ...: context deadline exceeded"),
			wantMessage: "Connection timeout - unable to reach Nominal API",
			wantStatus:  http.StatusRequestTimeout,
		},
		{
			name:        "no such host",
			err:         errors.New("dial tcp: lookup nope: no such host"),
			wantMessage: "Unable to connect to Nominal API - check base URL",
			wantStatus:  http.StatusBadGateway,
		},
		{
			name:        "generic fallthrough",
			err:         errors.New("something else went wrong"),
			wantMessage: "Failed to connect to Nominal API",
			wantStatus:  http.StatusServiceUnavailable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotMsg, gotStatus := classifyConnectionError(tc.err)
			if gotStatus != tc.wantStatus {
				t.Errorf("status = %d, want %d", gotStatus, tc.wantStatus)
			}
			if !strings.HasPrefix(gotMsg, tc.wantMessage) && gotMsg != tc.wantMessage {
				t.Errorf("message = %q, want prefix %q", gotMsg, tc.wantMessage)
			}
		})
	}
}

// TestEntryPointsSeedRequestIdentity covers the three Grafana entry points
// — QueryData, CheckHealth, CallResource — to verify each calls
// contextWithPluginRequestIdentity so outbound HTTP carries a structured
// User-Agent rather than silently falling back to "unknown". A new entry
// point that forgets the helper will fail this test.
func TestEntryPointsSeedRequestIdentity(t *testing.T) {
	const pluginVersion = "9.9.9-test"

	type spy struct {
		ua string
	}
	newSpyServer := func(s *spy) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.ua = r.Header.Get("User-Agent")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(conjureErrorBody("00000000-0000-0000-0000-000000000000")))
		}))
	}

	newDatasource := func(srv *httptest.Server) (*Datasource, backend.DataSourceInstanceSettings) {
		conjureClient, err := conjurehttpclient.NewClient(
			conjurehttpclient.WithBaseURLs([]string{srv.URL}),
			conjurehttpclient.WithMiddleware(userAgentMiddleware()),
		)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		settings := backend.DataSourceInstanceSettings{
			JSONData:                []byte(`{"baseUrl": "` + srv.URL + `"}`),
			DecryptedSecureJSONData: map[string]string{"apiKey": "x"},
		}
		ds := &Datasource{
			settings:    settings,
			authService: authapi.NewAuthenticationServiceV2Client(conjureClient),
		}
		return ds, settings
	}

	wantPrefix := "nominal-grafana/" + pluginVersion

	t.Run("CheckHealth", func(t *testing.T) {
		s := &spy{}
		srv := newSpyServer(s)
		defer srv.Close()
		ds, settings := newDatasource(srv)

		_, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
			PluginContext: backend.PluginContext{
				PluginVersion:              pluginVersion,
				DataSourceInstanceSettings: &settings,
			},
		})
		if err != nil {
			t.Fatalf("CheckHealth returned err: %v", err)
		}
		if !strings.HasPrefix(s.ua, wantPrefix) {
			t.Errorf("CheckHealth UA = %q, want prefix %q (did the entry point skip contextWithPluginRequestIdentity?)", s.ua, wantPrefix)
		}
	})

	t.Run("QueryData", func(t *testing.T) {
		s := &spy{}
		srv := newSpyServer(s)
		defer srv.Close()
		ds, settings := newDatasource(srv)

		query := backend.DataQuery{
			RefID: "A",
			JSON:  []byte(`{"queryType": "connectionTest"}`),
		}
		_, err := ds.QueryData(context.Background(), &backend.QueryDataRequest{
			PluginContext: backend.PluginContext{
				PluginVersion:              pluginVersion,
				DataSourceInstanceSettings: &settings,
			},
			Queries: []backend.DataQuery{query},
		})
		if err != nil {
			t.Fatalf("QueryData returned err: %v", err)
		}
		if !strings.HasPrefix(s.ua, wantPrefix) {
			t.Errorf("QueryData UA = %q, want prefix %q (did the entry point skip contextWithPluginRequestIdentity?)", s.ua, wantPrefix)
		}
	})

	t.Run("CallResource", func(t *testing.T) {
		s := &spy{}
		srv := newSpyServer(s)
		defer srv.Close()
		ds, _ := newDatasource(srv)

		sender := &recordingCallResourceSender{}
		err := ds.CallResource(context.Background(), &backend.CallResourceRequest{
			PluginContext: backend.PluginContext{
				PluginVersion: pluginVersion,
			},
			Path:   "test",
			Method: http.MethodGet,
		}, sender)
		if err != nil {
			t.Fatalf("CallResource returned err: %v", err)
		}
		if !strings.HasPrefix(s.ua, wantPrefix) {
			t.Errorf("CallResource UA = %q, want prefix %q (did the entry point skip contextWithPluginRequestIdentity?)", s.ua, wantPrefix)
		}
	})
}

// recordingCallResourceSender is the minimal CallResourceResponseSender needed
// to drive CallResource in tests; it discards everything.
type recordingCallResourceSender struct{}

func (r *recordingCallResourceSender) Send(*backend.CallResourceResponse) error { return nil }

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
