package plugin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	"github.com/palantir/pkg/rid"
)

// ============================================================================
// CallResource routing tests (new routes only)
// ============================================================================

func TestCallResourceRouting(t *testing.T) {
	mockAuth := &mockAuthService{
		getMyProfileResponse: authapi.UserV2{
			Rid:         authapi.UserRid(rid.MustNew("user", "test", "user", "user123")),
			DisplayName: "Test User",
		},
	}

	// Create a test server that acts as the Nominal API proxy target
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"proxied": "true", "path": r.URL.Path})
	}))
	defer proxyServer.Close()

	ds := newTestDatasource(proxyServer.URL, mockAuth, &mockDatasourceService{})

	tests := []struct {
		name           string
		path           string
		method         string
		body           []byte
		expectStatus   int
		expectContains string
	}{
		{
			name:         "routes /assets",
			path:         "assets",
			method:       "POST",
			body:         []byte(`{}`),
			expectStatus: http.StatusOK,
		},
		{
			name:         "routes /datascopes without assetRid",
			path:         "datascopes",
			method:       "POST",
			body:         []byte(`{}`),
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "routes /channelvariables without assetRid",
			path:         "channelvariables",
			method:       "POST",
			body:         []byte(`{}`),
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "GET /assets returns 405",
			path:         "assets",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /datascopes returns 405",
			path:         "datascopes",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /channelvariables returns 405",
			path:         "channelvariables",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "POST /assets with invalid body returns 400",
			path:         "assets",
			method:       "POST",
			body:         []byte(`not json`),
			expectStatus: http.StatusBadRequest,
		},
		// GET 405: channels is only covered here, plus the leading-slash variants of each route.
		{
			name:         "GET /channels returns 405",
			path:         "channels",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /channels with slash returns 405",
			path:         "/channels",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /assets with slash returns 405",
			path:         "/assets",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /datascopes with slash returns 405",
			path:         "/datascopes",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /channelvariables with slash returns 405",
			path:         "/channelvariables",
			method:       "GET",
			expectStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &backend.CallResourceRequest{
				Path:   tt.path,
				Method: tt.method,
				Body:   tt.body,
			}
			resp := callResourceAndCapture(t, ds, req)
			if resp.Status != tt.expectStatus {
				t.Errorf("status = %d, want %d; body = %s", resp.Status, tt.expectStatus, string(resp.Body))
			}
			if tt.expectContains != "" && !strings.Contains(string(resp.Body), tt.expectContains) {
				t.Errorf("body %q does not contain %q", string(resp.Body), tt.expectContains)
			}
		})
	}
}

func TestSettingsLoadFailureUsesJSONResponse(t *testing.T) {
	ds := newTestDatasource("https://api.test.com", &mockAuthService{}, &mockDatasourceService{})
	ds.settings.JSONData = []byte(`{`)

	req := &backend.CallResourceRequest{
		Path:   "assets-by-rid",
		Method: http.MethodPost,
		Body:   []byte(`["ri.scout.test.asset.a"]`),
	}

	var captured *backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(resp *backend.CallResourceResponse) error {
		captured = resp
		return nil
	})

	err := ds.CallResource(context.Background(), req, sender)
	if err != nil {
		t.Fatalf("CallResource returned error: %v", err)
	}
	if captured == nil {
		t.Fatal("CallResource did not send a response")
	}
	if captured.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", captured.Status, string(captured.Body))
	}
	if got := captured.Headers["Content-Type"]; len(got) != 1 || got[0] != "application/json" {
		t.Fatalf("Content-Type header = %v, want [application/json]", got)
	}
	if !strings.Contains(string(captured.Body), "Failed to load settings") {
		t.Fatalf("body = %s, want Failed to load settings", string(captured.Body))
	}
}

func TestCallResourceRejectsBadRequests(t *testing.T) {
	upstreamHits := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		w.Write([]byte(`{}`))
	}))
	defer upstream.Close()
	ds := newTestDatasource(upstream.URL, &mockAuthService{}, &mockDatasourceService{})

	tests := []struct {
		name       string
		path       string
		method     string
		body       string
		wantStatus int
	}{
		{"unrouted path", "scout/v1/raw", http.MethodPost, `{}`, http.StatusNotFound},
		{"nominal prefix", "nominal/scout/v1/search-assets", http.MethodPost, `{}`, http.StatusNotFound},
		{"dot segments", "scout/v1/search-assets/../../authentication/api/v2/my/profile", http.MethodPost, `{}`, http.StatusNotFound},
		{"GET search-assets", "search-assets", http.MethodGet, ``, http.StatusMethodNotAllowed},
		{"DELETE assets-by-rid", "assets-by-rid", http.MethodDelete, ``, http.StatusMethodNotAllowed},
		{"assets-by-rid with object body", "assets-by-rid", http.MethodPost, `{}`, http.StatusBadRequest},
		{"assets-by-rid with null body", "assets-by-rid", http.MethodPost, `null`, http.StatusBadRequest},
		{"assets-by-rid with empty array", "assets-by-rid", http.MethodPost, `[]`, http.StatusBadRequest},
		{"assets-by-rid with malformed rid", "assets-by-rid", http.MethodPost, `["not-a-rid"]`, http.StatusBadRequest},
		{"search-assets with null body", "search-assets", http.MethodPost, `null`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Path: tt.path, Method: tt.method, Body: []byte(tt.body)})
			if resp.Status != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", resp.Status, tt.wantStatus, string(resp.Body))
			}
		})
	}
	if upstreamHits != 0 {
		t.Fatalf("upstream was called %d times for rejected requests", upstreamHits)
	}
}

func TestAssetEndpointsPostUpstream(t *testing.T) {
	tests := []struct {
		name             string
		path             string
		body             string
		upstreamStatus   int
		upstreamBody     string
		wantUpstreamPath string
		wantUpstreamBody string
		wantBodyContains string
	}{
		{"search-assets", "search-assets", `{"query":{"type":"searchText","searchText":"x"},"pageSize":50}`, http.StatusOK, `{"posted":true}`, "/scout/v1/search-assets", `"searchText":"x"`, `{"posted":true}`},
		{"assets-by-rid with leading slash", "/assets-by-rid", `["ri.scout.test.asset.a"]`, http.StatusOK, `{"posted":true}`, "/scout/v1/asset/multiple", `["ri.scout.test.asset.a"]`, `{"posted":true}`},
		{"search-assets alias", "scout/v1/search-assets", `{"query":{"type":"searchText","searchText":"x"},"pageSize":50}`, http.StatusOK, `{"posted":true}`, "/scout/v1/search-assets", `"searchText":"x"`, `{"posted":true}`},
		{"assets-by-rid alias", "scout/v1/asset/multiple", `["ri.scout.test.asset.a"]`, http.StatusOK, `{"posted":true}`, "/scout/v1/asset/multiple", `["ri.scout.test.asset.a"]`, `{"posted":true}`},
		{"assets-by-rid upstream error status", "assets-by-rid", `["ri.scout.test.asset.a"]`, http.StatusForbidden, `{"errorCode":"PERMISSION_DENIED","errorName":"Default:PermissionDenied","errorInstanceId":"abc-123"}`, "/scout/v1/asset/multiple", `["ri.scout.test.asset.a"]`, "abc-123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotMethod, gotAuth, gotCookie, gotBody string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotMethod = r.URL.Path, r.Method
				gotAuth, gotCookie = r.Header.Get("Authorization"), r.Header.Get("Cookie")
				b, _ := io.ReadAll(r.Body)
				gotBody = string(b)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.upstreamStatus)
				w.Write([]byte(tt.upstreamBody))
			}))
			defer upstream.Close()
			ds := newTestDatasource(upstream.URL, &mockAuthService{}, &mockDatasourceService{})

			req := &backend.CallResourceRequest{
				Path:   tt.path,
				Method: http.MethodPost,
				Body:   []byte(tt.body),
				Headers: map[string][]string{
					"Cookie":        {"session=secret"},
					"Authorization": {"Bearer user-token"},
				},
			}
			resp := callResourceAndCapture(t, ds, req)
			if resp.Status != tt.upstreamStatus {
				t.Fatalf("status = %d, want %d; body = %s", resp.Status, tt.upstreamStatus, string(resp.Body))
			}
			if !strings.Contains(string(resp.Body), tt.wantBodyContains) {
				t.Fatalf("body = %s, want it to contain %s", string(resp.Body), tt.wantBodyContains)
			}
			if gotPath != tt.wantUpstreamPath || gotMethod != http.MethodPost {
				t.Fatalf("upstream got %s %s, want POST %s", gotMethod, gotPath, tt.wantUpstreamPath)
			}
			if gotAuth != "Bearer test-api-key" {
				t.Fatalf("upstream Authorization = %q, want datasource API key", gotAuth)
			}
			if gotCookie != "" {
				t.Fatalf("caller Cookie was forwarded upstream: %q", gotCookie)
			}
			if !strings.Contains(gotBody, tt.wantUpstreamBody) {
				t.Fatalf("upstream body = %s, want it to contain %s", gotBody, tt.wantUpstreamBody)
			}
		})
	}
}
