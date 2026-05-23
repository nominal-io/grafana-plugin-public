package plugin

import (
	"encoding/json"
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

func TestCallResourceRouteVariants(t *testing.T) {
	mockAuth := &mockAuthService{
		getMyProfileResponse: authapi.UserV2{
			Rid:         authapi.UserRid(rid.MustNew("user", "test", "user", "user123")),
			DisplayName: "Test User",
		},
	}

	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"proxied":true}`))
	}))
	defer proxyServer.Close()

	ds := newTestDatasource(proxyServer.URL, mockAuth, &mockDatasourceService{})

	tests := []struct {
		name       string
		path       string
		method     string
		body       []byte
		wantStatus int
	}{
		{name: "test without slash", path: "test", method: "POST", wantStatus: http.StatusOK},
		{name: "test with slash", path: "/test", method: "POST", wantStatus: http.StatusOK},
		{name: "connection-test alias", path: "connection-test", method: "POST", wantStatus: http.StatusOK},
		{name: "connection-test alias with slash", path: "/connection-test", method: "POST", wantStatus: http.StatusOK},
		{name: "channels without slash", path: "channels", method: "GET", wantStatus: http.StatusMethodNotAllowed},
		{name: "channels with slash", path: "/channels", method: "GET", wantStatus: http.StatusMethodNotAllowed},
		{name: "assets without slash", path: "assets", method: "GET", wantStatus: http.StatusMethodNotAllowed},
		{name: "assets with slash", path: "/assets", method: "GET", wantStatus: http.StatusMethodNotAllowed},
		{name: "datascopes without slash", path: "datascopes", method: "GET", wantStatus: http.StatusMethodNotAllowed},
		{name: "datascopes with slash", path: "/datascopes", method: "GET", wantStatus: http.StatusMethodNotAllowed},
		{name: "channelvariables without slash", path: "channelvariables", method: "GET", wantStatus: http.StatusMethodNotAllowed},
		{name: "channelvariables with slash", path: "/channelvariables", method: "GET", wantStatus: http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := callResourceAndCapture(t, ds, &backend.CallResourceRequest{
				Path:   tt.path,
				Method: tt.method,
				Body:   tt.body,
			})
			if resp.Status != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", resp.Status, tt.wantStatus, string(resp.Body))
			}
		})
	}
}

func TestCallResourceProxyPaths(t *testing.T) {
	tests := []struct {
		name           string
		requestPath    string
		wantUpstream   string
		wantReqPath    string
		wantBodySubstr string
	}{
		{
			name:         "nominal prefix strips only nominal segment",
			requestPath:  "nominal/scout/v1/search-assets",
			wantUpstream: "/scout/v1/search-assets",
			wantReqPath:  "nominal/scout/v1/search-assets",
		},
		{
			name:         "unknown path proxies normalized path",
			requestPath:  "/scout/v1/raw",
			wantUpstream: "/scout/v1/raw",
			wantReqPath:  "/scout/v1/raw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath string
			proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"ok":true}`))
			}))
			defer proxyServer.Close()

			ds := newTestDatasource(proxyServer.URL, &mockAuthService{}, &mockDatasourceService{})
			req := &backend.CallResourceRequest{Path: tt.requestPath, Method: "POST", Body: []byte(`{}`)}

			resp := callResourceAndCapture(t, ds, req)
			if resp.Status != http.StatusOK {
				t.Fatalf("status = %d, want 200; body = %s", resp.Status, string(resp.Body))
			}
			if gotPath != tt.wantUpstream {
				t.Fatalf("upstream path = %q, want %q", gotPath, tt.wantUpstream)
			}
			if req.Path != tt.wantReqPath {
				t.Fatalf("request path was mutated to %q, want %q", req.Path, tt.wantReqPath)
			}
		})
	}
}

func TestProxyHeaderFiltering(t *testing.T) {
	mockAuth := &mockAuthService{
		getMyProfileResponse: authapi.UserV2{
			Rid:         authapi.UserRid(rid.MustNew("user", "test", "user", "user123")),
			DisplayName: "Test User",
		},
	}

	var receivedHeaders http.Header
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer proxyServer.Close()

	ds := newTestDatasource(proxyServer.URL, mockAuth, &mockDatasourceService{})

	req := &backend.CallResourceRequest{
		Path:   "scout/v1/some-endpoint",
		Method: "POST",
		Body:   []byte(`{}`),
		Headers: map[string][]string{
			"Content-Type":    {"application/json"},
			"Accept":          {"application/json"},
			"Cookie":          {"session=secret"},
			"Authorization":   {"Bearer user-token"},
			"X-Forwarded-For": {"192.168.1.1"},
			"X-Custom-Header": {"should-be-stripped"},
		},
	}

	resp := callResourceAndCapture(t, ds, req)
	if resp.Status != http.StatusOK {
		t.Fatalf("expected 200, got %d; body = %s", resp.Status, string(resp.Body))
	}

	if receivedHeaders.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type not forwarded: got %q", receivedHeaders.Get("Content-Type"))
	}
	if receivedHeaders.Get("Accept") != "application/json" {
		t.Errorf("Accept not forwarded: got %q", receivedHeaders.Get("Accept"))
	}

	if receivedHeaders.Get("Cookie") != "" {
		t.Errorf("Cookie header leaked through proxy: %q", receivedHeaders.Get("Cookie"))
	}
	if receivedHeaders.Get("X-Forwarded-For") != "" {
		t.Errorf("X-Forwarded-For header leaked through proxy: %q", receivedHeaders.Get("X-Forwarded-For"))
	}
	if receivedHeaders.Get("X-Custom-Header") != "" {
		t.Errorf("X-Custom-Header leaked through proxy: %q", receivedHeaders.Get("X-Custom-Header"))
	}

	authHeader := receivedHeaders.Get("Authorization")
	if authHeader != "Bearer test-api-key" {
		t.Errorf("Authorization header = %q, want %q", authHeader, "Bearer test-api-key")
	}
}
