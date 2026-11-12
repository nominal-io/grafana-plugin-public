package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdkconfig "github.com/grafana/grafana-plugin-sdk-go/config"
	"github.com/nominal-io/grafana-plugin-public/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	workspaceapi "github.com/nominal-io/nominal-api-go/security/api/workspace"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

// grafanaIDToken builds the unsigned part of the ID token Grafana forwards as X-Grafana-Id.
func grafanaIDToken(aud, identifier string) string {
	claims, _ := json.Marshal(map[string]string{"aud": aud, "identifier": identifier, "sub": "user:1"})
	return "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".sig"
}

// nominalToken is a Nominal access token for user; tokens with different n are different tokens for the same user.
func nominalToken(user string, n int) string {
	claims, _ := json.Marshal(map[string]any{"sub": user, "exp": n})
	return "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".sig"
}

func oauthTestContext(identifier string) context.Context {
	ctx := withGrafanaUser(context.Background(), grafanaIDToken("org:1", identifier))
	return sdkconfig.WithGrafanaConfig(ctx, sdkconfig.NewGrafanaCfg(map[string]string{sdkconfig.AppURL: "http://localhost:3000/"}))
}

// fakeNominalAuth serves /oauth/v1/token, answering each grant with respond.
type fakeNominalAuth struct {
	mu      sync.Mutex
	forms   []url.Values
	respond func(form url.Values) (int, string)
}

func (g *fakeNominalAuth) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth/v1/token" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		_ = r.ParseForm()
		g.mu.Lock()
		g.forms = append(g.forms, r.PostForm)
		g.mu.Unlock()
		status, body := g.respond(r.PostForm)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newOAuthTestDatasource(uid, baseURL string, client *http.Client) *Datasource {
	jsonData, _ := json.Marshal(map[string]string{
		"authMode":      "oauth",
		"oauthClientId": "test-client",
		"baseUrl":       baseURL + "/api",
	})
	return &Datasource{
		settings:           backend.DataSourceInstanceSettings{UID: uid, JSONData: jsonData, DecryptedSecureJSONData: map[string]string{"oauthClientSecret": "test-secret"}},
		resourceHTTPClient: client,
		oauthSessions:      newSessionStore(),
	}
}

func callResourceWithContext(t *testing.T, ctx context.Context, ds *Datasource, req *backend.CallResourceRequest) *backend.CallResourceResponse {
	t.Helper()
	var captured *backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(resp *backend.CallResourceResponse) error {
		captured = resp
		return nil
	})
	if err := ds.CallResource(ctx, req, sender); err != nil {
		t.Fatalf("CallResource returned error: %v", err)
	}
	if captured == nil {
		t.Fatal("CallResource did not send a response")
	}
	return captured
}

// startOAuthSignIn calls oauth/start and returns the parsed authorize URL.
func startOAuthSignIn(t *testing.T, ctx context.Context, ds *Datasource) *url.URL {
	t.Helper()
	resp := callResourceWithContext(t, ctx, ds, &backend.CallResourceRequest{Path: "oauth/start", Method: http.MethodPost})
	if resp.Status != http.StatusOK {
		t.Fatalf("oauth/start returned %d: %s", resp.Status, resp.Body)
	}
	var body struct {
		AuthorizeURL string `json:"authorizeUrl"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		t.Fatal(err)
	}
	authorizeURL, err := url.Parse(body.AuthorizeURL)
	if err != nil {
		t.Fatal(err)
	}
	return authorizeURL
}

func oauthCallback(t *testing.T, ctx context.Context, ds *Datasource, query url.Values) *backend.CallResourceResponse {
	t.Helper()
	return callResourceWithContext(t, ctx, ds, &backend.CallResourceRequest{
		Path: "oauth/callback", Method: http.MethodGet, URL: "oauth/callback?" + query.Encode(),
	})
}

func TestOAuthLoginRedirectsBackToTheGrafanaPath(t *testing.T) {
	auth := &fakeNominalAuth{respond: func(url.Values) (int, string) {
		return http.StatusOK, `{"access_token":"alice-nominal-token","refresh_token":"r1","expires_in":3600}`
	}}
	srv := auth.start(t)
	ds := newOAuthTestDatasource("oauth-login", srv.URL, srv.Client())
	alice := oauthTestContext("alice")
	returnTo := "/d/abc/flight-test?orgId=1&var-asset=a"

	resp := callResourceWithContext(t, alice, ds, &backend.CallResourceRequest{
		Path: "oauth/login", Method: http.MethodGet, URL: "oauth/login?" + url.Values{"returnTo": {returnTo}}.Encode(),
	})
	if resp.Status != http.StatusFound {
		t.Fatalf("oauth/login returned %d: %s", resp.Status, resp.Body)
	}
	authorizeURL, err := url.Parse(resp.Headers["Location"][0])
	if err != nil || authorizeURL.Scheme+"://"+authorizeURL.Host+authorizeURL.Path != srv.URL+"/oauth/v1/authorize" {
		t.Fatalf("oauth/login redirected to %q", resp.Headers["Location"])
	}

	resp = oauthCallback(t, alice, ds, url.Values{"code": {"code-1"}, "state": {authorizeURL.Query().Get("state")}})
	if resp.Status != http.StatusFound || resp.Headers["Location"][0] != returnTo {
		t.Fatalf("callback returned %d %v, want a redirect to %q", resp.Status, resp.Headers["Location"], returnTo)
	}
	if ds.oauthSessions.getSession(oauthUser(alice)) == nil {
		t.Fatal("alice should be signed in")
	}
}

func TestOAuthLoginRejectsReturnToOutsideGrafana(t *testing.T) {
	ds := newOAuthTestDatasource("oauth-login-reject", "https://unused.test", http.DefaultClient)
	for _, returnTo := range []string{"", "d/abc", "https://evil.test/d/abc", "//evil.test/d/abc", "/\\evil.test/d/abc", "/d/abc\r\nSet-Cookie: x=1"} {
		resp := callResourceWithContext(t, oauthTestContext("alice"), ds, &backend.CallResourceRequest{
			Path: "oauth/login", Method: http.MethodGet, URL: "oauth/login?" + url.Values{"returnTo": {returnTo}}.Encode(),
		})
		if resp.Status != http.StatusBadRequest {
			t.Errorf("returnTo %q returned %d, want 400", returnTo, resp.Status)
		}
	}
}

func TestOAuthSignInQueriesAsTheSignedInUser(t *testing.T) {
	auth := &fakeNominalAuth{respond: func(url.Values) (int, string) {
		return http.StatusOK, `{"access_token":"alice-nominal-token","refresh_token":"r1","expires_in":3600}`
	}}
	srv := auth.start(t)
	ds := newOAuthTestDatasource("oauth-flow", srv.URL, srv.Client())
	alice, bob := oauthTestContext("alice"), oauthTestContext("bob")

	authorizeURL := startOAuthSignIn(t, alice, ds)
	params := authorizeURL.Query()
	if got := authorizeURL.Scheme + "://" + authorizeURL.Host + authorizeURL.Path; got != srv.URL+"/oauth/v1/authorize" {
		t.Fatalf("authorize URL = %q", got)
	}
	wantRedirect := "http://localhost:3000/api/datasources/uid/oauth-flow/resources/oauth/callback"
	if params.Get("redirect_uri") != wantRedirect || params.Get("client_id") != "test-client" || params.Get("code_challenge_method") != "S256" {
		t.Fatalf("unexpected authorize params: %v", params)
	}

	resp := oauthCallback(t, alice, ds, url.Values{"code": {"code-1"}, "state": {params.Get("state")}})
	if resp.Status != http.StatusOK {
		t.Fatalf("callback returned %d: %s", resp.Status, resp.Body)
	}

	form := auth.forms[0]
	challenge := sha256.Sum256([]byte(form.Get("code_verifier")))
	if base64.RawURLEncoding.EncodeToString(challenge[:]) != params.Get("code_challenge") {
		t.Fatal("code_verifier sent to Nominal auth does not match the code_challenge")
	}
	if form.Get("grant_type") != "authorization_code" || form.Get("code") != "code-1" || form.Get("redirect_uri") != wantRedirect || form.Get("client_secret") != "test-secret" {
		t.Fatalf("unexpected token form: %v", form)
	}

	config, ok, _ := ds.loadResourceSettings(alice, backend.CallResourceResponseSenderFunc(func(*backend.CallResourceResponse) error { return nil }), "test")
	if !ok || config.Secrets.AuthToken != "alice-nominal-token" {
		t.Fatalf("alice should query with her Nominal token, got ok=%v", ok)
	}
	var bobResp *backend.CallResourceResponse
	_, ok, _ = ds.loadResourceSettings(bob, backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { bobResp = r; return nil }), "test")
	if ok || bobResp.Status != http.StatusUnauthorized {
		t.Fatal("bob has not signed in and must not get alice's token")
	}
}

func TestOAuthCallbackRejectsAnotherUsersState(t *testing.T) {
	auth := &fakeNominalAuth{respond: func(url.Values) (int, string) {
		return http.StatusOK, `{"access_token":"t","expires_in":3600}`
	}}
	srv := auth.start(t)
	ds := newOAuthTestDatasource("oauth-other-user", srv.URL, srv.Client())

	state := startOAuthSignIn(t, oauthTestContext("alice"), ds).Query().Get("state")
	if resp := oauthCallback(t, oauthTestContext("bob"), ds, url.Values{"code": {"c"}, "state": {state}}); resp.Status != http.StatusBadRequest {
		t.Fatalf("bob completing alice's sign-in returned %d", resp.Status)
	}
	// The state is single use, so alice cannot replay it either.
	if resp := oauthCallback(t, oauthTestContext("alice"), ds, url.Values{"code": {"c"}, "state": {state}}); resp.Status != http.StatusBadRequest {
		t.Fatalf("replayed state returned %d", resp.Status)
	}
	if len(auth.forms) != 0 {
		t.Fatal("no code should have been redeemed")
	}
}

func TestOAuthRefreshesAnExpiringToken(t *testing.T) {
	auth := &fakeNominalAuth{respond: func(form url.Values) (int, string) {
		if form.Get("grant_type") == "refresh_token" && form.Get("refresh_token") == "r1" {
			return http.StatusOK, `{"access_token":"t2","expires_in":3600}`
		}
		return http.StatusBadRequest, `{"error":"invalid_grant"}`
	}}
	srv := auth.start(t)
	ds := newOAuthTestDatasource("oauth-refresh", srv.URL, srv.Client())
	ctx := oauthTestContext("alice")
	key := oauthUser(ctx)
	session := &oauthSession{accessToken: "t1", refreshToken: "r1", expiry: time.Now().Add(10 * time.Second)}
	ds.oauthSessions.putSession(key, session)

	config, err := models.LoadPluginSettings(ds.settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.applyOAuthCredential(ctx, config); err != nil || config.Secrets.AuthToken != "t2" {
		t.Fatalf("expected refreshed token t2, got %q, err %v", config.Secrets.AuthToken, err)
	}
	if session.refreshToken != "r1" {
		t.Fatal("refresh token must be kept when Nominal auth does not rotate it")
	}

	session.refreshToken, session.expiry = "revoked", time.Now()
	if err := ds.applyOAuthCredential(ctx, config); err != errSignInRequired {
		t.Fatalf("failed refresh should require sign-in, got %v", err)
	}
	if ds.oauthSessions.getSession(key) != nil {
		t.Fatal("a session whose refresh token Nominal auth rejected must be dropped")
	}
}

func TestOAuthRefreshKeepsTheSessionWhenNominalAuthIsUnavailable(t *testing.T) {
	auth := &fakeNominalAuth{respond: func(url.Values) (int, string) {
		return http.StatusServiceUnavailable, `{"error":"temporarily_unavailable"}`
	}}
	srv := auth.start(t)
	ds := newOAuthTestDatasource("oauth-refresh-unavailable", srv.URL, srv.Client())
	ctx := oauthTestContext("alice")
	session := &oauthSession{accessToken: "t1", refreshToken: "r1", expiry: time.Now()}
	ds.oauthSessions.putSession(oauthUser(ctx), session)

	config, err := models.LoadPluginSettings(ds.settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.applyOAuthCredential(ctx, config); err != errSignInRequired {
		t.Fatalf("failed refresh should require sign-in, got %v", err)
	}
	if ds.oauthSessions.getSession(oauthUser(ctx)) != session || session.refreshToken != "r1" {
		t.Fatal("a transient refresh failure must keep the session")
	}
}

func TestOAuthRefreshSurvivesACancelledQuery(t *testing.T) {
	auth := &fakeNominalAuth{respond: func(url.Values) (int, string) {
		return http.StatusOK, `{"access_token":"t2","refresh_token":"r2","expires_in":3600}`
	}}
	srv := auth.start(t)
	ds := newOAuthTestDatasource("oauth-refresh-cancelled", srv.URL, srv.Client())
	ctx, cancel := context.WithCancel(oauthTestContext("alice"))
	session := &oauthSession{accessToken: "t1", refreshToken: "r1", expiry: time.Now()}
	ds.oauthSessions.putSession(oauthUser(ctx), session)
	cancel()

	config, err := models.LoadPluginSettings(ds.settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.applyOAuthCredential(ctx, config); err != nil || config.Secrets.AuthToken != "t2" {
		t.Fatalf("expected refreshed token t2, got %q, err %v", config.Secrets.AuthToken, err)
	}
	if session.refreshToken != "r2" {
		t.Fatal("the rotated refresh token must be stored even though the query was cancelled")
	}
}

func TestOAuthModeNeverSendsASavedAPIKey(t *testing.T) {
	ds := newOAuthTestDatasource("oauth-stale-key", "https://unused.test", http.DefaultClient)
	ds.settings.DecryptedSecureJSONData["apiKey"] = "stale-api-key"

	config, err := models.LoadPluginSettings(ds.settings)
	if err != nil {
		t.Fatal(err)
	}
	if config.Secrets.AuthToken != "" {
		t.Fatalf("OAuth mode must not load the saved API key, got %q", config.Secrets.AuthToken)
	}
}

func TestOAuthQueryWithoutSignInIsUnauthorized(t *testing.T) {
	ds := newOAuthTestDatasource("oauth-no-session", "https://unused.test", http.DefaultClient)
	resp, err := ds.QueryData(oauthTestContext("alice"), &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{DataSourceInstanceSettings: &ds.settings},
		Queries:       []backend.DataQuery{{RefID: "A"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.Responses["A"]
	if got.Status != backend.StatusUnauthorized || !strings.Contains(got.Error.Error(), "sign in") {
		t.Fatalf("expected a sign-in error, got %d %v", got.Status, got.Error)
	}
	if got.ErrorSource != backend.ErrorSourceDownstream {
		t.Fatalf("sign-in required is not a plugin error, got source %q", got.ErrorSource)
	}
}

func TestOAuthSessionsAreScopedToTheGrafanaOrg(t *testing.T) {
	ds := newOAuthTestDatasource("oauth-orgs", "https://unused.test", http.DefaultClient)
	org1 := oauthTestContext("alice")
	org2 := withGrafanaUser(context.Background(), grafanaIDToken("org:2", "alice"))
	ds.oauthSessions.putSession(oauthUser(org1), &oauthSession{accessToken: "t1", expiry: time.Now().Add(time.Hour)})

	config, err := models.LoadPluginSettings(ds.settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.applyOAuthCredential(org1, config); err != nil || config.Secrets.AuthToken != "t1" {
		t.Fatalf("alice in org 1 should get her token, got %q, err %v", config.Secrets.AuthToken, err)
	}
	if err := ds.applyOAuthCredential(org2, config); err != errSignInRequired {
		t.Fatalf("alice in org 2 has not signed in, got %v", err)
	}
}

// oauthCacheRequests are a Grafana user's requests with a Nominal token: alice's refreshed token reuses her
// entry, and every other request is a new Grafana user and Nominal user pair.
type oauthCacheRequest struct{ grafanaUser, token string }

var oauthCacheRequests = []oauthCacheRequest{
	{"grafana-a", nominalToken("alice", 1)},
	{"grafana-a", nominalToken("bob", 1)},
	{"grafana-a", nominalToken("alice", 2)},
	{"grafana-b", nominalToken("alice", 1)},
}

func TestOAuthDefaultWorkspaceIsCachedPerGrafanaAndNominalUser(t *testing.T) {
	service := &mockWorkspaceService{defaultWorkspace: &workspaceapi.Workspace{Rid: sqlTestWorkspaceRid(t)}}
	ds := &Datasource{workspaceService: service, defaultSQLWorkspace: newTTLCache[string](time.Minute), usesOAuth: true}
	requests := append(slices.Clone(oauthCacheRequests), oauthCacheRequest{"grafana-a", "not-a-jwt"}, oauthCacheRequest{"grafana-a", "also-not-a-jwt"})
	for _, r := range requests {
		if _, err := ds.resolveSQLWorkspace(oauthTestContext(r.grafanaUser), bearertoken.Token(r.token)); err != nil {
			t.Fatal(err)
		}
	}
	if service.defaultCalls != 5 {
		t.Errorf("GetDefaultWorkspace calls = %d, want one per Grafana and Nominal user pair and one per token without a user", service.defaultCalls)
	}
}

func TestOAuthCatalogCachesArePerGrafanaAndNominalUser(t *testing.T) {
	assetRid := "ri.scout.main.asset.shared"
	dataSourceRid := "ri.scout.main.data-source.dataset1"
	server, assetFetches := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {Rid: assetRid, DataScopes: []AssetDataScope{
			{DataScopeName: "scope-a", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
		}},
	}, nil)
	defer server.Close()
	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{searchChannelsResponse: datasourceapi.SearchChannelsResponse{
		Results: []datasourceapi.ChannelMetadata{{
			Name:       api.Channel("state"),
			DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "dataset1")),
			DataType:   &stringType,
		}},
	}}
	catalog := newNominalCatalog(server.Client(), mockDS)

	for _, r := range oauthCacheRequests {
		config := &models.PluginSettings{BaseUrl: server.URL, AuthMode: "oauth", Secrets: &models.SecretPluginSettings{AuthToken: r.token}}
		qm := NominalQueryModel{AssetRid: assetRid, DataScopeName: "scope-a", Channel: "state"}
		catalog.InferChannelMetadata(oauthTestContext(r.grafanaUser), config, &qm)
		if qm.ChannelDataType != ChannelDataTypeString {
			t.Fatalf("ChannelDataType = %q, want %q", qm.ChannelDataType, ChannelDataTypeString)
		}
	}
	if got := assetFetches.Load(); got != 3 {
		t.Errorf("asset fetches = %d, want one per Grafana and Nominal user pair", got)
	}
	if got := mockDS.searchChannelsCallCount(); got != 3 {
		t.Errorf("SearchChannels calls = %d, want one per Grafana and Nominal user pair", got)
	}
}

func TestOAuthOriginIsTheNominalAPIOrigin(t *testing.T) {
	for baseURL, want := range map[string]string{
		"https://api.nominal.test/api": "https://api.nominal.test",
		"":                             "https://api.gov.nominal.io",
		"http://api.nominal.test/api":  "",
		"javascript:alert(1)":          "",
		"https:///api":                 "",
	} {
		got, err := oauthOrigin(&models.PluginSettings{BaseUrl: baseURL})
		if got != want || (err != nil) != (want == "") {
			t.Errorf("oauthOrigin(%q) = %q, %v; want %q", baseURL, got, err, want)
		}
	}
}

func TestOAuthSessionsAreNotInheritedByARecreatedUser(t *testing.T) {
	ds := newOAuthTestDatasource("oauth-recreated", "https://unused.test", http.DefaultClient)
	// A recreated user keeps the login, email and numeric ID but gets a new UID.
	ds.oauthSessions.putSession(oauthUser(oauthTestContext("alice-uid")), &oauthSession{accessToken: "t1", expiry: time.Now().Add(time.Hour)})

	config, err := models.LoadPluginSettings(ds.settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.applyOAuthCredential(oauthTestContext("recreated-alice-uid"), config); err != errSignInRequired {
		t.Fatalf("a recreated user must not get alice's token, got %v", err)
	}
}

func TestOAuthUserComesFromTheForwardedIDToken(t *testing.T) {
	ds := newOAuthTestDatasource("oauth-id-token", "https://unused.test", http.DefaultClient)
	ds.oauthSessions.putSession(oauthUser(oauthTestContext("alice")), &oauthSession{accessToken: "t1", expiry: time.Now().Add(time.Hour)})
	status := func(idToken string) bool {
		resp := callResourceWithContext(t, context.Background(), ds, &backend.CallResourceRequest{
			Path: "oauth/status", Method: http.MethodPost, Headers: map[string][]string{backend.GrafanaUserSignInTokenHeaderName: {idToken}},
		})
		var body struct {
			SignedIn bool `json:"signedIn"`
		}
		if err := json.Unmarshal(resp.Body, &body); err != nil {
			t.Fatal(err)
		}
		return body.SignedIn
	}

	if !status(grafanaIDToken("org:1", "alice")) {
		t.Fatal("alice's forwarded ID token should find her sign-in")
	}
	for _, idToken := range []string{"", "not-a-jwt", grafanaIDToken("org:1", "")} {
		if status(idToken) {
			t.Fatalf("ID token %q must not identify alice", idToken)
		}
	}
}

func TestOAuthSettingsChangeSignsUsersOut(t *testing.T) {
	ctx := oauthTestContext("alice")
	newInstance := func(workspaceRid string) (*Datasource, *models.PluginSettings) {
		jsonData, _ := json.Marshal(map[string]string{"authMode": "oauth", "oauthClientId": "c", "baseUrl": "https://api.nominal.test/api", "workspaceRid": workspaceRid})
		settings := backend.DataSourceInstanceSettings{UID: "oauth-settings", JSONData: jsonData}
		instance, err := NewDatasource(context.Background(), settings)
		if err != nil {
			t.Fatal(err)
		}
		ds := instance.(*Datasource)
		t.Cleanup(ds.Dispose)
		config, err := models.LoadPluginSettings(settings)
		if err != nil {
			t.Fatal(err)
		}
		return ds, config
	}

	before, config := newInstance("")
	before.oauthSessions.putSession(oauthUser(ctx), &oauthSession{accessToken: "t1", expiry: time.Now().Add(time.Hour)})
	if err := before.applyOAuthCredential(ctx, config); err != nil {
		t.Fatalf("alice should be signed in before the change, got %v", err)
	}

	// Grafana builds a new instance when the data source settings are saved.
	after, config := newInstance(sqlTestWorkspaceRid(t).String())
	if err := after.applyOAuthCredential(ctx, config); err != errSignInRequired {
		t.Fatalf("a settings change should sign alice out, got %v", err)
	}
}

func TestOAuthHealthCheckWithoutSignInChecksTheClient(t *testing.T) {
	auth := &fakeNominalAuth{respond: func(form url.Values) (int, string) {
		if form.Get("client_id") != "test-client" || form.Get("client_secret") != "test-secret" {
			return http.StatusUnauthorized, `{"error":"invalid_client"}`
		}
		return http.StatusBadRequest, `{"error":"invalid_grant"}`
	}}
	srv := auth.start(t)
	ds := newOAuthTestDatasource("oauth-health", srv.URL, srv.Client())
	checkHealth := func() *backend.CheckHealthResult {
		result, err := ds.CheckHealth(oauthTestContext("admin"), &backend.CheckHealthRequest{
			PluginContext: backend.PluginContext{DataSourceInstanceSettings: &ds.settings},
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	if result := checkHealth(); result.Status != backend.HealthStatusOk {
		t.Fatalf("a valid client should pass without a sign-in, got %v: %s", result.Status, result.Message)
	}
	if form := auth.forms[0]; form.Get("grant_type") != "refresh_token" {
		t.Fatalf("the check must not redeem an authorization code, sent %v", form)
	}

	ds.settings.DecryptedSecureJSONData = map[string]string{"oauthClientSecret": "wrong"}
	if result := checkHealth(); result.Status != backend.HealthStatusError || !strings.Contains(result.Message, "Client Secret") {
		t.Fatalf("a wrong secret should fail the check, got %v: %s", result.Status, result.Message)
	}
}
