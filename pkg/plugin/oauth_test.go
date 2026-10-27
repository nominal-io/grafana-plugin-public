package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	"github.com/palantir/pkg/rid"
)

func oauthTestContext(login string) context.Context {
	ctx := backend.WithUser(context.Background(), &backend.User{Login: login})
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
		settings:           backend.DataSourceInstanceSettings{UID: uid, JSONData: jsonData},
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
	if form.Get("grant_type") != "authorization_code" || form.Get("code") != "code-1" || form.Get("redirect_uri") != wantRedirect {
		t.Fatalf("unexpected token form: %v", form)
	}

	config, ok, _ := ds.loadResourceSettings(alice, backend.CallResourceResponseSenderFunc(func(*backend.CallResourceResponse) error { return nil }), "test")
	if !ok || config.Secrets.ApiKey != "alice-nominal-token" {
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
	if err := ds.applyOAuthCredential(ctx, config); err != nil || config.Secrets.ApiKey != "t2" {
		t.Fatalf("expected refreshed token t2, got %q, err %v", config.Secrets.ApiKey, err)
	}
	if session.refreshToken != "r1" {
		t.Fatal("refresh token must be kept when Nominal auth does not rotate it")
	}

	session.refreshToken, session.expiry = "revoked", time.Now()
	if err := ds.applyOAuthCredential(ctx, config); err != errSignInRequired {
		t.Fatalf("failed refresh should require sign-in, got %v", err)
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
}

func TestOAuthSessionsAreScopedToTheGrafanaOrg(t *testing.T) {
	ds := newOAuthTestDatasource("oauth-orgs", "https://unused.test", http.DefaultClient)
	org1 := backend.WithPluginContext(oauthTestContext("alice"), backend.PluginContext{OrgID: 1})
	org2 := backend.WithPluginContext(oauthTestContext("alice"), backend.PluginContext{OrgID: 2})
	ds.oauthSessions.putSession(oauthUser(org1), &oauthSession{accessToken: "t1", expiry: time.Now().Add(time.Hour)})

	config, err := models.LoadPluginSettings(ds.settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.applyOAuthCredential(org1, config); err != nil || config.Secrets.ApiKey != "t1" {
		t.Fatalf("alice in org 1 should get her token, got %q, err %v", config.Secrets.ApiKey, err)
	}
	if err := ds.applyOAuthCredential(org2, config); err != errSignInRequired {
		t.Fatalf("alice in org 2 has not signed in, got %v", err)
	}
}

func TestOAuthDefaultWorkspaceIsCachedPerUser(t *testing.T) {
	service := &mockWorkspaceService{defaultWorkspace: &workspaceapi.Workspace{Rid: sqlTestWorkspaceRid(t)}}
	ds := &Datasource{workspaceService: service, defaultSQLWorkspace: newTTLCache[string](time.Minute), usesOAuth: true}
	for _, login := range []string{"alice", "bob", "alice"} {
		if _, err := ds.resolveSQLWorkspace(oauthTestContext(login), "k"); err != nil {
			t.Fatal(err)
		}
	}
	if service.defaultCalls != 2 {
		t.Errorf("GetDefaultWorkspace calls = %d, want one per user", service.defaultCalls)
	}
}

func TestOAuthCatalogCachesArePerUser(t *testing.T) {
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
	config := &models.PluginSettings{BaseUrl: server.URL, AuthMode: "oauth", Secrets: &models.SecretPluginSettings{ApiKey: "t"}}
	catalog := newNominalCatalog(server.Client(), mockDS)

	for _, login := range []string{"alice", "bob", "alice"} {
		qm := NominalQueryModel{AssetRid: assetRid, DataScopeName: "scope-a", Channel: "state"}
		catalog.InferChannelMetadata(oauthTestContext(login), config, &qm)
		if qm.ChannelDataType != ChannelDataTypeString {
			t.Fatalf("%s ChannelDataType = %q, want %q", login, qm.ChannelDataType, ChannelDataTypeString)
		}
	}
	if got := assetFetches.Load(); got != 2 {
		t.Errorf("asset fetches = %d, want one per user", got)
	}
	if got := mockDS.searchChannelsCallCount(); got != 2 {
		t.Errorf("SearchChannels calls = %d, want one per user", got)
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

func TestOAuthSessionsAreBoundToTheUsersEmail(t *testing.T) {
	ds := newOAuthTestDatasource("oauth-email", "https://unused.test", http.DefaultClient)
	alice := backend.WithUser(context.Background(), &backend.User{Login: "alice", Email: "alice@nominal.io"})
	reused := backend.WithUser(context.Background(), &backend.User{Login: "alice", Email: "someone-else@nominal.io"})
	ds.oauthSessions.putSession(oauthUser(alice), &oauthSession{accessToken: "t1", expiry: time.Now().Add(time.Hour)})

	config, err := models.LoadPluginSettings(ds.settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.applyOAuthCredential(reused, config); err != errSignInRequired {
		t.Fatalf("a reused login with another email must not get alice's token, got %v", err)
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
