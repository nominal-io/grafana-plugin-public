package plugin

// Per-user Nominal sign-in through Nominal auth's OAuth endpoints.
// Tokens live in plugin memory, so sign-ins are lost on plugin restart and are not shared across
// Grafana replicas.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	sdkconfig "github.com/grafana/grafana-plugin-sdk-go/config"
	"github.com/nominal-io/grafana-plugin-public/pkg/models"
)

var errSignInRequired = errors.New("sign in to Nominal to query this data source")

type oauthSession struct {
	mu           sync.Mutex
	accessToken  string
	refreshToken string
	expiry       time.Time
}

func (s *oauthSession) updateAccessToken(tokens *nominalAuthTokenResponse) {
	s.accessToken = tokens.AccessToken
	if tokens.RefreshToken != "" {
		s.refreshToken = tokens.RefreshToken
	}
	s.expiry = time.Now().Add(time.Duration(tokens.ExpiresIn) * time.Second)
}

type oauthPendingLogin struct {
	verifier    string
	redirectURI string
	user        string
	expiry      time.Time
}

// sessionStore holds one data source instance's sign-ins. Grafana rebuilds the instance on a settings
// change, which signs every user out.
type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]*oauthSession
	pending  map[string]oauthPendingLogin
}

func newSessionStore() *sessionStore {
	return &sessionStore{sessions: map[string]*oauthSession{}, pending: map[string]oauthPendingLogin{}}
}

func (s *sessionStore) getSession(key string) *oauthSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[key]
}

func (s *sessionStore) putSession(key string, session *oauthSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[key] = session
}

func (s *sessionStore) deleteSession(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, key)
}

func (s *sessionStore) addPendingLogin(state string, pending oauthPendingLogin) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, p := range s.pending {
		if now.After(p.expiry) {
			delete(s.pending, k)
		}
	}
	s.pending[state] = pending
}

// popPendingLogin returns and removes the login started with state, so each state is single use.
func (s *sessionStore) popPendingLogin(state string) (oauthPendingLogin, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, ok := s.pending[state]
	delete(s.pending, state)
	if !ok || time.Now().After(pending.expiry) {
		return oauthPendingLogin{}, false
	}
	return pending, true
}

// oauthUser identifies the signed-in Grafana user by org, login and email, so a renamed or reused
// login does not inherit another user's sign-in.
func oauthUser(ctx context.Context) string {
	if user := backend.UserFromContext(ctx); user != nil && user.Login != "" {
		return strconv.FormatInt(backend.PluginConfigFromContext(ctx).OrgID, 10) + "\x00" + user.Login + "\x00" + user.Email
	}
	return ""
}

func oauthCacheKey(ctx context.Context, usesOAuth bool, key string) string {
	if !usesOAuth {
		return key
	}
	return oauthUser(ctx) + "\x00" + key
}

func (d *Datasource) applyOAuthCredential(ctx context.Context, config *models.PluginSettings) error {
	if !config.UsesOAuth() {
		return nil
	}
	token, err := d.getAccessToken(ctx, config)
	if err != nil {
		return err
	}
	config.Secrets.ApiKey = token
	return nil
}

func (d *Datasource) getAccessToken(ctx context.Context, config *models.PluginSettings) (string, error) {
	user := oauthUser(ctx)
	if user == "" {
		return "", errSignInRequired
	}
	session := d.oauthSessions.getSession(user)
	if session == nil {
		return "", errSignInRequired
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if time.Until(session.expiry) > time.Minute {
		return session.accessToken, nil
	}
	if session.refreshToken == "" {
		return "", errSignInRequired
	}
	tokens, err := d.nominalAuthToken(ctx, config, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {session.refreshToken},
		"client_id":     {config.OAuthClientID},
	})
	if err != nil {
		log.DefaultLogger.FromContext(ctx).Warn("Nominal token refresh failed", "error", err)
		return "", errSignInRequired
	}
	session.updateAccessToken(tokens)
	return session.accessToken, nil
}

type nominalAuthTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

func oauthOrigin(config *models.PluginSettings) (string, error) {
	baseURL := config.GetAPIBaseURL()
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return "", errors.New("OAuth sign-in needs an https Nominal API URL")
	}
	return parsed.Scheme + "://" + parsed.Host, nil
}

// nominalAuthToken calls /oauth/v1/token with either grant.
func (d *Datasource) nominalAuthToken(ctx context.Context, config *models.PluginSettings, form url.Values) (*nominalAuthTokenResponse, error) {
	origin, err := oauthOrigin(config)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+"/oauth/v1/token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := d.resourceHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Nominal auth token endpoint returned %d: %s", resp.StatusCode, body)
	}
	var tokens nominalAuthTokenResponse
	if err := json.Unmarshal(body, &tokens); err != nil {
		return nil, fmt.Errorf("could not parse Nominal auth token response: %w", err)
	}
	if tokens.AccessToken == "" {
		return nil, errors.New("Nominal auth token response has no access_token")
	}
	return &tokens, nil
}

func buildRedirectURI(ctx context.Context, datasourceUID string) (string, error) {
	cfg := sdkconfig.GrafanaConfigFromContext(ctx)
	if cfg == nil {
		return "", errors.New("grafana config is unavailable")
	}
	appURL, err := cfg.AppURL()
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(appURL, "/") + "/api/datasources/uid/" + url.PathEscape(datasourceUID) + "/resources/oauth/callback", nil
}

func randomURLToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func textResponse(sender backend.CallResourceResponseSender, status int, message string) error {
	return sender.Send(&backend.CallResourceResponse{
		Status:  status,
		Headers: map[string][]string{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:    []byte(message),
	})
}

// handleOAuthStart builds the /oauth/v1/authorize URL and params for the UI to open.
func (h *NominalResourceHandler) handleOAuthStart(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	config, err := models.LoadPluginSettings(h.datasource.settings)
	if err != nil || !config.UsesOAuth() || config.OAuthClientID == "" {
		return jsonErrorResponse(sender, http.StatusBadRequest, "OAuth sign-in is not configured for this data source")
	}
	origin, err := oauthOrigin(config)
	if err != nil {
		return jsonErrorResponse(sender, http.StatusBadRequest, err.Error())
	}
	user := oauthUser(ctx)
	if user == "" {
		return jsonErrorResponse(sender, http.StatusUnauthorized, "No signed-in Grafana user")
	}
	redirectURI, err := buildRedirectURI(ctx, h.datasource.settings.UID)
	if err != nil {
		log.DefaultLogger.FromContext(ctx).Error("Could not build OAuth redirect URI", "error", err)
		return jsonErrorResponse(sender, http.StatusInternalServerError, "Could not build the sign-in redirect URI")
	}

	state, verifier := randomURLToken(), randomURLToken()
	challenge := sha256.Sum256([]byte(verifier))
	h.datasource.oauthSessions.addPendingLogin(state, oauthPendingLogin{
		verifier:    verifier,
		redirectURI: redirectURI,
		user:        user,
		expiry:      time.Now().Add(10 * time.Minute),
	})
	authorizeURL := origin + "/oauth/v1/authorize?" + url.Values{
		"client_id":             {config.OAuthClientID},
		"redirect_uri":          {redirectURI},
		"response_type":         {"code"},
		"state":                 {state},
		"code_challenge":        {base64.RawURLEncoding.EncodeToString(challenge[:])},
		"code_challenge_method": {"S256"},
	}.Encode()
	return jsonMarshalResponse(sender, http.StatusOK, map[string]string{"authorizeUrl": authorizeURL})
}

// handleOAuthCallback handles the redirect back from Nominal after the user signs in to the IdP.
func (h *NominalResourceHandler) handleOAuthCallback(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if req.Method != http.MethodGet {
		return textResponse(sender, http.StatusMethodNotAllowed, "Method not allowed")
	}
	var params url.Values
	if parsed, err := url.Parse(req.URL); err == nil {
		params = parsed.Query()
	}
	user := oauthUser(ctx)
	pending, ok := h.datasource.oauthSessions.popPendingLogin(params.Get("state"))
	if !ok || user == "" || pending.user != user {
		return textResponse(sender, http.StatusBadRequest, "This sign-in expired or was started by a different Grafana user. Close this window and try again.")
	}
	if oauthErr := params.Get("error"); oauthErr != "" {
		return textResponse(sender, http.StatusBadRequest, "Nominal sign-in failed: "+oauthErr)
	}
	if params.Get("code") == "" {
		return textResponse(sender, http.StatusBadRequest, "Nominal sign-in returned no authorization code")
	}
	config, err := models.LoadPluginSettings(h.datasource.settings)
	if err != nil {
		return textResponse(sender, http.StatusInternalServerError, "Failed to load data source settings")
	}
	tokens, err := h.datasource.nominalAuthToken(ctx, config, url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {params.Get("code")},
		"redirect_uri":  {pending.redirectURI},
		"client_id":     {config.OAuthClientID},
		"code_verifier": {pending.verifier},
	})
	if err != nil {
		log.DefaultLogger.FromContext(ctx).Error("Nominal authorization code exchange failed", "error", err)
		return textResponse(sender, http.StatusBadGateway, "Nominal sign-in failed while exchanging the authorization code. Close this window and try again.")
	}
	session := &oauthSession{}
	session.updateAccessToken(tokens)
	h.datasource.oauthSessions.putSession(user, session)
	return textResponse(sender, http.StatusOK, "Signed in to Nominal. You can close this window.")
}

func (h *NominalResourceHandler) handleOAuthStatus(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	config, err := models.LoadPluginSettings(h.datasource.settings)
	if err != nil {
		return jsonErrorResponse(sender, http.StatusInternalServerError, "Failed to load settings")
	}
	signedIn := !config.UsesOAuth()
	if !signedIn {
		_, tokenErr := h.datasource.getAccessToken(ctx, config)
		signedIn = tokenErr == nil
	}
	return jsonMarshalResponse(sender, http.StatusOK, map[string]bool{"signedIn": signedIn})
}

func (h *NominalResourceHandler) handleOAuthLogout(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	if ok, err := requirePost(req, sender); !ok {
		return err
	}
	if user := oauthUser(ctx); user != "" {
		h.datasource.oauthSessions.deleteSession(user)
	}
	return jsonMarshalResponse(sender, http.StatusOK, map[string]bool{"signedIn": false})
}
