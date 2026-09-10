package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	conjureerrors "github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/errors"
	"github.com/palantir/pkg/bearertoken"
)

type userAgentComponents struct {
	PluginVersion  string
	GoOS           string
	GoArch         string
	GoVersion      string
	GrafanaVersion string

	// Optional usage context, rendered as key/value tokens when set.
	DatasourceUID string
	OrgRid        string // Nominal org behind the API key
	RequestKind   string // query, alert, health, or resource-<path>
	DashboardUID  string
}

const unknownComponent = "unknown"

func userAgentComponentsFromPluginContext(pc backend.PluginContext) userAgentComponents {
	c := userAgentComponents{
		PluginVersion:  pc.PluginVersion,
		GoOS:           runtime.GOOS,
		GoArch:         runtime.GOARCH,
		GoVersion:      runtime.Version(),
		GrafanaVersion: unknownComponent,
	}
	if c.PluginVersion == "" {
		c.PluginVersion = unknownComponent
	}
	if pc.UserAgent != nil {
		if v := pc.UserAgent.GrafanaVersion(); v != "" {
			c.GrafanaVersion = v
		}
	}
	if pc.DataSourceInstanceSettings != nil {
		c.DatasourceUID = pc.DataSourceInstanceSettings.UID
	}
	return c
}

// formatUserAgent keeps the fixed "nominal-grafana/..." prefix so existing log
// filters keep working, then appends a key/value token per set optional field.
func formatUserAgent(c userAgentComponents) string {
	goVer := strings.TrimPrefix(c.GoVersion, "go")
	var b strings.Builder
	fmt.Fprintf(&b, "nominal-grafana/%s (%s-%s) go/%s grafana/%s",
		c.PluginVersion, c.GoOS, c.GoArch, goVer, c.GrafanaVersion)
	for _, kv := range [][2]string{
		{"ds", c.DatasourceUID}, {"org", c.OrgRid},
		{"req", c.RequestKind}, {"dash", c.DashboardUID},
	} {
		if kv[1] != "" {
			// Header and JSON values must not split into extra tokens.
			b.WriteString(" " + kv[0] + "/" + strings.Join(strings.Fields(kv[1]), ""))
		}
	}
	return b.String()
}

// fallbackUserAgentString is computed once at package init — the values feeding
// it (runtime build info) cannot change at runtime, and the fallback path is
// taken on every request whose context lacks UA components.
var fallbackUserAgentString = formatUserAgent(userAgentComponents{
	PluginVersion:  unknownComponent,
	GoOS:           runtime.GOOS,
	GoArch:         runtime.GOARCH,
	GoVersion:      runtime.Version(),
	GrafanaVersion: unknownComponent,
})

type uaContextKey struct{}

func contextWithUserAgentComponents(ctx context.Context, c userAgentComponents) context.Context {
	return context.WithValue(ctx, uaContextKey{}, c)
}

func userAgentComponentsFromContext(ctx context.Context) (userAgentComponents, bool) {
	c, ok := ctx.Value(uaContextKey{}).(userAgentComponents)
	return c, ok
}

// contextWithRequestIdentity adds the Nominal org to c and stores it in ctx so
// every downstream HTTP client carries the full User-Agent. Each entry point
// (QueryData, CheckHealth, CallResource) must call it first; one that does not
// falls back to the "unknown" UA, which observability_test.go guards against.
func (d *Datasource) contextWithRequestIdentity(ctx context.Context, c userAgentComponents) context.Context {
	// The org lookup is itself an outbound call, so it carries the rest of the UA.
	c.OrgRid = d.resolveOrgRid(contextWithUserAgentComponents(ctx, c))
	return contextWithUserAgentComponents(ctx, c)
}

// resolveOrgRid looks up the org behind the API key once per instance. A failed
// lookup is retried on the next request.
func (d *Datasource) resolveOrgRid(ctx context.Context) string {
	apiKey := d.settings.DecryptedSecureJSONData["apiKey"]
	if d.authService == nil || apiKey == "" {
		return ""
	}
	d.orgRidMu.Lock()
	defer d.orgRidMu.Unlock()
	if d.orgRid == "" {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		profile, err := d.authService.GetMyProfile(ctx, bearertoken.Token(apiKey))
		if err != nil {
			log.DefaultLogger.Debug("Org lookup failed; User-Agent omits org", "error", err)
			return ""
		}
		d.orgRid = profile.OrgRid.String()
	}
	return d.orgRid
}

type userAgentTransport struct {
	next http.RoundTripper
}

func newUserAgentTransport(next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return &userAgentTransport{next: next}
}

func (t *userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request before mutating headers — RoundTripper contract.
	r := req.Clone(req.Context())
	ua := fallbackUserAgentString
	if c, ok := userAgentComponentsFromContext(r.Context()); ok {
		ua = formatUserAgent(c)
	}
	r.Header.Set("User-Agent", ua)
	return t.next.RoundTrip(r)
}

// errorDetails is the unified projection of an error's Nominal classification,
// extracted from either a typed conjureerrors.Error or a raw-HTTP *apiError.
// Status is 0 for transport-level failures that never received an HTTP response.
type errorDetails struct {
	Status     int
	InstanceID string
	Code       string
	Name       string
}

func (d errorDetails) empty() bool {
	return d.Status == 0 && d.InstanceID == "" && d.Code == "" && d.Name == ""
}

// extractErrorDetails returns the zero value when err carries no Nominal
// classification (transport-level failures or plain errors).
func extractErrorDetails(err error) errorDetails {
	if err == nil {
		return errorDetails{}
	}
	var cErr conjureerrors.Error
	if errors.As(err, &cErr) {
		return errorDetails{
			Status:     cErr.Code().StatusCode(),
			InstanceID: cErr.InstanceID().String(),
			Code:       cErr.Code().String(),
			Name:       cErr.Name(),
		}
	}
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return errorDetails{
			Status:     apiErr.Status,
			InstanceID: apiErr.InstanceID,
			Code:       apiErr.ErrorCode,
			Name:       apiErr.ErrorName,
		}
	}
	// Conjure wraps non-Conjure error responses (empty body, non-JSON, malformed
	// JSON) as a werror carrying a typed `statusCode` param rather than a
	// conjureerrors.Error — so a proxy-served 401 HTML page still classifies as
	// 401 here without reaching string matching.
	if status, ok := conjurehttpclient.StatusCodeFromError(err); ok {
		return errorDetails{Status: status}
	}
	return errorDetails{}
}

// logErrorWithConjureFields logs at error level with structured Conjure error
// taxonomy (instance ID, code, name) appended. extra is the caller's existing
// key/value fields, applied after the standard "error" + Conjure fields.
func logErrorWithConjureFields(msg string, err error, extra ...any) {
	fields := make([]any, 0, len(extra)+2+6)
	fields = append(fields, "error", err)
	fields = append(fields, errorFieldsFromConjure(err)...)
	fields = append(fields, extra...)
	log.DefaultLogger.Error(msg, fields...)
}

// errorFieldsFromConjure returns structured Conjure-error fields for either
// the typed generated-client path or the raw-HTTP path (via *apiError). Both
// emit the same triple {error_instance_id, error_code, error_name} so log
// consumers don't have to special-case by source.
func errorFieldsFromConjure(err error) []any {
	d := extractErrorDetails(err)
	if d.empty() {
		return nil
	}
	fields := make([]any, 0, 6)
	if d.InstanceID != "" {
		fields = append(fields, "error_instance_id", d.InstanceID)
	}
	if d.Code != "" {
		fields = append(fields, "error_code", d.Code)
	}
	if d.Name != "" {
		fields = append(fields, "error_name", d.Name)
	}
	return fields
}

// appendInstanceID returns msg with " (errorInstanceId: <id>)" appended when
// err carries one. Use only when msg does not already include err.Error() —
// Conjure errors trail with an unlabeled "(id)" that would duplicate. Use
// formatUserError instead when interpolating err itself.
func appendInstanceID(msg string, err error) string {
	id := extractErrorDetails(err).InstanceID
	if id == "" {
		return msg
	}
	return fmt.Sprintf("%s (errorInstanceId: %s)", msg, id)
}

// instanceIDFromError returns the Conjure errorInstanceId carried by err,
// reading from either the typed Conjure error or the raw-HTTP *apiError type.
// Returns "" when neither is present.
func instanceIDFromError(err error) string {
	return extractErrorDetails(err).InstanceID
}

// apiError is the typed error returned by the raw-HTTP fetchers. It carries
// only the Conjure classification triple (errorCode, errorName, errorInstanceId)
// — never the response body's free-form text or the `parameters` map, both of
// which can include user-supplied values. This is the structural fix that
// makes raw-HTTP error logging match the generated-client path without
// re-leaking body content.
type apiError struct {
	Status     int
	ErrorCode  string
	ErrorName  string
	InstanceID string
}

func (e *apiError) Error() string {
	switch {
	case e.InstanceID != "":
		return fmt.Sprintf("API returned status %d: %s %s (errorInstanceId: %s)",
			e.Status, e.ErrorCode, e.ErrorName, e.InstanceID)
	case e.ErrorCode != "" || e.ErrorName != "":
		return fmt.Sprintf("API returned status %d: %s %s", e.Status, e.ErrorCode, e.ErrorName)
	default:
		return fmt.Sprintf("API returned status %d", e.Status)
	}
}

// newAPIError parses a Conjure error body when present and returns an
// *apiError carrying status + classification fields. Body content beyond the
// three classification fields is deliberately discarded.
func newAPIError(status int, body []byte) *apiError {
	e := &apiError{Status: status}
	var parsed struct {
		ErrorCode       string `json:"errorCode"`
		ErrorName       string `json:"errorName"`
		ErrorInstanceID string `json:"errorInstanceId"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil {
		e.ErrorCode = parsed.ErrorCode
		e.ErrorName = parsed.ErrorName
		e.InstanceID = parsed.ErrorInstanceID
	}
	return e
}

// classifyConnectionError returns a user-facing message (with errorInstanceId
// labeled when present) and an HTTP status for a connect-time failure.
// HTTP-backed errors are classified by typed status; transport-level errors
// fall to string matching since net.Error variants don't expose a uniform
// typed surface.
func classifyConnectionError(err error) (message string, httpStatus int) {
	if d := extractErrorDetails(err); d.Status != 0 {
		if d.Status == http.StatusUnauthorized {
			return appendInstanceID("Invalid API key - authentication failed", err), http.StatusUnauthorized
		}
		return appendInstanceID("Failed to connect to Nominal API", err), http.StatusServiceUnavailable
	}

	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "timeout") || strings.Contains(errStr, "context deadline exceeded"):
		return appendInstanceID("Connection timeout - unable to reach Nominal API", err), http.StatusRequestTimeout
	case strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "no such host"):
		return appendInstanceID("Unable to connect to Nominal API - check base URL", err), http.StatusBadGateway
	}

	return appendInstanceID("Failed to connect to Nominal API", err), http.StatusServiceUnavailable
}

// formatUserError builds a "<prefix>: <details>" message with a labeled
// trace ID for any error carrying the Conjure classification triple (typed
// Conjure errors or raw-HTTP *apiError), avoiding the duplicate ID that "%v"
// on a Conjure error would produce. Falls back to fmt.Sprintf("%s: %v", ...)
// for transport-level and other unclassified errors.
func formatUserError(prefix string, err error) string {
	d := extractErrorDetails(err)
	switch {
	case d.Code != "" || d.Name != "" || d.InstanceID != "":
		return fmt.Sprintf("%s: %s %s (errorInstanceId: %s)", prefix, d.Code, d.Name, d.InstanceID)
	case d.Status != 0:
		return fmt.Sprintf("%s: API returned status %d", prefix, d.Status)
	default:
		return fmt.Sprintf("%s: %v", prefix, err)
	}
}

func userAgentMiddleware() conjurehttpclient.Middleware {
	return conjurehttpclient.MiddlewareFunc(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		return newUserAgentTransport(next).RoundTrip(req)
	})
}
