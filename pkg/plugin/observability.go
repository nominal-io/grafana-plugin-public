package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	conjureerrors "github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/errors"
)

type userAgentComponents struct {
	PluginVersion  string
	GoOS           string
	GoArch         string
	GoVersion      string
	GrafanaVersion string
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
	return c
}

func formatUserAgent(c userAgentComponents) string {
	goVer := strings.TrimPrefix(c.GoVersion, "go")
	return fmt.Sprintf("nominal-grafana/%s (%s-%s) go/%s grafana/%s",
		c.PluginVersion, c.GoOS, c.GoArch, goVer, c.GrafanaVersion)
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

// contextWithPluginRequestIdentity decorates ctx with the User-Agent
// components derived from a Grafana PluginContext, so any downstream HTTP
// client (Conjure middleware or raw transport) carries identifying headers.
// Every Grafana entry point — QueryData, CheckHealth, CallResource — should
// call this at the top of its handler. New entry points that skip it will
// silently fall back to the "unknown" UA, which makes outbound traffic
// indistinguishable from a misconfigured caller; tests in observability_test.go
// guard against that regression for the three known entry points.
func contextWithPluginRequestIdentity(ctx context.Context, pc backend.PluginContext) context.Context {
	return contextWithUserAgentComponents(ctx, userAgentComponentsFromPluginContext(pc))
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

// errorDetails carries the Conjure classification triple extracted from an
// error, regardless of whether it arrived via the generated Conjure client
// (typed conjureerrors.Error) or one of the raw-HTTP fetchers (typed
// *apiError). All user-facing and log-side helpers below project from this
// single representation, so the {instance, code, name} vocabulary lives in
// one place.
type errorDetails struct {
	InstanceID string
	Code       string
	Name       string
}

func (d errorDetails) empty() bool {
	return d.InstanceID == "" && d.Code == "" && d.Name == ""
}

// extractErrorDetails parses err into its Conjure classification triple,
// returning the zero value when err carries no Nominal classification (e.g.
// transport-level failures or plain errors).
func extractErrorDetails(err error) errorDetails {
	if err == nil {
		return errorDetails{}
	}
	var cErr conjureerrors.Error
	if errors.As(err, &cErr) {
		return errorDetails{
			InstanceID: cErr.InstanceID().String(),
			Code:       cErr.Code().String(),
			Name:       cErr.Name(),
		}
	}
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return errorDetails{
			InstanceID: apiErr.InstanceID,
			Code:       apiErr.ErrorCode,
			Name:       apiErr.ErrorName,
		}
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
	return []any{
		"error_instance_id", d.InstanceID,
		"error_code", d.Code,
		"error_name", d.Name,
	}
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

// classifyConnectionError categorizes a connect-time failure and returns the
// user-facing message (with errorInstanceId labeled when present) plus the
// HTTP status code to surface from CallResource. CheckHealth uses only the
// message — it always returns HealthStatusError — but the same classification
// keeps the wording consistent across both surfaces.
//
// Recognized buckets:
//   - 401 / "unauthorized"                          -> 401 + auth message
//   - "timeout" / "context deadline exceeded"       -> 408 + timeout message
//   - "connection refused" / "no such host"         -> 502 + connectivity message
//   - anything else                                 -> 503 + generic message
func classifyConnectionError(err error) (message string, httpStatus int) {
	msg := "Failed to connect to Nominal API"
	status := http.StatusServiceUnavailable

	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "401") || strings.Contains(errStr, "unauthorized"):
		msg = "Invalid API key - authentication failed"
		status = http.StatusUnauthorized
	case strings.Contains(errStr, "timeout") || strings.Contains(errStr, "context deadline exceeded"):
		msg = "Connection timeout - unable to reach Nominal API"
		status = http.StatusRequestTimeout
	case strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "no such host"):
		msg = "Unable to connect to Nominal API - check base URL"
		status = http.StatusBadGateway
	}

	return appendInstanceID(msg, err), status
}

// formatUserError builds a "<prefix>: <details>" message with a labeled
// trace ID for any error carrying the Conjure classification triple (typed
// Conjure errors or raw-HTTP *apiError), avoiding the duplicate ID that "%v"
// on a Conjure error would produce. Falls back to fmt.Sprintf("%s: %v", ...)
// for transport-level and other unclassified errors.
func formatUserError(prefix string, err error) string {
	d := extractErrorDetails(err)
	if d.empty() {
		return fmt.Sprintf("%s: %v", prefix, err)
	}
	return fmt.Sprintf("%s: %s %s (errorInstanceId: %s)", prefix, d.Code, d.Name, d.InstanceID)
}

func userAgentMiddleware() conjurehttpclient.Middleware {
	return conjurehttpclient.MiddlewareFunc(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		return newUserAgentTransport(next).RoundTrip(req)
	})
}
