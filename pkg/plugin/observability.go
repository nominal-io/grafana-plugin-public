package plugin

import (
	"context"
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

func errorFieldsFromConjure(err error) []any {
	var cErr conjureerrors.Error
	if !errors.As(err, &cErr) {
		return nil
	}
	return []any{
		"error_instance_id", cErr.InstanceID().String(),
		"error_code", cErr.Code().String(),
		"error_name", cErr.Name(),
	}
}

// appendInstanceID returns msg with " (errorInstanceId: <id>)" appended when
// err carries a Conjure error instance ID. Returns msg unchanged otherwise.
// Used for user-facing error strings so support reps can grep screenshots for
// the trace ID by label. Use this only when msg does not already include the
// underlying error's stringified form — Conjure's Error() trails with an
// unlabeled (id), which would duplicate. Use formatUserError instead when
// interpolating err itself.
func appendInstanceID(msg string, err error) string {
	var cErr conjureerrors.Error
	if !errors.As(err, &cErr) {
		return msg
	}
	return fmt.Sprintf("%s (errorInstanceId: %s)", msg, cErr.InstanceID())
}

// formatUserError builds a "<prefix>: <details>" message with a labeled
// trace ID for Conjure errors, avoiding the duplicate ID that "%v" on a
// Conjure error would produce. Falls back to fmt.Sprintf("%s: %v", ...) for
// non-Conjure errors.
func formatUserError(prefix string, err error) string {
	var cErr conjureerrors.Error
	if errors.As(err, &cErr) {
		return fmt.Sprintf("%s: %s %s (errorInstanceId: %s)", prefix, cErr.Code(), cErr.Name(), cErr.InstanceID())
	}
	return fmt.Sprintf("%s: %v", prefix, err)
}

func userAgentMiddleware() conjurehttpclient.Middleware {
	return conjurehttpclient.MiddlewareFunc(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		return newUserAgentTransport(next).RoundTrip(req)
	})
}
