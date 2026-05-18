package plugin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
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

func fallbackUserAgent() string {
	return formatUserAgent(userAgentComponents{
		PluginVersion:  unknownComponent,
		GoOS:           runtime.GOOS,
		GoArch:         runtime.GOARCH,
		GoVersion:      runtime.Version(),
		GrafanaVersion: unknownComponent,
	})
}

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
	ua := fallbackUserAgent()
	if c, ok := userAgentComponentsFromContext(r.Context()); ok {
		ua = formatUserAgent(c)
	}
	r.Header.Set("User-Agent", ua)
	return t.next.RoundTrip(r)
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

func userAgentMiddleware() conjurehttpclient.Middleware {
	return conjurehttpclient.MiddlewareFunc(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		return newUserAgentTransport(next).RoundTrip(req)
	})
}

func formatUserVisibleError(prefix string, err error) string {
	if err == nil {
		return prefix
	}
	var cErr conjureerrors.Error
	if errors.As(err, &cErr) {
		return fmt.Sprintf("%s: %v (errorInstanceId: %s)", prefix, err, cErr.InstanceID())
	}
	return fmt.Sprintf("%s: %v", prefix, err)
}
