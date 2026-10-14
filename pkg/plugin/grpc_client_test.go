package plugin

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// newInMemoryConn serves the services register adds over an in-memory connection.
func newInMemoryConn(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
	})
	return conn
}

func TestDialNominalGRPC(t *testing.T) {
	for _, tc := range []struct {
		baseURL    string
		wantTarget string
	}{
		{baseURL: "https://api.gov.nominal.io/api", wantTarget: "api.gov.nominal.io"},
		{baseURL: "https://nominal.example.com:8443/api/", wantTarget: "nominal.example.com:8443"},
	} {
		conn, err := dialNominalGRPC(tc.baseURL, "test")
		if err != nil {
			t.Fatalf("dialNominalGRPC(%q) error = %v", tc.baseURL, err)
		}
		if got := conn.Target(); got != tc.wantTarget {
			t.Errorf("dialNominalGRPC(%q) target = %q, want %q", tc.baseURL, got, tc.wantTarget)
		}
		_ = conn.Close()
	}
	for _, baseURL := range []string{"", "api.gov.nominal.io", "http://admin:secret@nominal.internal/api", "https://%zz"} {
		if _, err := dialNominalGRPC(baseURL, "test"); err == nil || strings.Contains(err.Error(), "secret") {
			t.Errorf("dialNominalGRPC(%q) error = %v, want an error that does not repeat the URL", baseURL, err)
		}
	}
}
