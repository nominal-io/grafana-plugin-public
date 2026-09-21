package plugin

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type sqlQueryHandler func(*sqlv1.SqlServiceQueryRequest, grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error

type fakeSqlService struct {
	sqlv1.UnimplementedSqlServiceServer
	query sqlQueryHandler
}

func (f *fakeSqlService) Query(req *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
	return f.query(req, stream)
}

// newFakeSqlClient serves query over an in-memory gRPC connection and returns a client bound to it.
func newFakeSqlClient(t *testing.T, query sqlQueryHandler) *sqlClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	sqlv1.RegisterSqlServiceServer(server, &fakeSqlService{query: query})
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := newSqlClientFromConn(conn)
	t.Cleanup(func() {
		_ = client.Close()
		server.Stop()
		_ = listener.Close()
	})
	return client
}

// sqlPayloadServer streams payload as a single response message.
func sqlPayloadServer(payload []byte) sqlQueryHandler {
	return func(_ *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		return stream.Send(&sqlv1.SqlServiceQueryResponse{QueryId: "q", Payload: payload})
	}
}

// sqlStatusError mirrors scout's SqlErrors: a generic status message plus an ErrorInfo whose
// metadata carries sqlQueryId and, optionally, a specific detail.
func sqlStatusError(code codes.Code, message, reason, queryId, detail string) error {
	st := status.New(code, message)
	if reason != "" {
		md := map[string]string{"sqlQueryId": queryId}
		if detail != "" {
			md["detail"] = detail
		}
		var err error
		st, err = st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "nominal.sql.v1", Metadata: md})
		if err != nil {
			panic(err)
		}
	}
	return st.Err()
}

func TestSqlClientQueryContract(t *testing.T) {
	var got *sqlv1.SqlServiceQueryRequest
	var authorization []string
	client := newFakeSqlClient(t, func(req *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		got = req
		md, _ := metadata.FromIncomingContext(stream.Context())
		authorization = md.Get("authorization")
		for _, chunk := range []string{"str", "eam"} {
			if err := stream.Send(&sqlv1.SqlServiceQueryResponse{QueryId: "q", Payload: []byte(chunk)}); err != nil {
				return err
			}
		}
		return nil
	})
	r, err := client.Query(context.Background(), "token", "workspace", "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil || string(body) != "stream" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	if got.GetQuery() != "SELECT 1" || got.GetWorkspaceRid() != "workspace" || got.GetResultFormat() != sqlv1.SqlServiceQueryResultFormat_SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM {
		t.Fatalf("request=%v", got)
	}
	if got.MaxRows != nil {
		t.Fatal("max_rows sent")
	}
	if len(authorization) != 1 || authorization[0] != "Bearer token" {
		t.Fatalf("authorization=%v", authorization)
	}
}

func TestSqlClientEndpointErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want backend.Status
	}{
		{sqlStatusError(codes.InvalidArgument, "SQL query is invalid", "SQL_ERROR_INVALID_QUERY", "q", "bad query"), backend.StatusBadRequest},
		{sqlStatusError(codes.Internal, "boom", "", "", ""), backend.StatusInternal},
		{sqlStatusError(codes.ResourceExhausted, "", "SQL_ERROR_RESOURCE_EXHAUSTED", "", ""), backend.StatusTooManyRequests},
		{sqlStatusError(codes.Unauthenticated, "no", "", "", ""), backend.StatusUnauthorized},
		{sqlStatusError(codes.PermissionDenied, "no", "", "", ""), backend.StatusForbidden},
	} {
		client := newFakeSqlClient(t, func(*sqlv1.SqlServiceQueryRequest, grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
			return tc.err
		})
		_, err := client.Query(context.Background(), "k", "w", "select")
		var endpoint *sqlEndpointError
		if !errors.As(err, &endpoint) || endpoint.backendStatus() != tc.want {
			t.Fatalf("%v: %v", tc.err, err)
		}
		if endpoint.Code == codes.InvalidArgument && endpoint.Error() != "bad query (sqlQueryId: q)" {
			t.Fatal(endpoint.Error())
		}
		if endpoint.Code == codes.ResourceExhausted && endpoint.Error() != "SQL_ERROR_RESOURCE_EXHAUSTED" {
			t.Fatal(endpoint.Error())
		}
	}
}

func TestSqlClientSurfacesMidStreamErrors(t *testing.T) {
	client := newFakeSqlClient(t, func(_ *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		if err := stream.Send(&sqlv1.SqlServiceQueryResponse{QueryId: "q", Payload: []byte("partial")}); err != nil {
			return err
		}
		return sqlStatusError(codes.Internal, "worker died", "SQL_ERROR_EXECUTION_FAILED", "q", "")
	})
	r, err := client.Query(context.Background(), "k", "w", "select")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	var endpoint *sqlEndpointError
	if string(body) != "partial" || !errors.As(err, &endpoint) || endpoint.Error() != "worker died (sqlQueryId: q)" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func blockingSqlServer(started chan<- struct{}) sqlQueryHandler {
	return func(_ *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		close(started)
		<-stream.Context().Done()
		return stream.Context().Err()
	}
}

func TestSqlClientCancelsRequests(t *testing.T) {
	started := make(chan struct{})
	client := newFakeSqlClient(t, blockingSqlServer(started))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.Query(ctx, "key", "workspace", "SELECT 1"); result <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not cancel")
	}
}

func TestSqlClientTimesOut(t *testing.T) {
	started := make(chan struct{})
	client := newFakeSqlClient(t, blockingSqlServer(started))
	client.timeout = 50 * time.Millisecond
	_, err := client.Query(context.Background(), "key", "workspace", "SELECT 1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout, got %v", err)
	}
}

func TestSqlGrpcTarget(t *testing.T) {
	for _, tc := range []struct{ url, target string }{
		{"https://api.gov.nominal.io/api", "api.gov.nominal.io:443"},
		{"https://api-staging.gov.nominal.io:8443/api/", "api-staging.gov.nominal.io:8443"},
		{"http://localhost:8080/api", "localhost:8080"},
		{"http://127.0.0.1:8080/api", "127.0.0.1:8080"},
	} {
		target, creds, err := sqlGrpcTarget(tc.url)
		if err != nil || target != tc.target || creds == nil {
			t.Fatalf("%s: target=%q creds=%v err=%v", tc.url, target, creds, err)
		}
	}
	for _, bad := range []string{"", "api.gov.nominal.io", "ftp://api.gov.nominal.io", "http://api.gov.nominal.io/api"} {
		if _, _, err := sqlGrpcTarget(bad); err == nil {
			t.Fatalf("%q: expected error", bad)
		}
	}
}
