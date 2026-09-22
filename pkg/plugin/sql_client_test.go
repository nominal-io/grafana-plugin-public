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

type fakeSQLService struct {
	sqlv1.UnimplementedSqlServiceServer
	query   sqlQueryHandler
	catalog func(context.Context) error
}

func (f *fakeSQLService) Query(req *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
	return f.query(req, stream)
}

func (f *fakeSQLService) GetSqlCatalog(ctx context.Context, _ *sqlv1.GetSqlCatalogRequest) (*sqlv1.GetSqlCatalogResponse, error) {
	if f.catalog == nil {
		return f.UnimplementedSqlServiceServer.GetSqlCatalog(ctx, nil)
	}
	if err := f.catalog(ctx); err != nil {
		return nil, err
	}
	return &sqlv1.GetSqlCatalogResponse{}, nil
}

// newFakeSQLClient serves service over an in-memory gRPC connection and returns a client bound to it.
func newFakeSQLClient(t *testing.T, service *fakeSQLService) *sqlClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	sqlv1.RegisterSqlServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	client := newSQLClientFromConn(conn)
	t.Cleanup(func() {
		_ = client.Close()
		server.Stop()
		_ = listener.Close()
	})
	return client
}

func newFakeSQLQueryClient(t *testing.T, query sqlQueryHandler) *sqlClient {
	t.Helper()
	return newFakeSQLClient(t, &fakeSQLService{query: query})
}

// sqlPayloadServer streams payload as a single response message.
func sqlPayloadServer(payload []byte) sqlQueryHandler {
	return func(_ *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		return stream.Send(&sqlv1.SqlServiceQueryResponse{QueryId: "q", Payload: payload})
	}
}

// sqlStatusError builds an error shaped like the SQL service's: a generic status message plus an
// ErrorInfo whose metadata carries sqlQueryId and, optionally, a specific detail.
func sqlStatusError(t *testing.T, code codes.Code, message, reason, queryID, detail string) error {
	t.Helper()
	st := status.New(code, message)
	if reason == "" {
		return st.Err()
	}
	md := map[string]string{"sqlQueryId": queryID}
	if detail != "" {
		md["detail"] = detail
	}
	st, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "nominal.sql.v1", Metadata: md})
	if err != nil {
		t.Fatalf("status.WithDetails() error = %v", err)
	}
	return st.Err()
}

func TestSQLClientQuerySendsRequest(t *testing.T) {
	var got *sqlv1.SqlServiceQueryRequest
	var authorization []string
	client := newFakeSQLQueryClient(t, func(req *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
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
		t.Fatalf("Query() error = %v", err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil || string(body) != "stream" {
		t.Fatalf("io.ReadAll() = %q, %v; want %q, nil", body, err, "stream")
	}
	if got.GetQuery() != "SELECT 1" || got.GetWorkspaceRid() != "workspace" {
		t.Errorf("request query, workspace = %q, %q; want %q, %q", got.GetQuery(), got.GetWorkspaceRid(), "SELECT 1", "workspace")
	}
	if want := sqlv1.SqlServiceQueryResultFormat_SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM; got.GetResultFormat() != want {
		t.Errorf("request result format = %v, want %v", got.GetResultFormat(), want)
	}
	if got.MaxRows != nil {
		t.Errorf("request max rows = %d, want unset", got.GetMaxRows())
	}
	if len(authorization) != 1 || authorization[0] != "Bearer token" {
		t.Errorf("authorization metadata = %q, want [%q]", authorization, "Bearer token")
	}
}

func TestSQLClientEndpointErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus backend.Status
		wantMsg    string
	}{
		{
			name:       "detail and query ID",
			err:        sqlStatusError(t, codes.InvalidArgument, "SQL query is invalid", "SQL_ERROR_INVALID_QUERY", "q", "bad query"),
			wantStatus: backend.StatusBadRequest,
			wantMsg:    "bad query (sqlQueryId: q)",
		},
		{
			name:       "status message only",
			err:        sqlStatusError(t, codes.Internal, "boom", "", "", ""),
			wantStatus: backend.StatusInternal,
			wantMsg:    "boom",
		},
		{
			name:       "reason without message",
			err:        sqlStatusError(t, codes.ResourceExhausted, "", "SQL_ERROR_RESOURCE_EXHAUSTED", "", ""),
			wantStatus: backend.StatusTooManyRequests,
			wantMsg:    "SQL_ERROR_RESOURCE_EXHAUSTED",
		},
		{
			name:       "no message",
			err:        status.Error(codes.NotFound, ""),
			wantStatus: backend.StatusNotFound,
			wantMsg:    "SQL endpoint returned NotFound",
		},
		{
			name:       "unauthenticated",
			err:        sqlStatusError(t, codes.Unauthenticated, "no", "", "", ""),
			wantStatus: backend.StatusUnauthorized,
			wantMsg:    "no",
		},
		{
			name:       "permission denied",
			err:        sqlStatusError(t, codes.PermissionDenied, "no", "", "", ""),
			wantStatus: backend.StatusForbidden,
			wantMsg:    "no",
		},
		{
			name:       "unavailable",
			err:        sqlStatusError(t, codes.Unavailable, "throttled", "", "", ""),
			wantStatus: backend.StatusBadGateway,
			wantMsg:    "throttled",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newFakeSQLQueryClient(t, func(*sqlv1.SqlServiceQueryRequest, grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
				return tc.err
			})
			_, err := client.Query(context.Background(), "k", "w", "select")
			endpoint, ok := errors.AsType[*sqlEndpointError](err)
			if !ok {
				t.Fatalf("Query() error = %v, want *sqlEndpointError", err)
			}
			if got := endpoint.backendStatus(); got != tc.wantStatus {
				t.Errorf("backendStatus() = %v, want %v", got, tc.wantStatus)
			}
			if got := endpoint.Error(); got != tc.wantMsg {
				t.Errorf("Error() = %q, want %q", got, tc.wantMsg)
			}
		})
	}
}

func TestSQLClientSurfacesMidStreamErrors(t *testing.T) {
	failure := sqlStatusError(t, codes.Internal, "worker died", "SQL_ERROR_EXECUTION_FAILED", "q", "")
	client := newFakeSQLQueryClient(t, func(_ *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		if err := stream.Send(&sqlv1.SqlServiceQueryResponse{QueryId: "q", Payload: []byte("partial")}); err != nil {
			return err
		}
		return failure
	})
	r, err := client.Query(context.Background(), "k", "w", "select")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if string(body) != "partial" {
		t.Errorf("body = %q, want %q", body, "partial")
	}
	const wantMsg = "worker died (sqlQueryId: q)"
	if endpoint, ok := errors.AsType[*sqlEndpointError](err); !ok || endpoint.Error() != wantMsg {
		t.Fatalf("io.ReadAll() error = %v, want %q", err, wantMsg)
	}
	if _, again := r.Read(make([]byte, 1)); !errors.Is(again, err) {
		t.Errorf("Read() after failure = %v, want %v", again, err)
	}
}

// blockingSQLServer never answers until release is closed, so the client side
// decides how the call ends (cancellation or its own deadline).
func blockingSQLServer(started chan<- struct{}, release <-chan struct{}) sqlQueryHandler {
	return func(*sqlv1.SqlServiceQueryRequest, grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		close(started)
		<-release
		return nil
	}
}

func TestSQLClientCancelsRequests(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	client := newFakeSQLQueryClient(t, blockingSQLServer(started, release))
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
			t.Fatalf("Query() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not cancel")
	}
}

func TestSQLClientTimesOut(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	client := newFakeSQLQueryClient(t, blockingSQLServer(started, release))
	client.timeout = 50 * time.Millisecond
	_, err := client.Query(context.Background(), "key", "workspace", "SELECT 1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Query() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestSQLClientCheckConnection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		catalog func(context.Context) error
		wantErr string
	}{
		{name: "reachable", catalog: func(context.Context) error { return nil }},
		{name: "rejected key", catalog: func(context.Context) error { return status.Error(codes.Unauthenticated, "invalid token") }, wantErr: "invalid token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var authorization []string
			client := newFakeSQLClient(t, &fakeSQLService{catalog: func(ctx context.Context) error {
				md, _ := metadata.FromIncomingContext(ctx)
				authorization = md.Get("authorization")
				return tc.catalog(ctx)
			}})
			err := client.CheckConnection(context.Background(), "token")
			if gotErr := errorText(err); gotErr != tc.wantErr {
				t.Errorf("CheckConnection() error = %q, want %q", gotErr, tc.wantErr)
			}
			if len(authorization) != 1 || authorization[0] != "Bearer token" {
				t.Errorf("authorization metadata = %q, want [%q]", authorization, "Bearer token")
			}
		})
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestGRPCTarget(t *testing.T) {
	for _, tc := range []struct {
		baseURL string
		want    string
		wantErr bool
	}{
		{baseURL: "https://api.gov.nominal.io/api", want: "api.gov.nominal.io:443"},
		{baseURL: "https://api-staging.gov.nominal.io:8443/api/", want: "api-staging.gov.nominal.io:8443"},
		{baseURL: "", wantErr: true},
		{baseURL: "api.gov.nominal.io", wantErr: true},
		{baseURL: "ftp://api.gov.nominal.io", wantErr: true},
		{baseURL: "http://localhost:8080/api", wantErr: true},
		{baseURL: "https://%zz", wantErr: true},
	} {
		t.Run(tc.baseURL, func(t *testing.T) {
			got, err := grpcTarget(tc.baseURL)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("grpcTarget(%q) = %q, %v; want %q, error %t", tc.baseURL, got, err, tc.want, tc.wantErr)
			}
		})
	}
}
