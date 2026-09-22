package plugin

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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

func (f *fakeSQLService) GetSqlCatalog(ctx context.Context, req *sqlv1.GetSqlCatalogRequest) (*sqlv1.GetSqlCatalogResponse, error) {
	if f.catalog == nil {
		return f.UnimplementedSqlServiceServer.GetSqlCatalog(ctx, req)
	}
	if err := f.catalog(ctx); err != nil {
		return nil, err
	}
	return &sqlv1.GetSqlCatalogResponse{}, nil
}

// newFakeSQLService serves service over an in-memory connection and returns the generated client.
func newFakeSQLService(t *testing.T, service *fakeSQLService) sqlv1.SqlServiceClient {
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
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
	})
	return sqlv1.NewSqlServiceClient(conn)
}

// sqlStatusError builds an error shaped like the SQL service's: a generic status message plus an
// ErrorInfo whose metadata carries the query ID and a specific detail.
func sqlStatusError(t *testing.T, code codes.Code, message, queryID, detail string) error {
	t.Helper()
	st, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{
		Reason:   "SQL_ERROR",
		Domain:   "nominal.sql.v1",
		Metadata: map[string]string{"sqlQueryId": queryID, "detail": detail},
	})
	if err != nil {
		t.Fatalf("status.WithDetails() error = %v", err)
	}
	return st.Err()
}

func TestDialSQL(t *testing.T) {
	for _, tc := range []struct {
		baseURL    string
		wantTarget string
	}{
		{baseURL: "https://api.gov.nominal.io/api", wantTarget: "api.gov.nominal.io"},
		{baseURL: "https://nominal.example.com:8443/api/", wantTarget: "nominal.example.com:8443"},
	} {
		conn, err := dialSQL(tc.baseURL, "test")
		if err != nil {
			t.Fatalf("dialSQL(%q) error = %v", tc.baseURL, err)
		}
		if got := conn.Target(); got != tc.wantTarget {
			t.Errorf("dialSQL(%q) target = %q, want %q", tc.baseURL, got, tc.wantTarget)
		}
		_ = conn.Close()
	}
	for _, baseURL := range []string{"", "api.gov.nominal.io", "http://admin:secret@nominal.internal/api", "https://%zz"} {
		if _, err := dialSQL(baseURL, "test"); err == nil || strings.Contains(err.Error(), "secret") {
			t.Errorf("dialSQL(%q) error = %v, want an error that does not repeat the URL", baseURL, err)
		}
	}
}

func TestSQLStreamReader(t *testing.T) {
	failure := sqlStatusError(t, codes.Internal, "SQL query failed", "q-1", "worker died")
	client := newFakeSQLService(t, &fakeSQLService{query: func(req *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		for _, chunk := range []string{"str", "eam"} {
			if err := stream.Send(&sqlv1.SqlServiceQueryResponse{Payload: []byte(chunk)}); err != nil {
				return err
			}
		}
		if req.GetQuery() == "fail" {
			return failure
		}
		return nil
	}})
	read := func(sql string) (string, *sqlStreamReader, error) {
		stream, err := client.Query(context.Background(), &sqlv1.SqlServiceQueryRequest{Query: sql})
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}
		reader := newSQLStreamReader(context.Background(), stream)
		body, err := io.ReadAll(reader)
		return string(body), reader, err
	}

	if body, _, err := read("ok"); body != "stream" || err != nil {
		t.Errorf("read(ok) = %q, %v; want %q, nil", body, err, "stream")
	}
	body, reader, err := read("fail")
	if body != "stream" {
		t.Errorf("read(fail) body = %q, want the chunks sent before the failure", body)
	}
	if endpoint, ok := errors.AsType[*sqlEndpointError](err); !ok || endpoint.Error() != "worker died (sqlQueryId: q-1)" {
		t.Fatalf("read(fail) error = %v, want the SQL service's error", err)
	}
	if _, again := reader.Read(make([]byte, 1)); again != err {
		t.Errorf("Read() after the failure = %v, want the same error", again)
	}
}

func TestSQLQueryError(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithTimeout(context.Background(), 0)
	defer cancelExpired()
	for _, tc := range []struct {
		name       string
		ctx        context.Context
		err        error
		wantErr    error
		wantStatus backend.Status
		wantMsg    string
	}{
		{name: "cancelled by Grafana", ctx: cancelled, err: status.Error(codes.Canceled, "canceled"), wantErr: context.Canceled},
		{name: "Grafana deadline", ctx: expired, err: status.Error(codes.DeadlineExceeded, "deadline"), wantErr: context.DeadlineExceeded},
		{
			name:       "rejected query",
			ctx:        context.Background(),
			err:        sqlStatusError(t, codes.InvalidArgument, "SQL query is invalid", "q-2", "Column 'x' not found"),
			wantStatus: backend.StatusBadRequest,
			wantMsg:    "Column 'x' not found (sqlQueryId: q-2)",
		},
		{
			name:       "server time limit",
			ctx:        context.Background(),
			err:        sqlStatusError(t, codes.DeadlineExceeded, "SQL query timed out", "q-3", ""),
			wantStatus: backend.StatusTimeout,
			wantMsg:    "SQL query timed out (sqlQueryId: q-3)",
		},
		{
			name:       "status without details",
			ctx:        context.Background(),
			err:        status.Error(codes.Unauthenticated, ""),
			wantStatus: backend.StatusUnauthorized,
			wantMsg:    "SQL endpoint returned Unauthenticated",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sqlQueryError(tc.ctx, tc.err)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("sqlQueryError() = %v, want %v", err, tc.wantErr)
				}
				return
			}
			endpoint, ok := errors.AsType[*sqlEndpointError](err)
			if !ok {
				t.Fatalf("sqlQueryError() = %v, want a *sqlEndpointError", err)
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
