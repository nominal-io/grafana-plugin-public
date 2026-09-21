package plugin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const sqlQueryTimeout = 30 * time.Second

// sqlClient runs SQL through the generated nominal.sql.v1.SqlService gRPC client.
type sqlClient struct {
	conn    *grpc.ClientConn
	service sqlv1.SqlServiceClient
	timeout time.Duration
}

// newSqlClient dials the Nominal gRPC endpoint on the host of the API base URL.
func newSqlClient(baseURL string) (*sqlClient, error) {
	target, creds, err := sqlGrpcTarget(baseURL)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds), grpc.WithUserAgent(fallbackUserAgentString))
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL gRPC client: %w", err)
	}
	return newSqlClientFromConn(conn), nil
}

func newSqlClientFromConn(conn *grpc.ClientConn) *sqlClient {
	return &sqlClient{conn: conn, service: sqlv1.NewSqlServiceClient(conn), timeout: sqlQueryTimeout}
}

func (c *sqlClient) Close() error {
	return c.conn.Close()
}

// sqlGrpcTarget maps https://api.gov.nominal.io/api to api.gov.nominal.io:443 with TLS.
// Like the Nominal Python client, plaintext is allowed only for http URLs on a loopback
// host, for local development against a stack on the same machine.
func sqlGrpcTarget(baseURL string) (string, credentials.TransportCredentials, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Hostname() == "" {
		return "", nil, fmt.Errorf("invalid Nominal API base URL %q", baseURL)
	}
	port := parsed.Port()
	switch parsed.Scheme {
	case "https":
		if port == "" {
			port = "443"
		}
		return net.JoinHostPort(parsed.Hostname(), port), credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}), nil
	case "http":
		if !isLoopbackHost(parsed.Hostname()) {
			return "", nil, fmt.Errorf("plaintext http is only allowed for loopback hosts in Nominal API base URL %q; use https", baseURL)
		}
		if port == "" {
			port = "80"
		}
		return net.JoinHostPort(parsed.Hostname(), port), insecure.NewCredentials(), nil
	default:
		return "", nil, fmt.Errorf("unsupported scheme %q in Nominal API base URL", parsed.Scheme)
	}
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Query streams the Arrow IPC result for sql as one reader over the concatenated response
// payloads. The first response is awaited here so validation and auth failures surface as errors.
func (c *sqlClient) Query(ctx context.Context, token, workspaceRid, sql string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	stream, err := c.service.Query(ctx, &sqlv1.SqlServiceQueryRequest{
		Query:        sql,
		WorkspaceRid: workspaceRid,
		ResultFormat: sqlv1.SqlServiceQueryResultFormat_SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM,
	})
	if err != nil {
		queryErr := sqlQueryError(ctx, err)
		cancel()
		return nil, queryErr
	}
	reader := &sqlStreamReader{stream: stream, cancel: cancel}
	first, err := stream.Recv()
	switch {
	case err == nil:
		reader.buf = first.GetPayload()
	case errors.Is(err, io.EOF):
		reader.done = true
	default:
		queryErr := sqlQueryError(ctx, err)
		cancel()
		return nil, queryErr
	}
	return reader, nil
}

type sqlStreamReader struct {
	stream grpc.ServerStreamingClient[sqlv1.SqlServiceQueryResponse]
	cancel context.CancelFunc
	buf    []byte
	done   bool
}

func (r *sqlStreamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}
		resp, err := r.stream.Recv()
		if err != nil {
			r.done = true
			if errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			return 0, sqlQueryError(r.stream.Context(), err)
		}
		r.buf = resp.GetPayload()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *sqlStreamReader) Close() error {
	r.cancel()
	return nil
}

// sqlQueryError maps a failed RPC to a typed endpoint error, or to the context error when the
// call was cancelled or timed out on the client side.
func sqlQueryError(ctx context.Context, err error) error {
	st, ok := status.FromError(err)
	if ok && st.Code() != codes.Canceled && st.Code() != codes.DeadlineExceeded {
		return newSqlEndpointError(st)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return fmt.Errorf("SQL query timed out: %w", ctxErr)
		}
		return ctxErr
	}
	if !ok {
		return fmt.Errorf("SQL request failed: %w", err)
	}
	return newSqlEndpointError(st)
}

// sqlEndpointError is a gRPC status from the SQL service with its Nominal error details. The
// status message is the generic SqlError text; the ErrorInfo metadata carries the specific
// detail and the sqlQueryId.
type sqlEndpointError struct {
	Code       codes.Code
	Reason     string
	Detail     string
	SqlQueryId string
}

func newSqlEndpointError(st *status.Status) *sqlEndpointError {
	e := &sqlEndpointError{Code: st.Code(), Detail: st.Message()}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			e.Reason = info.GetReason()
			e.SqlQueryId = info.GetMetadata()["sqlQueryId"]
			if detail := info.GetMetadata()["detail"]; detail != "" {
				e.Detail = detail
			}
		}
	}
	return e
}

func (e *sqlEndpointError) Error() string {
	message := e.Detail
	if message == "" {
		message = e.Reason
	}
	if message == "" {
		message = fmt.Sprintf("SQL endpoint returned %s", e.Code)
	}
	if e.SqlQueryId != "" {
		message += " (sqlQueryId: " + e.SqlQueryId + ")"
	}
	return message
}

func (e *sqlEndpointError) backendStatus() backend.Status {
	switch e.Code {
	case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition, codes.OutOfRange:
		return backend.StatusBadRequest
	case codes.Unauthenticated:
		return backend.StatusUnauthorized
	case codes.PermissionDenied:
		return backend.StatusForbidden
	case codes.ResourceExhausted:
		return backend.StatusTooManyRequests
	case codes.DeadlineExceeded:
		return backend.StatusTimeout
	default:
		return backend.StatusInternal
	}
}
