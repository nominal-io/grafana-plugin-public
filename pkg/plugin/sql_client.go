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
	"github.com/palantir/pkg/bearertoken"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// sqlQueryTimeout matches the SQL service's own limit, so the service decides when a query has
	// run too long.
	sqlQueryTimeout      = 2 * time.Minute
	sqlConnectionTimeout = 15 * time.Second
)

// sqlClient runs SQL through the generated nominal.sql.v1.SqlService gRPC client.
type sqlClient struct {
	conn    *grpc.ClientConn
	service sqlv1.SqlServiceClient
	timeout time.Duration
}

// newSQLClient connects over TLS to the host of the API base URL, so
// https://api.gov.nominal.io/api becomes api.gov.nominal.io:443.
func newSQLClient(baseURL, userAgent string) (*sqlClient, error) {
	target, err := grpcTarget(baseURL)
	if err != nil {
		return nil, err
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds), grpc.WithUserAgent(userAgent))
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL gRPC client: %w", err)
	}
	return newSQLClientFromConn(conn), nil
}

func newSQLClientFromConn(conn *grpc.ClientConn) *sqlClient {
	return &sqlClient{conn: conn, service: sqlv1.NewSqlServiceClient(conn), timeout: sqlQueryTimeout}
}

func (c *sqlClient) Close() error {
	return c.conn.Close()
}

func grpcTarget(baseURL string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid Nominal API base URL: %w", err)
	}
	if parsed.Hostname() == "" {
		return "", fmt.Errorf("invalid Nominal API base URL %q: missing host", baseURL)
	}
	if parsed.Scheme != "https" {
		return "", fmt.Errorf("Nominal API base URL %q must use https for SQL queries", baseURL)
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	return net.JoinHostPort(parsed.Hostname(), port), nil
}

func withBearerToken(ctx context.Context, token bearertoken.Token) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+string(token))
}

// CheckConnection confirms that the SQL service is reachable and accepts token.
func (c *sqlClient) CheckConnection(ctx context.Context, token bearertoken.Token) error {
	ctx, cancel := context.WithTimeout(withBearerToken(ctx, token), sqlConnectionTimeout)
	defer cancel()
	if _, err := c.service.GetSqlCatalog(ctx, &sqlv1.GetSqlCatalogRequest{}); err != nil {
		return sqlQueryError(ctx, err)
	}
	return nil
}

// Query streams the Arrow IPC result of sql as one reader over the response payloads. It waits for
// the first response, so validation and authorization failures are returned here.
func (c *sqlClient) Query(ctx context.Context, token bearertoken.Token, workspaceRID, sql string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(withBearerToken(ctx, token), c.timeout)
	stream, err := c.service.Query(ctx, &sqlv1.SqlServiceQueryRequest{
		Query:        sql,
		WorkspaceRid: workspaceRID,
		ResultFormat: sqlv1.SqlServiceQueryResultFormat_SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM,
	})
	if err != nil {
		err = sqlQueryError(ctx, err)
		cancel()
		return nil, err
	}
	first, err := stream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		err = sqlQueryError(ctx, err)
		cancel()
		return nil, err
	}
	reader := &sqlStreamReader{stream: stream, cancel: cancel, buf: first.GetPayload()}
	if err != nil {
		reader.err = io.EOF
	}
	return reader, nil
}

type sqlStreamReader struct {
	stream grpc.ServerStreamingClient[sqlv1.SqlServiceQueryResponse]
	cancel context.CancelFunc
	buf    []byte
	err    error
}

func (r *sqlStreamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		resp, err := r.stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			r.err = io.EOF
		case err != nil:
			r.err = sqlQueryError(r.stream.Context(), err)
		default:
			r.buf = resp.GetPayload()
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *sqlStreamReader) Close() error {
	r.cancel()
	return nil
}

// sqlQueryError maps a failed RPC to a *sqlEndpointError, or to the context error when the call was
// cancelled or ran out of time on either side.
func sqlQueryError(ctx context.Context, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("SQL request failed: %w", err)
	}
	if st.Code() == codes.Canceled || st.Code() == codes.DeadlineExceeded {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return ctxErr
		}
		// The server can observe the propagated deadline before the local timer fires.
		if st.Code() == codes.DeadlineExceeded || ctx.Err() != nil {
			return fmt.Errorf("SQL query timed out: %w", context.DeadlineExceeded)
		}
	}
	return newSQLEndpointError(st)
}

// sqlEndpointError is a gRPC status from the SQL service. The status message is generic; the
// ErrorInfo metadata carries the specific detail and the server's query ID.
type sqlEndpointError struct {
	Code    codes.Code
	Reason  string
	Detail  string
	QueryID string
}

func newSQLEndpointError(st *status.Status) *sqlEndpointError {
	e := &sqlEndpointError{Code: st.Code(), Detail: st.Message()}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			e.Reason = info.GetReason()
			e.QueryID = info.GetMetadata()["sqlQueryId"]
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
	if e.QueryID != "" {
		message += " (sqlQueryId: " + e.QueryID + ")"
	}
	return message
}

func (e *sqlEndpointError) backendStatus() backend.Status {
	switch e.Code {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return backend.StatusBadRequest
	case codes.NotFound:
		return backend.StatusNotFound
	case codes.Unauthenticated:
		return backend.StatusUnauthorized
	case codes.PermissionDenied:
		return backend.StatusForbidden
	case codes.ResourceExhausted:
		return backend.StatusTooManyRequests
	case codes.Unimplemented:
		return backend.StatusNotImplemented
	case codes.Unavailable:
		return backend.StatusBadGateway
	default:
		return backend.StatusInternal
	}
}
