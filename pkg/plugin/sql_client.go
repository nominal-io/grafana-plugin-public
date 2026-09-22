package plugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"

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

// dialSQL connects to the SQL service on the host of the API base URL. gRPC dials a host rather
// than a URL and defaults to port 443.
func dialSQL(baseURL, userAgent string) (*grpc.ClientConn, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("SQL queries need an https Nominal API base URL")
	}
	return grpc.NewClient(parsed.Host, grpc.WithTransportCredentials(credentials.NewTLS(nil)), grpc.WithUserAgent(userAgent))
}

// withBearerToken attaches the API key to a SQL service call.
func withBearerToken(ctx context.Context, token bearertoken.Token) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+string(token))
}

// sqlStreamReader reads the Arrow IPC chunks of a SQL response stream as one byte stream.
type sqlStreamReader struct {
	ctx    context.Context
	stream grpc.ServerStreamingClient[sqlv1.SqlServiceQueryResponse]
	buf    []byte
	err    error
}

func newSQLStreamReader(ctx context.Context, stream grpc.ServerStreamingClient[sqlv1.SqlServiceQueryResponse]) *sqlStreamReader {
	return &sqlStreamReader{ctx: ctx, stream: stream}
}

func (r *sqlStreamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 && r.err == nil {
		resp, err := r.stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			r.err = io.EOF
		case err != nil:
			r.err = sqlQueryError(r.ctx, err)
		default:
			r.buf = resp.GetPayload()
		}
	}
	if len(r.buf) == 0 {
		return 0, r.err
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// sqlQueryError returns the caller's context error when the call was cancelled or ran out of
// time, and otherwise a *sqlEndpointError describing the SQL service's status.
func sqlQueryError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("SQL request failed: %w", err)
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
	case codes.DeadlineExceeded:
		return backend.StatusTimeout
	case codes.Unavailable:
		return backend.StatusBadGateway
	default:
		return backend.StatusInternal
	}
}
