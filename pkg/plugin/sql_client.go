package plugin

import (
	"context"
	"errors"
	"net/url"

	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	"github.com/palantir/pkg/bearertoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
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

// sqlStreamReader reads the Arrow IPC chunks of a SQL response stream as one byte stream. After the
// last chunk it returns io.EOF, or the stream's gRPC error if the query failed.
type sqlStreamReader struct {
	stream grpc.ServerStreamingClient[sqlv1.SqlServiceQueryResponse]
	buf    []byte
	err    error
}

func (r *sqlStreamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 && r.err == nil {
		var resp *sqlv1.SqlServiceQueryResponse
		resp, r.err = r.stream.Recv()
		r.buf = resp.GetPayload()
	}
	if len(r.buf) == 0 {
		return 0, r.err
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}
