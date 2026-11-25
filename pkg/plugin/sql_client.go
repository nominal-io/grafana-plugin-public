package plugin

import (
	"context"

	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	"google.golang.org/grpc"
)

// sqlStreamReader reads the Arrow IPC chunks of a SQL response stream as one byte stream. After the
// last chunk it returns io.EOF, or the stream's gRPC error if the query failed.
type sqlStreamReader struct {
	stream grpc.ServerStreamingClient[sqlv1.SqlServiceQueryResponse]
	cancel context.CancelFunc
	buf    []byte
	err    error
}

// Close cancels the stream's context, which ends the query on the server.
func (r *sqlStreamReader) Close() error {
	r.cancel()
	return nil
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
