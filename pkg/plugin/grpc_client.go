package plugin

import (
	"context"
	"errors"
	"net/url"

	"github.com/palantir/pkg/bearertoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// grpcWindowSize is where grpc-go's flow-control tuning stops. Starting there instead of at 64 KiB
// keeps the first large SQL result on a new connection from waiting on window updates.
const grpcWindowSize = 16 << 20

// dialNominalGRPC connects to the Nominal gRPC services on the host of the API base URL. gRPC
// dials a host rather than a URL and defaults to port 443.
func dialNominalGRPC(baseURL, userAgent string) (*grpc.ClientConn, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("the Nominal API base URL is not an https URL")
	}
	return grpc.NewClient(parsed.Host,
		grpc.WithTransportCredentials(credentials.NewTLS(nil)),
		grpc.WithUserAgent(userAgent),
		grpc.WithInitialWindowSize(grpcWindowSize),
		grpc.WithInitialConnWindowSize(grpcWindowSize),
	)
}

// withBearerToken attaches the API key to a gRPC call.
func withBearerToken(ctx context.Context, token bearertoken.Token) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+string(token))
}
