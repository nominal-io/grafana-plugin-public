package plugin

import (
	"context"
	"fmt"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/codecs"
	"github.com/palantir/pkg/bearertoken"
	werror "github.com/palantir/witchcraft-go-error"
)

// computeClient is the generated compute client with BatchComputeWithUnits
// rebuilt so the plugin controls how the response body is decoded.
type computeClient struct {
	computeapi1.ComputeServiceClient
	client conjurehttpclient.Client
}

func newComputeClient(client conjurehttpclient.Client) computeClient {
	return computeClient{ComputeServiceClient: computeapi1.NewComputeServiceClient(client), client: client}
}

// BatchComputeWithUnits matches the generated method except for the response
// decoder. Server errors decode as generic Conjure errors, because the Nominal
// API client's typed error registry is internal to it. A generic error keeps
// its code and name even when its parameters do not fit the typed definition,
// where the generated client falls back to the bare HTTP status.
func (c computeClient) BatchComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
	var returnVal *computeapi.BatchComputeWithUnitsResponse
	if _, err := c.client.Post(ctx,
		conjurehttpclient.WithRPCMethodName("BatchComputeWithUnits"),
		conjurehttpclient.WithHeader("Authorization", fmt.Sprint("Bearer ", authHeader)),
		conjurehttpclient.WithPathf("/compute/v2/compute/batch"),
		conjurehttpclient.WithJSONRequest(requestArg),
		conjurehttpclient.WithResponseBody(&returnVal, codecs.JSON),
	); err != nil {
		return computeapi.BatchComputeWithUnitsResponse{}, werror.WrapWithContextParams(ctx, err, "batchComputeWithUnits failed")
	}
	if returnVal == nil {
		return computeapi.BatchComputeWithUnitsResponse{}, werror.ErrorWithContextParams(ctx, "batchComputeWithUnits response cannot be nil")
	}
	return *returnVal, nil
}
