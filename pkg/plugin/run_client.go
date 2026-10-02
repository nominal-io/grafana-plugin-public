package plugin

import (
	"context"
	"fmt"

	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	runapi1 "github.com/nominal-io/nominal-api-go/scout/run/api1"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
)

// runAPI is the part of the Nominal run service the plugin calls. The
// generated service client is not used: importing it links every generated API
// package, and two of those register the same conjure error name, which panics
// at startup.
type runAPI interface {
	GetRuns(ctx context.Context, authHeader bearertoken.Token, runRids []runapi.RunRid) (map[runapi.RunRid]runapi1.Run, error)
	SearchRuns(ctx context.Context, authHeader bearertoken.Token, req runapi.SearchRunsRequest) (runapi1.SearchRunsResponse, error)
}

type runAPIClient struct {
	client conjurehttpclient.Client
}

func newRunAPIClient(client conjurehttpclient.Client) runAPI {
	return &runAPIClient{client: client}
}

func (c *runAPIClient) GetRuns(ctx context.Context, authHeader bearertoken.Token, runRids []runapi.RunRid) (map[runapi.RunRid]runapi1.Run, error) {
	var out map[runapi.RunRid]runapi1.Run
	_, err := c.client.Post(ctx,
		conjurehttpclient.WithRPCMethodName("GetRuns"),
		conjurehttpclient.WithHeader("Authorization", fmt.Sprint("Bearer ", authHeader)),
		conjurehttpclient.WithPathf("/scout/v1/run/multiple"),
		conjurehttpclient.WithJSONRequest(runRids),
		conjurehttpclient.WithJSONResponse(&out),
	)
	// A deleted or inaccessible run comes back as this error, not as a missing
	// key. It arrives untyped: the generated decoder is internal to the API module.
	if extractErrorDetails(err).Name == "Scout:RunNotFound" {
		return map[runapi.RunRid]runapi1.Run{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getRuns failed: %w", err)
	}
	return out, nil
}

func (c *runAPIClient) SearchRuns(ctx context.Context, authHeader bearertoken.Token, req runapi.SearchRunsRequest) (runapi1.SearchRunsResponse, error) {
	var out runapi1.SearchRunsResponse
	_, err := c.client.Post(ctx,
		conjurehttpclient.WithRPCMethodName("SearchRuns"),
		conjurehttpclient.WithHeader("Authorization", fmt.Sprint("Bearer ", authHeader)),
		conjurehttpclient.WithPathf("/scout/v1/search-runs"),
		conjurehttpclient.WithJSONRequest(req),
		conjurehttpclient.WithJSONResponse(&out),
	)
	if err != nil {
		return runapi1.SearchRunsResponse{}, fmt.Errorf("searchRuns failed: %w", err)
	}
	return out, nil
}
