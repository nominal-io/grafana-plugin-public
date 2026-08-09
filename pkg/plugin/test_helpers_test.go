package plugin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	"github.com/palantir/pkg/bearertoken"
)

type mockDatasourceService struct {
	searchChannelsResponse datasourceapi.SearchChannelsResponse
	searchChannelsError    error
	searchChannelsRequest  datasourceapi.SearchChannelsRequest
	searchChannelsCalls    int
	// searchChannelsFunc, when non-nil, overrides searchChannelsResponse/searchChannelsError.
	searchChannelsFunc func(ctx context.Context, authHeader bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error)
}

func (m *mockDatasourceService) SearchChannels(ctx context.Context, authHeader bearertoken.Token, queryArg datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
	m.searchChannelsCalls++
	m.searchChannelsRequest = queryArg
	if m.searchChannelsFunc != nil {
		return m.searchChannelsFunc(ctx, authHeader, queryArg)
	}
	return m.searchChannelsResponse, m.searchChannelsError
}

func (m *mockDatasourceService) SearchFilteredChannels(ctx context.Context, authHeader bearertoken.Token, queryArg datasourceapi.SearchFilteredChannelsRequest) (datasourceapi.SearchFilteredChannelsResponse, error) {
	return datasourceapi.SearchFilteredChannelsResponse{}, nil
}

func (m *mockDatasourceService) SearchHierarchicalChannels(ctx context.Context, authHeader bearertoken.Token, queryArg datasourceapi.SearchHierarchicalChannelsRequest) (datasourceapi.SearchHierarchicalChannelsResponse, error) {
	return datasourceapi.SearchHierarchicalChannelsResponse{}, nil
}

func (m *mockDatasourceService) IndexChannelPrefixTree(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.IndexChannelPrefixTreeRequest) (datasourceapi.ChannelPrefixTree, error) {
	return datasourceapi.ChannelPrefixTree{}, nil
}

func (m *mockDatasourceService) BatchGetChannelPrefixTrees(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.BatchGetChannelPrefixTreeRequest) (datasourceapi.BatchGetChannelPrefixTreeResponse, error) {
	return datasourceapi.BatchGetChannelPrefixTreeResponse{}, nil
}

func (m *mockDatasourceService) GetAvailableTagsForChannel(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.GetAvailableTagsForChannelRequest) (datasourceapi.GetAvailableTagsForChannelResponse, error) {
	return datasourceapi.GetAvailableTagsForChannelResponse{}, nil
}

func (m *mockDatasourceService) GetDataScopeBounds(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.BatchGetDataScopeBoundsRequest) (datasourceapi.BatchGetDataScopeBoundsResponse, error) {
	return datasourceapi.BatchGetDataScopeBoundsResponse{}, nil
}

func (m *mockDatasourceService) GetTagValuesForDataSource(ctx context.Context, authHeader bearertoken.Token, dataSourceRidArg rids.DataSourceRid, requestArg datasourceapi.GetTagValuesForDataSourceRequest) (map[api.TagName][]api.TagValue, error) {
	return nil, nil
}

func (m *mockDatasourceService) GetAvailableTagKeys(ctx context.Context, authHeader bearertoken.Token, dataSourceRidArg rids.DataSourceRid, requestArg datasourceapi.GetAvailableTagKeysRequest) (datasourceapi.GetAvailableTagKeysResponse, error) {
	return datasourceapi.GetAvailableTagKeysResponse{}, nil
}

func (m *mockDatasourceService) GetAvailableTagValues(ctx context.Context, authHeader bearertoken.Token, dataSourceRidArg rids.DataSourceRid, requestArg datasourceapi.GetAvailableTagValuesRequest) (datasourceapi.GetAvailableTagValuesResponse, error) {
	return datasourceapi.GetAvailableTagValuesResponse{}, nil
}

func (m *mockDatasourceService) BatchGetSeriesCount(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.BatchGetSeriesCountRequest) (datasourceapi.BatchGetSeriesCountResponse, error) {
	return datasourceapi.BatchGetSeriesCountResponse{}, nil
}

func (m *mockDatasourceService) GetMatchingChannelsWithTags(ctx context.Context, authHeader bearertoken.Token, requestArg datasourceapi.GetMatchingChannelsWithTagsRequest) (datasourceapi.GetMatchingChannelsWithTagsResponse, error) {
	return datasourceapi.GetMatchingChannelsWithTagsResponse{}, nil
}

var _ datasourceservice.DataSourceServiceClient = (*mockDatasourceService)(nil)

func newCountingAssetServer(t *testing.T, assets map[string]SingleAssetResponse, fetchCount *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/scout/v1/asset/multiple" {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}

		(*fetchCount)++
		var assetRids []string
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &assetRids); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		result := make(map[string]SingleAssetResponse)
		for _, assetRid := range assetRids {
			if asset, ok := assets[assetRid]; ok {
				result[assetRid] = asset
			}
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
}
