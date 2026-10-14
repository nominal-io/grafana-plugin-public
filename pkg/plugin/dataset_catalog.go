package plugin

import (
	"context"
	"strings"

	"github.com/nominal-io/grafana-plugin-public/pkg/models"
	datasetsv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/datasets/v1"
	"github.com/palantir/pkg/bearertoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultDatasetSearchResults = 50
	maxDatasetSearchResults     = 1000
	// datasetPageSize keeps a page of full dataset records, descriptions included, under gRPC's
	// default 4 MiB message limit.
	datasetPageSize = 250
)

// The dataset service shares the SQL connection, which needs an https base URL.
var errNoDatasetService = status.Error(codes.FailedPrecondition, "Datasets need an https Nominal API base URL")

type datasetItem struct {
	Rid     string `json:"rid"`
	Name    string `json:"name"`
	StartMs *int64 `json:"startMs,omitempty"`
	EndMs   *int64 `json:"endMs,omitempty"`
}

// datasetContext bounds a dataset call like the other resource routes and attaches the caller's token.
func datasetContext(ctx context.Context, config *models.PluginSettings) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, resourceRequestTimeout)
	return withBearerToken(ctx, bearertoken.Token(config.Secrets.AuthToken)), cancel
}

// searchDatasets returns up to maxResults non-archived datasets, most recently ingested first. A
// maxResults of zero or less means the default; values above 1000 are capped.
func (d *Datasource) searchDatasets(ctx context.Context, config *models.PluginSettings, searchText string, maxResults int) ([]datasetItem, error) {
	if d.datasetService == nil {
		return nil, errNoDatasetService
	}
	if maxResults <= 0 {
		maxResults = defaultDatasetSearchResults
	}
	maxResults = min(maxResults, maxDatasetSearchResults)

	queries := []*datasetsv1.DatasetQuery{{Query: &datasetsv1.DatasetQuery_IsArchived{IsArchived: false}}}
	if searchText = strings.TrimSpace(searchText); searchText != "" {
		queries = append(queries, &datasetsv1.DatasetQuery{Query: &datasetsv1.DatasetQuery_SearchText{SearchText: searchText}})
	}
	if config.WorkspaceRid != "" {
		queries = append(queries, &datasetsv1.DatasetQuery{Query: &datasetsv1.DatasetQuery_WorkspaceRid{WorkspaceRid: config.WorkspaceRid}})
	}
	ctx, cancel := datasetContext(ctx, config)
	defer cancel()
	req := &datasetsv1.SearchDatasetsRequest{
		Query: &datasetsv1.DatasetQuery{Query: &datasetsv1.DatasetQuery_And{And: &datasetsv1.DatasetQueryList{Queries: queries}}},
		Sort:  &datasetsv1.DatasetSort{Field: datasetsv1.DatasetSortField_DATASET_SORT_FIELD_LAST_INGESTED_AT, Descending: true},
	}
	// A page can hold fewer datasets than asked for while more remain.
	var datasets []*datasetsv1.Dataset
	for {
		req.PageSize = new(int32(min(maxResults-len(datasets), datasetPageSize)))
		resp, err := d.datasetService.SearchDatasets(ctx, req)
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, resp.GetDatasets()...)
		if req.PageToken = resp.GetNextPageToken(); req.PageToken == "" || len(datasets) >= maxResults {
			return datasetItems(datasets[:min(len(datasets), maxResults)]), nil
		}
	}
}

// datasetsByRid resolves datasets by RID. The API leaves out RIDs it does not know.
func (d *Datasource) datasetsByRid(ctx context.Context, config *models.PluginSettings, rids []string) ([]datasetItem, error) {
	if d.datasetService == nil {
		return nil, errNoDatasetService
	}
	ctx, cancel := datasetContext(ctx, config)
	defer cancel()
	resp, err := d.datasetService.BatchGetDatasets(ctx, &datasetsv1.BatchGetDatasetsRequest{DatasetRids: rids})
	if err != nil {
		return nil, err
	}
	return datasetItems(resp.GetDatasets()), nil
}

func datasetItems(datasets []*datasetsv1.Dataset) []datasetItem {
	items := make([]datasetItem, 0, len(datasets))
	for _, dataset := range datasets {
		item := datasetItem{Rid: dataset.GetRid(), Name: dataset.GetName()}
		if bounds := dataset.GetBounds(); bounds.GetTimeBase() == datasetsv1.TimeBase_TIME_BASE_ABSOLUTE && bounds.GetStart() != nil && bounds.GetEnd() != nil {
			item.StartMs = new(bounds.GetStart().AsTime().UnixMilli())
			item.EndMs = new(bounds.GetEnd().AsTime().UnixMilli())
		}
		items = append(items, item)
	}
	return items
}
