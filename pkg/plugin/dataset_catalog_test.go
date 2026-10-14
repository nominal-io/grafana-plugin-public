package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdklog "github.com/grafana/grafana-plugin-sdk-go/backend/log"
	datasetsv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/datasets/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeDatasetService answers every call from datasets, or with err, and records each request
// and the last authorization header.
type fakeDatasetService struct {
	datasetsv1.UnimplementedDatasetServiceServer
	datasets []*datasetsv1.Dataset
	err      error
	requests []proto.Message
	auth     []string
}

func (f *fakeDatasetService) record(ctx context.Context, req proto.Message) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.requests, f.auth = append(f.requests, req), md.Get("authorization")
}

// SearchDatasets returns one dataset per page, so a search must follow page tokens.
func (f *fakeDatasetService) SearchDatasets(ctx context.Context, req *datasetsv1.SearchDatasetsRequest) (*datasetsv1.SearchDatasetsResponse, error) {
	f.record(ctx, req)
	if f.err != nil {
		return nil, f.err
	}
	i, _ := strconv.Atoi(req.GetPageToken())
	resp := &datasetsv1.SearchDatasetsResponse{Datasets: f.datasets[i : i+1]}
	if i+1 < len(f.datasets) {
		resp.NextPageToken = strconv.Itoa(i + 1)
	}
	return resp, nil
}

func (f *fakeDatasetService) BatchGetDatasets(ctx context.Context, req *datasetsv1.BatchGetDatasetsRequest) (*datasetsv1.BatchGetDatasetsResponse, error) {
	f.record(ctx, req)
	return &datasetsv1.BatchGetDatasetsResponse{Datasets: f.datasets}, f.err
}

func withDatasetService(t *testing.T, ds *Datasource, service *fakeDatasetService) *Datasource {
	t.Helper()
	conn := newInMemoryConn(t, func(s *grpc.Server) { datasetsv1.RegisterDatasetServiceServer(s, service) })
	ds.datasetService = datasetsv1.NewDatasetServiceClient(conn)
	return ds
}

// Only absolute bounds with both ends become startMs and endMs.
var (
	testDatasets = []*datasetsv1.Dataset{
		{Rid: "ri.catalog.main.dataset.a", Name: "Absolute", Bounds: &datasetsv1.DataBounds{
			Start:    &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 250000000},
			End:      &timestamppb.Timestamp{Seconds: 1700000100},
			TimeBase: datasetsv1.TimeBase_TIME_BASE_ABSOLUTE,
		}},
		{Rid: "ri.catalog.main.dataset.b", Name: "Relative", Bounds: &datasetsv1.DataBounds{
			Start:    &timestamppb.Timestamp{Seconds: 5},
			End:      &timestamppb.Timestamp{Seconds: 6},
			TimeBase: datasetsv1.TimeBase_TIME_BASE_RELATIVE,
		}},
		{Rid: "ri.catalog.main.dataset.c", Name: "No end", Bounds: &datasetsv1.DataBounds{
			Start:    &timestamppb.Timestamp{Seconds: 1700000000},
			TimeBase: datasetsv1.TimeBase_TIME_BASE_ABSOLUTE,
		}},
	}
	testDatasetsJSON = `[{"rid":"ri.catalog.main.dataset.a","name":"Absolute","startMs":1700000000250,"endMs":1700000100000},` +
		`{"rid":"ri.catalog.main.dataset.b","name":"Relative"},{"rid":"ri.catalog.main.dataset.c","name":"No end"}]`
)

func TestHandleDatasetsSearch(t *testing.T) {
	notArchived := &datasetsv1.DatasetQuery{Query: &datasetsv1.DatasetQuery_IsArchived{IsArchived: false}}
	text := &datasetsv1.DatasetQuery{Query: &datasetsv1.DatasetQuery_SearchText{SearchText: "abc"}}
	inWorkspace := &datasetsv1.DatasetQuery{Query: &datasetsv1.DatasetQuery_WorkspaceRid{WorkspaceRid: testWorkspaceRid}}
	sent := func(queries ...*datasetsv1.DatasetQuery) *datasetsv1.SearchDatasetsRequest {
		return &datasetsv1.SearchDatasetsRequest{
			Query:    &datasetsv1.DatasetQuery{Query: &datasetsv1.DatasetQuery_And{And: &datasetsv1.DatasetQueryList{Queries: queries}}},
			PageSize: new(int32(50)),
			Sort:     &datasetsv1.DatasetSort{Field: datasetsv1.DatasetSortField_DATASET_SORT_FIELD_LAST_INGESTED_AT, Descending: true},
		}
	}

	tests := []struct {
		name, workspace, request string
		wantSent                 *datasetsv1.SearchDatasetsRequest
	}{
		{"no workspace", "", `{"searchText":" abc "}`, sent(notArchived, text)},
		{"workspace", testWorkspaceRid, `{"searchText":"abc"}`, sent(notArchived, text, inWorkspace)},
		{"blank text omits searchText", "", `{"searchText":"  "}`, sent(notArchived)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &fakeDatasetService{datasets: testDatasets}
			ds := withDatasetService(t, newWorkspaceTestDatasource(t, "", tt.workspace, nil), service)
			resp := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Path: "datasets", Method: http.MethodPost, Body: []byte(tt.request)})
			if resp.Status != http.StatusOK {
				t.Fatalf("status = %d; body = %s", resp.Status, resp.Body)
			}
			if !proto.Equal(service.requests[0], tt.wantSent) {
				t.Errorf("sent %v, want %v", service.requests[0], tt.wantSent)
			}
			if string(resp.Body) != testDatasetsJSON {
				t.Errorf("response = %s, want %s", resp.Body, testDatasetsJSON)
			}
		})
	}
}

func TestHandleDatasetsByRid(t *testing.T) {
	service := &fakeDatasetService{datasets: testDatasets}
	ds := withDatasetService(t, newTestDatasource("", &mockAuthService{}, &mockDatasourceService{}), service)
	rids := []string{"ri.catalog.main.dataset.a", "ri.catalog.main.dataset.missing", "ri.catalog.main.dataset.b", "ri.catalog.main.dataset.c"}
	resp := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Path: "datasets-by-rid", Method: http.MethodPost,
		Body: []byte(`{"rids":["` + rids[0] + `","` + rids[1] + `","` + rids[2] + `","` + rids[3] + `"]}`)})
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d; body = %s", resp.Status, resp.Body)
	}
	if want := (&datasetsv1.BatchGetDatasetsRequest{DatasetRids: rids}); len(service.requests) != 1 || !proto.Equal(service.requests[0], want) {
		t.Errorf("sent %v, want only %v", service.requests, want)
	}
	if !slices.Equal(service.auth, []string{"Bearer test-api-key"}) {
		t.Errorf("authorization = %q, want the API key", service.auth)
	}
	if string(resp.Body) != testDatasetsJSON {
		t.Errorf("response = %s, want %s", resp.Body, testDatasetsJSON)
	}
}

func TestHandleDatasetsPaging(t *testing.T) {
	for _, tc := range []struct {
		name, request     string
		wantPages         []string
		wantSizes         []int32
		wantResponseItems int
	}{
		{"follows tokens to the last page", `{}`, []string{"", "1", "2"}, []int32{50, 49, 48}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &fakeDatasetService{datasets: testDatasets}
			ds := withDatasetService(t, newTestDatasource("", &mockAuthService{}, &mockDatasourceService{}), service)
			resp := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Path: "datasets", Method: http.MethodPost, Body: []byte(tc.request)})
			var pages []string
			var sizes []int32
			for _, req := range service.requests {
				pages = append(pages, req.(*datasetsv1.SearchDatasetsRequest).GetPageToken())
				sizes = append(sizes, req.(*datasetsv1.SearchDatasetsRequest).GetPageSize())
			}
			if !slices.Equal(pages, tc.wantPages) || !slices.Equal(sizes, tc.wantSizes) {
				t.Errorf("page tokens %q sizes %v, want %q %v", pages, sizes, tc.wantPages, tc.wantSizes)
			}
			var items []datasetItem
			if err := json.Unmarshal(resp.Body, &items); err != nil || len(items) != tc.wantResponseItems {
				t.Errorf("response = %s, want %d datasets", resp.Body, tc.wantResponseItems)
			}
		})
	}
}

func TestHandleDatasetsErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		service  *fakeDatasetService
		want     int
		wantBody string
	}{
		{"unauthenticated", &fakeDatasetService{err: status.Error(codes.Unauthenticated, "bad key")}, http.StatusUnauthorized, ""},
		{"unavailable", &fakeDatasetService{err: status.Error(codes.Unavailable, "down")}, http.StatusBadGateway, ""},
		{"rejected", &fakeDatasetService{err: status.Error(codes.InvalidArgument, "bad workspace")}, http.StatusBadRequest, "Check the data source configuration"},
		{"no gRPC connection", nil, http.StatusBadRequest, "https Nominal API base URL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := newTestDatasource("", &mockAuthService{}, &mockDatasourceService{})
			if tc.service != nil {
				ds = withDatasetService(t, ds, tc.service)
			}
			for _, req := range []*backend.CallResourceRequest{
				{Path: "datasets", Method: http.MethodPost, Body: []byte(`{}`)},
				{Path: "datasets-by-rid", Method: http.MethodPost, Body: []byte(`{"rids":["ri.catalog.main.dataset.a"]}`)},
			} {
				if resp := callResourceAndCapture(t, ds, req); resp.Status != tc.want || !strings.Contains(string(resp.Body), tc.wantBody) {
					t.Errorf("%s status = %d, want %d with %q; body = %s", req.Path, resp.Status, tc.want, tc.wantBody, resp.Body)
				}
			}
		})
	}
}

// cancelingDatasetService cancels the caller's request mid-search, as Grafana does when a newer
// dropdown search replaces it.
type cancelingDatasetService struct {
	datasetsv1.UnimplementedDatasetServiceServer
	cancel context.CancelFunc
}

func (c cancelingDatasetService) SearchDatasets(ctx context.Context, _ *datasetsv1.SearchDatasetsRequest) (*datasetsv1.SearchDatasetsResponse, error) {
	c.cancel()
	<-ctx.Done()
	return nil, ctx.Err()
}

type errorCountingLogger struct {
	sdklog.Logger
	errors atomic.Int32
}

func (l *errorCountingLogger) Error(string, ...any) { l.errors.Add(1) }

func TestHandleDatasetsCancelledIsNotAnError(t *testing.T) {
	logger := &errorCountingLogger{Logger: sdklog.NewNullLogger()}
	previous := sdklog.DefaultLogger
	sdklog.DefaultLogger = logger
	t.Cleanup(func() { sdklog.DefaultLogger = previous })

	ctx, cancel := context.WithCancel(context.Background())
	ds := newTestDatasource("", &mockAuthService{}, &mockDatasourceService{})
	conn := newInMemoryConn(t, func(s *grpc.Server) {
		datasetsv1.RegisterDatasetServiceServer(s, cancelingDatasetService{cancel: cancel})
	})
	ds.datasetService = datasetsv1.NewDatasetServiceClient(conn)
	err := ds.CallResource(ctx, &backend.CallResourceRequest{Path: "datasets", Method: http.MethodPost, Body: []byte(`{}`)},
		backend.CallResourceResponseSenderFunc(func(*backend.CallResourceResponse) error { return nil }))
	if err != nil || logger.errors.Load() != 0 {
		t.Errorf("CallResource error = %v, error logs = %d; want neither", err, logger.errors.Load())
	}
}
