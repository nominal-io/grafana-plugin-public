package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	authapi "github.com/nominal-io/nominal-api-go/authentication/api"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	datasourceservice "github.com/nominal-io/nominal-api-go/scout/datasource"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/safelong"
	"github.com/palantir/pkg/uuid"
)

// ============================================================================
// Mock services for CallResource handler tests
// ============================================================================

// mockAuthService implements authapi.AuthenticationServiceV2Client for testing
type mockAuthService struct {
	getMyProfileResponse authapi.UserV2
	getMyProfileError    error
}

func (m *mockAuthService) GetMyProfile(ctx context.Context, authHeader bearertoken.Token) (authapi.UserV2, error) {
	return m.getMyProfileResponse, m.getMyProfileError
}

func (m *mockAuthService) UpdateMyProfile(ctx context.Context, authHeader bearertoken.Token, req authapi.UpdateMyProfileRequest) (authapi.UserV2, error) {
	return authapi.UserV2{}, nil
}

func (m *mockAuthService) GetMySettings(ctx context.Context, authHeader bearertoken.Token) (authapi.UserSettings, error) {
	return authapi.UserSettings{}, nil
}

func (m *mockAuthService) UpdateMySettings(ctx context.Context, authHeader bearertoken.Token, settings authapi.UserSettings) (authapi.UserSettings, error) {
	return authapi.UserSettings{}, nil
}

func (m *mockAuthService) GetMyOrgSettings(ctx context.Context, authHeader bearertoken.Token) (authapi.OrgSettings, error) {
	return authapi.OrgSettings{}, nil
}

func (m *mockAuthService) UpdateMyOrgSettings(ctx context.Context, authHeader bearertoken.Token, settings authapi.OrgSettings) (authapi.OrgSettings, error) {
	return authapi.OrgSettings{}, nil
}

func (m *mockAuthService) SearchUsersV2(ctx context.Context, authHeader bearertoken.Token, req authapi.SearchUsersRequest) (authapi.SearchUsersResponseV2, error) {
	return authapi.SearchUsersResponseV2{}, nil
}

func (m *mockAuthService) GetUsers(ctx context.Context, authHeader bearertoken.Token, userRids []authapi.UserRid) ([]authapi.UserV2, error) {
	return nil, nil
}

func (m *mockAuthService) GetUser(ctx context.Context, authHeader bearertoken.Token, userRid authapi.UserRid) (authapi.UserV2, error) {
	return authapi.UserV2{}, nil
}

func (m *mockAuthService) DismissMyCoachmark(ctx context.Context, authHeader bearertoken.Token, requestArg authapi.DismissCoachmarkRequest) (authapi.CoachmarkDismissal, error) {
	return authapi.CoachmarkDismissal{}, nil
}

func (m *mockAuthService) IsMyCoachmarkDismissed(ctx context.Context, authHeader bearertoken.Token, coachmarkIdArg string) (bool, error) {
	return false, nil
}

func (m *mockAuthService) GetJwks(ctx context.Context) (authapi.Jwks, error) {
	return authapi.Jwks{}, nil
}

func (m *mockAuthService) GenerateMediaMtxToken(ctx context.Context, authHeader bearertoken.Token, requestArg authapi.GenerateMediaMtxTokenRequest) (authapi.GenerateMediaMtxTokenResponse, error) {
	return authapi.GenerateMediaMtxTokenResponse{}, nil
}

func (m *mockAuthService) GetMyCoachmarkDismissals(ctx context.Context, authHeader bearertoken.Token, requestArg authapi.GetCoachmarkDismissalsRequest) (authapi.GetCoachmarkDismissalsResponse, error) {
	return authapi.GetCoachmarkDismissalsResponse{}, nil
}

func (m *mockAuthService) ResetMyCoachmarkDismissal(ctx context.Context, authHeader bearertoken.Token, coachmarkIdArg string) error {
	return nil
}

// mockDatasourceService implements datasourceservice.DataSourceServiceClient for testing
type mockDatasourceService struct {
	searchChannelsResponse datasourceapi.SearchChannelsResponse
	searchChannelsError    error
	searchChannelsRequest  datasourceapi.SearchChannelsRequest
	searchChannelsCalls    int
	// searchChannelsFunc, when non-nil, overrides searchChannelsResponse/searchChannelsError.
	// This allows tests to return different responses on successive calls (e.g. pagination).
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

// Verify mock types implement their interfaces at compile time
var _ authapi.AuthenticationServiceV2Client = (*mockAuthService)(nil)
var _ datasourceservice.DataSourceServiceClient = (*mockDatasourceService)(nil)

// callResourceAndCapture is a test helper that calls CallResource and captures the response
func callResourceAndCapture(t *testing.T, ds *Datasource, req *backend.CallResourceRequest) *backend.CallResourceResponse {
	t.Helper()
	var captured *backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(resp *backend.CallResourceResponse) error {
		captured = resp
		return nil
	})
	err := ds.CallResource(context.Background(), req, sender)
	if err != nil {
		t.Fatalf("CallResource returned error: %v", err)
	}
	if captured == nil {
		t.Fatal("CallResource did not send a response")
	}
	return captured
}

// newTestAssetServer creates an httptest server that handles asset-related API endpoints.
// It returns the server (caller must defer Close) and configures:
//   - POST /scout/v1/asset/multiple — batch asset lookup by RID
//   - POST /scout/v1/search-assets — paginated asset search
func newTestAssetServer(t *testing.T, assets map[string]SingleAssetResponse, searchResults []AssetResponse) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/scout/v1/asset/multiple":
			var rids []string
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &rids); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			result := make(map[string]SingleAssetResponse)
			for _, rid := range rids {
				if asset, ok := assets[rid]; ok {
					result[rid] = asset
				}
			}
			json.NewEncoder(w).Encode(result)

		case "/scout/v1/search-assets":
			if len(searchResults) > 0 {
				json.NewEncoder(w).Encode(searchResults[0])
			} else {
				json.NewEncoder(w).Encode(AssetResponse{})
			}

		default:
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		}
	}))
}

// newTestDatasource creates a Datasource for testing CallResource handlers.
func newTestDatasource(baseURL string, authSvc authapi.AuthenticationServiceV2Client, dsSvc datasourceservice.DataSourceServiceClient) *Datasource {
	return &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData:                []byte(fmt.Sprintf(`{"baseUrl": "%s"}`, baseURL)),
			DecryptedSecureJSONData: map[string]string{"apiKey": "test-api-key"},
		},
		authService:        authSvc,
		datasourceService:  dsSvc,
		resourceHTTPClient: &http.Client{},
	}
}

func newTestQueryExecution(ds *Datasource, config *models.PluginSettings) *NominalQueryExecution {
	if config == nil {
		config = &models.PluginSettings{
			Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
		}
	}
	return newNominalQueryExecution(ds, config)
}

// mustMarshal is a test helper that panics on marshal failure
func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// mockComputeService implements computeapi1.ComputeServiceClient for testing
type mockComputeService struct {
	mu                    sync.Mutex
	batchComputeCalls     int
	lastBatchRequest      computeapi1.BatchComputeWithUnitsRequest
	batchRequests         []computeapi1.BatchComputeWithUnitsRequest
	batchComputeResponse  computeapi.BatchComputeWithUnitsResponse
	batchComputeResponses []computeapi.BatchComputeWithUnitsResponse
	batchComputeError     error
	batchComputeErrors    []error
	singleComputeCalls    int
	// batchComputeFunc, if set, is called instead of using the static responses.
	// Useful for tests with nondeterministic call ordering (e.g. parallel batches).
	batchComputeFunc func(requestArg computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error)

	killCalls []killCall
}

// killCall records what one BatchKillRequests carried, including the identity
// on its context.
type killCall struct {
	ids []uuid.UUID
	ua  userAgentComponents
}

func (m *mockComputeService) Compute(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ComputeNodeRequest) (computeapi.ComputeNodeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.singleComputeCalls++
	return computeapi.ComputeNodeResponse{}, nil
}

func (m *mockComputeService) ParameterizedCompute(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ParameterizedComputeNodeRequest) (computeapi.ParameterizedComputeNodeResponse, error) {
	return computeapi.ParameterizedComputeNodeResponse{}, nil
}

func (m *mockComputeService) ComputeUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ComputeUnitsRequest) (computeapi.ComputeUnitResult, error) {
	return computeapi.ComputeUnitResult{}, nil
}

func (m *mockComputeService) BatchComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
	m.mu.Lock()
	m.batchComputeCalls++
	m.lastBatchRequest = requestArg
	m.batchRequests = append(m.batchRequests, requestArg)
	callback := m.batchComputeFunc
	callIndex := m.batchComputeCalls - 1
	var indexedErr error
	if callIndex < len(m.batchComputeErrors) {
		indexedErr = m.batchComputeErrors[callIndex]
	}
	var indexedResp *computeapi.BatchComputeWithUnitsResponse
	if callIndex < len(m.batchComputeResponses) {
		indexedResp = &m.batchComputeResponses[callIndex]
	}
	fallbackErr := m.batchComputeError
	fallbackResp := m.batchComputeResponse
	m.mu.Unlock()

	// A blocking callback must not hold the mock mutex: BatchKillRequests and
	// the snapshot helpers take it, so a callback that waits on a kill landing
	// would deadlock.
	if callback != nil {
		return callback(requestArg)
	}

	if indexedErr != nil {
		return computeapi.BatchComputeWithUnitsResponse{}, indexedErr
	}
	if indexedResp != nil {
		return *indexedResp, nil
	}
	if fallbackErr != nil {
		return computeapi.BatchComputeWithUnitsResponse{}, fallbackErr
	}
	return fallbackResp, nil
}

func (m *mockComputeService) BatchComputeUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.BatchComputeUnitsRequest) (computeapi.BatchComputeUnitResult, error) {
	return computeapi.BatchComputeUnitResult{}, nil
}

func (m *mockComputeService) ComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ComputeWithUnitsRequest) (computeapi.ComputeWithUnitsResponse, error) {
	return computeapi.ComputeWithUnitsResponse{}, nil
}

func (m *mockComputeService) BatchKillRequests(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi.BatchKillRequestsRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ua, _ := userAgentComponentsFromContext(ctx)
	m.killCalls = append(m.killCalls, killCall{
		ids: append([]uuid.UUID(nil), requestArg.RequestIds...),
		ua:  ua,
	})
	return nil
}

func (m *mockComputeService) killCallsSnapshot() []killCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]killCall(nil), m.killCalls...)
}

// createMockComputeResult creates a mock ComputeWithUnitsResult with numeric data
func createMockComputeResult(values []float64) computeapi.ComputeWithUnitsResult {
	timestamps := make([]api.Timestamp, len(values))
	baseTime := int64(1704067200) // 2024-01-01 00:00:00 UTC
	for i := range timestamps {
		timestamps[i] = api.Timestamp{
			Seconds: safelong.SafeLong(baseTime + int64(i*60)),
			Nanos:   safelong.SafeLong(0),
		}
	}

	numericPlot := computeapi.NumericPlot{
		Timestamps: timestamps,
		Values:     values,
	}

	computeResponse := computeapi.NewComputeNodeResponseFromNumeric(numericPlot)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)

	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

// createMockEnumComputeResult creates a mock ComputeWithUnitsResult with enum data
func createMockEnumComputeResult(categories []string, indices []int) computeapi.ComputeWithUnitsResult {
	timestamps := make([]api.Timestamp, len(indices))
	baseTime := int64(1704067200) // 2024-01-01 00:00:00 UTC
	for i := range timestamps {
		timestamps[i] = api.Timestamp{
			Seconds: safelong.SafeLong(baseTime + int64(i*60)),
			Nanos:   safelong.SafeLong(0),
		}
	}

	enumPlot := computeapi.EnumPlot{
		Timestamps: timestamps,
		Values:     indices,
		Categories: categories,
	}

	computeResponse := computeapi.NewComputeNodeResponseFromEnum(enumPlot)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)

	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

// newCountingAssetServer is like newTestAssetServer but also counts requests
// to the /scout/v1/asset/multiple endpoint.
func newCountingAssetServer(t *testing.T, assets map[string]SingleAssetResponse, fetchCount *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/scout/v1/asset/multiple" {
			*fetchCount++
			var rids []string
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &rids); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			result := make(map[string]SingleAssetResponse)
			for _, rid := range rids {
				if asset, ok := assets[rid]; ok {
					result[rid] = asset
				}
			}
			json.NewEncoder(w).Encode(result)
		} else {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		}
	}))
}

type testArrowMultiAggNullPattern func(row int, column int) bool

// createTestArrowMultiAgg builds an Arrow IPC buffer with end_bucket_timestamp
// plus multiple named float64 columns (e.g. "mean", "min", "max").
func createTestArrowMultiAgg(timestamps []int64, columns map[string][]float64) []byte {
	return createTestArrowMultiAggWithNullPattern(nil, timestamps, columns, nil)
}

func createTestArrowMultiAggWithNullPattern(
	tb testing.TB,
	timestamps []int64,
	columns map[string][]float64,
	nullPattern testArrowMultiAggNullPattern,
) []byte {
	if tb != nil {
		tb.Helper()
	}
	failf := func(format string, args ...any) {
		if tb != nil {
			tb.Fatalf(format, args...)
		}
		panic(fmt.Sprintf(format, args...))
	}

	pool := memory.DefaultAllocator
	fields := []arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
	}
	// Deterministic column order across all standard aggregations.
	colOrder := []string{"mean", "min", "max", "count", "variance"}
	var orderedNames []string
	for _, name := range colOrder {
		if _, ok := columns[name]; ok {
			orderedNames = append(orderedNames, name)
		}
	}
	if len(orderedNames) != len(columns) {
		failf("unsupported multi-aggregation fixture columns; supported columns are %v", colOrder)
	}
	for _, name := range orderedNames {
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: true})
	}
	schema := arrow.NewSchema(fields, nil)

	tsBuilder := array.NewInt64Builder(pool)
	defer tsBuilder.Release()
	for _, ts := range timestamps {
		tsBuilder.Append(ts)
	}
	tsArr := tsBuilder.NewArray()
	defer tsArr.Release()

	arrays := []arrow.Array{tsArr}
	for column, name := range orderedNames {
		if len(columns[name]) != len(timestamps) {
			failf("column %q has %d values, want %d", name, len(columns[name]), len(timestamps))
		}
		b := array.NewFloat64Builder(pool)
		for row, v := range columns[name] {
			if nullPattern != nil && nullPattern(row, column) {
				b.AppendNull()
				continue
			}
			b.Append(v)
		}
		arr := b.NewArray()
		defer arr.Release()
		arrays = append(arrays, arr)
		b.Release()
	}

	rec := array.NewRecord(schema, arrays, int64(len(timestamps)))
	defer rec.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		failf("write multi-aggregation Arrow record: %v", err)
	}
	if err := writer.Close(); err != nil {
		failf("close multi-aggregation Arrow writer: %v", err)
	}
	return buf.Bytes()
}

// testTimestamp builds an api.Timestamp at whole-second precision.
func testTimestamp(seconds int64) api.Timestamp {
	return api.Timestamp{
		Seconds: safelong.SafeLong(seconds),
		Nanos:   safelong.SafeLong(0),
	}
}

// createMockPagedLogResult creates a mock ComputeWithUnitsResult with paged log data.
// A nil timestamps slice auto-generates one ascending minute-spaced timestamp per
// message; pass timestamps explicitly to control ordering or length mismatches.
func createMockPagedLogResult(messages []string, args []map[string]string, timestamps []api.Timestamp) computeapi.ComputeWithUnitsResult {
	baseTime := int64(1704067200) // 2024-01-01 00:00:00 UTC
	if timestamps == nil {
		timestamps = make([]api.Timestamp, len(messages))
		for i := range messages {
			timestamps[i] = testTimestamp(baseTime + int64(i*60))
		}
	}
	values := make([]computeapi.LogValue, len(messages))
	for i, msg := range messages {
		values[i] = computeapi.LogValue{
			Message: msg,
			Id:      [16]byte{byte(i)},
		}
		if args != nil && i < len(args) {
			values[i].Args = args[i]
		}
	}
	pagedLog := computeapi.PagedLogPlot{
		Timestamps: timestamps,
		Values:     values,
	}
	computeResponse := computeapi.NewComputeNodeResponseFromPagedLog(pagedLog)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)
	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

// strPtr is a helper to create a *string
func strPtr(s string) *string {
	return &s
}

func newQueryRequestForURL(baseURL string, queries []backend.DataQuery) *backend.QueryDataRequest {
	return &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(fmt.Sprintf(`{"baseUrl":%q}`, baseURL)),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}
}

const testBaseURL = "https://api.test.com"

func testDatasourceSettings() backend.DataSourceInstanceSettings {
	return backend.DataSourceInstanceSettings{JSONData: []byte(fmt.Sprintf(`{"baseUrl":%q}`, testBaseURL))}
}

func newQueryRequest(queries []backend.DataQuery) *backend.QueryDataRequest {
	return newQueryRequestForURL(testBaseURL, queries)
}
