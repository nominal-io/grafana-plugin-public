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
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/grafana-plugin-public/pkg/models"
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

type mockDatasourceService struct {
	mu                     sync.Mutex // guards searchChannelsCalls and searchChannelsRequest
	searchChannelsResponse datasourceapi.SearchChannelsResponse
	searchChannelsError    error
	searchChannelsRequest  datasourceapi.SearchChannelsRequest
	searchChannelsCalls    int
	// searchChannelsFunc, when non-nil, overrides the static response and error,
	// so successive calls can differ (pagination).
	searchChannelsFunc func(ctx context.Context, authHeader bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error)
}

func (m *mockDatasourceService) SearchChannels(ctx context.Context, authHeader bearertoken.Token, queryArg datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
	m.mu.Lock()
	m.searchChannelsCalls++
	m.searchChannelsRequest = queryArg
	m.mu.Unlock()
	if m.searchChannelsFunc != nil {
		return m.searchChannelsFunc(ctx, authHeader, queryArg)
	}
	return m.searchChannelsResponse, m.searchChannelsError
}

func (m *mockDatasourceService) searchChannelsCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.searchChannelsCalls
}

func (m *mockDatasourceService) searchChannelsRequestSnapshot() datasourceapi.SearchChannelsRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.searchChannelsRequest
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

var _ authapi.AuthenticationServiceV2Client = (*mockAuthService)(nil)
var _ datasourceservice.DataSourceServiceClient = (*mockDatasourceService)(nil)

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

func newTestAssetServer(t *testing.T, assets map[string]SingleAssetResponse, searchResults []AssetResponse) *httptest.Server {
	t.Helper()
	server, _ := newCountingAssetServer(t, assets, searchResults)
	return server
}

// newCountingAssetServer is newTestAssetServer plus a count of batch asset
// lookups, for tests that assert how often the backend was actually reached.
func newCountingAssetServer(t *testing.T, assets map[string]SingleAssetResponse, searchResults []AssetResponse) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var assetFetches atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/scout/v1/asset/multiple":
			assetFetches.Add(1)
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
	return server, &assetFetches
}

func newTestDatasource(baseURL string, authSvc authapi.AuthenticationServiceV2Client, dsSvc datasourceservice.DataSourceServiceClient) *Datasource {
	return withCatalog(&Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData:                []byte(fmt.Sprintf(`{"baseUrl": "%s"}`, baseURL)),
			DecryptedSecureJSONData: map[string]string{"apiKey": "test-api-key"},
		},
		authService:         authSvc,
		datasourceService:   dsSvc,
		resourceHTTPClient:  &http.Client{},
		defaultSQLWorkspace: newTTLCache[string](catalogCacheTTL),
	})
}

// withCatalog wires a test Datasource to a catalog the way NewDatasource does.
func withCatalog(ds *Datasource) *Datasource {
	ds.nominalCatalog = newNominalCatalog(ds.resourceHTTPClient, ds.datasourceService)
	ds.templateVariableCatalog = newTemplateVariableCatalog(ds.nominalCatalog)
	return ds
}

func newTestQueryExecution(ds *Datasource, config *models.PluginSettings) *NominalQueryExecution {
	if config == nil {
		config = &models.PluginSettings{
			Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
		}
	}
	return newNominalQueryExecution(ds, config)
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

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
	// batchComputeFunc, if set, replaces the static responses. Use it when call
	// order is nondeterministic (parallel batches).
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

// Column order is reversed from production (timestamp first) on purpose, to
// exercise name-based column lookup.
func createTestArrowBucketedNumeric(timestamps []int64, means []float64, nullMask []bool) []byte {
	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "mean", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	tsBuilder := array.NewInt64Builder(pool)
	meanBuilder := array.NewFloat64Builder(pool)
	defer tsBuilder.Release()
	defer meanBuilder.Release()

	for i, ts := range timestamps {
		tsBuilder.Append(ts)
		if nullMask != nil && nullMask[i] {
			meanBuilder.AppendNull()
		} else {
			meanBuilder.Append(means[i])
		}
	}

	tsArr := tsBuilder.NewArray()
	meanArr := meanBuilder.NewArray()
	defer tsArr.Release()
	defer meanArr.Release()

	rec := array.NewRecord(schema, []arrow.Array{tsArr, meanArr}, int64(len(timestamps)))
	defer rec.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		panic(err)
	}
	writer.Close()
	return buf.Bytes()
}

type nullableInt64Values struct {
	values []int64
	nulls  []bool
}

func buildFirstLastArrow(
	tb testing.TB,
	endBucketTs []int64,
	firstValues []float64,
	firstTimestamps nullableInt64Values,
	lastValues []float64,
	lastTimestamps nullableInt64Values,
) []byte {
	tb.Helper()
	rows := len(endBucketTs)
	if len(firstValues) != rows {
		tb.Fatalf("len(firstValues) = %d, want %d", len(firstValues), rows)
	}
	if len(firstTimestamps.values) != rows {
		tb.Fatalf("len(firstTimestamps.values) = %d, want %d", len(firstTimestamps.values), rows)
	}
	if firstTimestamps.nulls != nil && len(firstTimestamps.nulls) != rows {
		tb.Fatalf("len(firstTimestamps.nulls) = %d, want %d", len(firstTimestamps.nulls), rows)
	}
	if len(lastValues) != rows {
		tb.Fatalf("len(lastValues) = %d, want %d", len(lastValues), rows)
	}
	if len(lastTimestamps.values) != rows {
		tb.Fatalf("len(lastTimestamps.values) = %d, want %d", len(lastTimestamps.values), rows)
	}
	if lastTimestamps.nulls != nil && len(lastTimestamps.nulls) != rows {
		tb.Fatalf("len(lastTimestamps.nulls) = %d, want %d", len(lastTimestamps.nulls), rows)
	}

	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "first_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "first_timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "last_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "last_timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	tsBuilder := array.NewInt64Builder(pool)
	firstValueBuilder := array.NewFloat64Builder(pool)
	firstTimestampBuilder := array.NewInt64Builder(pool)
	lastValueBuilder := array.NewFloat64Builder(pool)
	lastTimestampBuilder := array.NewInt64Builder(pool)
	for row := 0; row < rows; row++ {
		tsBuilder.Append(endBucketTs[row])
		firstValueBuilder.Append(firstValues[row])
		if firstTimestamps.nulls != nil && firstTimestamps.nulls[row] {
			firstTimestampBuilder.AppendNull()
		} else {
			firstTimestampBuilder.Append(firstTimestamps.values[row])
		}
		lastValueBuilder.Append(lastValues[row])
		if lastTimestamps.nulls != nil && lastTimestamps.nulls[row] {
			lastTimestampBuilder.AppendNull()
		} else {
			lastTimestampBuilder.Append(lastTimestamps.values[row])
		}
	}

	tsArr := tsBuilder.NewArray()
	firstValueArr := firstValueBuilder.NewArray()
	firstTimestampArr := firstTimestampBuilder.NewArray()
	lastValueArr := lastValueBuilder.NewArray()
	lastTimestampArr := lastTimestampBuilder.NewArray()
	tsBuilder.Release()
	firstValueBuilder.Release()
	firstTimestampBuilder.Release()
	lastValueBuilder.Release()
	lastTimestampBuilder.Release()

	rec := array.NewRecord(schema, []arrow.Array{
		tsArr,
		firstValueArr,
		firstTimestampArr,
		lastValueArr,
		lastTimestampArr,
	}, int64(rows))
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		tb.Fatalf("write FIRST/LAST Arrow record: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("close FIRST/LAST Arrow writer: %v", err)
	}

	rec.Release()
	tsArr.Release()
	firstValueArr.Release()
	firstTimestampArr.Release()
	lastValueArr.Release()
	lastTimestampArr.Release()

	return buf.Bytes()
}

type testArrowMultiAggNullPattern func(row int, column int) bool

// Arrow buffer with end_bucket_timestamp plus named float64 columns ("mean", "min", ...).
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

// A nil timestamps slice yields one ascending minute-spaced timestamp per message.
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

func createMockErrorResult(code int, errorType string) computeapi.ComputeWithUnitsResult {
	errorResult := computeapi.ErrorResult{
		Code:      computeapi.ErrorCode(code),
		ErrorType: computeapi.ErrorType(errorType),
	}

	computeResult := computeapi.NewComputeNodeResultFromError(errorResult)

	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

// Mirrors production: numeric queries send OutputFormat=ARROW_V3 and receive
// an ArrowBucketedNumericPlot with a mean column.
func createMockArrowComputeResult(values []float64) computeapi.ComputeWithUnitsResult {
	baseTime := int64(1704067200000000000) // 2024-01-01 00:00:00 UTC in nanos
	timestamps := make([]int64, len(values))
	for i := range timestamps {
		timestamps[i] = baseTime + int64(i*60)*1_000_000_000
	}
	arrowBytes := createTestArrowBucketedNumeric(timestamps, values, nil)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	computeResponse := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)
	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

func createMockEnumPointComputeResult(value string) computeapi.ComputeWithUnitsResult {
	enumPoint := computeapi.EnumPoint{
		Timestamp: api.Timestamp{
			Seconds: safelong.SafeLong(1704067200),
			Nanos:   safelong.SafeLong(0),
		},
		Value: value,
	}

	computeResponse := computeapi.NewComputeNodeResponseFromEnumPoint(&enumPoint)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)

	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}

// Arrow buffer in the FIRST_POINT/LAST_POINT schema: end_bucket_timestamp,
// first_value, first_timestamp, last_value, last_timestamp.
func createTestArrowFirstLast(
	tb testing.TB,
	endBucketTs []int64,
	firstValues []float64, firstTimestamps []int64,
	lastValues []float64, lastTimestamps []int64,
) []byte {
	tb.Helper()
	return buildFirstLastArrow(tb, endBucketTs, firstValues, nullableInt64Values{
		values: firstTimestamps,
	}, lastValues, nullableInt64Values{
		values: lastTimestamps,
	})
}

func createMockLogPointResult(message string, args map[string]string) computeapi.ComputeWithUnitsResult {
	logPoint := computeapi.LogPoint{
		Timestamp: api.Timestamp{
			Seconds: safelong.SafeLong(1704067200),
			Nanos:   safelong.SafeLong(0),
		},
		Value: computeapi.LogValue{
			Message: message,
			Id:      [16]byte{0x01},
			Args:    args,
		},
	}
	computeResponse := computeapi.NewComputeNodeResponseFromLogPoint(&logPoint)
	computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)
	return computeapi.ComputeWithUnitsResult{
		ComputeResult: computeResult,
	}
}
