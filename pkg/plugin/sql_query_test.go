package plugin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdkconfig "github.com/grafana/grafana-plugin-sdk-go/config"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	workspaceapi "github.com/nominal-io/nominal-api-go/security/api/workspace"
	sqlv1 "github.com/nominal-io/nominal-api-protos-go/nominal/protos/sql/v1"
	"github.com/palantir/pkg/bearertoken"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func sqlTestWorkspaceRid(t *testing.T) rids.WorkspaceRid {
	t.Helper()
	workspace, err := parseWorkspaceRid(testWorkspaceRid)
	if err != nil {
		t.Fatalf("parseWorkspaceRid() error = %v", err)
	}
	return *workspace
}

func sqlTestExecution(t *testing.T, service sqlv1.SqlServiceClient) *NominalQueryExecution {
	t.Helper()
	workspace := sqlTestWorkspaceRid(t)
	return newNominalQueryExecution(&Datasource{workspaceRid: &workspace, sqlService: service}, &models.PluginSettings{Secrets: &models.SecretPluginSettings{ApiKey: "k"}})
}

func sqlQueryService(t *testing.T, query sqlQueryHandler) sqlv1.SqlServiceClient {
	t.Helper()
	return newFakeSQLService(t, &fakeSQLService{query: query})
}

// sqlStatusError builds an error shaped like the SQL service's: a generic status message plus an
// ErrorInfo whose metadata carries the query ID and a specific detail.
func sqlStatusError(t *testing.T, code codes.Code, message, queryID, detail string) error {
	t.Helper()
	st, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{
		Reason:   "SQL_ERROR",
		Domain:   "nominal.sql.v1",
		Metadata: map[string]string{"sqlQueryId": queryID, "detail": detail},
	})
	if err != nil {
		t.Fatalf("status.WithDetails() error = %v", err)
	}
	return st.Err()
}

// sqlPayloadServer streams payload as a single response message.
func sqlPayloadServer(payload []byte) sqlQueryHandler {
	return func(_ *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		return stream.Send(&sqlv1.SqlServiceQueryResponse{QueryId: "q", Payload: payload})
	}
}

func sqlValueStream(t *testing.T, value float64) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64}}, nil)
	return sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) { b.Field(0).(*array.Float64Builder).Append(value) })
}

func executeTestSQLQuery(t *testing.T, ctx context.Context, e *NominalQueryExecution, queryJSON string) backend.DataResponse {
	t.Helper()
	prepared, response := e.prepareQuery(context.Background(), backend.DataQuery{RefID: "A", JSON: []byte(queryJSON)})
	if response != nil {
		t.Fatalf("prepareQuery() error = %v", response.Error)
	}
	return e.executeSQLQuery(ctx, prepared)
}

func TestPrepareQuerySQL(t *testing.T) {
	e := newTestQueryExecution(&Datasource{}, nil)
	for _, tc := range []struct {
		name       string
		json       string
		wantFormat sqlutil.FormatQueryOption
	}{
		{name: "table", json: `{"queryType":"sql","rawSql":"SELECT 1","format":"table"}`, wantFormat: sqlutil.FormatOptionTable},
		{name: "time series", json: `{"queryType":"sql","rawSql":"SELECT 1","format":"timeseries"}`, wantFormat: sqlutil.FormatOptionTimeSeries},
		{name: "Grafana time_series spelling", json: `{"queryType":"sql","rawSql":"SELECT 1","format":"time_series"}`, wantFormat: sqlutil.FormatOptionTimeSeries},
		{name: "no format", json: `{"queryType":"sql","rawSql":"SELECT 1"}`, wantFormat: sqlutil.FormatOptionTimeSeries},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := backend.DataQuery{RefID: "A", Interval: time.Minute, JSON: []byte(tc.json)}
			prepared, response := e.prepareQuery(context.Background(), q)
			if response != nil {
				t.Fatalf("prepareQuery() error = %v", response.Error)
			}
			if prepared.Kind != preparedQuerySQL || prepared.SQL == nil {
				t.Fatalf("prepareQuery() = %+v, want a SQL query", prepared)
			}
			if got := *prepared.SQL; got.RawSQL != "SELECT 1" || got.Format != tc.wantFormat || got.RefID != "A" || got.Interval != time.Minute {
				t.Errorf("prepared SQL = %+v, want SELECT 1 with format %v for A", got, tc.wantFormat)
			}
		})
	}
	_, response := e.prepareQuery(context.Background(), backend.DataQuery{JSON: []byte(`{"queryType":"sql","rawSql":" "}`)})
	if response == nil || response.Error.Error() != "SQL query is empty" {
		t.Errorf("prepareQuery(blank SQL) = %v, want %q", response, "SQL query is empty")
	}
}

func TestResolveSQLWorkspace(t *testing.T) {
	t.Run("caches the default workspace", func(t *testing.T) {
		service := &mockWorkspaceService{defaultWorkspace: &workspaceapi.Workspace{Rid: sqlTestWorkspaceRid(t)}}
		ds := &Datasource{workspaceService: service}
		for range 2 {
			if got, err := ds.resolveSQLWorkspace(context.Background(), "k"); err != nil || got != testWorkspaceRid {
				t.Fatalf("resolveSQLWorkspace() = %q, %v; want %q", got, err, testWorkspaceRid)
			}
		}
		if service.defaultCalls != 1 {
			t.Errorf("GetDefaultWorkspace calls = %d, want 1", service.defaultCalls)
		}
	})
	t.Run("prefers the configured workspace", func(t *testing.T) {
		workspace := sqlTestWorkspaceRid(t)
		service := &mockWorkspaceService{}
		ds := &Datasource{workspaceRid: &workspace, workspaceService: service}
		if got, err := ds.resolveSQLWorkspace(context.Background(), "k"); err != nil || got != testWorkspaceRid || service.defaultCalls != 0 {
			t.Errorf("resolveSQLWorkspace() = %q, %v after %d lookups; want %q without a lookup", got, err, service.defaultCalls, testWorkspaceRid)
		}
	})
	t.Run("reports a missing default workspace", func(t *testing.T) {
		ds := &Datasource{workspaceService: &mockWorkspaceService{}}
		if _, err := ds.resolveSQLWorkspace(context.Background(), "k"); !errors.Is(err, errNoSQLWorkspace) {
			t.Errorf("resolveSQLWorkspace() error = %v, want errNoSQLWorkspace", err)
		}
	})
	t.Run("retries after a failed lookup", func(t *testing.T) {
		service := &mockWorkspaceService{}
		service.defaultFunc = func() (*workspaceapi.Workspace, error) {
			if service.defaultCalls == 1 {
				return nil, errors.New("unavailable")
			}
			return &workspaceapi.Workspace{Rid: sqlTestWorkspaceRid(t)}, nil
		}
		ds := &Datasource{workspaceService: service}
		if _, err := ds.resolveSQLWorkspace(context.Background(), "k"); err == nil {
			t.Fatal("resolveSQLWorkspace() error = nil on the failed lookup")
		}
		if got, err := ds.resolveSQLWorkspace(context.Background(), "k"); err != nil || got != testWorkspaceRid {
			t.Errorf("resolveSQLWorkspace() after a failure = %q, %v; want %q", got, err, testWorkspaceRid)
		}
	})
}

func TestExecuteSQLQuery(t *testing.T) {
	var request *sqlv1.SqlServiceQueryRequest
	var authorization []string
	e := sqlTestExecution(t, sqlQueryService(t, func(req *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		request = req
		md, _ := metadata.FromIncomingContext(stream.Context())
		authorization = md.Get("authorization")
		return sqlPayloadServer(sqlValueStream(t, 42))(req, stream)
	}))
	for _, tc := range []struct {
		format  string
		wantVis data.VisType
	}{
		{format: "table", wantVis: data.VisTypeTable},
		{format: "timeseries", wantVis: data.VisTypeGraph},
	} {
		t.Run(tc.format, func(t *testing.T) {
			response := executeTestSQLQuery(t, context.Background(), e, `{"queryType":"sql","rawSql":"SELECT 42","format":"`+tc.format+`"}`)
			if response.Error != nil || len(response.Frames) != 1 {
				t.Fatalf("executeSQLQuery() = %v, %d frames; want one frame", response.Error, len(response.Frames))
			}
			frame := response.Frames[0]
			if frame.RefID != "A" || frame.Meta.ExecutedQueryString != "SELECT 42" || frame.Meta.PreferredVisualization != tc.wantVis {
				t.Errorf("frame RefID, meta = %q, %+v; want A with SELECT 42 and %s", frame.RefID, frame.Meta, tc.wantVis)
			}
		})
	}
	if request.GetQuery() != "SELECT 42" || request.GetWorkspaceRid() != testWorkspaceRid || request.MaxRows != nil {
		t.Errorf("request = %v, want SELECT 42 in %s without a row cap", request, testWorkspaceRid)
	}
	if want := sqlv1.SqlServiceQueryResultFormat_SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM; request.GetResultFormat() != want {
		t.Errorf("request result format = %v, want %v", request.GetResultFormat(), want)
	}
	if len(authorization) != 1 || authorization[0] != "Bearer k" {
		t.Errorf("authorization metadata = %q, want [%q]", authorization, "Bearer k")
	}
}

func TestExecuteSQLQueryAppliesGrafanaRowLimit(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	stream := sqlArrowStream(t, schema, 1, func(b *array.RecordBuilder) { b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil) })
	e := sqlTestExecution(t, sqlQueryService(t, sqlPayloadServer(stream)))
	ctx := sdkconfig.WithGrafanaConfig(context.Background(), sdkconfig.NewGrafanaCfg(map[string]string{
		sdkconfig.SQLRowLimit:                      "2",
		sdkconfig.SQLMaxOpenConnsDefault:           "100",
		sdkconfig.SQLMaxIdleConnsDefault:           "100",
		sdkconfig.SQLMaxConnLifetimeSecondsDefault: "14400",
	}))
	response := executeTestSQLQuery(t, ctx, e, `{"queryType":"sql","rawSql":"SELECT v","format":"table"}`)
	if response.Error != nil || response.Frames[0].Rows() != 2 {
		t.Fatalf("executeSQLQuery() = %v; want 2 rows", response.Error)
	}
	if meta := response.Frames[0].Meta; len(meta.Notices) != 1 || meta.ExecutedQueryString != "SELECT v" {
		t.Errorf("frame meta = %+v, want the row-limit notice and the executed query", meta)
	}
}

func TestExecuteSQLQueryErrors(t *testing.T) {
	invalid := sqlStatusError(t, codes.InvalidArgument, "SQL query is invalid", "q-1", "bad query")
	throttled := sqlStatusError(t, codes.ResourceExhausted, "throttled", "q-2", "")
	for _, tc := range []struct {
		name       string
		handler    sqlQueryHandler
		timeout    time.Duration
		wantStatus backend.Status
		wantMsg    string
	}{
		{
			name: "rejected query",
			handler: func(*sqlv1.SqlServiceQueryRequest, grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
				return invalid
			},
			wantStatus: backend.StatusBadRequest,
			wantMsg:    "bad query (sqlQueryId: q-1)",
		},
		{
			name: "failure after the first batch",
			handler: func(_ *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
				payload := sqlValueStream(t, 1)
				if err := stream.Send(&sqlv1.SqlServiceQueryResponse{Payload: payload[:len(payload)-8]}); err != nil {
					return err
				}
				return throttled
			},
			wantStatus: backend.StatusTooManyRequests,
			wantMsg:    "throttled (sqlQueryId: q-2)",
		},
		{
			name: "Grafana deadline",
			handler: func(_ *sqlv1.SqlServiceQueryRequest, stream grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
				<-stream.Context().Done()
				return stream.Context().Err()
			},
			timeout:    50 * time.Millisecond,
			wantStatus: backend.StatusTimeout,
			wantMsg:    "SQL query timed out",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.timeout)
				defer cancel()
			}
			response := executeTestSQLQuery(t, ctx, sqlTestExecution(t, sqlQueryService(t, tc.handler)), `{"queryType":"sql","rawSql":"SELECT x FROM t WHERE $__timeFilter(ts)"}`)
			if response.Status != tc.wantStatus || response.ErrorSource != backend.ErrorSourceDownstream {
				t.Errorf("status, source = %v, %q; want %v, downstream", response.Status, response.ErrorSource, tc.wantStatus)
			}
			if response.Error == nil || response.Error.Error() != tc.wantMsg {
				t.Errorf("error = %v, want %q", response.Error, tc.wantMsg)
			}
			if len(response.Frames) != 1 || response.Frames[0].Meta.ExecutedQueryString == "" {
				t.Errorf("frames = %v, want one frame with the expanded SQL for the query inspector", response.Frames)
			}
		})
	}
}

func TestSQLErrorResponse(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithTimeout(context.Background(), 0)
	defer cancelExpired()
	invalid := sqlStatusError(t, codes.InvalidArgument, "SQL query is invalid", "q-2", "Column 'x' not found")
	for _, tc := range []struct {
		name       string
		ctx        context.Context
		err        error
		wantStatus backend.Status
		wantSource backend.ErrorSource
		wantMsg    string
	}{
		{name: "cancelled by Grafana", ctx: cancelled, err: status.Error(codes.Canceled, "canceled"), wantStatus: backend.StatusInternal, wantSource: backend.ErrorSourceDownstream, wantMsg: "SQL query was cancelled"},
		{name: "Grafana deadline", ctx: expired, err: status.Error(codes.DeadlineExceeded, "deadline"), wantStatus: backend.StatusTimeout, wantSource: backend.ErrorSourceDownstream, wantMsg: "SQL query timed out"},
		{name: "rejected query", ctx: context.Background(), err: invalid, wantStatus: backend.StatusBadRequest, wantSource: backend.ErrorSourceDownstream, wantMsg: "Column 'x' not found (sqlQueryId: q-2)"},
		{name: "rejected mid-stream", ctx: context.Background(), err: fmt.Errorf("failed to read Arrow stream: %w", invalid), wantStatus: backend.StatusBadRequest, wantSource: backend.ErrorSourceDownstream, wantMsg: "Column 'x' not found (sqlQueryId: q-2)"},
		{name: "server time limit", ctx: context.Background(), err: sqlStatusError(t, codes.DeadlineExceeded, "SQL query timed out", "q-3", ""), wantStatus: backend.StatusTimeout, wantSource: backend.ErrorSourceDownstream, wantMsg: "SQL query timed out (sqlQueryId: q-3)"},
		{name: "status without details", ctx: context.Background(), err: status.Error(codes.Unauthenticated, ""), wantStatus: backend.StatusUnauthorized, wantSource: backend.ErrorSourceDownstream, wantMsg: "SQL endpoint returned Unauthenticated"},
		{name: "plugin error", ctx: context.Background(), err: errors.New("SQL response was empty"), wantStatus: backend.StatusInternal, wantMsg: "SQL response was empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := sqlErrorResponse(tc.ctx, tc.err)
			if response.Status != tc.wantStatus || response.ErrorSource != tc.wantSource {
				t.Errorf("status, source = %v, %q; want %v, %q", response.Status, response.ErrorSource, tc.wantStatus, tc.wantSource)
			}
			if response.Error == nil || response.Error.Error() != tc.wantMsg {
				t.Errorf("error = %v, want %q", response.Error, tc.wantMsg)
			}
		})
	}
}

func TestExecuteSQLQueryReportsUnusableBaseURL(t *testing.T) {
	_, sqlErr := dialSQL("http://example/api", "test")
	if sqlErr == nil {
		t.Fatal("dialSQL(http URL) error = nil, want an error")
	}
	workspace := sqlTestWorkspaceRid(t)
	e := newNominalQueryExecution(&Datasource{workspaceRid: &workspace, sqlErr: sqlErr}, &models.PluginSettings{Secrets: &models.SecretPluginSettings{ApiKey: "k"}})
	response := executeTestSQLQuery(t, context.Background(), e, `{"queryType":"sql","rawSql":"SELECT 1"}`)
	if response.Status != backend.StatusBadRequest || response.Error == nil || response.Error.Error() != sqlErr.Error() {
		t.Errorf("executeSQLQuery() = %v, %v; want %v, %q", response.Status, response.Error, backend.StatusBadRequest, sqlErr)
	}
}

func TestExecuteSQLQueryRecoversPanics(t *testing.T) {
	e := sqlTestExecution(t, sqlQueryService(t, sqlPayloadServer(nil)))
	response := e.executeSQLQuery(context.Background(), preparedQuery{Kind: preparedQuerySQL})
	if response.Status != backend.StatusInternal || response.Error == nil {
		t.Errorf("executeSQLQuery(nil query) = %v, %v; want an internal error instead of a panic", response.Status, response.Error)
	}
}

func TestQueryDataMixesSQLAndCompute(t *testing.T) {
	var calls atomic.Int32
	client := sqlQueryService(t, func(req *sqlv1.SqlServiceQueryRequest, s grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		calls.Add(1)
		return sqlPayloadServer(sqlValueStream(t, 42))(req, s)
	})
	workspace := sqlTestWorkspaceRid(t)
	compute := &mockComputeService{batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{createMockArrowComputeResult([]float64{7})},
	}}
	ds := &Datasource{workspaceRid: &workspace, sqlService: client, computeService: compute}
	req := newQueryRequest([]backend.DataQuery{
		{RefID: "SQL", JSON: []byte(`{"queryType":"sql","rawSql":"SELECT 42","format":"table"}`)},
		{RefID: "Compute", JSON: []byte(`{"queryType":"timeShift","assetRid":"ri.nominal.asset.1","channel":"temp","dataScopeName":"default","buckets":100}`), TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(3600, 0)}},
		{RefID: "InvalidSQL", JSON: []byte(`{"queryType":"sql","rawSql":" "}`)},
	})
	response, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("QueryData() error = %v", err)
	}
	for _, ref := range []string{"SQL", "Compute"} {
		if result, ok := response.Responses[ref]; !ok || result.Error != nil || len(result.Frames) != 1 || result.Frames[0].Rows() != 1 {
			t.Errorf("response %s = %+v, want one frame with one row", ref, result)
		}
	}
	if response.Responses["InvalidSQL"].Error == nil {
		t.Error("response InvalidSQL has no error, want the empty-SQL error")
	}
	if calls.Load() != 1 || compute.batchComputeCalls != 1 || len(compute.lastBatchRequest.Requests) != 1 {
		t.Errorf("SQL calls, Compute calls, Compute requests = %d, %d, %d; want 1, 1, 1", calls.Load(), compute.batchComputeCalls, len(compute.lastBatchRequest.Requests))
	}
}

func TestSQLQueriesRunInParallelWithinTheLimit(t *testing.T) {
	var active, peak, total atomic.Int32
	stream := sqlValueStream(t, 1)
	client := sqlQueryService(t, func(req *sqlv1.SqlServiceQueryRequest, s grpc.ServerStreamingServer[sqlv1.SqlServiceQueryResponse]) error {
		current := active.Add(1)
		defer active.Add(-1)
		total.Add(1)
		for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
		}
		time.Sleep(10 * time.Millisecond)
		return sqlPayloadServer(stream)(req, s)
	})
	queries := make([]backend.DataQuery, 20)
	for i := range queries {
		queries[i] = backend.DataQuery{RefID: strconv.Itoa(i), JSON: []byte(`{"queryType":"sql","rawSql":"SELECT 1","format":"table"}`)}
	}
	response := sqlTestExecution(t, client).Execute(context.Background(), queries)
	if got := peak.Load(); got < 2 || got > maxConcurrentSQLQueries {
		t.Errorf("peak concurrent SQL queries = %d, want between 2 and %d", got, maxConcurrentSQLQueries)
	}
	if total.Load() != 20 || len(response.Responses) != 20 {
		t.Errorf("SQL calls, responses = %d, %d; want 20, 20", total.Load(), len(response.Responses))
	}
	for ref, res := range response.Responses {
		if res.Error != nil {
			t.Errorf("response %s error = %v", ref, res.Error)
		}
	}
}

func TestCheckSQLReportsServiceErrors(t *testing.T) {
	denied := sqlStatusError(t, codes.PermissionDenied, "SQL is not enabled for this key", "", "")
	client := newFakeSQLService(t, &fakeSQLService{catalog: func(context.Context) error { return denied }})
	workspace := sqlTestWorkspaceRid(t)
	ds := &Datasource{workspaceRid: &workspace, sqlService: client}
	if err := ds.checkSQL(context.Background(), bearertoken.Token("k")); err == nil || err.Error() != "SQL is not enabled for this key" {
		t.Errorf("checkSQL() error = %v, want the SQL service's error", err)
	}
}
