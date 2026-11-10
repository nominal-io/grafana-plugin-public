package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/api/rids"
	nominalapi "github.com/nominal-io/nominal-api-go/io/nominal/api"
	nominaldatasource "github.com/nominal-io/nominal-api-go/io/nominal/datasource"
	scoutapi "github.com/nominal-io/nominal-api-go/scout/api"
	assetapi "github.com/nominal-io/nominal-api-go/scout/asset/api"
	assetservice "github.com/nominal-io/nominal-api-go/scout/assets"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	writerapi "github.com/nominal-io/nominal-api-go/storage/writer/api"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

const liveNominalTestEnv = "NOMINAL_LIVE_TESTS"
const liveNominalDataScopeName = "primary"
const liveNominalChannelName = "temperature"
const liveNominalCSV = `timestamp,relative_minutes,temperature,humidity,state
2024-09-05T18:00:00Z,0,20,50,idle
2024-09-05T18:01:00Z,1,21,49,idle
2024-09-05T18:02:00Z,2,22,48,idle
2024-09-05T18:03:00Z,3,23,47,idle
2024-09-05T18:04:00Z,4,24,46,idle
2024-09-05T18:05:00Z,5,25,45,run
2024-09-05T18:06:00Z,6,26,44,run
2024-09-05T18:07:00Z,7,27,43,run
2024-09-05T18:08:00Z,8,28,42,run
2024-09-05T18:09:00Z,9,29,41,run
`

type liveNominalQueryTarget struct {
	datasetRid    string
	assetRid      string
	channel       string
	dataScopeName string
	from          time.Time
	to            time.Time
}

type liveNominalAPIClients struct {
	workspace *rids.WorkspaceRid
	baseURL   string
	http      *http.Client
	token     bearertoken.Token
	asset     assetservice.AssetServiceClient
	writer    writerapi.NominalChannelWriterServiceClient
}

type liveNominalDataset struct {
	Rid rid.ResourceIdentifier `json:"rid"`
}

type liveNominalDatasetFile struct {
	IngestStatus liveNominalIngestStatus `json:"ingestStatus"`
}

type liveNominalIngestStatus struct {
	Type  string                        `json:"type"`
	Error *liveNominalIngestStatusError `json:"error,omitempty"`
}

type liveNominalIngestStatusError struct {
	ErrorType string `json:"errorType"`
	Message   string `json:"message"`
}

func liveNominalSettings(t *testing.T) backend.DataSourceInstanceSettings {
	t.Helper()

	if os.Getenv(liveNominalTestEnv) != "1" {
		t.Skipf("set %s=1 to run live Nominal API integration tests", liveNominalTestEnv)
	}

	apiKey := os.Getenv("NOMINAL_API_KEY")
	if apiKey == "" {
		t.Fatalf("NOMINAL_API_KEY is required when %s=1", liveNominalTestEnv)
	}

	baseURL := os.Getenv("NOMINAL_BASE_URL")
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}

	jsonData, err := json.Marshal(map[string]any{"baseUrl": baseURL, "workspaceRid": os.Getenv("NOMINAL_WORKSPACE_RID")})
	if err != nil {
		t.Fatalf("failed to marshal datasource JSON: %v", err)
	}

	return backend.DataSourceInstanceSettings{
		ID:       1,
		UID:      "live-nominal-test",
		Name:     "Live Nominal Test",
		Type:     "nominaltest-nominalds-datasource",
		JSONData: jsonData,
		DecryptedSecureJSONData: map[string]string{
			"apiKey": apiKey,
		},
	}
}

func liveNominalDatasource(t *testing.T, settings backend.DataSourceInstanceSettings) *Datasource {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	instance, err := NewDatasource(ctx, settings)
	if err != nil {
		t.Fatalf("failed to create datasource: %v", err)
	}

	ds, ok := instance.(*Datasource)
	if !ok {
		t.Fatalf("expected *Datasource from NewDatasource, got %T", instance)
	}
	t.Cleanup(ds.Dispose)
	return ds
}

func TestLiveNominalCheckHealthIntegration(t *testing.T) {
	settings := liveNominalSettings(t)
	ds := liveNominalDatasource(t, settings)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result, err := ds.CheckHealth(ctx, &backend.CheckHealthRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &settings,
		},
	})
	if err != nil {
		t.Fatalf("unexpected health check error: %v", err)
	}
	if result.Status != backend.HealthStatusOk {
		t.Fatalf("expected health status OK, got %s: %s", result.Status, result.Message)
	}
}

func TestLiveNominalQueryDataIntegration(t *testing.T) {
	settings := liveNominalSettings(t)

	target, ok := liveNominalQueryTargetFromEnv(t)
	if !ok {
		target = createLiveNominalQueryTarget(t, settings)
	}

	buckets := liveNominalQueryBuckets(t)
	ds := liveNominalDatasource(t, settings)
	query := NominalQueryModel{
		AssetRid:      target.assetRid,
		Channel:       target.channel,
		DataScopeName: target.dataScopeName,
		Buckets:       buckets,
	}
	request := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &settings,
		},
		Queries: []backend.DataQuery{
			{
				RefID:         "A",
				JSON:          mustMarshal(query),
				TimeRange:     backend.TimeRange{From: target.from, To: target.to},
				MaxDataPoints: int64(buckets),
			},
		},
	}

	_, expected := liveNominalCSVSamples(t)
	response := pollLiveQuery(t, ds, request, func(responses backend.Responses) error {
		response := responses["A"]
		values := liveNominalValues(t, response)
		if len(values) > len(expected) {
			t.Fatalf("live query returned %d non-null values but the fixture ingested %d; the extra points come from upstream of the plugin", len(values), len(expected))
		}
		if response.Error != nil || len(values) < len(expected) {
			return fmt.Errorf("%d of %d non-null values, error: %v", len(values), len(expected), response.Error)
		}
		return nil
	})["A"]
	assertLiveNominalNumericResponse(t, response, target.channel, expected)
}

func TestLiveNominalLTTBRawPointsIntegration(t *testing.T) {
	settings := liveNominalSettings(t)
	target := createLiveNominalQueryTarget(t, settings)
	ds := liveNominalDatasource(t, settings)
	query := NominalQueryModel{
		AssetRid: target.assetRid, Channel: target.channel, DataScopeName: target.dataScopeName,
		ChannelDataType: ChannelDataTypeNumeric, Aggregations: []string{AggLTTB}, Buckets: 4,
	}
	request := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{DataSourceInstanceSettings: &settings},
		Queries: []backend.DataQuery{{
			RefID: "A", JSON: mustMarshal(query),
			TimeRange: backend.TimeRange{From: target.from, To: target.to}, MaxDataPoints: 4,
		}},
	}
	response := pollLiveQuery(t, ds, request, func(responses backend.Responses) error {
		response := responses["A"]
		if values := liveNominalValues(t, response); response.Error != nil || len(values) == 0 {
			return fmt.Errorf("no LTTB points, error: %v", response.Error)
		}
		return nil
	})["A"]
	frame := response.Frames[0]
	if want := target.channel + " (lttb)"; frame.Name != want {
		t.Fatalf("LTTB frame name %q, want %q", frame.Name, want)
	}
	n := frame.Fields[1].Len()
	if n != 4 {
		t.Fatalf("LTTB returned %d points, want 4", n)
	}
	firstPoint := time.Date(2024, 9, 5, 18, 0, 0, 0, time.UTC)
	// LTTB always keeps the first and last raw points.
	lastPoint := firstPoint.Add(9 * time.Minute)
	if got := frame.Fields[0].At(0).(time.Time); !got.Equal(firstPoint) {
		t.Fatalf("LTTB first point at %v, want %v", got, firstPoint)
	}
	if got := frame.Fields[0].At(n - 1).(time.Time); !got.Equal(lastPoint) {
		t.Fatalf("LTTB last point at %v, want %v", got, lastPoint)
	}
	for i := 0; i < frame.Fields[1].Len(); i++ {
		pointTime := frame.Fields[0].At(i).(time.Time)
		want := 20 + pointTime.Sub(firstPoint).Minutes()
		value, err := frame.Fields[1].FloatAt(i)
		if err != nil || value != want {
			t.Fatalf("LTTB returned %v (%v) at %v, want raw CSV value %v", value, err, pointTime, want)
		}
	}
}

func TestLiveNominalStringChannelIntegration(t *testing.T) {
	settings := liveNominalSettings(t)
	target := createLiveNominalQueryTarget(t, settings)
	ds := liveNominalDatasource(t, settings)
	// 20 buckets over 11 minutes are 33 s wide, so each one-per-minute sample has its own bucket.
	query := NominalQueryModel{
		AssetRid: target.assetRid, Channel: "state", DataScopeName: target.dataScopeName,
		ChannelDataType: ChannelDataTypeString, Buckets: 20,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := ds.QueryData(ctx, &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{DataSourceInstanceSettings: &settings},
		Queries: []backend.DataQuery{{
			RefID: "A", JSON: mustMarshal(query),
			TimeRange: backend.TimeRange{From: target.from, To: target.to}, MaxDataPoints: 20,
		}},
	})
	if err != nil {
		t.Fatalf("string QueryData failed: %v", err)
	}
	response, ok := resp.Responses["A"]
	if !ok || response.Error != nil || len(response.Frames) != 1 || len(response.Frames[0].Fields) != 2 {
		t.Fatalf("string response missing or failed: %+v", response)
	}
	timeField, valueField := response.Frames[0].Fields[0], response.Frames[0].Fields[1]
	want := []string{"idle", "idle", "idle", "idle", "idle", "run", "run", "run", "run", "run"}
	var got []string
	for i := 0; i < valueField.Len(); i++ {
		got = append(got, valueField.At(i).(string))
		if tm := timeField.At(i).(time.Time); tm.After(target.to) {
			t.Errorf("row %d at %v is after the range end %v", i, tm, target.to)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestLiveNominalSQLQueryIntegration(t *testing.T) {
	settings := liveNominalSettings(t)
	// SQL needs a dataset selector, so this test deliberately reuses the self-provisioned target.
	target := createLiveNominalQueryTarget(t, settings)
	ds := liveNominalDatasource(t, settings)
	// The first point at 18:00 must be excluded, and 18:02 must be included.
	// Rounding either bound to seconds makes the row-count assertion fail.
	start := time.Date(2024, 9, 5, 18, 0, 0, 500000000, time.UTC)
	end := time.Date(2024, 9, 5, 18, 2, 0, 500000000, time.UTC)
	queries := []backend.DataQuery{
		{RefID: "A", TimeRange: backend.TimeRange{From: start, To: end}, Interval: time.Minute, JSON: mustMarshal(map[string]string{
			"queryType": queryTypeSQL, "format": "table",
			"rawSql": fmt.Sprintf("SELECT ts, value FROM points_double WHERE dataset_rid = '%s' AND channel = 'temperature' AND $__timeFilter(ts) ORDER BY ts", target.datasetRid),
		})},
		{RefID: "B", TimeRange: backend.TimeRange{From: start, To: end}, Interval: time.Minute, JSON: mustMarshal(map[string]string{
			"queryType": queryTypeSQL, "format": "timeseries",
			"rawSql": fmt.Sprintf("SELECT $__timeGroup(ts) AS \"time\", channel, AVG(value) AS \"value\" FROM points_double WHERE dataset_rid = '%s' AND channel IN ('temperature') AND $__timeFilter(ts) GROUP BY 1, 2 ORDER BY 1", target.datasetRid),
		})},
	}
	req := &backend.QueryDataRequest{PluginContext: backend.PluginContext{DataSourceInstanceSettings: &settings}, Queries: queries}
	responses := pollLiveQuery(t, ds, req, liveSQLRowsReady(t, 2))
	for _, ref := range []string{"A", "B"} {
		response := responses[ref]
		if got := response.Frames[0].RefID; got != ref {
			t.Fatalf("frame for %s has RefID %q", ref, got)
		}
		for i, want := range []float64{21, 22} {
			got, valid := response.Frames[0].Fields[1].ConcreteAt(i)
			if !valid || got != want {
				t.Fatalf("%s row %d: got %v, want %v", ref, i, got, want)
			}
		}
	}
}

func TestLiveNominalSQLLogsIntegration(t *testing.T) {
	settings := liveNominalSettings(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	clients, _, dataset := createTempLiveNominalDataset(t, ctx, settings)
	ds := liveNominalDatasource(t, settings)

	// The first two rows fall inside the same millisecond, both after its start.
	base := time.Date(2024, 9, 5, 18, 0, 0, 0, time.UTC)
	row := func(offset time.Duration, message string) writerapi.LogPoint {
		args := map[string]string{"level": "error", "host": "h1"}
		return writerapi.LogPoint{Timestamp: timestampFromTime(base.Add(offset)), Value: writerapi.LogValue{Message: message, Args: args}}
	}
	err := clients.writer.WriteLogs(ctx, clients.token, rids.NominalDataSourceOrDatasetRid(dataset.Rid), writerapi.WriteLogsRequest{
		Channel: new(nominalapi.Channel("grafana-live-logs")),
		Logs:    []writerapi.LogPoint{row(200*time.Microsecond, "first"), row(500*time.Microsecond, "second"), row(time.Second, "third")},
	})
	if err != nil {
		t.Fatalf("failed to write live logs: %v", err)
	}

	queryJSON := map[string]string{
		"queryType": queryTypeSQL, "format": "logs",
		"rawSql": fmt.Sprintf("SELECT ts, channel, message, args FROM logs WHERE dataset_rid = '%s' AND $__timeFilter(ts) ORDER BY ts DESC LIMIT 10", dataset.Rid),
	}
	query := backend.DataQuery{
		RefID:     "A",
		TimeRange: backend.TimeRange{From: base.Add(-time.Minute), To: base.Add(time.Minute)},
		JSON:      mustMarshal(queryJSON),
	}
	req := &backend.QueryDataRequest{PluginContext: backend.PluginContext{DataSourceInstanceSettings: &settings}, Queries: []backend.DataQuery{query}}

	frame := pollLiveQuery(t, ds, req, liveSQLRowsReady(t, 3))["A"].Frames[0]
	for i, wantBody := range []string{"third", "second", "first"} {
		body, _, rawLabels := logRow(frame, i)
		if body != wantBody {
			t.Errorf("row %d body = %q, want %q (newest first)", i, body, wantBody)
		}
		labels := parseLogLabels(t, json.RawMessage(rawLabels))
		for key, want := range map[string]string{"channel": "grafana-live-logs", "level": "error", "host": "h1"} {
			if labels[key] != want {
				t.Errorf("row %d label %q = %q, want %q", i, key, labels[key], want)
			}
		}
	}

	// Infinite scroll continues from "first", the oldest line shown: Grafana ends the range at the
	// start of its millisecond, and the query carries the line's id. The page ends at that line's
	// time, inclusive, so it holds only "first". Without the id it would also hold "second", which
	// shares the millisecond.
	_, firstID, _ := logRow(frame, 2)
	nextPage := query
	nextPage.TimeRange.To = base
	nextJSON := maps.Clone(queryJSON)
	nextJSON["scrollId"] = firstID
	nextPage.JSON = mustMarshal(nextJSON)
	nextReq := &backend.QueryDataRequest{PluginContext: req.PluginContext, Queries: []backend.DataQuery{nextPage}}
	next := pollLiveQuery(t, ds, nextReq, liveSQLRowsReady(t, 1))["A"].Frames[0]
	body, id, _ := logRow(next, 0)
	wantBody, wantID, _ := logRow(frame, 2)
	if body != wantBody || id != wantID {
		t.Errorf("next page row = %q with id %s, want %q with id %s as on the first page", body, id, wantBody, wantID)
	}
}

func liveNominalQueryTargetFromEnv(t *testing.T) (liveNominalQueryTarget, bool) {
	t.Helper()

	assetRid := os.Getenv("NOMINAL_QUERY_ASSET_RID")
	channel := os.Getenv("NOMINAL_QUERY_CHANNEL")
	dataScopeName := os.Getenv("NOMINAL_QUERY_DATA_SCOPE_NAME")
	if assetRid == "" && channel == "" && dataScopeName == "" {
		return liveNominalQueryTarget{}, false
	}
	if assetRid == "" || channel == "" || dataScopeName == "" {
		t.Fatalf("NOMINAL_QUERY_ASSET_RID, NOMINAL_QUERY_CHANNEL, and NOMINAL_QUERY_DATA_SCOPE_NAME must be set together")
	}

	to := time.Now().UTC()
	from := to.Add(-15 * time.Minute)
	if rawFrom := os.Getenv("NOMINAL_QUERY_FROM"); rawFrom != "" {
		parsedFrom, err := time.Parse(time.RFC3339, rawFrom)
		if err != nil {
			t.Fatalf("NOMINAL_QUERY_FROM must be RFC3339: %v", err)
		}
		from = parsedFrom
	}
	if rawTo := os.Getenv("NOMINAL_QUERY_TO"); rawTo != "" {
		parsedTo, err := time.Parse(time.RFC3339, rawTo)
		if err != nil {
			t.Fatalf("NOMINAL_QUERY_TO must be RFC3339: %v", err)
		}
		to = parsedTo
	}

	return liveNominalQueryTarget{
		assetRid:      assetRid,
		channel:       channel,
		dataScopeName: dataScopeName,
		from:          from,
		to:            to,
	}, true
}

func liveNominalQueryBuckets(t *testing.T) int {
	t.Helper()

	buckets := 100
	if rawBuckets := os.Getenv("NOMINAL_QUERY_BUCKETS"); rawBuckets != "" {
		parsedBuckets, err := strconv.Atoi(rawBuckets)
		if err != nil {
			t.Fatalf("NOMINAL_QUERY_BUCKETS must be an integer: %v", err)
		}
		buckets = parsedBuckets
	}
	return buckets
}

// createTempLiveNominalDataset builds API clients and creates a dataset that is archived when the
// test ends. It skips first unless writes to the default base URL are explicitly allowed.
func createTempLiveNominalDataset(t *testing.T, ctx context.Context, settings backend.DataSourceInstanceSettings) (liveNominalAPIClients, string, liveNominalDataset) {
	t.Helper()

	baseURL := liveNominalBaseURLFromSettings(t, settings)
	if baseURL == defaultAPIBaseURL && os.Getenv("NOMINAL_ALLOW_DEFAULT_LIVE_WRITES") != "1" {
		t.Skip("live QueryData test creates temporary Nominal resources; set NOMINAL_BASE_URL explicitly or NOMINAL_ALLOW_DEFAULT_LIVE_WRITES=1 to use the default URL")
	}

	clients := newLiveNominalAPIClients(t, settings)
	name := fmt.Sprintf("grafana-plugin-live-%d", time.Now().UnixNano())
	isV2Dataset := true
	dataset, err := clients.createDataset(ctx, name, isV2Dataset)
	if err != nil {
		t.Fatalf("failed to create live Nominal dataset: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := clients.archiveDataset(cleanupCtx, dataset.Rid); err != nil {
			t.Logf("failed to archive live Nominal dataset %s: %v", dataset.Rid, err)
		}
	})
	return clients, name, dataset
}

func createLiveNominalQueryTarget(t *testing.T, settings backend.DataSourceInstanceSettings) liveNominalQueryTarget {
	t.Helper()

	ctx, cancel := context.WithDeadline(context.Background(), liveDeadline(t))
	defer cancel()

	clients, name, dataset := createTempLiveNominalDataset(t, ctx, settings)

	asset, err := clients.asset.CreateAsset(ctx, clients.token, assetapi.CreateAssetRequest{
		Title:     name,
		Workspace: clients.workspace,
	})
	if err != nil {
		t.Fatalf("failed to create live Nominal asset: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := clients.asset.Archive(cleanupCtx, clients.token, asset.Rid, nil); err != nil {
			t.Logf("failed to archive live Nominal asset %s: %v", asset.Rid, err)
		}
	})

	_, err = clients.asset.AddDataScopesToAsset(ctx, clients.token, asset.Rid, assetapi.AddDataScopesToAssetRequest{
		DataScopes: []assetapi.CreateAssetDataScope{
			{
				DataScopeName: scoutapi.DataSourceRefName(liveNominalDataScopeName),
				DataSource:    runapi.NewDataSourceFromDataset(rids.DatasetRid(dataset.Rid)),
				SeriesTags:    map[nominalapi.TagName]nominalapi.TagValue{},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to add live Nominal data scope: %v", err)
	}

	fileID := ingestLiveNominalCSV(t, ctx, clients, dataset.Rid)
	waitForLiveNominalIngest(t, ctx, clients, dataset.Rid, fileID)

	from, to := liveNominalCSVTimeRange(t)
	return liveNominalQueryTarget{
		datasetRid:    dataset.Rid.String(),
		assetRid:      asset.Rid.String(),
		channel:       liveNominalChannelName,
		dataScopeName: liveNominalDataScopeName,
		from:          from,
		to:            to,
	}
}

// liveDeadline is when live waits must stop: a minute before the test deadline, since a test
// timeout skips t.Cleanup and leaks the temporary resources.
func liveDeadline(t *testing.T) time.Time {
	deadline, ok := t.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Minute)
	}
	return deadline.Add(-time.Minute)
}

// pollLiveQuery repeats req until ready returns nil and returns those responses. A query right
// after a write may not see it yet, and a later query may.
func pollLiveQuery(t *testing.T, ds *Datasource, req *backend.QueryDataRequest, ready func(backend.Responses) error) backend.Responses {
	t.Helper()
	const queryTimeout = 30 * time.Second
	const interval = 5 * time.Second
	deadline := liveDeadline(t)
	start := time.Now()
	attempts := 0
	var notReady error
	for time.Until(deadline) >= queryTimeout {
		attempts++
		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
		resp, err := ds.QueryData(ctx, req)
		timedOut := ctx.Err() != nil
		cancel()
		switch {
		case timedOut:
			notReady = fmt.Errorf("attempt exceeded %s", queryTimeout)
		case err != nil:
			t.Fatalf("unexpected QueryData error: %v", err)
		default:
			for _, query := range req.Queries {
				if _, ok := resp.Responses[query.RefID]; !ok {
					t.Fatalf("missing response for query %s; got refs %v", query.RefID, responseRefs(resp))
				}
			}
			if notReady = ready(resp.Responses); notReady == nil {
				return resp.Responses
			}
		}
		t.Logf("live query attempt %d not ready, retrying in %s: %v", attempts, interval, notReady)
		time.Sleep(interval)
	}
	t.Fatalf("live query not ready after %d attempts over %s: %v", attempts, time.Since(start).Round(time.Second), notReady)
	return nil
}

// liveSQLRowsReady accepts SQL responses once each has one frame with want rows. Not found is
// retried, since a new dataset may not be queryable yet. Any other error, or more rows than want,
// fails at once.
func liveSQLRowsReady(t *testing.T, want int) func(backend.Responses) error {
	return func(responses backend.Responses) error {
		for _, ref := range slices.Sorted(maps.Keys(responses)) {
			response := responses[ref]
			if response.Error != nil {
				if response.Status != backend.StatusNotFound {
					t.Fatalf("unexpected SQL error for %s (status %d): %v", ref, response.Status, response.Error)
				}
				return fmt.Errorf("%s: %v", ref, response.Error)
			}
			if len(response.Frames) != 1 {
				t.Fatalf("expected one frame for %s, got %d", ref, len(response.Frames))
			}
			if rows := response.Frames[0].Rows(); rows > want {
				t.Fatalf("%s returned %d rows, want %d", ref, rows, want)
			} else if rows < want {
				return fmt.Errorf("%s: %d of %d rows", ref, rows, want)
			}
		}
		return nil
	}
}

func responseRefs(resp *backend.QueryDataResponse) []string {
	refs := make([]string, 0, len(resp.Responses))
	for refID := range resp.Responses {
		refs = append(refs, refID)
	}
	sort.Strings(refs)
	return refs
}

func liveNominalBaseURLFromSettings(t *testing.T, settings backend.DataSourceInstanceSettings) string {
	t.Helper()

	var jsonData struct {
		BaseURL string `json:"baseUrl"`
	}
	if len(settings.JSONData) > 0 {
		if err := json.Unmarshal(settings.JSONData, &jsonData); err != nil {
			t.Fatalf("failed to unmarshal datasource JSONData: %v", err)
		}
	}

	baseURL := jsonData.BaseURL
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	return strings.TrimSuffix(baseURL, "/")
}

func newLiveNominalAPIClients(t *testing.T, settings backend.DataSourceInstanceSettings) liveNominalAPIClients {
	t.Helper()

	apiKey := settings.DecryptedSecureJSONData["apiKey"]
	if apiKey == "" {
		t.Fatal("apiKey missing from live Nominal settings")
	}

	conjureClient, err := conjurehttpclient.NewClient(
		conjurehttpclient.WithBaseURLs([]string{liveNominalBaseURLFromSettings(t, settings)}),
	)
	if err != nil {
		t.Fatalf("failed to create live Nominal API client: %v", err)
	}

	var config struct {
		WorkspaceRid string `json:"workspaceRid"`
	}
	if err := json.Unmarshal(settings.JSONData, &config); err != nil {
		t.Fatal(err)
	}
	workspace, err := parseWorkspaceRid(config.WorkspaceRid)
	if err != nil {
		t.Fatal(err)
	}
	return liveNominalAPIClients{
		workspace: workspace,
		baseURL:   liveNominalBaseURLFromSettings(t, settings),
		http:      &http.Client{Timeout: 30 * time.Second},
		token:     bearertoken.Token(apiKey),
		asset:     assetservice.NewAssetServiceClient(conjureClient),
		writer:    writerapi.NewNominalChannelWriterServiceClient(conjureClient),
	}
}

func (c liveNominalAPIClients) createDataset(ctx context.Context, name string, isV2Dataset bool) (liveNominalDataset, error) {
	var dataset liveNominalDataset
	request := map[string]any{
		"name":           name,
		"metadata":       map[string]string{},
		"originMetadata": map[string]any{},
		"labels":         []string{},
		"properties":     map[string]string{},
		"markingRids":    []string{},
		"isV2Dataset":    isV2Dataset,
	}
	if c.workspace != nil {
		request["workspace"] = c.workspace.String()
	}
	err := c.doJSON(ctx, http.MethodPost, "/catalog/v1/datasets", request, &dataset)
	return dataset, err
}

func (c liveNominalAPIClients) archiveDataset(ctx context.Context, datasetRid rid.ResourceIdentifier) error {
	path := fmt.Sprintf("/catalog/v1/datasets/%s/archive", url.PathEscape(datasetRid.String()))
	return c.doJSON(ctx, http.MethodPost, path, nil, nil)
}

func (c liveNominalAPIClients) getDatasetFile(ctx context.Context, datasetRid rid.ResourceIdentifier, fileID nominaldatasource.DatasetFileId) (liveNominalDatasetFile, error) {
	path := fmt.Sprintf(
		"/catalog/v1/dataset/%s/file/%s",
		url.PathEscape(datasetRid.String()),
		url.PathEscape(fileID.String()),
	)
	var file liveNominalDatasetFile
	err := c.doJSON(ctx, http.MethodGet, path, nil, &file)
	return file, err
}

func (c liveNominalAPIClients) doJSON(ctx context.Context, method, path string, requestBody any, responseBody any) error {
	var body io.Reader
	if requestBody != nil {
		jsonBytes, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		body = bytes.NewReader(jsonBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s failed with %s: %s", method, path, resp.Status, strings.TrimSpace(string(bodyBytes)))
	}

	if responseBody == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(responseBody); err != nil {
		return fmt.Errorf("decode response body: %w", err)
	}
	return nil
}

func ingestLiveNominalCSV(t *testing.T, ctx context.Context, clients liveNominalAPIClients, datasetRid rid.ResourceIdentifier) nominaldatasource.DatasetFileId {
	t.Helper()

	csvBytes := []byte(liveNominalCSV)
	query := url.Values{}
	query.Set("fileName", "grafana-plugin-live.csv")
	query.Set("sizeBytes", strconv.Itoa(len(csvBytes)))
	if clients.workspace != nil {
		query.Set("workspace", clients.workspace.String())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, clients.baseURL+"/upload/v1/upload-file?"+query.Encode(), bytes.NewReader(csvBytes))
	if err != nil {
		t.Fatalf("failed to create live Nominal CSV upload request: %v", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", clients.token))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := clients.http.Do(req)
	if err != nil {
		t.Fatalf("failed to upload live Nominal CSV: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("failed to upload live Nominal CSV: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var s3Path string
	if err := json.NewDecoder(resp.Body).Decode(&s3Path); err != nil {
		t.Fatalf("failed to decode live Nominal CSV upload response: %v", err)
	}

	request := map[string]any{"options": map[string]any{
		"type": "csv",
		"csv": map[string]any{
			"source":               map[string]any{"type": "s3", "s3": map[string]any{"path": s3Path}},
			"target":               map[string]any{"type": "existing", "existing": map[string]any{"datasetRid": datasetRid}},
			"timestampMetadata":    map[string]any{"seriesName": "timestamp", "timestampType": map[string]any{"type": "absolute", "absolute": map[string]any{"type": "iso8601", "iso8601": map[string]any{}}}},
			"channelPrefix":        nil,
			"channelNameOverrides": map[string]string{},
			"excludeColumns":       []string{},
		},
	}}
	var response struct {
		Details struct {
			Type    string `json:"type"`
			Dataset struct {
				DatasetFileID *nominaldatasource.DatasetFileId `json:"datasetFileId"`
			} `json:"dataset"`
		} `json:"details"`
	}
	if err := clients.doJSON(ctx, http.MethodPost, "/ingest/v1/ingest", request, &response); err != nil {
		t.Fatalf("failed to ingest live Nominal CSV: %v", err)
	}
	if response.Details.Type != "dataset" || response.Details.Dataset.DatasetFileID == nil {
		t.Fatalf("expected dataset file id in ingest response, got details type %q", response.Details.Type)
	}
	return *response.Details.Dataset.DatasetFileID
}

func waitForLiveNominalIngest(t *testing.T, ctx context.Context, clients liveNominalAPIClients, datasetRid rid.ResourceIdentifier, fileID nominaldatasource.DatasetFileId) {
	t.Helper()

	start := time.Now()
	var lastStatus string
	var lastErr error
	for {
		file, err := clients.getDatasetFile(ctx, datasetRid, fileID)
		if err == nil {
			complete, statusErr := file.IngestStatus.isComplete()
			if statusErr != nil {
				t.Fatalf("live Nominal ingest failed: %v", statusErr)
			}
			if complete {
				t.Logf("live Nominal CSV ingest completed after %s", time.Since(start).Round(time.Second))
				return
			}
			lastStatus = file.IngestStatus.Type
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			t.Fatalf("live Nominal CSV ingest not complete after %s; last status %q, last error: %v",
				time.Since(start).Round(time.Second), lastStatus, lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}

func (s liveNominalIngestStatus) isComplete() (bool, error) {
	switch s.Type {
	case "success", "deletionInProgress", "deleted":
		return true, nil
	case "inProgress":
		return false, nil
	case "error":
		if s.Error == nil {
			return false, fmt.Errorf("ingest failed without error details")
		}
		return false, fmt.Errorf("%s: %s", s.Error.ErrorType, s.Error.Message)
	default:
		return false, fmt.Errorf("unknown ingest status %q", s.Type)
	}
}

// liveNominalCSVSamples reads fixture timestamps and channel values.
func liveNominalCSVSamples(t *testing.T) ([]time.Time, []float64) {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(liveNominalCSV), "\n")
	column := slices.Index(strings.Split(lines[0], ","), liveNominalChannelName)
	if column < 0 {
		t.Fatalf("liveNominalCSV has no %q column", liveNominalChannelName)
	}
	var timestamps []time.Time
	var values []float64
	for _, line := range lines[1:] {
		fields := strings.Split(line, ",")
		timestamp, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			t.Fatalf("liveNominalCSV timestamp %q: %v", fields[0], err)
		}
		value, err := strconv.ParseFloat(fields[column], 64)
		if err != nil {
			t.Fatalf("liveNominalCSV %s %q: %v", liveNominalChannelName, fields[column], err)
		}
		timestamps = append(timestamps, timestamp)
		values = append(values, value)
	}
	return timestamps, values
}

// liveNominalCSVTimeRange pads the fixture's time range.
func liveNominalCSVTimeRange(t *testing.T) (time.Time, time.Time) {
	timestamps, _ := liveNominalCSVSamples(t)
	return timestamps[0].Add(-time.Minute), timestamps[len(timestamps)-1].Add(time.Minute)
}

// liveNominalValues excludes empty buckets, which appear as nil-valued rows.
func liveNominalValues(t *testing.T, response backend.DataResponse) []float64 {
	t.Helper()
	var values []float64
	for _, frame := range response.Frames {
		if len(frame.Fields) < 2 {
			continue
		}
		field := frame.Fields[1]
		for i := 0; i < field.Len(); i++ {
			raw, ok := field.ConcreteAt(i)
			if !ok {
				continue
			}
			value, isFloat := raw.(float64)
			if !isFloat {
				t.Fatalf("expected live query value to be numeric, got %T", raw)
			}
			values = append(values, value)
		}
	}
	return values
}

func assertLiveNominalNumericResponse(t *testing.T, response backend.DataResponse, channel string, expected []float64) {
	t.Helper()

	if len(response.Frames) == 0 {
		t.Fatalf("expected at least one frame for live query")
	}
	frame := response.Frames[0]
	if frame.Name != channel {
		t.Fatalf("expected live query frame name %q, got %q", channel, frame.Name)
	}
	if len(frame.Fields) < 2 {
		t.Fatalf("expected live query frame to contain time and value fields, got %d fields", len(frame.Fields))
	}

	values := liveNominalValues(t, response)
	if !slices.Equal(values, expected) {
		t.Fatalf("expected live query values to match the ingested CSV %v, got %v", expected, values)
	}
}
