package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/api/rids"
	ingestapi "github.com/nominal-io/nominal-api-go/ingest/api"
	nominalapi "github.com/nominal-io/nominal-api-go/io/nominal/api"
	nominaldatasource "github.com/nominal-io/nominal-api-go/io/nominal/datasource"
	scoutapi "github.com/nominal-io/nominal-api-go/scout/api"
	assetapi "github.com/nominal-io/nominal-api-go/scout/asset/api"
	assetservice "github.com/nominal-io/nominal-api-go/scout/assets"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	uploadapi "github.com/nominal-io/nominal-api-go/upload/api"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
	"github.com/palantir/pkg/safelong"
)

const liveNominalTestEnv = "NOMINAL_LIVE_TESTS"
const liveNominalDataScopeName = "primary"
const liveNominalChannelName = "temperature"
const liveNominalCSV = `timestamp,relative_minutes,temperature,humidity
2024-09-05T18:00:00Z,0,20,50
2024-09-05T18:01:00Z,1,21,49
2024-09-05T18:02:00Z,2,22,48
2024-09-05T18:03:00Z,3,23,47
2024-09-05T18:04:00Z,4,24,46
2024-09-05T18:05:00Z,5,25,45
2024-09-05T18:06:00Z,6,26,44
2024-09-05T18:07:00Z,7,27,43
2024-09-05T18:08:00Z,8,28,42
2024-09-05T18:09:00Z,9,29,41
`

type liveNominalQueryTarget struct {
	assetRid      string
	channel       string
	dataScopeName string
	from          time.Time
	to            time.Time
}

type liveNominalAPIClients struct {
	baseURL string
	http    *http.Client
	token   bearertoken.Token
	asset   assetservice.AssetServiceClient
	ingest  ingestapi.IngestServiceClient
	upload  uploadapi.UploadServiceClient
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

	jsonData, err := json.Marshal(map[string]string{"baseUrl": baseURL})
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := ds.QueryData(ctx, &backend.QueryDataRequest{
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
	})
	if err != nil {
		t.Fatalf("unexpected QueryData error: %v", err)
	}

	response, ok := resp.Responses["A"]
	if !ok {
		t.Fatalf("missing response for query A; got refs %v", responseRefs(resp))
	}
	if response.Error != nil {
		t.Fatalf("unexpected response error: %v", response.Error)
	}
	assertLiveNominalNumericResponse(t, response, target.channel)
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

func createLiveNominalQueryTarget(t *testing.T, settings backend.DataSourceInstanceSettings) liveNominalQueryTarget {
	t.Helper()

	baseURL := liveNominalBaseURLFromSettings(t, settings)
	if baseURL == defaultAPIBaseURL && os.Getenv("NOMINAL_ALLOW_DEFAULT_LIVE_WRITES") != "1" {
		t.Skip("live QueryData test creates temporary Nominal resources; set NOMINAL_BASE_URL explicitly or NOMINAL_ALLOW_DEFAULT_LIVE_WRITES=1 to use the default URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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

	asset, err := clients.asset.CreateAsset(ctx, clients.token, assetapi.CreateAssetRequest{
		Title: name,
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

	from, to := liveNominalCSVTimeRange()
	return liveNominalQueryTarget{
		assetRid:      asset.Rid.String(),
		channel:       liveNominalChannelName,
		dataScopeName: liveNominalDataScopeName,
		from:          from,
		to:            to,
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

	var jsonData map[string]string
	if len(settings.JSONData) > 0 {
		if err := json.Unmarshal(settings.JSONData, &jsonData); err != nil {
			t.Fatalf("failed to unmarshal datasource JSONData: %v", err)
		}
	}

	baseURL := jsonData["baseUrl"]
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

	return liveNominalAPIClients{
		baseURL: liveNominalBaseURLFromSettings(t, settings),
		http:    &http.Client{Timeout: 30 * time.Second},
		token:   bearertoken.Token(apiKey),
		asset:   assetservice.NewAssetServiceClient(conjureClient),
		ingest:  ingestapi.NewIngestServiceClient(conjureClient),
		upload:  uploadapi.NewUploadServiceClient(conjureClient),
	}
}

func (c liveNominalAPIClients) createDataset(ctx context.Context, name string, isV2Dataset bool) (liveNominalDataset, error) {
	var dataset liveNominalDataset
	err := c.doJSON(ctx, http.MethodPost, "/catalog/v1/datasets", map[string]any{
		"name":           name,
		"metadata":       map[string]string{},
		"originMetadata": map[string]any{},
		"labels":         []string{},
		"properties":     map[string]string{},
		"markingRids":    []string{},
		"isV2Dataset":    isV2Dataset,
	}, &dataset)
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
	csvSize := safelong.SafeLong(len(csvBytes))
	reader := bytes.NewReader(csvBytes)
	s3Path, err := clients.upload.UploadFile(
		ctx,
		clients.token,
		"grafana-plugin-live.csv",
		&csvSize,
		nil,
		conjurehttpclient.RequestBodyInMemory(reader),
	)
	if err != nil {
		t.Fatalf("failed to upload live Nominal CSV: %v", err)
	}

	response, err := clients.ingest.Ingest(ctx, clients.token, ingestapi.IngestRequest{
		Options: ingestapi.NewIngestOptionsFromCsv(ingestapi.CsvOpts{
			Source: ingestapi.NewIngestSourceFromS3(ingestapi.S3IngestSource{
				Path: string(s3Path),
			}),
			Target: ingestapi.NewDatasetIngestTargetFromExisting(ingestapi.ExistingDatasetIngestDestination{
				DatasetRid: datasetRid,
			}),
			TimestampMetadata: ingestapi.TimestampMetadata{
				SeriesName: "timestamp",
				TimestampType: ingestapi.NewTimestampTypeFromAbsolute(
					ingestapi.NewAbsoluteTimestampFromIso8601(ingestapi.Iso8601Timestamp{}),
				),
			},
			ExcludeColumns: []nominalapi.ColumnName{},
		}),
	})
	if err != nil {
		t.Fatalf("failed to ingest live Nominal CSV: %v", err)
	}

	fileID, err := liveNominalDatasetFileID(response)
	if err != nil {
		t.Fatalf("failed to read live Nominal ingest response: %v", err)
	}
	return fileID
}

func liveNominalDatasetFileID(response ingestapi.IngestResponse) (nominaldatasource.DatasetFileId, error) {
	var fileID nominaldatasource.DatasetFileId
	details := response.Details
	err := (&details).Accept(liveNominalIngestDetailsVisitor{fileID: &fileID})
	return fileID, err
}

type liveNominalIngestDetailsVisitor struct {
	fileID *nominaldatasource.DatasetFileId
}

func (v liveNominalIngestDetailsVisitor) VisitDataset(details ingestapi.IngestDatasetFileDetails) error {
	if details.DatasetFileId == nil {
		return fmt.Errorf("dataset ingest response did not include a dataset file id")
	}
	*v.fileID = nominaldatasource.DatasetFileId(*details.DatasetFileId)
	return nil
}

func (v liveNominalIngestDetailsVisitor) VisitVideo(ingestapi.IngestVideoFileDetails) error {
	return fmt.Errorf("expected dataset ingest details, got video ingest details")
}

func (v liveNominalIngestDetailsVisitor) VisitUnknown(typeName string) error {
	return fmt.Errorf("unknown ingest details type %q", typeName)
}

func waitForLiveNominalIngest(t *testing.T, ctx context.Context, clients liveNominalAPIClients, datasetRid rid.ResourceIdentifier, fileID nominaldatasource.DatasetFileId) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for {
		file, err := clients.getDatasetFile(ctx, datasetRid, fileID)
		if err == nil {
			complete, statusErr := file.IngestStatus.isComplete()
			if statusErr != nil {
				t.Fatalf("live Nominal ingest failed: %v", statusErr)
			}
			if complete {
				return
			}
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for live Nominal CSV ingest to complete; last error: %v", lastErr)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for live Nominal CSV ingest to complete: %v", ctx.Err())
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

func liveNominalCSVTimeRange() (time.Time, time.Time) {
	return time.Date(2024, 9, 5, 17, 59, 0, 0, time.UTC),
		time.Date(2024, 9, 5, 18, 10, 0, 0, time.UTC)
}

func assertLiveNominalNumericResponse(t *testing.T, response backend.DataResponse, channel string) {
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
	if frame.Fields[0].Len() == 0 || frame.Fields[1].Len() == 0 {
		t.Fatalf("expected live query frame to contain data points")
	}

	sawValue := false
	for i := 0; i < frame.Fields[1].Len(); i++ {
		rawValue := frame.Fields[1].At(i)
		var value *float64
		switch typed := rawValue.(type) {
		case nil:
			continue
		case *float64:
			value = typed
		case float64:
			value = &typed
		default:
			t.Fatalf("expected live query value to be numeric, got %T", rawValue)
		}
		if value == nil {
			continue
		}
		if *value < 20 || *value > 29 {
			t.Fatalf("expected live query value to come from the ingested CSV range [20, 29], got %v", *value)
		}
		sawValue = true
	}
	if !sawValue {
		t.Fatalf("expected at least one non-null value from live query")
	}
}
