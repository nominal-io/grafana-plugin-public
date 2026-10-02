package plugin

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-io/grafana-plugin-public/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	"github.com/palantir/pkg/rid"
)

func TestPrepareQueryAppliesTemplateVariablesAndDefaultsAggregations(t *testing.T) {
	ds := withCatalog(&Datasource{})
	config := &models.PluginSettings{Secrets: &models.SecretPluginSettings{ApiKey: "test-key"}}
	query := backend.DataQuery{
		RefID: "A",
		JSON: mustMarshal(NominalQueryModel{
			AssetRid:      "${asset}",
			Channel:       "$channel",
			DataScopeName: "$scope",
			Buckets:       100,
			TemplateVariables: map[string]interface{}{
				"asset":   "ri.scout.main.asset.1",
				"channel": "temperature",
				"scope":   "default",
			},
		}),
	}

	prepared, prepErr := newTestQueryExecution(ds, config).prepareQuery(context.Background(), query)
	if prepErr != nil {
		t.Fatalf("unexpected preparation error: %v", prepErr.Error)
	}

	if prepared.Kind != preparedQueryBatchable {
		t.Fatalf("expected batchable query, got kind %d", prepared.Kind)
	}
	if prepared.Model.AssetRid != "ri.scout.main.asset.1" {
		t.Errorf("AssetRid = %q, want resolved asset RID", prepared.Model.AssetRid)
	}
	if prepared.Model.Channel != "temperature" {
		t.Errorf("Channel = %q, want temperature", prepared.Model.Channel)
	}
	if prepared.Model.DataScopeName != "default" {
		t.Errorf("DataScopeName = %q, want default", prepared.Model.DataScopeName)
	}
	if prepared.Model.ExplicitAggregations {
		t.Error("expected defaulted aggregations to be marked implicit")
	}
	if len(prepared.Model.Aggregations) != 1 || prepared.Model.Aggregations[0] != AggMean {
		t.Fatalf("Aggregations = %v, want [%s]", prepared.Model.Aggregations, AggMean)
	}
}

func TestPrepareQueryAggregationRules(t *testing.T) {
	ds := withCatalog(&Datasource{})
	config := &models.PluginSettings{Secrets: &models.SecretPluginSettings{ApiKey: "test-key"}}

	tests := []struct {
		name                  string
		model                 NominalQueryModel
		wantErr               string
		wantAggregations      []string
		wantExplicit          bool
		wantPreparedQueryKind preparedQueryKind
		wantRawLTTB           bool
	}{
		{
			name: "explicit numeric aggregations are deduped in order",
			model: NominalQueryModel{
				AssetRid:        "ri.scout.main.asset.1",
				Channel:         "temperature",
				DataScopeName:   "default",
				ChannelDataType: "numeric",
				Aggregations:    []string{AggMin, AggMax, AggMin},
				Buckets:         100,
			},
			wantAggregations:      []string{AggMin, AggMax},
			wantExplicit:          true,
			wantPreparedQueryKind: preparedQueryBatchable,
		},
		{
			name: "invalid numeric aggregation is rejected",
			model: NominalQueryModel{
				AssetRid:        "ri.scout.main.asset.1",
				Channel:         "temperature",
				DataScopeName:   "default",
				ChannelDataType: "numeric",
				Aggregations:    []string{"BOGUS"},
				Buckets:         100,
			},
			wantErr: "unsupported aggregation \"BOGUS\"; valid options are COUNT, FIRST_POINT, LAST_POINT, MAX, MEAN, MIN, VARIANCE, LTTB",
		},
		{
			name:    "buckets above the API limit are rejected",
			model:   NominalQueryModel{AssetRid: "ri.scout.main.asset.1", Channel: "temperature", DataScopeName: "default", ChannelDataType: "numeric", Buckets: 10001},
			wantErr: "buckets must be at most 10000, got 10001",
		},
		{
			name:        "LTTB is accepted as a raw numeric selection",
			model:       NominalQueryModel{AssetRid: "ri.scout.main.asset.1", Channel: "temperature", DataScopeName: "default", ChannelDataType: "numeric", Aggregations: []string{AggLTTB}, Buckets: 100},
			wantRawLTTB: true, wantExplicit: true, wantPreparedQueryKind: preparedQueryBatchable,
		},
		{
			name:        "repeated LTTB is accepted",
			model:       NominalQueryModel{AssetRid: "ri.scout.main.asset.1", Channel: "temperature", DataScopeName: "default", ChannelDataType: "numeric", Aggregations: []string{AggLTTB, AggLTTB}, Buckets: 100},
			wantRawLTTB: true, wantExplicit: true, wantPreparedQueryKind: preparedQueryBatchable,
		},
		{
			name:    "LTTB cannot be combined with bucket aggregation",
			model:   NominalQueryModel{AssetRid: "ri.scout.main.asset.1", Channel: "temperature", DataScopeName: "default", ChannelDataType: "numeric", Aggregations: []string{AggMean, AggLTTB}, Buckets: 100},
			wantErr: "LTTB cannot be combined",
		},
		{
			name: "string channels skip numeric aggregation validation",
			model: NominalQueryModel{
				AssetRid:        "ri.scout.main.asset.1",
				Channel:         "state",
				DataScopeName:   "default",
				ChannelDataType: "string",
				Aggregations:    []string{"BOGUS"},
				Buckets:         100,
			},
			wantAggregations:      []string{"BOGUS"},
			wantExplicit:          true,
			wantPreparedQueryKind: preparedQueryBatchable,
		},
		{
			name: "log channels skip numeric aggregation validation",
			model: NominalQueryModel{
				AssetRid:        "ri.scout.main.asset.1",
				Channel:         "app.logs",
				DataScopeName:   "default",
				ChannelDataType: "log",
				Aggregations:    []string{"BOGUS"},
				Buckets:         100,
			},
			wantAggregations:      []string{"BOGUS"},
			wantExplicit:          true,
			wantPreparedQueryKind: preparedQueryBatchable,
		},
		{
			name: "connection test skips normal validation",
			model: NominalQueryModel{
				QueryType: "connectionTest",
			},
			wantPreparedQueryKind: preparedQueryConnectionTest,
		},
		{
			name: "legacy constant query is prepared as legacy",
			model: NominalQueryModel{
				Constant: 42,
			},
			wantAggregations:      []string{AggMean},
			wantPreparedQueryKind: preparedQueryLegacy,
		},
		{
			name: "asset channel query without data scope is rejected",
			model: NominalQueryModel{
				AssetRid: "ri.scout.main.asset.1",
				Channel:  "temperature",
				Buckets:  100,
			},
			wantErr: "dataScopeName is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := backend.DataQuery{RefID: "A", JSON: mustMarshal(tt.model)}
			prepared, prepErr := newTestQueryExecution(ds, config).prepareQuery(context.Background(), query)

			if tt.wantErr != "" {
				if prepErr == nil {
					t.Fatalf("expected preparation error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(prepErr.Error.Error(), tt.wantErr) {
					t.Fatalf("preparation error = %v, want containing %q", prepErr.Error, tt.wantErr)
				}
				if prepErr.Status != backend.StatusBadRequest {
					t.Errorf("status = %d, want %d", prepErr.Status, backend.StatusBadRequest)
				}
				return
			}
			if prepErr != nil {
				t.Fatalf("unexpected preparation error: %v", prepErr.Error)
			}
			if prepared.Kind != tt.wantPreparedQueryKind {
				t.Fatalf("prepared kind = %d, want %d", prepared.Kind, tt.wantPreparedQueryKind)
			}
			if prepared.Model.ExplicitAggregations != tt.wantExplicit {
				t.Errorf("ExplicitAggregations = %v, want %v", prepared.Model.ExplicitAggregations, tt.wantExplicit)
			}
			if prepared.Model.RawLTTB != tt.wantRawLTTB {
				t.Errorf("RawLTTB = %v, want %v", prepared.Model.RawLTTB, tt.wantRawLTTB)
			}
			if fmt.Sprint(prepared.Model.Aggregations) != fmt.Sprint(tt.wantAggregations) {
				t.Errorf("Aggregations = %v, want %v", prepared.Model.Aggregations, tt.wantAggregations)
			}
		})
	}
}

func TestPrepareQueryInfersMissingChannelType(t *testing.T) {
	tests := []struct {
		name       string
		channel    string
		seriesType api.SeriesDataType_Value
		buckets    int
		want       string
	}{
		{"string channel saved as numeric", "state", api.SeriesDataType_STRING, 100, "string"},
		{"log channel saved as numeric skips the buckets limit", "app.logs", api.SeriesDataType_LOG, 10001, "log"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assetRid := "ri.scout.main.asset.prepare1"
			dataSourceRid := "ri.scout.main.data-source.ds1"
			server := newTestAssetServer(t, map[string]SingleAssetResponse{
				assetRid: {
					Rid:   assetRid,
					Title: "Test Asset",
					DataScopes: []AssetDataScope{
						{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
					},
				},
			}, nil)
			defer server.Close()

			seriesType := api.New_SeriesDataType(tt.seriesType)
			mockDS := &mockDatasourceService{
				searchChannelsResponse: datasourceapi.SearchChannelsResponse{
					Results: []datasourceapi.ChannelMetadata{
						{
							Name:       api.Channel(tt.channel),
							DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1")),
							DataType:   &seriesType,
						},
					},
				},
			}
			ds := withCatalog(&Datasource{
				datasourceService:  mockDS,
				resourceHTTPClient: server.Client(),
			})
			config := &models.PluginSettings{
				BaseUrl: server.URL,
				Secrets: &models.SecretPluginSettings{
					ApiKey: "test-key",
				},
			}
			query := backend.DataQuery{
				RefID: "A",
				JSON: mustMarshal(NominalQueryModel{
					AssetRid:        assetRid,
					Channel:         tt.channel,
					DataScopeName:   "default",
					ChannelDataType: "numeric",
					Aggregations:    []string{AggMean},
					Buckets:         tt.buckets,
				}),
			}

			prepared, prepErr := newTestQueryExecution(ds, config).prepareQuery(context.Background(), query)
			if prepErr != nil {
				t.Fatalf("unexpected preparation error: %v", prepErr.Error)
			}
			if prepared.Model.ChannelDataType != tt.want {
				t.Fatalf("ChannelDataType = %q, want %s", prepared.Model.ChannelDataType, tt.want)
			}
			if got := mockDS.searchChannelsCallCount(); got != 1 {
				t.Fatalf("expected one channel lookup, got %d", got)
			}
		})
	}
}

func TestApplyChannelMetadataPreservesOmittedFields(t *testing.T) {
	qm := NominalQueryModel{
		ChannelDataType: "numeric",
		ChannelUnit:     "Cel",
	}

	applyChannelMetadata(&qm, channelMetadataCacheEntry{unit: "psia"})
	if qm.ChannelDataType != "numeric" {
		t.Errorf("ChannelDataType = %q, want existing numeric type preserved", qm.ChannelDataType)
	}
	if qm.ChannelUnit != "psia" {
		t.Errorf("ChannelUnit = %q, want psia", qm.ChannelUnit)
	}

	applyChannelMetadata(&qm, channelMetadataCacheEntry{channelDataType: "string"})
	if qm.ChannelDataType != "string" {
		t.Errorf("ChannelDataType = %q, want string", qm.ChannelDataType)
	}
	if qm.ChannelUnit != "psia" {
		t.Errorf("ChannelUnit = %q, want existing psia unit preserved", qm.ChannelUnit)
	}
}

// Covers the SearchChannels result shapes inferChannelMetadata must handle:
// type + unit, nil unit, nil DataType + unit, and no name match.
func TestPrepareQueryInfersChannelUnit(t *testing.T) {
	const (
		assetRid      = "ri.scout.main.asset.unitprobe"
		dataSourceRid = "ri.scout.main.data-source.ds1"
	)
	setupServer := func(t *testing.T) *httptest.Server {
		dsRidRef := dataSourceRid
		return newTestAssetServer(t, map[string]SingleAssetResponse{
			assetRid: {
				Rid:   assetRid,
				Title: "Test Asset",
				DataScopes: []AssetDataScope{
					{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dsRidRef}},
				},
			},
		}, nil)
	}

	dsRid := rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))
	numericType := api.New_SeriesDataType(api.SeriesDataType_DOUBLE)

	// Every case calls prepareQuery twice: the first call sets the model shape,
	// the second must hit the cache and make no SearchChannels call.
	tests := []struct {
		name           string
		queryChannel   string
		searchChannels []datasourceapi.ChannelMetadata
		wantUnit       string
		wantDataType   string // expected ChannelDataType on the prepared model
	}{
		{
			name:         "non-nil DataType + non-nil Unit: both populated, cache hit restores both",
			queryChannel: "engine_temp",
			searchChannels: []datasourceapi.ChannelMetadata{{
				Name:       api.Channel("engine_temp"),
				DataSource: dsRid,
				DataType:   &numericType,
				Unit:       &runapi.Unit{Symbol: "Cel"},
			}},
			wantUnit:     "Cel",
			wantDataType: "numeric",
		},
		{
			name:         "non-nil DataType + nil Unit: ChannelUnit stays empty",
			queryChannel: "engine_temp",
			searchChannels: []datasourceapi.ChannelMetadata{{
				Name:       api.Channel("engine_temp"),
				DataSource: dsRid,
				DataType:   &numericType,
				// Unit deliberately nil
			}},
			wantUnit:     "",
			wantDataType: "numeric",
		},
		{
			name:         "nil DataType + non-nil Unit: ChannelUnit still populated (cache-write ordering guard)",
			queryChannel: "engine_temp",
			searchChannels: []datasourceapi.ChannelMetadata{{
				Name:       api.Channel("engine_temp"),
				DataSource: dsRid,
				// DataType deliberately nil
				Unit: &runapi.Unit{Symbol: "psia"},
			}},
			// An empty inferred type must not short-circuit the unit write.
			wantUnit:     "psia",
			wantDataType: "numeric", // frontend-supplied type stands when ChannelMetadata.DataType is nil
		},
		{
			name:           "no name match: empty cache entry written, no re-search on second call",
			queryChannel:   "missing_channel",
			searchChannels: []datasourceapi.ChannelMetadata{}, // empty results
			wantUnit:       "",
			wantDataType:   "numeric", // unchanged from the frontend-supplied value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := setupServer(t)
			defer server.Close()

			mockDS := &mockDatasourceService{
				searchChannelsResponse: datasourceapi.SearchChannelsResponse{Results: tt.searchChannels},
			}
			ds := withCatalog(&Datasource{datasourceService: mockDS, resourceHTTPClient: server.Client()})
			config := &models.PluginSettings{
				BaseUrl: server.URL,
				Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
			}
			query := backend.DataQuery{
				RefID: "A",
				JSON: mustMarshal(NominalQueryModel{
					AssetRid: assetRid, Channel: tt.queryChannel, DataScopeName: "default",
					ChannelDataType: "numeric", Aggregations: []string{AggMean}, Buckets: 100,
				}),
			}

			prep1, err1 := newTestQueryExecution(ds, config).prepareQuery(context.Background(), query)
			if err1 != nil {
				t.Fatalf("first prepare: %v", err1.Error)
			}
			if prep1.Model.ChannelUnit != tt.wantUnit {
				t.Errorf("first call ChannelUnit = %q, want %q", prep1.Model.ChannelUnit, tt.wantUnit)
			}
			if prep1.Model.ChannelDataType != tt.wantDataType {
				t.Errorf("first call ChannelDataType = %q, want %q", prep1.Model.ChannelDataType, tt.wantDataType)
			}

			// Second call must hit the cache regardless of the first-call shape
			// (populated, type-only, unit-only, or empty miss).
			prep2, err2 := newTestQueryExecution(ds, config).prepareQuery(context.Background(), query)
			if err2 != nil {
				t.Fatalf("second prepare: %v", err2.Error)
			}
			if prep2.Model.ChannelUnit != tt.wantUnit {
				t.Errorf("cache-hit ChannelUnit = %q, want %q", prep2.Model.ChannelUnit, tt.wantUnit)
			}
			if got := mockDS.searchChannelsCallCount(); got != 1 {
				t.Errorf("expected 1 SearchChannels call (cache hit on second), got %d", got)
			}
		})
	}
}

func TestPrepareQueryResolvesRuns(t *testing.T) {
	const (
		assetRid      = "ri.scout.main.asset.runprobe"
		otherAssetRid = "ri.scout.main.asset.other"
		run1          = "ri.scout.main.run.1"
		run2          = "ri.scout.main.run.2"
		run3          = "ri.scout.main.run.3"
	)
	server := newTestAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {Rid: assetRid, Title: "Test Asset"},
	}, nil)
	defer server.Close()

	runService := newMockRunService(testRun(run1, assetRid), testRun(run2, assetRid, otherAssetRid))
	runService.failFor = run3
	ds := withCatalog(&Datasource{
		datasourceService:  &mockDatasourceService{},
		resourceHTTPClient: server.Client(),
		runService:         runService,
	})
	config := &models.PluginSettings{
		BaseUrl: server.URL,
		Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
	}

	runQuery := func(runRid string) NominalQueryModel {
		return NominalQueryModel{ComputeBy: computeByRun, RunRid: runRid, Channel: "temp", DataScopeName: "default", Buckets: 100}
	}
	templated := runQuery("$run")
	templated.TemplateVariables = map[string]interface{}{"run": run1}
	noRun := NominalQueryModel{ComputeBy: computeByRun}
	badAsset := NominalQueryModel{AssetRid: "*", Channel: "temp", DataScopeName: "default", Buckets: 100,
		TemplateSources: templateSources{AssetRid: &templateSource{Raw: "$asset", Name: "asset"}}}

	tests := []struct {
		name    string
		model   NominalQueryModel
		wantRun string
		wantErr string
	}{
		{"single-asset run", runQuery(run1), run1, ""},
		{"multi-asset run", runQuery(run2), "", "spans 2 assets"},
		{"missing run", runQuery("ri.scout.main.run.9"), "", "not found"},
		{"not a run RID", runQuery(assetRid), "", "is not a run RID"},
		{"templated asset that is not an RID", badAsset, "", "Query validation failed: Asset is `$asset`, currently `*`. That is not an asset RID."},
		{"no runRid", noRun, "", "runRid is required"},
		{"templated runRid", templated, run1, ""},
		{"run lookup fails", runQuery(run3), "", "Failed to load run"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inRun := backend.TimeRange{From: time.Unix(1700000000, 0), To: time.Unix(1700001000, 0)}
			query := backend.DataQuery{RefID: "A", TimeRange: inRun, JSON: mustMarshal(tt.model)}
			prep, resp := newTestQueryExecution(ds, config).prepareQuery(context.Background(), query)
			if tt.wantErr != "" {
				if resp == nil || resp.Error == nil || !strings.Contains(resp.Error.Error(), tt.wantErr) {
					t.Fatalf("response = %+v, want error containing %q", resp, tt.wantErr)
				}
				return
			}
			if resp != nil {
				t.Fatalf("unexpected error: %v", resp.Error)
			}
			if prep.Kind != preparedQueryBatchable {
				t.Errorf("Kind = %v, want batchable", prep.Kind)
			}
			if prep.Model.AssetRid != assetRid {
				t.Errorf("AssetRid = %q, want %q", prep.Model.AssetRid, assetRid)
			}
			if prep.Model.Run == nil || prep.Model.Run.Rid != tt.wantRun {
				t.Errorf("Run = %+v, want rid %q", prep.Model.Run, tt.wantRun)
			}
		})
	}
}

func TestLogChannelSkipsAggregationValidation(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockPagedLogResult([]string{"test entry"}, []map[string]string{{"k": "v"}}, nil),
			},
		},
	}

	ds := withCatalog(&Datasource{
		settings:       testDatasourceSettings(),
		computeService: mockService,
	})

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// A log query with no aggregations must neither get the default ["MEAN"]
	// nor be rejected.
	req := newQueryRequest([]backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.test", Channel: "app.logs", DataScopeName: "default", ChannelDataType: "log", Buckets: 100}),
			TimeRange: timeRange,
		},
	})

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	response, ok := resp.Responses["A"]
	if !ok {
		t.Fatal("expected response for refID A")
	}
	// No StatusBadRequest about aggregations.
	if response.Status == backend.StatusBadRequest {
		t.Errorf("log query was rejected with bad request: %v", response.Error)
	}
	// Should produce a log frame, not a numeric frame.
	if len(response.Frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(response.Frames))
	}
	if response.Frames[0].Meta == nil || response.Frames[0].Meta.Type != data.FrameTypeLogLines {
		t.Errorf("expected FrameTypeLogLines, got %v", response.Frames[0].Meta)
	}
}

func TestRidFieldError(t *testing.T) {
	const (
		validRun   = "ri.scout.main.run.1"
		validRun2  = "ri.scout.main.run.2"
		validAsset = "ri.nominal.asset.test"
	)
	src := func(raw string) *templateSource {
		return &templateSource{Raw: raw, Name: strings.TrimLeft(raw, "${")}
	}
	tests := []struct {
		name      string
		label     string
		ridType   string
		noun      string
		value     string
		src       *templateSource
		wantParts []string
		wantNone  []string
	}{
		{"templated asset", "Asset", "asset", "an asset RID", "*", src("$asset"), []string{"not an asset RID", "Check that variable's query."}, []string{"a asset", "nominal_nominalds"}},
		{"templated run", "Run", "run", "a run RID", "12", src("$myvar"), []string{"Check that variable's query."}, []string{"nominal_nominalds"}},
		{"unknown variable", "Run", "run", "a run RID", "$foo", src("$foo"), []string{"No variable named `foo`."}, nil},
		{"multi-value variable", "Run", "run", "a run RID", "{" + validRun + "," + validRun2 + "}", src("$run"), []string{"currently 2 values"}, nil},
		{"valid templated RID", "Run", "run", "a run RID", validRun, src("$run"), nil, nil},
		{"literal asset text", "Asset", "asset", "an asset RID", validAsset, nil, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ridFieldError(tt.label, tt.ridType, tt.noun, tt.value, tt.src)
			if len(tt.wantParts) == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want error containing %q, got nil", tt.wantParts)
			}
			for _, part := range tt.wantParts {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q missing %q", err, part)
				}
			}
			for _, part := range tt.wantNone {
				if strings.Contains(err.Error(), part) {
					t.Errorf("error %q should not contain %q", err, part)
				}
			}
		})
	}
}
