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
	"github.com/nominal-inc/nominal-ds/pkg/models"
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
			wantErr: "unsupported aggregation \"BOGUS\"",
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
			if fmt.Sprint(prepared.Model.Aggregations) != fmt.Sprint(tt.wantAggregations) {
				t.Errorf("Aggregations = %v, want %v", prepared.Model.Aggregations, tt.wantAggregations)
			}
		})
	}
}

func TestPrepareQueryInfersMissingChannelType(t *testing.T) {
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

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("state"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1")),
					DataType:   &stringType,
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
			Channel:         "state",
			DataScopeName:   "default",
			ChannelDataType: "numeric",
			Aggregations:    []string{AggMean},
			Buckets:         100,
		}),
	}

	prepared, prepErr := newTestQueryExecution(ds, config).prepareQuery(context.Background(), query)
	if prepErr != nil {
		t.Fatalf("unexpected preparation error: %v", prepErr.Error)
	}
	if prepared.Model.ChannelDataType != "string" {
		t.Fatalf("ChannelDataType = %q, want string", prepared.Model.ChannelDataType)
	}
	if got := mockDS.searchChannelsCallCount(); got != 1 {
		t.Fatalf("expected one channel lookup, got %d", got)
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
