package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	"github.com/nominal-io/nominal-api-go/api/rids"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
	"github.com/palantir/pkg/safelong"
)

func newTestQueryExecution(ds *Datasource, config *models.PluginSettings) *NominalQueryExecution {
	if config == nil {
		config = &models.PluginSettings{
			Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
		}
	}
	return newNominalQueryExecution(ds, config)
}

func TestPrepareQueryAppliesTemplateVariablesAndDefaultsAggregations(t *testing.T) {
	ds := &Datasource{}
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
	ds := &Datasource{}
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
	ds := &Datasource{
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	}
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
	if mockDS.searchChannelsCalls != 1 {
		t.Fatalf("expected one channel lookup, got %d", mockDS.searchChannelsCalls)
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

func TestChannelMetadataEntryForExactMatch(t *testing.T) {
	numericType := api.New_SeriesDataType(api.SeriesDataType_DOUBLE)

	tests := []struct {
		name        string
		channels    []datasourceapi.ChannelMetadata
		channelName string
		wantEntry   channelMetadataCacheEntry
		wantOK      bool
	}{
		{
			name: "exact match returns normalized type and trimmed unit",
			channels: []datasourceapi.ChannelMetadata{{
				Name:     api.Channel("engine_temp"),
				DataType: &numericType,
				Unit:     &runapi.Unit{Symbol: " Cel "},
			}},
			channelName: "engine_temp",
			wantEntry: channelMetadataCacheEntry{
				channelDataType: "numeric",
				unit:            "Cel",
			},
			wantOK: true,
		},
		{
			name: "case mismatch is ignored",
			channels: []datasourceapi.ChannelMetadata{{
				Name:     api.Channel("Engine_Temp"),
				DataType: &numericType,
				Unit:     &runapi.Unit{Symbol: "Cel"},
			}},
			channelName: "engine_temp",
			wantOK:      false,
		},
		{
			name: "exact match with no usable metadata is ignored",
			channels: []datasourceapi.ChannelMetadata{{
				Name: api.Channel("engine_temp"),
			}},
			channelName: "engine_temp",
			wantOK:      false,
		},
		{
			name: "unit-only exact match returns entry",
			channels: []datasourceapi.ChannelMetadata{{
				Name: api.Channel("engine_temp"),
				Unit: &runapi.Unit{Symbol: "psia"},
			}},
			channelName: "engine_temp",
			wantEntry: channelMetadataCacheEntry{
				unit: "psia",
			},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := channelMetadataEntryForExactMatch(tt.channels, tt.channelName)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got.channelDataType != tt.wantEntry.channelDataType {
				t.Errorf("channelDataType = %q, want %q", got.channelDataType, tt.wantEntry.channelDataType)
			}
			if got.unit != tt.wantEntry.unit {
				t.Errorf("unit = %q, want %q", got.unit, tt.wantEntry.unit)
			}
		})
	}
}

// TestPrepareQueryInfersChannelUnit guards the unit branch of inferChannelMetadata.
// Covers the SearchChannels result shapes inferChannelMetadata must handle:
// type + unit, nil unit, nil DataType + unit, and no name match (all cached).
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

	// Every case exercises the same flow: prepareQuery twice → assert the model
	// shape from the first call and that the second call hits the cache (no
	// second SearchChannels). The varying inputs are the SearchChannels response
	// and the queried channel name; the varying outputs are the resolved
	// ChannelUnit and ChannelDataType.
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
			ds := &Datasource{datasourceService: mockDS, resourceHTTPClient: server.Client()}
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
			if mockDS.searchChannelsCalls != 1 {
				t.Errorf("expected 1 SearchChannels call (cache hit on second), got %d", mockDS.searchChannelsCalls)
			}
		})
	}
}

func TestPartitionPreparedQueriesKeepsQueryModelPairs(t *testing.T) {
	prepared := []preparedQuery{
		{
			Query: backend.DataQuery{RefID: "numeric"},
			Model: NominalQueryModel{Channel: "temperature", ChannelDataType: "numeric"},
			Kind:  preparedQueryBatchable,
		},
		{
			Query: backend.DataQuery{RefID: "logs"},
			Model: NominalQueryModel{Channel: "app.logs", ChannelDataType: "log"},
			Kind:  preparedQueryBatchable,
		},
		{
			Query: backend.DataQuery{RefID: "string"},
			Model: NominalQueryModel{Channel: "state", ChannelDataType: "string"},
			Kind:  preparedQueryBatchable,
		},
	}

	logBatch, otherBatch := partitionPreparedQueries(prepared)

	if len(logBatch.queries) != 1 || len(logBatch.models) != 1 {
		t.Fatalf("expected one log query/model pair, got %d queries and %d models", len(logBatch.queries), len(logBatch.models))
	}
	if logBatch.queries[0].RefID != "logs" || logBatch.models[0].Channel != "app.logs" {
		t.Fatalf("log pair was not preserved: query=%v model=%v", logBatch.queries[0].RefID, logBatch.models[0].Channel)
	}

	if len(otherBatch.queries) != 2 || len(otherBatch.models) != 2 {
		t.Fatalf("expected two non-log query/model pairs, got %d queries and %d models", len(otherBatch.queries), len(otherBatch.models))
	}
	for i := range otherBatch.queries {
		if otherBatch.queries[i].RefID == "numeric" && otherBatch.models[i].Channel != "temperature" {
			t.Fatalf("numeric query/model pair was not preserved: model=%v", otherBatch.models[i].Channel)
		}
		if otherBatch.queries[i].RefID == "string" && otherBatch.models[i].Channel != "state" {
			t.Fatalf("string query/model pair was not preserved: model=%v", otherBatch.models[i].Channel)
		}
	}
}

func TestQueryDataWithNilDataSourceInstanceSettings(t *testing.T) {
	ds := &Datasource{}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: nil, // Not configured
		},
		Queries: []backend.DataQuery{
			{RefID: "A"},
			{RefID: "B"},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return error responses for all queries
	if len(resp.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(resp.Responses))
	}

	for refID, response := range resp.Responses {
		if response.Error == nil {
			t.Errorf("expected error for query %s, got nil", refID)
		}
		if response.Status != backend.StatusBadRequest {
			t.Errorf("expected StatusBadRequest for query %s, got %v", refID, response.Status)
		}
	}
}

func TestCheckHealthWithNilDataSourceInstanceSettings(t *testing.T) {
	ds := &Datasource{}

	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: nil,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != backend.HealthStatusError {
		t.Fatalf("expected error health status, got %v", result.Status)
	}
	if result.Message != "Data source is not configured" {
		t.Fatalf("expected not configured message, got %q", result.Message)
	}
}

func TestQueryDataWithInvalidJSON(t *testing.T) {
	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{
				RefID: "A",
				JSON:  []byte(`{invalid json`),
			},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(resp.Responses))
	}

	response := resp.Responses["A"]
	if response.Error == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
	if response.Status != backend.StatusBadRequest {
		t.Errorf("expected StatusBadRequest, got %v", response.Status)
	}
}

func TestQueryDataRoutesQueriesByType(t *testing.T) {
	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name           string
		queries        []backend.DataQuery
		expectedRefIDs []string
		checkFrame     func(t *testing.T, refID string, response backend.DataResponse)
	}{
		{
			name: "routes legacy constant query correctly",
			queries: []backend.DataQuery{
				{
					RefID:     "LegacyConstant",
					JSON:      mustMarshal(NominalQueryModel{Constant: 42.0}),
					TimeRange: timeRange,
				},
			},
			expectedRefIDs: []string{"LegacyConstant"},
			checkFrame: func(t *testing.T, refID string, response backend.DataResponse) {
				if response.Error != nil {
					t.Errorf("unexpected error for %s: %v", refID, response.Error)
				}
				if len(response.Frames) != 1 {
					t.Fatalf("expected 1 frame for %s, got %d", refID, len(response.Frames))
				}
				if response.Frames[0].Name != "response" {
					t.Errorf("expected frame name 'response' for %s, got %q", refID, response.Frames[0].Name)
				}
			},
		},
		{
			name: "routes legacy query text query correctly",
			queries: []backend.DataQuery{
				{
					RefID:     "LegacyQueryText",
					JSON:      mustMarshal(NominalQueryModel{QueryText: "SELECT * FROM data"}),
					TimeRange: timeRange,
				},
			},
			expectedRefIDs: []string{"LegacyQueryText"},
			checkFrame: func(t *testing.T, refID string, response backend.DataResponse) {
				if response.Error != nil {
					t.Errorf("unexpected error for %s: %v", refID, response.Error)
				}
				if len(response.Frames) != 1 {
					t.Fatalf("expected 1 frame for %s, got %d", refID, len(response.Frames))
				}
			},
		},
		{
			name: "handles multiple legacy queries",
			queries: []backend.DataQuery{
				{
					RefID:     "A",
					JSON:      mustMarshal(NominalQueryModel{Constant: 10.0}),
					TimeRange: timeRange,
				},
				{
					RefID:     "B",
					JSON:      mustMarshal(NominalQueryModel{Constant: 20.0}),
					TimeRange: timeRange,
				},
			},
			expectedRefIDs: []string{"A", "B"},
			checkFrame: func(t *testing.T, refID string, response backend.DataResponse) {
				if response.Error != nil {
					t.Errorf("unexpected error for %s: %v", refID, response.Error)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &backend.QueryDataRequest{
				PluginContext: backend.PluginContext{
					DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
						JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
						DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
					},
				},
				Queries: tt.queries,
			}

			resp, err := ds.QueryData(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(resp.Responses) != len(tt.expectedRefIDs) {
				t.Fatalf("expected %d responses, got %d", len(tt.expectedRefIDs), len(resp.Responses))
			}

			for _, refID := range tt.expectedRefIDs {
				response, ok := resp.Responses[refID]
				if !ok {
					t.Errorf("expected response for %q", refID)
					continue
				}
				if tt.checkFrame != nil {
					tt.checkFrame(t, refID, response)
				}
			}
		})
	}
}

// mustMarshal is a test helper that panics on marshal failure
func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func makeBatchableQueries(count int, timeRange backend.TimeRange) []backend.DataQuery {
	queries := make([]backend.DataQuery, count)
	for i := 0; i < count; i++ {
		queries[i] = backend.DataQuery{
			RefID:     fmt.Sprintf("Q%03d", i),
			JSON:      mustMarshal(NominalQueryModel{AssetRid: fmt.Sprintf("ri.nominal.asset.%d", i+1), Channel: fmt.Sprintf("temp%d", i+1), DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		}
	}
	return queries
}

func makeBatchComputeWithUnitsResponse(count int) computeapi.BatchComputeWithUnitsResponse {
	results := make([]computeapi.ComputeWithUnitsResult, count)
	for i := 0; i < count; i++ {
		results[i] = createMockArrowComputeResult([]float64{float64(i + 1)})
	}
	return computeapi.BatchComputeWithUnitsResponse{Results: results}
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
	defer m.mu.Unlock()
	m.batchComputeCalls++
	m.lastBatchRequest = requestArg
	m.batchRequests = append(m.batchRequests, requestArg)

	if m.batchComputeFunc != nil {
		return m.batchComputeFunc(requestArg)
	}

	callIndex := m.batchComputeCalls - 1
	if callIndex < len(m.batchComputeErrors) && m.batchComputeErrors[callIndex] != nil {
		return computeapi.BatchComputeWithUnitsResponse{}, m.batchComputeErrors[callIndex]
	}
	if callIndex < len(m.batchComputeResponses) {
		return m.batchComputeResponses[callIndex], nil
	}
	if m.batchComputeError != nil {
		return computeapi.BatchComputeWithUnitsResponse{}, m.batchComputeError
	}
	return m.batchComputeResponse, nil
}

func (m *mockComputeService) BatchComputeUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.BatchComputeUnitsRequest) (computeapi.BatchComputeUnitResult, error) {
	return computeapi.BatchComputeUnitResult{}, nil
}

func (m *mockComputeService) ComputeWithUnits(ctx context.Context, authHeader bearertoken.Token, requestArg computeapi1.ComputeWithUnitsRequest) (computeapi.ComputeWithUnitsResponse, error) {
	return computeapi.ComputeWithUnitsResponse{}, nil
}

func TestBatchQueryExecution(t *testing.T) {
	// Create mock compute service
	mockService := &mockComputeService{}

	// Create mock response with Arrow bucketed numeric results (production format)
	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0, 3.0}),
			createMockArrowComputeResult([]float64{4.0, 5.0, 6.0}),
			createMockArrowComputeResult([]float64{7.0, 8.0, 9.0}),
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// Create 3 batchable queries (asset + channel)
	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify batch was called exactly once (not 3 times).
	// All queries above leave ChannelDataType unset, so they land in the non-log
	// partition and produce a single batch call. The log partition is empty and
	// makes no backend call. If you add a log query here, expect 2 batch calls.
	// See TestMixedLogNumericParallelBatch for the partitioned scenario.
	if mockService.batchComputeCalls != 1 {
		t.Errorf("expected 1 batch compute call, got %d", mockService.batchComputeCalls)
	}

	// Verify single compute was never called
	if mockService.singleComputeCalls != 0 {
		t.Errorf("expected 0 single compute calls, got %d", mockService.singleComputeCalls)
	}

	// Verify the batch request contained all 3 queries
	if len(mockService.lastBatchRequest.Requests) != 3 {
		t.Errorf("expected 3 requests in batch, got %d", len(mockService.lastBatchRequest.Requests))
	}

	// Verify we got responses for all queries
	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	for _, refID := range []string{"A", "B", "C"} {
		response, ok := resp.Responses[refID]
		if !ok {
			t.Errorf("expected response for %q", refID)
			continue
		}
		if response.Error != nil {
			t.Errorf("unexpected error for %s: %v", refID, response.Error)
		}
	}
}

func TestBatchQueryChunksAtSubrequestLimit(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponses: []computeapi.BatchComputeWithUnitsResponse{
			makeBatchComputeWithUnitsResponse(maxBatchComputeSubrequests),
			makeBatchComputeWithUnitsResponse(1),
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	queries := makeBatchableQueries(maxBatchComputeSubrequests+1, timeRange)

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockService.batchComputeCalls != 2 {
		t.Fatalf("expected 2 batch compute calls, got %d", mockService.batchComputeCalls)
	}
	if len(mockService.batchRequests) != 2 {
		t.Fatalf("expected 2 recorded batch requests, got %d", len(mockService.batchRequests))
	}
	if len(mockService.batchRequests[0].Requests) != maxBatchComputeSubrequests {
		t.Fatalf("expected first chunk size %d, got %d", maxBatchComputeSubrequests, len(mockService.batchRequests[0].Requests))
	}
	if len(mockService.batchRequests[1].Requests) != 1 {
		t.Fatalf("expected second chunk size 1, got %d", len(mockService.batchRequests[1].Requests))
	}
	if len(resp.Responses) != len(queries) {
		t.Fatalf("expected %d responses, got %d", len(queries), len(resp.Responses))
	}

	for _, q := range queries {
		response := resp.Responses[q.RefID]
		if response.Error != nil {
			t.Fatalf("expected no error for %s, got %v", q.RefID, response.Error)
		}
	}
}

func TestQueryDataInfersMissingStringChannelType(t *testing.T) {
	assetRid := "ri.scout.main.asset.abc123"
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
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockEnumComputeResult([]string{"idle", "active"}, []int{0, 1}),
			},
		},
	}

	ds := &Datasource{
		computeService:     mockCompute,
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{
				RefID: "A",
				JSON: mustMarshal(NominalQueryModel{
					AssetRid:      assetRid,
					Channel:       "state",
					DataScopeName: "default",
					Buckets:       100,
				}),
				TimeRange: timeRange,
			},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockCompute.batchComputeCalls != 1 {
		t.Fatalf("expected 1 batch compute call, got %d", mockCompute.batchComputeCalls)
	}
	if len(mockCompute.lastBatchRequest.Requests) != 1 {
		t.Fatalf("expected 1 compute request, got %d", len(mockCompute.lastBatchRequest.Requests))
	}
	if len(mockDS.searchChannelsRequest.ExactMatch) != 1 || mockDS.searchChannelsRequest.ExactMatch[0] != "state" {
		t.Fatalf("expected exact-match channel lookup for state, got %v", mockDS.searchChannelsRequest.ExactMatch)
	}

	response := resp.Responses["A"]
	if response.Error != nil {
		t.Fatalf("unexpected response error: %v", response.Error)
	}

	series := summarizeSeriesFromNode(t, mockCompute.lastBatchRequest.Requests[0].Node)
	if kind := seriesKind(t, series.Input); kind != "enum" {
		t.Fatalf("expected enum compute request after inferring string type, got series kind %q", kind)
	}
}

// TestMixedTypeTemplateVariableWithExplicitAggregations verifies that a saved
// numeric query with explicit aggregations correctly handles expansion into both
// string and numeric channels. inferChannelMetadata must override the saved type
// per-query so the string channel gets an enum request (not Arrow numeric).
func TestMixedTypeTemplateVariableWithExplicitAggregations(t *testing.T) {
	assetRid := "ri.scout.main.asset.abc123"
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
	numericType := api.New_SeriesDataType(api.SeriesDataType_DOUBLE)
	mockDS := &mockDatasourceService{
		searchChannelsFunc: func(_ context.Context, _ bearertoken.Token, req datasourceapi.SearchChannelsRequest) (datasourceapi.SearchChannelsResponse, error) {
			if len(req.ExactMatch) == 0 {
				return datasourceapi.SearchChannelsResponse{}, nil
			}
			chName := req.ExactMatch[0]
			dsRid := rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1"))
			switch chName {
			case "state":
				return datasourceapi.SearchChannelsResponse{
					Results: []datasourceapi.ChannelMetadata{
						{Name: api.Channel("state"), DataSource: dsRid, DataType: &stringType},
					},
				}, nil
			case "temperature":
				return datasourceapi.SearchChannelsResponse{
					Results: []datasourceapi.ChannelMetadata{
						{Name: api.Channel("temperature"), DataSource: dsRid, DataType: &numericType},
					},
				}, nil
			default:
				return datasourceapi.SearchChannelsResponse{}, nil
			}
		},
	}

	// Both queries are batched into a single API call. First result is Arrow
	// bucketed numeric (with mean+min columns), second is enum for the string channel.
	arrowBytes := createTestArrowMultiAgg(
		[]int64{1000000000000, 2000000000000},
		map[string][]float64{"mean": {10.0, 20.0}, "min": {5.0, 15.0}},
	)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				{ComputeResult: computeapi.NewComputeNodeResultFromSuccess(
					computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot),
				)},
				createMockEnumComputeResult([]string{"idle", "active"}, []int{0, 1}),
			},
		},
	}

	ds := &Datasource{
		computeService:     mockCompute,
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// Two queries simulating template variable expansion: same asset, explicit
	// aggregations, but one channel is numeric and the other is string.
	// Both start with channelDataType "numeric" (inherited from the saved query).
	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{
				RefID: "A",
				JSON: mustMarshal(NominalQueryModel{
					AssetRid:        assetRid,
					Channel:         "temperature",
					DataScopeName:   "default",
					ChannelDataType: "numeric",
					Aggregations:    []string{"MEAN", "MIN"},
					Buckets:         100,
				}),
				TimeRange: timeRange,
			},
			{
				RefID: "B",
				JSON: mustMarshal(NominalQueryModel{
					AssetRid:        assetRid,
					Channel:         "state",
					DataScopeName:   "default",
					ChannelDataType: "numeric", // saved as numeric, but actually string
					Aggregations:    []string{"MEAN", "MIN"},
					Buckets:         100,
				}),
				TimeRange: timeRange,
			},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Both queries should succeed.
	if resp.Responses["A"].Error != nil {
		t.Fatalf("query A error: %v", resp.Responses["A"].Error)
	}
	if resp.Responses["B"].Error != nil {
		t.Fatalf("query B error: %v", resp.Responses["B"].Error)
	}

	// Both queries should be batched into a single API call.
	if mockCompute.batchComputeCalls != 1 {
		t.Fatalf("expected 1 batch compute call, got %d", mockCompute.batchComputeCalls)
	}
	if len(mockCompute.lastBatchRequest.Requests) != 2 {
		t.Fatalf("expected 2 requests in batch, got %d", len(mockCompute.lastBatchRequest.Requests))
	}

	// Verify the numeric query (temperature) built an Arrow request with output fields.
	numericSeries := summarizeSeriesFromNode(t, mockCompute.lastBatchRequest.Requests[0].Node)
	if kind := seriesKind(t, numericSeries.Input); kind != "numeric" {
		t.Errorf("expected numeric series, got kind %q", kind)
	}
	if !isArrowV3(numericSeries.OutputFormat) {
		t.Errorf("expected numeric request with ARROW_V3 output format, got %v", numericSeries.OutputFormat)
	}

	// Verify the string query (state) built an enum request (no output format).
	enumSeries := summarizeSeriesFromNode(t, mockCompute.lastBatchRequest.Requests[1].Node)
	if kind := seriesKind(t, enumSeries.Input); kind != "enum" {
		t.Errorf("expected enum series, got kind %q", kind)
	}
	if enumSeries.OutputFormat != nil {
		t.Errorf("expected enum request without output format, got %v", enumSeries.OutputFormat)
	}
}

func TestBatchQueryChunkTransportErrorOnlyFailsThatChunk(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponses: []computeapi.BatchComputeWithUnitsResponse{
			makeBatchComputeWithUnitsResponse(maxBatchComputeSubrequests),
		},
		batchComputeErrors: []error{
			nil,
			fmt.Errorf("API error: service unavailable"),
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	queries := makeBatchableQueries(maxBatchComputeSubrequests+1, timeRange)

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockService.batchComputeCalls != 2 {
		t.Fatalf("expected 2 batch compute calls, got %d", mockService.batchComputeCalls)
	}

	for i := 0; i < maxBatchComputeSubrequests; i++ {
		refID := fmt.Sprintf("Q%03d", i)
		response := resp.Responses[refID]
		if response.Error != nil {
			t.Fatalf("expected success for %s, got %v", refID, response.Error)
		}
	}

	failedChunkRefID := fmt.Sprintf("Q%03d", maxBatchComputeSubrequests)
	failedChunkResponse := resp.Responses[failedChunkRefID]
	if failedChunkResponse.Error == nil {
		t.Fatalf("expected error for %s, got nil", failedChunkRefID)
	}
	if !strings.Contains(failedChunkResponse.Error.Error(), "Batch compute failed") {
		t.Fatalf("expected batch failure message for %s, got %v", failedChunkRefID, failedChunkResponse.Error)
	}
}

func TestBatchQueryMixedWithLegacy(t *testing.T) {
	// Create mock compute service
	mockService := &mockComputeService{}

	// Create mock response for the 2 batchable queries (Arrow format, matching production)
	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0}),
			createMockArrowComputeResult([]float64{3.0, 4.0}),
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// Mix of batchable and legacy queries
	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{Constant: 42.0}), // Legacy - not batched
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify batch was called once for the 2 batchable queries
	if mockService.batchComputeCalls != 1 {
		t.Errorf("expected 1 batch compute call, got %d", mockService.batchComputeCalls)
	}

	// Verify the batch request contained only the 2 batchable queries
	if len(mockService.lastBatchRequest.Requests) != 2 {
		t.Errorf("expected 2 requests in batch, got %d", len(mockService.lastBatchRequest.Requests))
	}

	// Verify we got responses for all 3 queries
	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	// Legacy query B should have been handled separately
	respB, ok := resp.Responses["B"]
	if !ok {
		t.Error("expected response for legacy query B")
	} else if respB.Error != nil {
		t.Errorf("unexpected error for B: %v", respB.Error)
	} else if len(respB.Frames) != 1 || respB.Frames[0].Name != "response" {
		t.Error("legacy query B should have frame named 'response'")
	}
}

func TestBatchQueryError(t *testing.T) {
	// Create mock compute service that returns an error
	mockService := &mockComputeService{
		batchComputeError: fmt.Errorf("API error: service unavailable"),
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify we got error responses for all queries when batch fails
	for _, refID := range []string{"A", "B"} {
		response, ok := resp.Responses[refID]
		if !ok {
			t.Errorf("expected response for %q", refID)
			continue
		}
		if response.Error == nil {
			t.Errorf("expected error for %s, got nil", refID)
		}
		if !strings.Contains(response.Error.Error(), "Batch compute failed") {
			t.Errorf("expected batch error message for %s, got: %v", refID, response.Error)
		}
	}
}

func TestBatchQueryWithPartialErrors(t *testing.T) {
	// Create mock compute service
	mockService := &mockComputeService{}

	// Create mock response with mix of success and error results
	// This simulates a batch where one query fails (e.g., channel not found)
	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0, 3.0}), // Query A: Success
			createMockErrorResult(404, "CHANNEL_NOT_FOUND"),        // Query B: Error
			createMockArrowComputeResult([]float64{7.0, 8.0, 9.0}), // Query C: Success
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "nonexistent", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify we got responses for all queries
	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	// Query A should succeed
	respA := resp.Responses["A"]
	if respA.Error != nil {
		t.Errorf("expected no error for A, got: %v", respA.Error)
	}
	if len(respA.Frames) != 1 {
		t.Errorf("expected 1 frame for A, got %d", len(respA.Frames))
	}

	// Query B should have an error from the API
	respB := resp.Responses["B"]
	if respB.Error == nil {
		t.Error("expected error for B, got nil")
	} else {
		if !strings.Contains(respB.Error.Error(), "Compute error") {
			t.Errorf("expected 'Compute error' in message for B, got: %v", respB.Error)
		}
		// Error message format: "Compute error: %v (code: %v)" with ErrorType and ErrorCode
		if !strings.Contains(respB.Error.Error(), "CHANNEL_NOT_FOUND") {
			t.Errorf("expected error type 'CHANNEL_NOT_FOUND' in message for B, got: %v", respB.Error)
		}
		if !strings.Contains(respB.Error.Error(), "404") {
			t.Errorf("expected error code '404' in message for B, got: %v", respB.Error)
		}
	}

	// Query C should succeed despite B failing
	respC := resp.Responses["C"]
	if respC.Error != nil {
		t.Errorf("expected no error for C, got: %v", respC.Error)
	}
	if len(respC.Frames) != 1 {
		t.Errorf("expected 1 frame for C, got %d", len(respC.Frames))
	}
}

func TestBatchQueryWithMissingResults(t *testing.T) {
	// Create mock compute service that returns fewer results than queries
	mockService := &mockComputeService{}

	// Only return 2 results for 3 queries - simulates API bug or truncation
	mockService.batchComputeResponse = computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0, 3.0}),
			createMockArrowComputeResult([]float64{4.0, 5.0, 6.0}),
			// Missing third result
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// Create 3 batchable queries
	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify we got responses for all 3 queries
	if len(resp.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Responses))
	}

	// Query A should succeed (has result at index 0)
	respA := resp.Responses["A"]
	if respA.Error != nil {
		t.Errorf("expected no error for A, got: %v", respA.Error)
	}
	if len(respA.Frames) != 1 {
		t.Errorf("expected 1 frame for A, got %d", len(respA.Frames))
	}

	// Query B should succeed (has result at index 1)
	respB := resp.Responses["B"]
	if respB.Error != nil {
		t.Errorf("expected no error for B, got: %v", respB.Error)
	}
	if len(respB.Frames) != 1 {
		t.Errorf("expected 1 frame for B, got %d", len(respB.Frames))
	}

	// Query C should have an error (no result at index 2)
	respC := resp.Responses["C"]
	if respC.Error == nil {
		t.Error("expected error for C due to missing result, got nil")
	} else if !strings.Contains(respC.Error.Error(), "Missing result in batch response") {
		t.Errorf("expected 'Missing result in batch response' error for C, got: %v", respC.Error)
	}
}

func TestBatchQueryWithExtraResultsIgnoresExtras(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockArrowComputeResult([]float64{1.0, 2.0, 3.0}),
				createMockArrowComputeResult([]float64{4.0, 5.0, 6.0}),
				createMockArrowComputeResult([]float64{7.0, 8.0, 9.0}),
				createMockArrowComputeResult([]float64{10.0, 11.0, 12.0}),
			},
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	queries := []backend.DataQuery{
		{
			RefID:     "A",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "temp1", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "B",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temp2", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
		{
			RefID:     "C",
			JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.3", Channel: "temp3", DataScopeName: "ds1", Buckets: 100}),
			TimeRange: timeRange,
		},
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: queries,
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockService.lastBatchRequest.Requests) != len(queries) {
		t.Fatalf("expected %d compute requests, got %d", len(queries), len(mockService.lastBatchRequest.Requests))
	}
	if len(resp.Responses) != len(queries) {
		t.Fatalf("expected %d responses, got %d", len(queries), len(resp.Responses))
	}
	expectedFirst := map[string]float64{"A": 1.0, "B": 4.0, "C": 7.0}
	for _, q := range queries {
		response := resp.Responses[q.RefID]
		if response.Error != nil {
			t.Fatalf("expected no error for %s, got %v", q.RefID, response.Error)
		}
		if len(response.Frames) != 1 {
			t.Fatalf("expected 1 frame for %s, got %d", q.RefID, len(response.Frames))
		}
		v, ok := response.Frames[0].Fields[1].At(0).(*float64)
		if !ok || v == nil {
			t.Fatalf("expected %s first value %v, got %v", q.RefID, expectedFirst[q.RefID], v)
		}
		if *v != expectedFirst[q.RefID] {
			t.Fatalf("expected %s first value %v, got %v", q.RefID, expectedFirst[q.RefID], *v)
		}
	}
}

func TestErrorMessageFormatPreservation(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric channel error retains original format without hint", func(t *testing.T) {
		result := createMockErrorResult(404, "CHANNEL_NOT_FOUND")
		qm := NominalQueryModel{
			Channel:  "temperature",
			AssetRid: "ri.nominal.asset.test",
		}
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		// Original format must be preserved exactly
		if !strings.Contains(errMsg, "Compute error: CHANNEL_NOT_FOUND (code: 404)") {
			t.Errorf("expected original error format, got: %s", errMsg)
		}
		// Numeric errors must NOT have hints
		if strings.Contains(errMsg, "Hint:") {
			t.Errorf("numeric channel errors should not have hints, got: %s", errMsg)
		}
	})

	t.Run("generic compute error retains original format without hint", func(t *testing.T) {
		result := createMockErrorResult(500, "Compute:InternalError")
		qm := NominalQueryModel{
			Channel:  "pressure",
			AssetRid: "ri.nominal.asset.test",
		}
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Compute error: Compute:InternalError (code: 500)") {
			t.Errorf("expected original error format, got: %s", errMsg)
		}
		if strings.Contains(errMsg, "Hint:") {
			t.Errorf("generic errors should not have hints, got: %s", errMsg)
		}
	})

	t.Run("ChannelHasWrongType always includes hint regardless of ChannelDataType", func(t *testing.T) {
		// The hint fires for any ChannelHasWrongType error — the stored type and the
		// API's actual type disagree regardless of whether ChannelDataType is set.
		result := createMockErrorResult(400, "Compute:ChannelHasWrongType")
		qm := NominalQueryModel{
			Channel:         "status",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "string",
		}
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Compute error: Compute:ChannelHasWrongType (code: 400)") {
			t.Errorf("expected raw error in message, got: %s", errMsg)
		}
		if !strings.Contains(errMsg, "Hint:") {
			t.Errorf("expected hint for ChannelHasWrongType even when ChannelDataType is populated, got: %s", errMsg)
		}
	})

	t.Run("ChannelHasWrongType with empty metadata includes hint", func(t *testing.T) {
		result := createMockErrorResult(400, "Compute:ChannelHasWrongType")
		qm := NominalQueryModel{
			Channel:         "status",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "",
		}
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Hint:") {
			t.Errorf("expected hint when ChannelDataType is empty, got: %s", errMsg)
		}
	})

	t.Run("bare ChannelHasWrongType also gets hint", func(t *testing.T) {
		result := createMockErrorResult(400, "ChannelHasWrongType")
		qm := NominalQueryModel{
			Channel:         "mode",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "",
		}
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error == nil {
			t.Fatal("expected error response")
		}
		errMsg := resp.Error.Error()
		if !strings.Contains(errMsg, "Hint:") {
			t.Errorf("expected hint for bare ChannelHasWrongType, got: %s", errMsg)
		}
	})
}

func TestTransformBatchResultLegacyNumeric(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric batch result produces float64 value fields", func(t *testing.T) {
		values := []float64{1.5, 2.5, 3.5, 4.5}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "temperature",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		// Must have time + value fields
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}
		// Value field must be *float64 (nullable)
		valueField := frame.Fields[1]
		if valueField.Len() != len(values) {
			t.Fatalf("expected %d values, got %d", len(values), valueField.Len())
		}
		for i := 0; i < valueField.Len(); i++ {
			v, ok := valueField.At(i).(*float64)
			if !ok {
				t.Errorf("value at index %d is not *float64, got %T", i, valueField.At(i))
			}
			if v == nil || *v != values[i] {
				t.Errorf("value at index %d: expected %f, got %v", i, values[i], v)
			}
		}
	})

	t.Run("numeric frame does not have table type metadata", func(t *testing.T) {
		values := []float64{10.0, 20.0}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "pressure",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		// Numeric frames should NOT have FrameTypeTable metadata (that's for enum frames)
		if frame.Meta != nil && frame.Meta.Type == data.FrameTypeTable {
			t.Error("numeric frame should not have FrameTypeTable metadata")
		}
	})

	t.Run("numeric frame name includes channel name", func(t *testing.T) {
		values := []float64{5.0}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "velocity",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		if !strings.Contains(frame.Name, "velocity") {
			t.Errorf("expected frame name to contain 'velocity', got: %s", frame.Name)
		}
	})
}

// createMockErrorResult creates a mock ComputeWithUnitsResult with an error
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

// createMockArrowComputeResult creates a mock ComputeWithUnitsResult with Arrow
// bucketed numeric data (mean column). This mirrors production behavior where
// numeric queries send OutputFormat=ARROW_V3 and receive ArrowBucketedNumericPlot.
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

// createMockEnumPointComputeResult creates a mock ComputeWithUnitsResult with a single enum point
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

func TestEnumPlotTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("maps indices to category strings", func(t *testing.T) {
		categories := []string{"on", "off", "standby"}
		indices := []int{0, 2, 1, 0}

		result := createMockEnumComputeResult(categories, indices)
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}

		// Verify value field is string type
		valueField := frame.Fields[1]
		if valueField.Name != "value" {
			t.Errorf("expected field name 'value', got %q", valueField.Name)
		}

		// Check resolved string values
		if valueField.Len() != 4 {
			t.Fatalf("expected 4 values, got %d", valueField.Len())
		}
		expectedValues := []string{"on", "standby", "off", "on"}
		for i, expected := range expectedValues {
			actual, ok := valueField.At(i).(string)
			if !ok {
				t.Fatalf("value at index %d is not a string", i)
			}
			if actual != expected {
				t.Errorf("value at index %d: expected %q, got %q", i, expected, actual)
			}
		}
	})

	t.Run("handles out-of-bounds indices gracefully", func(t *testing.T) {
		categories := []string{"on", "off"}
		indices := []int{0, 5, 1} // index 5 is out of bounds

		result := createMockEnumComputeResult(categories, indices)
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		valueField := frame.Fields[1]
		if valueField.Len() != 3 {
			t.Fatalf("expected 3 values, got %d", valueField.Len())
		}
		if v := valueField.At(0).(string); v != "on" {
			t.Errorf("index 0: expected %q, got %q", "on", v)
		}
		if v := valueField.At(1).(string); v != "unknown(5)" {
			t.Errorf("index 1: expected %q, got %q", "unknown(5)", v)
		}
		if v := valueField.At(2).(string); v != "off" {
			t.Errorf("index 2: expected %q, got %q", "off", v)
		}
	})

	t.Run("handles empty enum response", func(t *testing.T) {
		categories := []string{"on", "off"}
		indices := []int{}

		result := createMockEnumComputeResult(categories, indices)
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}

		// Empty enum should have []string{} not []float64{}
		valueField := frame.Fields[1]
		if valueField.Len() != 0 {
			t.Errorf("expected 0 values for empty enum, got %d", valueField.Len())
		}
	})
}

func TestEnumPointTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("passes through resolved string value directly", func(t *testing.T) {
		result := createMockEnumPointComputeResult("active")
		qm := NominalQueryModel{
			Channel:  "status",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}

		valueField := frame.Fields[1]
		if valueField.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", valueField.Len())
		}
		if v := valueField.At(0).(string); v != "active" {
			t.Errorf("expected %q, got %q", "active", v)
		}
	})

	t.Run("nil enum point produces empty enum frame not numeric", func(t *testing.T) {
		computeResponse := computeapi.NewComputeNodeResponseFromEnumPoint(nil)
		computeResult := computeapi.NewComputeNodeResultFromSuccess(computeResponse)
		result := computeapi.ComputeWithUnitsResult{ComputeResult: computeResult}
		qm := NominalQueryModel{
			Channel:         "status",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "string",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}
		frame := resp.Frames[0]
		if frame.Meta == nil {
			t.Fatal("expected frame metadata on empty enum response")
		}
		if frame.Meta.Type != data.FrameTypeTable {
			t.Errorf("expected FrameTypeTable (enum path), got %v — nil enum points must not fall through to numeric", frame.Meta.Type)
		}
		if frame.Meta.PreferredVisualization != data.VisTypeTable {
			t.Errorf("expected VisTypeTable, got %v", frame.Meta.PreferredVisualization)
		}
	})
}

func TestDisplayNameFromDS(t *testing.T) {
	ds := &Datasource{}

	t.Run("numeric path with data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		values := []float64{1.0, 2.0}
		result := createMockComputeResult(values)
		qm := NominalQueryModel{
			Channel:  "temperature",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}
		frame := resp.Frames[0]
		// Fields: [time, value]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}
		valueField := frame.Fields[1]
		if valueField.Config == nil {
			t.Fatal("expected non-nil Config on value field")
		}
		if valueField.Config.DisplayNameFromDS != "temperature" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "temperature")
		}
	})

	t.Run("numeric path with empty data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		result := createMockComputeResult([]float64{})
		qm := NominalQueryModel{
			Channel:  "pressure",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		valueField := frame.Fields[1]
		if valueField.Config == nil {
			t.Fatal("expected non-nil Config on value field")
		}
		if valueField.Config.DisplayNameFromDS != "pressure" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "pressure")
		}
	})

	t.Run("enum path with data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		categories := []string{"on", "off"}
		indices := []int{0, 1}
		result := createMockEnumComputeResult(categories, indices)
		qm := NominalQueryModel{
			Channel:  "mode",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}
		frame := resp.Frames[0]
		if len(frame.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(frame.Fields))
		}
		valueField := frame.Fields[1]
		if valueField.Config == nil {
			t.Fatal("expected non-nil Config on value field")
		}
		if valueField.Config.DisplayNameFromDS != "mode" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "mode")
		}
	})

	t.Run("enum path with empty data sets DisplayNameFromDS to channel name", func(t *testing.T) {
		result := createMockEnumComputeResult([]string{"on", "off"}, []int{})
		qm := NominalQueryModel{
			Channel:  "state",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		frame := resp.Frames[0]
		valueField := frame.Fields[1]
		if valueField.Config == nil {
			t.Fatal("expected non-nil Config on value field")
		}
		if valueField.Config.DisplayNameFromDS != "state" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "state")
		}
	})
}

// TestFieldConfigUnit verifies FieldConfig.Unit wiring through the real
// transformBatchResult frame-construction path across its three branches:
// multi-agg, enum, and legacy single-numeric.
//
// Complements field_config_test.go which covers the builders in isolation —
// these tests guard the wire-up.
func TestFieldConfigUnit(t *testing.T) {
	ds := &Datasource{}

	// assertTimeFieldUnitFree confirms the unit lands only on the value field,
	// never on the time axis. Grafana ignores Unit on time fields today, but the
	// negative assertion guards against a future bug where the builder applies
	// FieldConfig to the wrong field.
	assertTimeFieldUnitFree := func(t *testing.T, frame *data.Frame) {
		t.Helper()
		if cfg := frame.Fields[0].Config; cfg != nil && cfg.Unit != "" {
			t.Errorf("time field must have no Unit, got %q", cfg.Unit)
		}
	}

	t.Run("legacy numeric path on Cel channel sets FieldConfig.Unit=celsius", func(t *testing.T) {
		// Legacy single-numeric branch, data present.
		result := createMockComputeResult([]float64{1.0, 2.0})
		qm := NominalQueryModel{
			Channel:     "engine_temp",
			AssetRid:    "ri.nominal.asset.test",
			ChannelUnit: "Cel",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "celsius" {
			t.Errorf("Unit = %q, want %q", valueField.Config.Unit, "celsius")
		}
		if valueField.Config.DisplayNameFromDS != "engine_temp" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "engine_temp")
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("legacy numeric path with empty data still sets Unit=celsius", func(t *testing.T) {
		// Legacy single-numeric branch, no data.
		result := createMockComputeResult([]float64{})
		qm := NominalQueryModel{
			Channel:     "engine_temp",
			AssetRid:    "ri.nominal.asset.test",
			ChannelUnit: "Cel",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "celsius" {
			t.Errorf("Unit = %q, want %q", valueField.Config.Unit, "celsius")
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("legacy numeric path with empty ChannelUnit leaves Unit empty", func(t *testing.T) {
		result := createMockComputeResult([]float64{1.0})
		qm := NominalQueryModel{
			Channel:  "engine_temp",
			AssetRid: "ri.nominal.asset.test",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "" {
			t.Errorf("Unit = %q, want empty", valueField.Config.Unit)
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("enum path on Cel-tagged channel does NOT set Unit", func(t *testing.T) {
		// Enum branch, data present. Even with ChannelUnit set, enum/string
		// frames must not carry a unit — numeric formatting is meaningless.
		result := createMockEnumComputeResult([]string{"on", "off"}, []int{0, 1})
		qm := NominalQueryModel{
			Channel:     "engine_state",
			AssetRid:    "ri.nominal.asset.test",
			ChannelUnit: "Cel", // would be wrong on an enum; builder must ignore it
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "" {
			t.Errorf("Unit = %q, want empty (enum frames carry no unit)", valueField.Config.Unit)
		}
		if valueField.Config.DisplayNameFromDS != "engine_state" {
			t.Errorf("DisplayNameFromDS = %q, want %q", valueField.Config.DisplayNameFromDS, "engine_state")
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("enum path with empty data does NOT set Unit", func(t *testing.T) {
		// Enum branch, no data.
		result := createMockEnumComputeResult([]string{"on", "off"}, []int{})
		qm := NominalQueryModel{
			Channel:     "engine_state",
			AssetRid:    "ri.nominal.asset.test",
			ChannelUnit: "Cel",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		valueField := resp.Frames[0].Fields[1]
		if valueField.Config.Unit != "" {
			t.Errorf("Unit = %q, want empty", valueField.Config.Unit)
		}
		assertTimeFieldUnitFree(t, resp.Frames[0])
	})

	t.Run("multi-agg MEAN+COUNT+VARIANCE on Cel channel: MEAN has unit, COUNT/VARIANCE do not", func(t *testing.T) {
		// Multi-agg branch, data present.
		ts := []int64{1000000000000, 2000000000000}
		columns := map[string][]float64{
			"mean":     {10.0, 20.0},
			"count":    {5, 5},
			"variance": {1.5, 2.5},
		}
		arrowBytes := createTestArrowMultiAgg(ts, columns)
		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
		result := computeapi.ComputeWithUnitsResult{
			ComputeResult: computeapi.NewComputeNodeResultFromSuccess(
				computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot),
			),
		}
		qm := NominalQueryModel{
			Channel:              "engine_temp",
			AssetRid:             "ri.nominal.asset.test",
			ChannelUnit:          "Cel",
			Aggregations:         []string{AggMean, AggCount, AggVariance},
			ExplicitAggregations: true,
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if len(resp.Frames) != 3 {
			t.Fatalf("expected 3 frames (mean, count, variance), got %d", len(resp.Frames))
		}

		// Frame order matches qm.Aggregations: [MEAN, COUNT, VARIANCE].
		expected := []struct {
			displayName string
			wantUnit    string
		}{
			{"engine_temp (mean)", "celsius"},
			{"engine_temp (count)", ""},
			{"engine_temp (variance)", ""},
		}
		for i, exp := range expected {
			valueField := resp.Frames[i].Fields[1]
			if valueField.Config == nil {
				t.Fatalf("frame[%d]: nil Config", i)
			}
			if valueField.Config.Unit != exp.wantUnit {
				t.Errorf("frame[%d] (%s).Unit = %q, want %q", i, exp.displayName, valueField.Config.Unit, exp.wantUnit)
			}
			if valueField.Config.DisplayNameFromDS != exp.displayName {
				t.Errorf("frame[%d].DisplayNameFromDS = %q, want %q", i, valueField.Config.DisplayNameFromDS, exp.displayName)
			}
			assertTimeFieldUnitFree(t, resp.Frames[i])
		}
	})
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

func TestInferChannelTypeDeduplicatesWithinRequest(t *testing.T) {
	assetRid := "ri.scout.main.asset.dedup1"
	dataSourceRid := "ri.scout.main.data-source.ds1"

	var assetFetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Test Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, &assetFetchCount)
	defer server.Close()

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("temperature"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1")),
					DataType:   &stringType,
				},
			},
		},
	}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockEnumComputeResult([]string{"a"}, []int{0}),
				createMockEnumComputeResult([]string{"b"}, []int{1}),
				createMockEnumComputeResult([]string{"c"}, []int{2}),
			},
		},
	}

	ds := &Datasource{
		computeService:     mockCompute,
		datasourceService:  mockDS,
		resourceHTTPClient: server.Client(),
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// 3 queries for the same asset+scope+channel — should only make 1 asset
	// fetch and 1 SearchChannels call.
	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
			{RefID: "B", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
			{RefID: "C", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
		},
	}

	_, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assetFetchCount != 1 {
		t.Errorf("expected 1 asset fetch call (cached), got %d", assetFetchCount)
	}
	if mockDS.searchChannelsCalls != 1 {
		t.Errorf("expected 1 SearchChannels call (deduplicated), got %d", mockDS.searchChannelsCalls)
	}
}

func TestAssetCacheTTLReusedAcrossRequests(t *testing.T) {
	assetRid := "ri.scout.main.asset.ttl1"
	dataSourceRid := "ri.scout.main.data-source.ds1"

	var assetFetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Test Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, &assetFetchCount)
	defer server.Close()

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("temperature"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds1")),
					DataType:   &stringType,
				},
			},
		},
	}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockEnumComputeResult([]string{"a"}, []int{0}),
			},
		},
	}

	ds := &Datasource{
		computeService:    mockCompute,
		datasourceService: mockDS,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	makeReq := func() *backend.QueryDataRequest {
		return &backend.QueryDataRequest{
			PluginContext: backend.PluginContext{
				DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
					JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
					DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
				},
			},
			Queries: []backend.DataQuery{
				{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
			},
		}
	}

	ds.resourceHTTPClient = server.Client()

	// Two separate QueryData calls should reuse the cached asset.
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if assetFetchCount != 1 {
		t.Errorf("expected 1 asset fetch across 2 QueryData calls (TTL cache), got %d", assetFetchCount)
	}
}

func TestChannelTypeCacheTTLReusedAcrossRequests(t *testing.T) {
	assetRid := "ri.scout.main.asset.ttl2"
	dataSourceRid := "ri.scout.main.data-source.ds2"

	var assetFetchCount int
	server := newCountingAssetServer(t, map[string]SingleAssetResponse{
		assetRid: {
			Rid:   assetRid,
			Title: "Test Asset",
			DataScopes: []AssetDataScope{
				{DataScopeName: "default", DataSource: AssetDataSource{Type: "dataset", Dataset: &dataSourceRid}},
			},
		},
	}, &assetFetchCount)
	defer server.Close()

	stringType := api.New_SeriesDataType(api.SeriesDataType_STRING)
	mockDS := &mockDatasourceService{
		searchChannelsResponse: datasourceapi.SearchChannelsResponse{
			Results: []datasourceapi.ChannelMetadata{
				{
					Name:       api.Channel("temperature"),
					DataSource: rids.DataSourceRid(rid.MustNew("scout", "main", "data-source", "ds2")),
					DataType:   &stringType,
				},
			},
		},
	}
	mockCompute := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockEnumComputeResult([]string{"a"}, []int{0}),
			},
		},
	}

	ds := &Datasource{
		computeService:    mockCompute,
		datasourceService: mockDS,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	makeReq := func() *backend.QueryDataRequest {
		return &backend.QueryDataRequest{
			PluginContext: backend.PluginContext{
				DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
					JSONData:                []byte(fmt.Sprintf("{\"baseUrl\":%q}", server.URL)),
					DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
				},
			},
			Queries: []backend.DataQuery{
				{RefID: "A", JSON: mustMarshal(NominalQueryModel{AssetRid: assetRid, Channel: "temperature", DataScopeName: "default", Buckets: 100}), TimeRange: timeRange},
			},
		}
	}

	ds.resourceHTTPClient = server.Client()

	// Two separate QueryData calls should reuse the cached channel type.
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := ds.QueryData(context.Background(), makeReq()); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if mockDS.searchChannelsCalls != 1 {
		t.Errorf("expected 1 SearchChannels call across 2 QueryData calls (TTL cache), got %d", mockDS.searchChannelsCalls)
	}
}

// --- Arrow IPC test helpers and tests ---
// NOTE: createTestArrowBucketedNumeric lives in aggregation_test.go but is
// available here since both files are in package plugin.

func TestTransformArrowBucketedNumericResponse(t *testing.T) {
	arrowBytes := createTestArrowBucketedNumeric(
		[]int64{1000000000000, 2000000000000, 3000000000000},
		[]float64{1.5, 2.5, 3.5},
		nil,
	)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	response := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)

	ds := &Datasource{}
	qm := NominalQueryModel{Aggregations: []string{"MEAN"}}
	result, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, qm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsEnum {
		t.Error("expected IsEnum=false for Arrow bucketed numeric")
	}
	if len(result.AggSeries) != 1 {
		t.Fatalf("expected 1 AggSeries, got %d", len(result.AggSeries))
	}
	series := result.AggSeries[0]
	if series.Name != "mean" {
		t.Errorf("AggSeries[0].Name = %q, want %q", series.Name, "mean")
	}
	if len(series.TimePoints) != 3 {
		t.Fatalf("expected 3 time points, got %d", len(series.TimePoints))
	}
	if len(series.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(series.Values))
	}
	if series.Values[0] == nil || *series.Values[0] != 1.5 {
		t.Errorf("Values[0] = %v, want 1.5", series.Values[0])
	}
	if series.Values[2] == nil || *series.Values[2] != 3.5 {
		t.Errorf("Values[2] = %v, want 3.5", series.Values[2])
	}
}

// createTestArrowMultiAgg builds an Arrow IPC buffer with end_bucket_timestamp
// plus multiple named float64 columns (e.g. "mean", "min", "max").
func createTestArrowMultiAgg(timestamps []int64, columns map[string][]float64) []byte {
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
	for _, name := range orderedNames {
		b := array.NewFloat64Builder(pool)
		for _, v := range columns[name] {
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
		panic(err)
	}
	writer.Close()
	return buf.Bytes()
}

func TestTransformArrowMultiAggregation(t *testing.T) {
	ts := []int64{1000000000000, 2000000000000, 3000000000000}
	columns := map[string][]float64{
		"mean": {1.5, 2.5, 3.5},
		"min":  {1.0, 2.0, 3.0},
		"max":  {2.0, 3.0, 4.0},
	}
	arrowBytes := createTestArrowMultiAgg(ts, columns)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	response := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)

	ds := &Datasource{}
	qm := NominalQueryModel{Aggregations: []string{"MEAN", "MIN", "MAX"}}
	result, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, qm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.AggSeries) != 3 {
		t.Fatalf("expected 3 AggSeries, got %d", len(result.AggSeries))
	}

	expected := []struct {
		name  string
		first float64
		last  float64
	}{
		{"mean", 1.5, 3.5},
		{"min", 1.0, 3.0},
		{"max", 2.0, 4.0},
	}
	for i, exp := range expected {
		s := result.AggSeries[i]
		if s.Name != exp.name {
			t.Errorf("AggSeries[%d].Name = %q, want %q", i, s.Name, exp.name)
		}
		if len(s.TimePoints) != 3 {
			t.Errorf("AggSeries[%d] has %d time points, want 3", i, len(s.TimePoints))
		}
		if len(s.Values) != 3 {
			t.Errorf("AggSeries[%d] has %d values, want 3", i, len(s.Values))
		}
		if s.Values[0] == nil || *s.Values[0] != exp.first {
			t.Errorf("AggSeries[%d].Values[0] = %v, want %v", i, s.Values[0], exp.first)
		}
		if s.Values[2] == nil || *s.Values[2] != exp.last {
			t.Errorf("AggSeries[%d].Values[2] = %v, want %v", i, s.Values[2], exp.last)
		}
	}
}

// createTestArrowFirstLast builds an Arrow IPC buffer matching the API schema for
// FIRST_POINT/LAST_POINT: first_value, first_timestamp, last_value, last_timestamp,
// plus the shared end_bucket_timestamp.
func createTestArrowFirstLast(
	endBucketTs []int64,
	firstValues []float64, firstTimestamps []int64,
	lastValues []float64, lastTimestamps []int64,
) []byte {
	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "first_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "first_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "last_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "last_timestamp", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	tsBuilder := array.NewInt64Builder(pool)
	defer tsBuilder.Release()
	for _, v := range endBucketTs {
		tsBuilder.Append(v)
	}
	tsArr := tsBuilder.NewArray()
	defer tsArr.Release()

	firstValBuilder := array.NewFloat64Builder(pool)
	defer firstValBuilder.Release()
	for _, v := range firstValues {
		firstValBuilder.Append(v)
	}
	firstValArr := firstValBuilder.NewArray()
	defer firstValArr.Release()

	firstTsBuilder := array.NewInt64Builder(pool)
	defer firstTsBuilder.Release()
	for _, v := range firstTimestamps {
		firstTsBuilder.Append(v)
	}
	firstTsArr := firstTsBuilder.NewArray()
	defer firstTsArr.Release()

	lastValBuilder := array.NewFloat64Builder(pool)
	defer lastValBuilder.Release()
	for _, v := range lastValues {
		lastValBuilder.Append(v)
	}
	lastValArr := lastValBuilder.NewArray()
	defer lastValArr.Release()

	lastTsBuilder := array.NewInt64Builder(pool)
	defer lastTsBuilder.Release()
	for _, v := range lastTimestamps {
		lastTsBuilder.Append(v)
	}
	lastTsArr := lastTsBuilder.NewArray()
	defer lastTsArr.Release()

	rec := array.NewRecord(schema, []arrow.Array{tsArr, firstValArr, firstTsArr, lastValArr, lastTsArr}, int64(len(endBucketTs)))
	defer rec.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		panic(err)
	}
	writer.Close()
	return buf.Bytes()
}

func TestTransformArrowFirstLastPoint(t *testing.T) {
	endBucketTs := []int64{1000000000000, 2000000000000, 3000000000000}
	firstValues := []float64{10.0, 20.0, 30.0}
	firstTimestamps := []int64{900000000000, 1900000000000, 2900000000000}
	lastValues := []float64{15.0, 25.0, 35.0}
	lastTimestamps := []int64{999000000000, 1999000000000, 2999000000000}

	arrowBytes := createTestArrowFirstLast(endBucketTs, firstValues, firstTimestamps, lastValues, lastTimestamps)
	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
	response := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)

	ds := &Datasource{}
	qm := NominalQueryModel{Aggregations: []string{"FIRST_POINT", "LAST_POINT"}}
	result, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, qm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.AggSeries) != 2 {
		t.Fatalf("expected 2 AggSeries, got %d", len(result.AggSeries))
	}

	// FIRST_POINT series
	first := result.AggSeries[0]
	if first.Name != "first" {
		t.Errorf("AggSeries[0].Name = %q, want %q", first.Name, "first")
	}
	if len(first.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(first.Values))
	}
	if first.Values[0] == nil || *first.Values[0] != 10.0 {
		t.Errorf("first.Values[0] = %v, want 10.0", first.Values[0])
	}
	// Verify FIRST_POINT uses its own timestamps, not end_bucket_timestamp
	if first.TimePoints[0] != time.Unix(0, 900000000000) {
		t.Errorf("first.TimePoints[0] = %v, want %v", first.TimePoints[0], time.Unix(0, 900000000000))
	}

	// LAST_POINT series
	last := result.AggSeries[1]
	if last.Name != "last" {
		t.Errorf("AggSeries[1].Name = %q, want %q", last.Name, "last")
	}
	if last.Values[2] == nil || *last.Values[2] != 35.0 {
		t.Errorf("last.Values[2] = %v, want 35.0", last.Values[2])
	}
	// Verify LAST_POINT uses its own timestamps
	if last.TimePoints[2] != time.Unix(0, 2999000000000) {
		t.Errorf("last.TimePoints[2] = %v, want %v", last.TimePoints[2], time.Unix(0, 2999000000000))
	}

	// Verify first and last have DIFFERENT time axes
	if first.TimePoints[0] == last.TimePoints[0] {
		t.Errorf("first and last should have different timestamps, both got %v", first.TimePoints[0])
	}
}

// TestTransformArrowMixedAggWithFirstPoint tests a query with both standard aggregations
// (which share end_bucket_timestamp) and FIRST_POINT (which has its own timestamp column).
func TestTransformArrowMixedAggWithFirstPoint(t *testing.T) {
	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "mean", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "first_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "first_timestamp", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	endTs := []int64{1000000000000, 2000000000000}
	meanVals := []float64{5.0, 6.0}
	firstVals := []float64{4.0, 5.5}
	firstTs := []int64{900000000000, 1900000000000}

	tsBuilder := array.NewInt64Builder(pool)
	for _, v := range endTs {
		tsBuilder.Append(v)
	}
	tsArr := tsBuilder.NewArray()
	defer tsArr.Release()
	tsBuilder.Release()

	meanBuilder := array.NewFloat64Builder(pool)
	for _, v := range meanVals {
		meanBuilder.Append(v)
	}
	meanArr := meanBuilder.NewArray()
	defer meanArr.Release()
	meanBuilder.Release()

	firstValBuilder := array.NewFloat64Builder(pool)
	for _, v := range firstVals {
		firstValBuilder.Append(v)
	}
	firstValArr := firstValBuilder.NewArray()
	defer firstValArr.Release()
	firstValBuilder.Release()

	firstTsBuilder := array.NewInt64Builder(pool)
	for _, v := range firstTs {
		firstTsBuilder.Append(v)
	}
	firstTsArr := firstTsBuilder.NewArray()
	defer firstTsArr.Release()
	firstTsBuilder.Release()

	rec := array.NewRecord(schema, []arrow.Array{tsArr, meanArr, firstValArr, firstTsArr}, 2)
	defer rec.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		panic(err)
	}
	writer.Close()

	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
	response := computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(arrowPlot)

	ds := &Datasource{}
	qm := NominalQueryModel{Aggregations: []string{"MEAN", "FIRST_POINT"}}
	result, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, qm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.AggSeries) != 2 {
		t.Fatalf("expected 2 AggSeries, got %d", len(result.AggSeries))
	}

	// MEAN uses shared end_bucket_timestamp
	meanSeries := result.AggSeries[0]
	if meanSeries.Name != "mean" {
		t.Errorf("AggSeries[0].Name = %q, want %q", meanSeries.Name, "mean")
	}
	if meanSeries.TimePoints[0] != time.Unix(0, 1000000000000) {
		t.Errorf("mean.TimePoints[0] = %v, want %v", meanSeries.TimePoints[0], time.Unix(0, 1000000000000))
	}

	// FIRST_POINT uses its own first_timestamp
	firstSeries := result.AggSeries[1]
	if firstSeries.Name != "first" {
		t.Errorf("AggSeries[1].Name = %q, want %q", firstSeries.Name, "first")
	}
	if firstSeries.TimePoints[0] != time.Unix(0, 900000000000) {
		t.Errorf("first.TimePoints[0] = %v, want %v (should use first_timestamp, not end_bucket_timestamp)",
			firstSeries.TimePoints[0], time.Unix(0, 900000000000))
	}
}

func TestTransformArrowNumericPlotReturnsError(t *testing.T) {
	arrowPlot := computeapi.ArrowNumericPlot{ArrowBinary: []byte{}}
	response := computeapi.NewComputeNodeResponseFromArrowNumeric(arrowPlot)

	ds := &Datasource{}
	_, err := newTestQueryExecution(ds, nil).transformNominalResponseFromClient(response, NominalQueryModel{})
	if err == nil {
		t.Fatal("expected error for ArrowNumericPlot, got nil")
	}
	if !strings.Contains(err.Error(), "ArrowNumericPlot unexpectedly") {
		t.Errorf("error should mention ArrowNumericPlot, got: %v", err)
	}
}

// --- Log query path tests ---

// createMockPagedLogResult creates a mock ComputeWithUnitsResult with paged log data.
func createMockPagedLogResult(messages []string, args []map[string]string) computeapi.ComputeWithUnitsResult {
	baseTime := int64(1704067200) // 2024-01-01 00:00:00 UTC
	timestamps := make([]api.Timestamp, len(messages))
	values := make([]computeapi.LogValue, len(messages))
	for i, msg := range messages {
		timestamps[i] = api.Timestamp{
			Seconds: safelong.SafeLong(baseTime + int64(i*60)),
			Nanos:   safelong.SafeLong(0),
		}
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

// createMockLogPointResult creates a mock ComputeWithUnitsResult with a single log point.
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

func parseLogLabels(t *testing.T, raw json.RawMessage) map[string]string {
	t.Helper()
	// json.Unmarshal("null") succeeds with a nil map, so guard against it before
	// accepting labels as a Grafana JSON object.
	if string(raw) == "null" {
		t.Fatalf("labels serialized to null, want a JSON object")
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("not valid JSON object: %v (raw=%q)", err, string(raw))
	}
	return m
}

func logFrameLabelsAt(t *testing.T, frame *data.Frame, row int) map[string]string {
	t.Helper()
	if len(frame.Fields) <= 3 {
		t.Fatalf("expected log frame labels field at index 3, got %d fields", len(frame.Fields))
	}
	labelsField := frame.Fields[3]
	if row < 0 || row >= labelsField.Len() {
		t.Fatalf("expected labels row %d within field length %d", row, labelsField.Len())
	}
	value := labelsField.At(row)
	raw, ok := value.(json.RawMessage)
	if !ok {
		t.Fatalf("labels row %d has type %T, want json.RawMessage", row, value)
	}
	return parseLogLabels(t, raw)
}

func TestMarshalLogArgs(t *testing.T) {
	t.Run("nil args with channel injects nominal.channel only", func(t *testing.T) {
		got := parseLogLabels(t, marshalLogArgs(nil, "engine.temp"))
		if len(got) != 1 || got["nominal.channel"] != "engine.temp" {
			t.Errorf("expected {nominal.channel: engine.temp}, got %v", got)
		}
	})

	t.Run("existing args preserved and nominal.channel added", func(t *testing.T) {
		got := parseLogLabels(t, marshalLogArgs(map[string]string{"host": "srv-1", "level": "error"}, "engine.temp"))
		if got["host"] != "srv-1" || got["level"] != "error" {
			t.Errorf("expected user args preserved, got %v", got)
		}
		if got["nominal.channel"] != "engine.temp" {
			t.Errorf("expected nominal.channel=engine.temp, got %v", got)
		}
	})

	t.Run("pre-existing nominal.channel arg is not clobbered", func(t *testing.T) {
		got := parseLogLabels(t, marshalLogArgs(map[string]string{"nominal.channel": "user-value"}, "engine.temp"))
		if got["nominal.channel"] != "user-value" {
			t.Errorf("expected user nominal.channel preserved, got %q", got["nominal.channel"])
		}
	})

	t.Run("empty channel does not inject nominal.channel", func(t *testing.T) {
		got := parseLogLabels(t, marshalLogArgs(map[string]string{"host": "srv-1"}, ""))
		if _, ok := got["nominal.channel"]; ok {
			t.Errorf("did not expect nominal.channel for empty channel, got %v", got)
		}
		if got["host"] != "srv-1" {
			t.Errorf("expected host preserved, got %v", got)
		}
	})

	t.Run("caller's input map is not mutated", func(t *testing.T) {
		in := map[string]string{"host": "srv-1"}
		_ = marshalLogArgs(in, "engine.temp")
		if _, ok := in["nominal.channel"]; ok {
			t.Errorf("input map was mutated: %v", in)
		}
	})
}

func TestLogPagedTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("transforms paged log entries into log frame", func(t *testing.T) {
		messages := []string{"error: disk full", "warn: high memory", "info: started"}
		args := []map[string]string{
			{"host": "srv-1", "level": "error"},
			{"host": "srv-2", "level": "warn"},
			{"host": "srv-1", "level": "info"},
		}
		result := createMockPagedLogResult(messages, args)
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if frame.Meta == nil || frame.Meta.Type != data.FrameTypeLogLines {
			t.Errorf("expected FrameTypeLogLines metadata")
		}
		if frame.Meta.PreferredVisualization != data.VisTypeLogs {
			t.Errorf("expected VisTypeLogs, got %v", frame.Meta.PreferredVisualization)
		}

		if len(frame.Fields) != 4 {
			t.Fatalf("expected 4 fields (timestamp, body, id, labels), got %d", len(frame.Fields))
		}

		bodyField := frame.Fields[1]
		if bodyField.Len() != 3 {
			t.Fatalf("expected 3 log entries, got %d", bodyField.Len())
		}

		// Verify sort order: newest first (highest timestamp first)
		tsField := frame.Fields[0]
		t0 := tsField.At(0).(time.Time)
		t2 := tsField.At(2).(time.Time)
		if !t0.After(t2) {
			t.Errorf("expected descending sort, but first timestamp %v is not after last %v", t0, t2)
		}

		// Verify body content is preserved (sorted order: index 2, 1, 0)
		if v := bodyField.At(0).(string); v != "info: started" {
			t.Errorf("expected newest message first, got %q", v)
		}
		if v := bodyField.At(2).(string); v != "error: disk full" {
			t.Errorf("expected oldest message last, got %q", v)
		}

		for i := 0; i < bodyField.Len(); i++ {
			logFrameLabelsAt(t, frame, i)
		}
	})

	t.Run("nil Args still yields injected nominal.channel label", func(t *testing.T) {
		messages := []string{"no-args entry"}
		result := createMockPagedLogResult(messages, nil) // nil args
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		parsed := logFrameLabelsAt(t, resp.Frames[0], 0)
		if len(parsed) != 1 || parsed["nominal.channel"] != "app.logs" {
			t.Errorf("expected {nominal.channel: app.logs} for nil Args, got %v", parsed)
		}
	})

	t.Run("empty log response produces frame with correct schema", func(t *testing.T) {
		result := createMockPagedLogResult([]string{}, nil)
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if frame.Meta == nil || frame.Meta.Type != data.FrameTypeLogLines {
			t.Errorf("expected FrameTypeLogLines metadata on empty frame")
		}
		if len(frame.Fields) != 4 {
			t.Fatalf("expected 4 fields even when empty, got %d", len(frame.Fields))
		}
	})
}

func TestLogPointTransformation(t *testing.T) {
	ds := &Datasource{}

	t.Run("transforms single log point", func(t *testing.T) {
		result := createMockLogPointResult("single entry", map[string]string{"host": "srv-1"})
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(resp.Frames))
		}

		frame := resp.Frames[0]
		if frame.Meta == nil || frame.Meta.Type != data.FrameTypeLogLines {
			t.Errorf("expected FrameTypeLogLines metadata")
		}

		bodyField := frame.Fields[1]
		if bodyField.Len() != 1 {
			t.Fatalf("expected 1 entry, got %d", bodyField.Len())
		}
		if v := bodyField.At(0).(string); v != "single entry" {
			t.Errorf("expected %q, got %q", "single entry", v)
		}
	})

	t.Run("nil Args on single log point still yields injected nominal.channel label", func(t *testing.T) {
		result := createMockLogPointResult("no-args", nil)
		qm := NominalQueryModel{
			Channel:         "app.logs",
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}

		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		parsed := logFrameLabelsAt(t, resp.Frames[0], 0)
		if len(parsed) != 1 || parsed["nominal.channel"] != "app.logs" {
			t.Errorf("expected {nominal.channel: app.logs} for nil Args, got %v", parsed)
		}
	})
}

func TestLogFramesCarryDistinctChannelLabels(t *testing.T) {
	ds := &Datasource{}

	channelLabel := func(t *testing.T, channel string) string {
		t.Helper()
		result := createMockPagedLogResult([]string{"entry"}, []map[string]string{{"host": "srv-1"}})
		qm := NominalQueryModel{
			Channel:         channel,
			AssetRid:        "ri.nominal.asset.test",
			ChannelDataType: "log",
		}
		resp := newTestQueryExecution(ds, nil).transformBatchResult(result, qm)
		if len(resp.Frames) != 1 {
			t.Fatalf("expected 1 frame for %q, got %d", channel, len(resp.Frames))
		}
		parsed := logFrameLabelsAt(t, resp.Frames[0], 0)
		if parsed["host"] != "srv-1" {
			t.Errorf("expected user arg host preserved for %q, got %v", channel, parsed)
		}
		return parsed["nominal.channel"]
	}

	a := channelLabel(t, "engine.temp")
	b := channelLabel(t, "engine.pressure")
	if a != "engine.temp" || b != "engine.pressure" {
		t.Errorf("expected per-channel labels, got a=%q b=%q", a, b)
	}
	if a == b {
		t.Errorf("expected distinct nominal.channel labels, both were %q", a)
	}
}

func TestMixedLogNumericParallelBatch(t *testing.T) {
	// With parallel goroutines, call ordering is nondeterministic.
	// Use batchComputeFunc to inspect each request and return the matching response.
	logResponse := computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockPagedLogResult([]string{"log entry"}, []map[string]string{{"k": "v"}}),
		},
	}
	numericResponse := computeapi.BatchComputeWithUnitsResponse{
		Results: []computeapi.ComputeWithUnitsResult{
			createMockArrowComputeResult([]float64{1.0, 2.0}),
		},
	}
	mockService := &mockComputeService{
		batchComputeFunc: func(req computeapi1.BatchComputeWithUnitsRequest) (computeapi.BatchComputeWithUnitsResponse, error) {
			// Inspect the serialized request to determine if it's a log or numeric query.
			// Log requests contain "log" series type; numeric requests contain "numeric".
			reqJSON, _ := json.Marshal(req)
			if strings.Contains(string(reqJSON), `"type":"log"`) {
				return logResponse, nil
			}
			return numericResponse, nil
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{
				RefID:     "LOG",
				JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.1", Channel: "app.logs", DataScopeName: "ds1", ChannelDataType: "log", Buckets: 100}),
				TimeRange: timeRange,
			},
			{
				RefID:     "NUM",
				JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.2", Channel: "temperature", DataScopeName: "ds1", ChannelDataType: "numeric", Buckets: 100, Aggregations: []string{"MEAN"}}),
				TimeRange: timeRange,
			},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have made 2 separate batch calls (parallel)
	if mockService.batchComputeCalls != 2 {
		t.Errorf("expected 2 batch compute calls for mixed log/numeric, got %d", mockService.batchComputeCalls)
	}

	// Each batch should contain exactly 1 request
	for i, req := range mockService.batchRequests {
		if len(req.Requests) != 1 {
			t.Errorf("batch call %d: expected 1 request, got %d", i, len(req.Requests))
		}
	}

	// Both refIDs should have responses
	if len(resp.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(resp.Responses))
	}

	logResp, ok := resp.Responses["LOG"]
	if !ok {
		t.Fatal("expected response for LOG refID")
	}
	numResp, ok := resp.Responses["NUM"]
	if !ok {
		t.Fatal("expected response for NUM refID")
	}

	// Both should have frames (no errors)
	if logResp.Error != nil {
		t.Errorf("unexpected error for LOG: %v", logResp.Error)
	}
	if numResp.Error != nil {
		t.Errorf("unexpected error for NUM: %v", numResp.Error)
	}
	if len(logResp.Frames) == 0 {
		t.Error("expected frames for LOG response")
	}
	if len(numResp.Frames) == 0 {
		t.Error("expected frames for NUM response")
	}
}

// strPtr is a helper to create a *string
func strPtr(s string) *string {
	return &s
}

func TestGetChannelDataType(t *testing.T) {
	tests := []struct {
		name     string
		dataType *api.SeriesDataType
		expected string
	}{
		{"nil dataType returns empty", nil, ""},
		{"STRING returns string", ptrSeriesDataType(api.SeriesDataType_STRING), "string"},
		{"STRING_ARRAY returns string", ptrSeriesDataType(api.SeriesDataType_STRING_ARRAY), "string"},
		{"LOG returns log", ptrSeriesDataType(api.SeriesDataType_LOG), "log"},
		{"DOUBLE returns numeric", ptrSeriesDataType(api.SeriesDataType_DOUBLE), "numeric"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := datasourceapi.ChannelMetadata{DataType: tt.dataType}
			got := getChannelDataType(ch)
			if got != tt.expected {
				t.Errorf("getChannelDataType() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func ptrSeriesDataType(v api.SeriesDataType_Value) *api.SeriesDataType {
	dt := api.New_SeriesDataType(v)
	return &dt
}

func TestLogChannelSkipsAggregationValidation(t *testing.T) {
	mockService := &mockComputeService{
		batchComputeResponse: computeapi.BatchComputeWithUnitsResponse{
			Results: []computeapi.ComputeWithUnitsResult{
				createMockPagedLogResult([]string{"test entry"}, []map[string]string{{"k": "v"}}),
			},
		},
	}

	ds := &Datasource{
		settings: backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"baseUrl": "https://api.test.com"}`),
		},
		computeService: mockService,
	}

	timeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	// A log query with no aggregations should NOT get default ["MEAN"] injected,
	// and should NOT be rejected for missing aggregations.
	req := &backend.QueryDataRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"baseUrl": "https://api.test.com"}`),
				DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
			},
		},
		Queries: []backend.DataQuery{
			{
				RefID:     "A",
				JSON:      mustMarshal(NominalQueryModel{AssetRid: "ri.nominal.asset.test", Channel: "app.logs", DataScopeName: "default", ChannelDataType: "log", Buckets: 100}),
				TimeRange: timeRange,
			},
		},
	}

	resp, err := ds.QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	response, ok := resp.Responses["A"]
	if !ok {
		t.Fatal("expected response for refID A")
	}
	// The query should not have been rejected — no StatusBadRequest error about aggregations.
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

func TestFieldConfigForNumeric(t *testing.T) {
	// End-to-end frame-building paths are covered by TestFieldConfigUnit above.
	// This test exercises only the helper's unique behavior: mapped unit applied,
	// unit suppressed when the aggregation does not carry it, and suffix fallthrough
	// for symbols not in unitSymbolToGrafanaID.
	tests := []struct {
		name               string
		channelUnit        string
		displayName        string
		carriesChannelUnit bool
		wantUnit           string
		wantDispName       string
	}{
		{
			name:               "applies mapped Grafana unit",
			channelUnit:        "Cel",
			displayName:        "engine_temp (mean)",
			carriesChannelUnit: true,
			wantUnit:           "celsius",
			wantDispName:       "engine_temp (mean)",
		},
		{
			name:               "suppresses unit when aggregation does not carry channel unit",
			channelUnit:        "Cel",
			displayName:        "engine_temp (count)",
			carriesChannelUnit: false,
			wantUnit:           "",
			wantDispName:       "engine_temp (count)",
		},
		{
			name:               "falls through to explicit suffix for unmapped symbol",
			channelUnit:        "asdfsdfs",
			displayName:        "weird_channel",
			carriesChannelUnit: true,
			wantUnit:           "suffix:asdfsdfs",
			wantDispName:       "weird_channel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := &NominalQueryModel{ChannelUnit: tt.channelUnit, Channel: "engine_temp"}
			got := fieldConfigForNumeric(qm, tt.displayName, tt.carriesChannelUnit)
			if got.Unit != tt.wantUnit {
				t.Errorf("Unit = %q, want %q", got.Unit, tt.wantUnit)
			}
			if got.DisplayNameFromDS != tt.wantDispName {
				t.Errorf("DisplayNameFromDS = %q, want %q", got.DisplayNameFromDS, tt.wantDispName)
			}
		})
	}
}

func TestFieldConfigForEnum(t *testing.T) {
	// Enum frames never carry a unit, regardless of what ChannelUnit holds.
	qm := &NominalQueryModel{Channel: "engine_state", ChannelUnit: "Cel"}
	got := fieldConfigForEnum(qm)
	if got.Unit != "" {
		t.Errorf("fieldConfigForEnum must not set Unit, got %q", got.Unit)
	}
	if got.DisplayNameFromDS != "engine_state" {
		t.Errorf("DisplayNameFromDS = %q, want %q", got.DisplayNameFromDS, "engine_state")
	}
}
