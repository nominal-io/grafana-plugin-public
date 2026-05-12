package plugin

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

const liveNominalTestEnv = "NOMINAL_LIVE_TESTS"

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

	assetRid := os.Getenv("NOMINAL_QUERY_ASSET_RID")
	channel := os.Getenv("NOMINAL_QUERY_CHANNEL")
	dataScopeName := os.Getenv("NOMINAL_QUERY_DATA_SCOPE_NAME")
	if assetRid == "" || channel == "" || dataScopeName == "" {
		t.Skip("set NOMINAL_QUERY_ASSET_RID, NOMINAL_QUERY_CHANNEL, and NOMINAL_QUERY_DATA_SCOPE_NAME to run the live QueryData integration test")
	}

	buckets := 100
	if rawBuckets := os.Getenv("NOMINAL_QUERY_BUCKETS"); rawBuckets != "" {
		parsedBuckets, err := strconv.Atoi(rawBuckets)
		if err != nil {
			t.Fatalf("NOMINAL_QUERY_BUCKETS must be an integer: %v", err)
		}
		buckets = parsedBuckets
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

	ds := liveNominalDatasource(t, settings)
	query := NominalQueryModel{
		AssetRid:        assetRid,
		Channel:         channel,
		DataScopeName:   dataScopeName,
		ChannelDataType: os.Getenv("NOMINAL_QUERY_CHANNEL_DATA_TYPE"),
		Buckets:         buckets,
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
				TimeRange:     backend.TimeRange{From: from, To: to},
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
	if len(response.Frames) == 0 {
		t.Fatalf("expected at least one frame for live query")
	}
}

func responseRefs(resp *backend.QueryDataResponse) []string {
	refs := make([]string, 0, len(resp.Responses))
	for refID := range resp.Responses {
		refs = append(refs, refID)
	}
	return refs
}
