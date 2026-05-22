package plugin

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
)

func TestBuildComputeContext(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name         string
		qm           NominalQueryModel
		startSeconds int64
		endSeconds   int64
		expectedVars int
	}{
		{
			name: "basic context with assetRid",
			qm: NominalQueryModel{
				AssetRid: "ri.nominal.asset.12345",
				Channel:  "temperature",
			},
			startSeconds: 1000,
			endSeconds:   2000,
			expectedVars: 1, // Just assetRid
		},
		{
			name: "context with template variables",
			qm: NominalQueryModel{
				AssetRid: "ri.nominal.asset.12345",
				Channel:  "temperature",
				TemplateVariables: map[string]interface{}{
					"env":    "prod",
					"region": "us-east",
				},
			},
			startSeconds: 1000,
			endSeconds:   2000,
			expectedVars: 3, // assetRid + 2 template vars
		},
		{
			name: "context with non-string template variables ignored",
			qm: NominalQueryModel{
				AssetRid: "ri.nominal.asset.12345",
				Channel:  "temperature",
				TemplateVariables: map[string]interface{}{
					"strVar": "value",
					"intVar": 123, // non-string, should be ignored
				},
			},
			startSeconds: 1000,
			endSeconds:   2000,
			expectedVars: 2, // assetRid + 1 string template var
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newTestQueryExecution(ds, nil).buildComputeContext(tt.qm, tt.startSeconds, tt.endSeconds)

			if len(ctx.Variables) != tt.expectedVars {
				t.Errorf("expected %d variables, got %d", tt.expectedVars, len(ctx.Variables))
			}

			// Verify assetRid is always present
			if _, ok := ctx.Variables["assetRid"]; !ok {
				t.Error("expected assetRid variable to be present")
			}

			// Verify non-string template variables are excluded
			if tt.qm.TemplateVariables != nil {
				for key, value := range tt.qm.TemplateVariables {
					_, isString := value.(string)
					_, inContext := ctx.Variables[computeapi.VariableName(key)]
					if !isString && inContext {
						t.Errorf("non-string variable %q should be excluded from context", key)
					}
					if isString && !inContext {
						t.Errorf("string variable %q should be included in context", key)
					}
				}
			}
		})
	}
}

func TestEffectiveBucketCount(t *testing.T) {
	tests := []struct {
		name          string
		buckets       int
		maxDataPoints int64
		want          int
	}{
		{"maxDataPoints caps buckets when smaller", 1000, 500, 500},
		{"maxDataPoints does not increase buckets", 500, 1000, 500},
		{"maxDataPoints used when buckets is zero", 0, 800, 800},
		{"maxDataPoints used when buckets is negative", -10, 300, 300},
		{"zero maxDataPoints uses saved buckets", 1000, 0, 1000},
		{"negative maxDataPoints uses saved buckets", 1000, -1, 1000},
		{"zero buckets and zero maxDataPoints stays zero", 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := NominalQueryModel{Buckets: tt.buckets}
			got := effectiveBucketCount(qm, tt.maxDataPoints)
			if got != tt.want {
				t.Errorf("effectiveBucketCount(Buckets=%d, maxDataPoints=%d) = %d, want %d", tt.buckets, tt.maxDataPoints, got, tt.want)
			}
		})
	}
}

func TestNumericOutputFields(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		want    []computeapi.NumericOutputField_Value
		wantNil bool
	}{
		{
			name: "preserves aggregation order",
			in:   []string{AggMean, AggMin, AggMax, AggCount, AggVariance, AggFirstPoint, AggLastPoint},
			want: []computeapi.NumericOutputField_Value{
				computeapi.NumericOutputField_MEAN,
				computeapi.NumericOutputField_MIN,
				computeapi.NumericOutputField_MAX,
				computeapi.NumericOutputField_COUNT,
				computeapi.NumericOutputField_VARIANCE,
				computeapi.NumericOutputField_FIRST_POINT,
				computeapi.NumericOutputField_LAST_POINT,
			},
		},
		{
			name:    "returns nil slice for nil aggregations",
			in:      nil,
			want:    nil,
			wantNil: true,
		},
		{
			name:    "returns nil slice for empty aggregations",
			in:      []string{},
			want:    nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := numericOutputFields(tt.in)
			if tt.wantNil && got != nil {
				t.Fatalf("numericOutputFields returned non-nil empty slice, want nil")
			}
			if len(got) != len(tt.want) {
				t.Fatalf("len(numericOutputFields) = %d, want %d; got %v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].Value() != tt.want[i] {
					t.Errorf("field[%d] = %s, want %s", i, got[i].Value(), tt.want[i])
				}
			}
		})
	}
}

func TestBuildComputeRequest(t *testing.T) {
	ds := &Datasource{}

	qm := NominalQueryModel{
		AssetRid:      "ri.nominal.asset.12345",
		Channel:       "temperature",
		DataScopeName: "default",
		Buckets:       100,
	}

	timeRange := backend.TimeRange{
		From: time.Unix(1704067200, 0), // 2024-01-01 00:00:00 UTC
		To:   time.Unix(1704153600, 0), // 2024-01-02 00:00:00 UTC
	}

	req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, timeRange, 0)

	// Verify start and end times
	if int64(req.Start.Seconds) != 1704067200 {
		t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, 1704067200)
	}
	if int64(req.End.Seconds) != 1704153600 {
		t.Errorf("End.Seconds = %d, want %d", req.End.Seconds, 1704153600)
	}
}

func TestBuildChannelSeries(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name          string
		assetRid      string
		channel       string
		dataScopeName string
	}{
		{
			name:          "builds series with all parameters",
			assetRid:      "ri.nominal.asset.123",
			channel:       "temperature",
			dataScopeName: "default",
		},
		{
			name:          "builds series with empty dataScopeName",
			assetRid:      "ri.nominal.asset.456",
			channel:       "pressure",
			dataScopeName: "",
		},
		{
			name:          "builds series with special characters in channel",
			assetRid:      "ri.nominal.asset.789",
			channel:       "sensor/value-1",
			dataScopeName: "scope_test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := newTestQueryExecution(ds, nil).buildChannelSeries(tt.assetRid, tt.channel, tt.dataScopeName)

			// Serialize to JSON to inspect the structure
			// This is more maintainable than using the visitor pattern with 35+ nil arguments
			jsonBytes, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("failed to marshal NumericSeries: %v", err)
			}

			jsonStr := string(jsonBytes)

			// Verify the series is a channel type (timeShift is added by buildComputeRequest, not here)
			if !strings.Contains(jsonStr, `"type":"channel"`) {
				t.Errorf("expected channel series type in JSON, got: %s", jsonStr)
			}

			// Verify it's an asset-based channel
			if !strings.Contains(jsonStr, `"type":"asset"`) {
				t.Errorf("expected asset channel type in JSON, got: %s", jsonStr)
			}

			// Verify the channel name is present as a literal
			expectedChannelLiteral := fmt.Sprintf(`"channel":{"type":"literal","literal":"%s"}`, tt.channel)
			if !strings.Contains(jsonStr, expectedChannelLiteral) {
				t.Errorf("expected channel literal %q in JSON, got: %s", tt.channel, jsonStr)
			}

			// Verify the dataScopeName is present as a literal
			expectedDataScopeLiteral := fmt.Sprintf(`"dataScopeName":{"type":"literal","literal":"%s"}`, tt.dataScopeName)
			if !strings.Contains(jsonStr, expectedDataScopeLiteral) {
				t.Errorf("expected dataScopeName literal %q in JSON, got: %s", tt.dataScopeName, jsonStr)
			}

			// Verify the assetRid is a variable reference (not a literal)
			// The actual assetRid value is passed via context variables
			expectedAssetRidVar := `"assetRid":{"type":"variable","variable":"assetRid"}`
			if !strings.Contains(jsonStr, expectedAssetRidVar) {
				t.Errorf("expected assetRid to be variable reference in JSON, got: %s", jsonStr)
			}
		})
	}
}

func TestBuildComputeRequestBranching(t *testing.T) {
	ds := &Datasource{}

	baseQM := NominalQueryModel{
		AssetRid:      "ri.nominal.asset.test",
		Channel:       "temperature",
		DataScopeName: "default",
		Buckets:       1000,
	}

	baseTimeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	t.Run("string ChannelDataType produces enum series", func(t *testing.T) {
		qm := baseQM
		qm.ChannelDataType = "string"
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		// The request should have a valid node
		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for enum request")
		}
		// Verify the timestamps are set
		if int64(req.Start.Seconds) != baseTimeRange.From.Unix() {
			t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, baseTimeRange.From.Unix())
		}
		if int64(req.End.Seconds) != baseTimeRange.To.Unix() {
			t.Errorf("End.Seconds = %d, want %d", req.End.Seconds, baseTimeRange.To.Unix())
		}

		// Serialize the node to JSON and verify it contains enum-specific types
		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal ComputableNode: %v", err)
		}
		jsonStr := string(jsonBytes)

		// Enum path should contain "enum" type markers in the serialized structure
		if !strings.Contains(jsonStr, `"type":"enum"`) {
			t.Errorf("expected enum series type in JSON for string ChannelDataType, got: %s", jsonStr)
		}
		// Should NOT contain numeric series type at the top level
		if strings.Contains(jsonStr, `"type":"numeric"`) {
			t.Errorf("expected no numeric series type in JSON for string ChannelDataType, got: %s", jsonStr)
		}
	})

	t.Run("numeric ChannelDataType produces numeric series", func(t *testing.T) {
		qm := baseQM
		qm.ChannelDataType = "numeric"
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for numeric request")
		}
		if int64(req.Start.Seconds) != baseTimeRange.From.Unix() {
			t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, baseTimeRange.From.Unix())
		}

		// Serialize and verify it contains numeric-specific types
		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal ComputableNode: %v", err)
		}
		jsonStr := string(jsonBytes)

		if !strings.Contains(jsonStr, `"type":"numeric"`) {
			t.Errorf("expected numeric series type in JSON for numeric ChannelDataType, got: %s", jsonStr)
		}
	})

	t.Run("empty ChannelDataType defaults to numeric series", func(t *testing.T) {
		qm := baseQM
		qm.ChannelDataType = ""
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for default request")
		}
		if int64(req.Start.Seconds) != baseTimeRange.From.Unix() {
			t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, baseTimeRange.From.Unix())
		}

		// Default should be numeric
		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal ComputableNode: %v", err)
		}
		jsonStr := string(jsonBytes)

		if !strings.Contains(jsonStr, `"type":"numeric"`) {
			t.Errorf("expected numeric series type in JSON for empty ChannelDataType, got: %s", jsonStr)
		}
		if strings.Contains(jsonStr, `"type":"enum"`) {
			t.Errorf("expected no enum series type in JSON for empty ChannelDataType, got: %s", jsonStr)
		}
	})

	t.Run("missing ChannelDataType defaults to numeric series", func(t *testing.T) {
		qm := baseQM
		// ChannelDataType is zero-value ""
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for missing ChannelDataType request")
		}
	})

	t.Run("string and numeric produce structurally different requests", func(t *testing.T) {
		stringQM := baseQM
		stringQM.ChannelDataType = "string"
		stringReq := newTestQueryExecution(ds, nil).buildComputeRequest(stringQM, baseTimeRange, 0)

		numericQM := baseQM
		numericQM.ChannelDataType = "numeric"
		numericReq := newTestQueryExecution(ds, nil).buildComputeRequest(numericQM, baseTimeRange, 0)

		stringJSON, _ := json.Marshal(stringReq.Node)
		numericJSON, _ := json.Marshal(numericReq.Node)

		if string(stringJSON) == string(numericJSON) {
			t.Error("expected different JSON for string vs numeric ChannelDataType, but they are identical")
		}
	})
}

func TestBuildComputeRequestMaxDataPoints(t *testing.T) {
	ds := &Datasource{}

	baseTimeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	extractBuckets := func(t *testing.T, req computeapi1.ComputeNodeRequest) int {
		t.Helper()
		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal node: %v", err)
		}
		var node struct {
			Series struct {
				Buckets *int `json:"buckets"`
			} `json:"series"`
		}
		if err := json.Unmarshal(jsonBytes, &node); err != nil {
			t.Fatalf("failed to unmarshal node: %v", err)
		}
		if node.Series.Buckets == nil {
			return 0
		}
		return *node.Series.Buckets
	}

	t.Run("maxDataPoints caps buckets when smaller", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       1000,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 500)
		if got := extractBuckets(t, req); got != 500 {
			t.Errorf("buckets = %d, want 500", got)
		}
	})

	t.Run("maxDataPoints does not increase buckets", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       500,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 1000)
		if got := extractBuckets(t, req); got != 500 {
			t.Errorf("buckets = %d, want 500", got)
		}
	})

	t.Run("maxDataPoints used when buckets is zero", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       0,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 800)
		if got := extractBuckets(t, req); got != 800 {
			t.Errorf("buckets = %d, want 800", got)
		}
	})

	t.Run("zero maxDataPoints uses saved buckets", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       1000,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)
		if got := extractBuckets(t, req); got != 1000 {
			t.Errorf("buckets = %d, want 1000", got)
		}
	})
}

func TestBuildEnumChannelSeries(t *testing.T) {
	ds := &Datasource{}

	t.Run("returns non-nil enum series", func(t *testing.T) {
		enumSeries := newTestQueryExecution(ds, nil).buildEnumChannelSeries("ri.nominal.asset.test", "status", "default")
		// Serialize to JSON to verify the structure
		jsonBytes, err := json.Marshal(enumSeries)
		if err != nil {
			t.Fatalf("failed to marshal EnumSeries: %v", err)
		}
		jsonStr := string(jsonBytes)

		// Should contain channel type (same as numeric path)
		if !strings.Contains(jsonStr, `"type":"channel"`) {
			t.Errorf("expected channel series type in JSON, got: %s", jsonStr)
		}
		if !strings.Contains(jsonStr, `"type":"asset"`) {
			t.Errorf("expected asset channel type in JSON, got: %s", jsonStr)
		}
		// Verify channel name is present
		if !strings.Contains(jsonStr, `"channel":{"type":"literal","literal":"status"}`) {
			t.Errorf("expected channel literal 'status' in JSON, got: %s", jsonStr)
		}
	})

	t.Run("mirrors buildChannelSeries asset channel structure", func(t *testing.T) {
		// Both builders should produce the same AssetChannel structure;
		// verify by checking the channel and dataScopeName appear identically.
		enumSeries := newTestQueryExecution(ds, nil).buildEnumChannelSeries("ri.nominal.asset.test", "sensor1", "scope1")
		numericSeries := newTestQueryExecution(ds, nil).buildChannelSeries("ri.nominal.asset.test", "sensor1", "scope1")

		enumJSON, _ := json.Marshal(enumSeries)
		numericJSON, _ := json.Marshal(numericSeries)

		// Both should contain the same channel literal
		expectedChannel := `"channel":{"type":"literal","literal":"sensor1"}`
		if !strings.Contains(string(enumJSON), expectedChannel) {
			t.Errorf("enum series missing expected channel literal, got: %s", string(enumJSON))
		}
		if !strings.Contains(string(numericJSON), expectedChannel) {
			t.Errorf("numeric series missing expected channel literal, got: %s", string(numericJSON))
		}

		// Both should contain the same dataScopeName literal
		expectedScope := `"dataScopeName":{"type":"literal","literal":"scope1"}`
		if !strings.Contains(string(enumJSON), expectedScope) {
			t.Errorf("enum series missing expected dataScopeName literal, got: %s", string(enumJSON))
		}
		if !strings.Contains(string(numericJSON), expectedScope) {
			t.Errorf("numeric series missing expected dataScopeName literal, got: %s", string(numericJSON))
		}
	})
}

func BenchmarkBuildComputeContext(b *testing.B) {
	ds := &Datasource{}
	qm := NominalQueryModel{
		AssetRid: "ri.nominal.asset.12345",
		Channel:  "temperature",
		TemplateVariables: map[string]interface{}{
			"env":    "prod",
			"region": "us-east",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newTestQueryExecution(ds, nil).buildComputeContext(qm, 1704067200, 1704153600)
	}
}

func TestBuildComputeRequestArrowFormat(t *testing.T) {
	ds := &Datasource{}
	baseTimeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	t.Run("numeric path sets ARROW_V3 and NumericOutputFields", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "temperature",
			ChannelDataType: "numeric",
			DataScopeName:   "default",
			Buckets:         1000,
			Aggregations:    []string{"MEAN"},
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}
		jsonStr := string(jsonBytes)

		if !strings.Contains(jsonStr, `"outputFormat"`) {
			t.Errorf("expected outputFormat in request JSON, got: %s", jsonStr)
		}
		if !strings.Contains(jsonStr, `"numericOutputFields"`) {
			t.Errorf("expected numericOutputFields in request JSON, got: %s", jsonStr)
		}
	})

	t.Run("empty ChannelDataType gets Arrow format", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.123",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       1000,
			Aggregations:  []string{"MEAN"},
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		jsonBytes, _ := json.Marshal(req.Node)
		jsonStr := string(jsonBytes)

		if !strings.Contains(jsonStr, `"outputFormat"`) {
			t.Errorf("expected outputFormat for default numeric path, got: %s", jsonStr)
		}
	})

	t.Run("string ChannelDataType stays LEGACY (no outputFormat)", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "status",
			ChannelDataType: "string",
			DataScopeName:   "default",
			Buckets:         1000,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		jsonBytes, _ := json.Marshal(req.Node)
		jsonStr := string(jsonBytes)

		if strings.Contains(jsonStr, `"outputFormat"`) {
			t.Errorf("expected no outputFormat for enum path, got: %s", jsonStr)
		}
		if strings.Contains(jsonStr, `"numericOutputFields"`) {
			t.Errorf("expected no numericOutputFields for enum path, got: %s", jsonStr)
		}
	})
}

func TestBuildComputeRequestLogPath(t *testing.T) {
	ds := &Datasource{}
	baseTimeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	t.Run("log ChannelDataType produces log series with page strategy", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "app.logs",
			ChannelDataType: "log",
			DataScopeName:   "default",
			Buckets:         1000,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		jsonBytes, err := json.Marshal(req.Node)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}
		jsonStr := string(jsonBytes)

		if !strings.Contains(jsonStr, `"type":"log"`) {
			t.Errorf("expected log series type in JSON, got: %s", jsonStr)
		}
		// Log path uses page strategy, not buckets
		if strings.Contains(jsonStr, `"outputFormat"`) {
			t.Errorf("expected no outputFormat for log path, got: %s", jsonStr)
		}
		if strings.Contains(jsonStr, `"numericOutputFields"`) {
			t.Errorf("expected no numericOutputFields for log path, got: %s", jsonStr)
		}
		if !strings.Contains(jsonStr, `"pageSize":-250`) {
			t.Errorf("expected newest-first log page size -250, got: %s", jsonStr)
		}
		if strings.Contains(jsonStr, `"buckets"`) {
			t.Errorf("expected no buckets for log path, got: %s", jsonStr)
		}
	})

	t.Run("log path is structurally different from numeric and enum", func(t *testing.T) {
		logQM := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "app.logs",
			ChannelDataType: "log",
			DataScopeName:   "default",
		}
		numericQM := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "temperature",
			ChannelDataType: "numeric",
			DataScopeName:   "default",
			Buckets:         1000,
			Aggregations:    []string{"MEAN"},
		}

		logReq := newTestQueryExecution(ds, nil).buildComputeRequest(logQM, baseTimeRange, 0)
		numericReq := newTestQueryExecution(ds, nil).buildComputeRequest(numericQM, baseTimeRange, 0)

		logJSON, _ := json.Marshal(logReq.Node)
		numericJSON, _ := json.Marshal(numericReq.Node)

		if string(logJSON) == string(numericJSON) {
			t.Error("expected different JSON for log vs numeric ChannelDataType")
		}
	})
}
