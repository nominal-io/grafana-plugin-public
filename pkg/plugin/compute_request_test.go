package plugin

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
)

// --- typed plan inspectors ---
//
// The compute request is built from Conjure union types whose arms are unexported, so
// tests introspect them through the generated Accept visitors rather than matching JSON
// substrings. These helpers are written once per union and reused, so assertions can name
// the planned series kind, channel binding, and page size directly — and they fail only on
// a genuine model change, not on a serialization-tag rename.

// seriesKind reports which arm of the Series union is set ("numeric", "enum", "log", ...).
func seriesKind(t *testing.T, s computeapi1.Series) string {
	t.Helper()
	var kind string
	err := s.AcceptFuncs(
		func(computeapi.Reference) error { kind = "raw"; return nil },
		func(computeapi1.BooleanSeries) error { kind = "boolean"; return nil },
		func(computeapi1.EnumSeries) error { kind = "enum"; return nil },
		func(computeapi1.NumericSeries) error { kind = "numeric"; return nil },
		func(computeapi1.LogSeries) error { kind = "log"; return nil },
		func(computeapi1.ArraySeries) error { kind = "array"; return nil },
		func(computeapi1.StructSeries) error { kind = "struct"; return nil },
		func(string) error { return fmt.Errorf("unknown series type") },
	)
	if err != nil {
		t.Fatalf("inspecting series kind: %v", err)
	}
	return kind
}

// stringConstantValue reports whether a StringConstant is a literal or variable, and its value.
func stringConstantValue(t *testing.T, c computeapi.StringConstant) (kind, value string) {
	t.Helper()
	err := c.AcceptFuncs(
		func(literal string) error { kind, value = "literal", literal; return nil },
		func(v computeapi.VariableName) error { kind, value = "variable", string(v); return nil },
		func(string) error { return fmt.Errorf("unknown string constant type") },
	)
	if err != nil {
		t.Fatalf("inspecting string constant: %v", err)
	}
	return kind, value
}

// summarizationPageSize returns the page size when the strategy is a page strategy.
func summarizationPageSize(t *testing.T, strategy computeapi.SummarizationStrategy) (size int, isPage bool) {
	t.Helper()
	err := strategy.AcceptFuncs(
		func(computeapi.DecimateStrategy) error { return nil },
		func(p computeapi.PageStrategy) error {
			return p.AcceptFuncs(
				func(info computeapi.PageInfo) error { size, isPage = info.PageSize, true; return nil },
				func(string) error { return fmt.Errorf("unknown page strategy type") },
			)
		},
		func(computeapi.TruncateStrategy) error { return nil },
		func(string) error { return fmt.Errorf("unknown summarization strategy type") },
	)
	if err != nil {
		t.Fatalf("inspecting page size: %v", err)
	}
	return size, isPage
}

// isArrowV3 reports whether the SummarizeSeries output format is ARROW_V3.
func isArrowV3(format *computeapi.OutputFormat) bool {
	return format != nil && format.Value() == computeapi.OutputFormat_ARROW_V3
}

// summarizeSeriesFromNode unwraps a ComputableNode to the SummarizeSeries it carries,
// so integration tests holding only the request Node can assert on the planned series
// kind and output format through the same typed inspectors as the unit tests.
func summarizeSeriesFromNode(t *testing.T, node computeapi1.ComputableNode) computeapi1.SummarizeSeries {
	t.Helper()
	var series computeapi1.SummarizeSeries
	err := node.AcceptFuncs(
		func(computeapi1.SummarizeRanges) error { return fmt.Errorf("expected series node, got ranges") },
		func(s computeapi1.SummarizeSeries) error { series = s; return nil },
		func(computeapi1.SelectValue) error { return fmt.Errorf("expected series node, got value") },
		func(computeapi1.SummarizeCartesian) error { return fmt.Errorf("expected series node, got cartesian") },
		func(computeapi1.SummarizeCartesian3d) error {
			return fmt.Errorf("expected series node, got cartesian3d")
		},
		func(computeapi1.FrequencyDomain) error { return fmt.Errorf("expected series node, got frequency") },
		func(computeapi1.FrequencyDomainV2) error { return fmt.Errorf("expected series node, got frequencyV2") },
		func(computeapi1.Histogram) error { return fmt.Errorf("expected series node, got histogram") },
		func(computeapi1.CurveFit) error { return fmt.Errorf("expected series node, got curve") },
		func(computeapi1.SummarizeMultivariate) error {
			return fmt.Errorf("expected series node, got multivariate")
		},
		func(string) error { return fmt.Errorf("unknown computable node type") },
	)
	if err != nil {
		t.Fatalf("inspecting computable node: %v", err)
	}
	return series
}

func TestBuildComputeContext(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name         string
		qm           NominalQueryModel
		expectedVars int
	}{
		{
			name: "basic context with assetRid",
			qm: NominalQueryModel{
				AssetRid: "ri.nominal.asset.12345",
				Channel:  "temperature",
			},
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
			expectedVars: 2, // assetRid + 1 string template var
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newTestQueryExecution(ds, nil).buildComputeContext(tt.qm)

			if len(ctx.Variables) != tt.expectedVars {
				t.Errorf("expected %d variables, got %d", tt.expectedVars, len(ctx.Variables))
			}

			// Verify assetRid is always present
			if _, ok := ctx.Variables[assetRidVariableName]; !ok {
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

// TestBuildComputeRequest is the integration-level test: it exercises the full
// buildComputeRequest path (time range -> node wrapping -> context), while the detailed
// per-kind plan shape is covered by the buildSeriesPlan tests below.
func TestBuildComputeRequest(t *testing.T) {
	ds := &Datasource{}

	qm := NominalQueryModel{
		AssetRid:      "ri.nominal.asset.12345",
		Channel:       "temperature",
		DataScopeName: "default",
		Buckets:       100,
	}

	timeRange := backend.TimeRange{
		From: time.Unix(1704067200, 100_000_000), // 2024-01-01 00:00:00.100 UTC
		To:   time.Unix(1704067200, 900_000_000), // 2024-01-01 00:00:00.900 UTC
	}

	req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, timeRange, 0)

	if int64(req.Start.Seconds) != 1704067200 {
		t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, 1704067200)
	}
	if int64(req.Start.Nanos) != 100_000_000 {
		t.Errorf("Start.Nanos = %d, want %d", req.Start.Nanos, 100_000_000)
	}
	if int64(req.End.Seconds) != 1704067200 {
		t.Errorf("End.Seconds = %d, want %d", req.End.Seconds, 1704067200)
	}
	if int64(req.End.Nanos) != 900_000_000 {
		t.Errorf("End.Nanos = %d, want %d", req.End.Nanos, 900_000_000)
	}
	// The series plan must be wrapped into a node.
	if req.Node == (computeapi1.ComputableNode{}) {
		t.Error("expected non-zero ComputableNode")
	}
}

func TestBuildAssetChannel(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name          string
		channel       string
		dataScopeName string
	}{
		{
			name:          "builds channel with all parameters",
			channel:       "temperature",
			dataScopeName: "default",
		},
		{
			name:          "builds channel with empty dataScopeName",
			channel:       "pressure",
			dataScopeName: "",
		},
		{
			name:          "builds channel with special characters in channel",
			channel:       "sensor/value-1",
			dataScopeName: "scope_test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			asset := newTestQueryExecution(ds, nil).buildAssetChannel(tt.channel, tt.dataScopeName)

			if kind, val := stringConstantValue(t, asset.Channel); kind != "literal" || val != tt.channel {
				t.Errorf("channel = (%s, %q), want (literal, %q)", kind, val, tt.channel)
			}
			if kind, val := stringConstantValue(t, asset.DataScopeName); kind != "literal" || val != tt.dataScopeName {
				t.Errorf("dataScopeName = (%s, %q), want (literal, %q)", kind, val, tt.dataScopeName)
			}
			// assetRid is a variable reference (not a literal); the value is supplied via context.
			if kind, val := stringConstantValue(t, asset.AssetRid); kind != "variable" || val != string(assetRidVariableName) {
				t.Errorf("assetRid = (%s, %q), want (variable, %q)", kind, val, assetRidVariableName)
			}
		})
	}
}

func TestBuildSeriesPlanBranching(t *testing.T) {
	ds := &Datasource{}
	qe := newTestQueryExecution(ds, nil)

	baseQM := NominalQueryModel{
		AssetRid:      "ri.nominal.asset.test",
		Channel:       "temperature",
		DataScopeName: "default",
		Buckets:       1000,
	}

	tests := []struct {
		name     string
		dataType string
		wantKind string
	}{
		{"string ChannelDataType produces enum series", ChannelDataTypeString, "enum"},
		{"numeric ChannelDataType produces numeric series", ChannelDataTypeNumeric, "numeric"},
		{"log ChannelDataType produces log series", ChannelDataTypeLog, "log"},
		{"empty ChannelDataType defaults to numeric series", "", "numeric"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := baseQM
			qm.ChannelDataType = tt.dataType
			plan := qe.buildSeriesPlan(qm, 0)
			if got := seriesKind(t, plan.Input); got != tt.wantKind {
				t.Errorf("series kind = %q, want %q", got, tt.wantKind)
			}
		})
	}

	t.Run("string and numeric produce different series kinds", func(t *testing.T) {
		stringQM := baseQM
		stringQM.ChannelDataType = ChannelDataTypeString
		numericQM := baseQM
		numericQM.ChannelDataType = ChannelDataTypeNumeric

		stringKind := seriesKind(t, qe.buildSeriesPlan(stringQM, 0).Input)
		numericKind := seriesKind(t, qe.buildSeriesPlan(numericQM, 0).Input)
		if stringKind == numericKind {
			t.Errorf("expected different series kinds for string vs numeric, both = %q", stringKind)
		}
	})
}

func TestBuildSeriesPlanBuckets(t *testing.T) {
	ds := &Datasource{}
	qe := newTestQueryExecution(ds, nil)

	wantBuckets := func(t *testing.T, plan computeapi1.SummarizeSeries, want int) {
		t.Helper()
		if plan.Buckets == nil {
			t.Fatalf("buckets = nil, want %d", want)
		}
		if *plan.Buckets != want {
			t.Errorf("buckets = %d, want %d", *plan.Buckets, want)
		}
	}

	tests := []struct {
		name          string
		buckets       int
		maxDataPoints int64
		want          int
	}{
		{"maxDataPoints caps buckets when smaller", 1000, 500, 500},
		{"maxDataPoints does not increase buckets", 500, 1000, 500},
		{"maxDataPoints used when buckets is zero", 0, 800, 800},
		{"zero maxDataPoints uses saved buckets", 1000, 0, 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := NominalQueryModel{
				AssetRid:      "ri.nominal.asset.test",
				Channel:       "temperature",
				DataScopeName: "default",
				Buckets:       tt.buckets,
			}
			wantBuckets(t, qe.buildSeriesPlan(qm, tt.maxDataPoints), tt.want)
		})
	}
}

func TestBuildSeriesPlanArrowFormat(t *testing.T) {
	ds := &Datasource{}
	qe := newTestQueryExecution(ds, nil)

	t.Run("numeric path sets ARROW_V3 and NumericOutputFields", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "temperature",
			ChannelDataType: ChannelDataTypeNumeric,
			DataScopeName:   "default",
			Buckets:         1000,
			Aggregations:    []string{AggMean},
		}
		plan := qe.buildSeriesPlan(qm, 0)

		if !isArrowV3(plan.OutputFormat) {
			t.Errorf("outputFormat = %v, want ARROW_V3", plan.OutputFormat)
		}
		if plan.NumericOutputFields == nil || len(*plan.NumericOutputFields) == 0 {
			t.Errorf("expected numericOutputFields to be set, got %v", plan.NumericOutputFields)
		}
	})

	t.Run("empty ChannelDataType gets Arrow format", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.123",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       1000,
			Aggregations:  []string{AggMean},
		}
		if plan := qe.buildSeriesPlan(qm, 0); !isArrowV3(plan.OutputFormat) {
			t.Errorf("outputFormat = %v, want ARROW_V3 for default numeric path", plan.OutputFormat)
		}
	})

	t.Run("string ChannelDataType stays LEGACY (no outputFormat)", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "status",
			ChannelDataType: ChannelDataTypeString,
			DataScopeName:   "default",
			Buckets:         1000,
		}
		plan := qe.buildSeriesPlan(qm, 0)

		if plan.OutputFormat != nil {
			t.Errorf("outputFormat = %v, want nil for enum path", plan.OutputFormat)
		}
		if plan.NumericOutputFields != nil {
			t.Errorf("numericOutputFields = %v, want nil for enum path", plan.NumericOutputFields)
		}
	})
}

func TestBuildSeriesPlanLogPath(t *testing.T) {
	ds := &Datasource{}
	qe := newTestQueryExecution(ds, nil)

	qm := NominalQueryModel{
		AssetRid:        "ri.nominal.asset.123",
		Channel:         "app.logs",
		ChannelDataType: ChannelDataTypeLog,
		DataScopeName:   "default",
		Buckets:         1000,
	}
	plan := qe.buildSeriesPlan(qm, 0)

	if got := seriesKind(t, plan.Input); got != "log" {
		t.Errorf("series kind = %q, want \"log\"", got)
	}
	// Log path uses a page strategy, not buckets or Arrow output.
	if plan.OutputFormat != nil {
		t.Errorf("outputFormat = %v, want nil for log path", plan.OutputFormat)
	}
	if plan.NumericOutputFields != nil {
		t.Errorf("numericOutputFields = %v, want nil for log path", plan.NumericOutputFields)
	}
	if plan.Buckets != nil {
		t.Errorf("buckets = %d, want nil for log path", *plan.Buckets)
	}
	if plan.SummarizationStrategy == nil {
		t.Fatal("expected a summarization strategy for log path")
	}
	size, isPage := summarizationPageSize(t, *plan.SummarizationStrategy)
	if !isPage {
		t.Error("expected a page strategy for log path")
	}
	if size != logPageSize {
		t.Errorf("pageSize = %d, want %d (newest-first)", size, logPageSize)
	}
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
		newTestQueryExecution(ds, nil).buildComputeContext(qm)
	}
}
