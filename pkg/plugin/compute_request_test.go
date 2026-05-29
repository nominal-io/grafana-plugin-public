package plugin

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
)

// --- typed plan inspectors ---
//
// These decode the compute request into typed shapes so tests can assert the planned
// series kind, buckets, output fields, and page strategy directly, instead of matching
// brittle JSON substrings (which fail on field renames or serialization-layout changes
// even when the compute model is correct). The Conjure union types expose only
// unexported fields — typed introspection is via the verbose Accept visitor — so a
// compact JSON decode is the pragmatic middle ground.

type stringConstantView struct {
	Type     string `json:"type"` // "literal" or "variable"
	Literal  string `json:"literal"`
	Variable string `json:"variable"`
}

type assetChannelView struct {
	AssetRid      stringConstantView `json:"assetRid"`
	Channel       stringConstantView `json:"channel"`
	DataScopeName stringConstantView `json:"dataScopeName"`
}

// channelSeriesView decodes {"type":"channel","channel":{"type":"asset","asset":{...}}},
// the shape shared by numeric and enum channel series.
type channelSeriesView struct {
	Type    string `json:"type"`
	Channel struct {
		Type  string           `json:"type"`
		Asset assetChannelView `json:"asset"`
	} `json:"channel"`
}

func decodeChannelSeries(t *testing.T, series interface{}) channelSeriesView {
	t.Helper()
	b, err := json.Marshal(series)
	if err != nil {
		t.Fatalf("failed to marshal series: %v", err)
	}
	var view channelSeriesView
	if err := json.Unmarshal(b, &view); err != nil {
		t.Fatalf("failed to unmarshal channel series: %v", err)
	}
	return view
}

// planView is a compact, typed projection of a SummarizeSeries node for assertions.
type planView struct {
	SeriesKind          string   // "numeric", "enum", or "log"
	Buckets             *int     // nil when not set (e.g. the log path)
	OutputFormat        string   // "" when not set
	NumericOutputFields []string // nil when not set
	PageSize            *int     // nil when not using a page strategy
}

func decodePlan(t *testing.T, node computeapi1.ComputableNode) planView {
	t.Helper()
	b, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("failed to marshal node: %v", err)
	}
	var raw struct {
		Series struct {
			Input struct {
				Type string `json:"type"`
			} `json:"input"`
			Buckets               *int     `json:"buckets"`
			OutputFormat          string   `json:"outputFormat"`
			NumericOutputFields   []string `json:"numericOutputFields"`
			SummarizationStrategy *struct {
				Page *struct {
					PageInfo *struct {
						PageSize *int `json:"pageSize"`
					} `json:"pageInfo"`
				} `json:"page"`
			} `json:"summarizationStrategy"`
		} `json:"series"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("failed to unmarshal node plan: %v", err)
	}
	view := planView{
		SeriesKind:          raw.Series.Input.Type,
		Buckets:             raw.Series.Buckets,
		OutputFormat:        raw.Series.OutputFormat,
		NumericOutputFields: raw.Series.NumericOutputFields,
	}
	if s := raw.Series.SummarizationStrategy; s != nil && s.Page != nil && s.Page.PageInfo != nil {
		view.PageSize = s.Page.PageInfo.PageSize
	}
	return view
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
		channel       string
		dataScopeName string
	}{
		{
			name:          "builds series with all parameters",
			channel:       "temperature",
			dataScopeName: "default",
		},
		{
			name:          "builds series with empty dataScopeName",
			channel:       "pressure",
			dataScopeName: "",
		},
		{
			name:          "builds series with special characters in channel",
			channel:       "sensor/value-1",
			dataScopeName: "scope_test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := newTestQueryExecution(ds, nil).buildChannelSeries(tt.channel, tt.dataScopeName)
			view := decodeChannelSeries(t, result)

			// Verify the series is an asset-based channel (timeShift is added by
			// buildSeriesPlan, not here).
			if view.Type != "channel" {
				t.Errorf("series type = %q, want \"channel\"", view.Type)
			}
			if view.Channel.Type != "asset" {
				t.Errorf("channel type = %q, want \"asset\"", view.Channel.Type)
			}

			asset := view.Channel.Asset
			if asset.Channel.Type != "literal" || asset.Channel.Literal != tt.channel {
				t.Errorf("channel constant = %+v, want literal %q", asset.Channel, tt.channel)
			}
			if asset.DataScopeName.Type != "literal" || asset.DataScopeName.Literal != tt.dataScopeName {
				t.Errorf("dataScopeName constant = %+v, want literal %q", asset.DataScopeName, tt.dataScopeName)
			}
			// assetRid is a variable reference (not a literal); the value is supplied
			// via context variables.
			if asset.AssetRid.Type != "variable" || asset.AssetRid.Variable != assetRidVariableName {
				t.Errorf("assetRid constant = %+v, want variable %q", asset.AssetRid, assetRidVariableName)
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
		qm.ChannelDataType = ChannelDataTypeString
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

		if got := decodePlan(t, req.Node).SeriesKind; got != "enum" {
			t.Errorf("series kind = %q, want \"enum\" for string ChannelDataType", got)
		}
	})

	t.Run("numeric ChannelDataType produces numeric series", func(t *testing.T) {
		qm := baseQM
		qm.ChannelDataType = ChannelDataTypeNumeric
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for numeric request")
		}
		if int64(req.Start.Seconds) != baseTimeRange.From.Unix() {
			t.Errorf("Start.Seconds = %d, want %d", req.Start.Seconds, baseTimeRange.From.Unix())
		}

		if got := decodePlan(t, req.Node).SeriesKind; got != "numeric" {
			t.Errorf("series kind = %q, want \"numeric\" for numeric ChannelDataType", got)
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

		if got := decodePlan(t, req.Node).SeriesKind; got != "numeric" {
			t.Errorf("series kind = %q, want \"numeric\" for empty ChannelDataType", got)
		}
	})

	t.Run("missing ChannelDataType defaults to numeric series", func(t *testing.T) {
		qm := baseQM
		// ChannelDataType is zero-value ""
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)

		if req.Node == (computeapi1.ComputableNode{}) {
			t.Fatal("expected non-zero ComputableNode for missing ChannelDataType request")
		}
		if got := decodePlan(t, req.Node).SeriesKind; got != "numeric" {
			t.Errorf("series kind = %q, want \"numeric\" for missing ChannelDataType", got)
		}
	})

	t.Run("string and numeric produce structurally different requests", func(t *testing.T) {
		stringQM := baseQM
		stringQM.ChannelDataType = ChannelDataTypeString
		stringReq := newTestQueryExecution(ds, nil).buildComputeRequest(stringQM, baseTimeRange, 0)

		numericQM := baseQM
		numericQM.ChannelDataType = ChannelDataTypeNumeric
		numericReq := newTestQueryExecution(ds, nil).buildComputeRequest(numericQM, baseTimeRange, 0)

		if stringKind, numericKind := decodePlan(t, stringReq.Node).SeriesKind, decodePlan(t, numericReq.Node).SeriesKind; stringKind == numericKind {
			t.Errorf("expected different series kinds for string vs numeric, both = %q", stringKind)
		}
	})
}

func TestBuildComputeRequestMaxDataPoints(t *testing.T) {
	ds := &Datasource{}

	baseTimeRange := backend.TimeRange{
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	wantBuckets := func(t *testing.T, req computeapi1.ComputeNodeRequest, want int) {
		t.Helper()
		got := decodePlan(t, req.Node).Buckets
		if got == nil {
			t.Fatalf("buckets = nil, want %d", want)
		}
		if *got != want {
			t.Errorf("buckets = %d, want %d", *got, want)
		}
	}

	t.Run("maxDataPoints caps buckets when smaller", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       1000,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 500)
		wantBuckets(t, req, 500)
	})

	t.Run("maxDataPoints does not increase buckets", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       500,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 1000)
		wantBuckets(t, req, 500)
	})

	t.Run("maxDataPoints used when buckets is zero", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       0,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 800)
		wantBuckets(t, req, 800)
	})

	t.Run("zero maxDataPoints uses saved buckets", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:      "ri.nominal.asset.test",
			Channel:       "temperature",
			DataScopeName: "default",
			Buckets:       1000,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)
		wantBuckets(t, req, 1000)
	})
}

func TestBuildEnumChannelSeries(t *testing.T) {
	ds := &Datasource{}

	t.Run("returns asset channel series", func(t *testing.T) {
		enumSeries := newTestQueryExecution(ds, nil).buildEnumChannelSeries("status", "default")
		view := decodeChannelSeries(t, enumSeries)

		// Should be an asset-based channel (same shape as the numeric path).
		if view.Type != "channel" {
			t.Errorf("series type = %q, want \"channel\"", view.Type)
		}
		if view.Channel.Type != "asset" {
			t.Errorf("channel type = %q, want \"asset\"", view.Channel.Type)
		}
		if asset := view.Channel.Asset; asset.Channel.Type != "literal" || asset.Channel.Literal != "status" {
			t.Errorf("channel constant = %+v, want literal \"status\"", asset.Channel)
		}
	})

	t.Run("mirrors buildChannelSeries asset channel structure", func(t *testing.T) {
		// Both builders should produce the same AssetChannel structure.
		enumView := decodeChannelSeries(t, newTestQueryExecution(ds, nil).buildEnumChannelSeries("sensor1", "scope1"))
		numericView := decodeChannelSeries(t, newTestQueryExecution(ds, nil).buildChannelSeries("sensor1", "scope1"))

		if enumView.Channel.Asset != numericView.Channel.Asset {
			t.Errorf("enum and numeric asset channels differ:\n enum:    %+v\n numeric: %+v", enumView.Channel.Asset, numericView.Channel.Asset)
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
		newTestQueryExecution(ds, nil).buildComputeContext(qm)
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
			ChannelDataType: ChannelDataTypeNumeric,
			DataScopeName:   "default",
			Buckets:         1000,
			Aggregations:    []string{"MEAN"},
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)
		plan := decodePlan(t, req.Node)

		if plan.OutputFormat != "ARROW_V3" {
			t.Errorf("outputFormat = %q, want \"ARROW_V3\"", plan.OutputFormat)
		}
		if len(plan.NumericOutputFields) == 0 {
			t.Errorf("expected numericOutputFields to be set, got %v", plan.NumericOutputFields)
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

		if got := decodePlan(t, req.Node).OutputFormat; got != "ARROW_V3" {
			t.Errorf("outputFormat = %q, want \"ARROW_V3\" for default numeric path", got)
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
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)
		plan := decodePlan(t, req.Node)

		if plan.OutputFormat != "" {
			t.Errorf("outputFormat = %q, want empty for enum path", plan.OutputFormat)
		}
		if plan.NumericOutputFields != nil {
			t.Errorf("numericOutputFields = %v, want nil for enum path", plan.NumericOutputFields)
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
			ChannelDataType: ChannelDataTypeLog,
			DataScopeName:   "default",
			Buckets:         1000,
		}
		req := newTestQueryExecution(ds, nil).buildComputeRequest(qm, baseTimeRange, 0)
		plan := decodePlan(t, req.Node)

		if plan.SeriesKind != "log" {
			t.Errorf("series kind = %q, want \"log\"", plan.SeriesKind)
		}
		// Log path uses a page strategy, not buckets or Arrow output.
		if plan.OutputFormat != "" {
			t.Errorf("outputFormat = %q, want empty for log path", plan.OutputFormat)
		}
		if plan.NumericOutputFields != nil {
			t.Errorf("numericOutputFields = %v, want nil for log path", plan.NumericOutputFields)
		}
		if plan.Buckets != nil {
			t.Errorf("buckets = %d, want nil for log path", *plan.Buckets)
		}
		if plan.PageSize == nil || *plan.PageSize != logPageSize {
			t.Errorf("pageSize = %v, want %d (newest-first) for log path", plan.PageSize, logPageSize)
		}
	})

	t.Run("log path is structurally different from numeric", func(t *testing.T) {
		logQM := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "app.logs",
			ChannelDataType: ChannelDataTypeLog,
			DataScopeName:   "default",
		}
		numericQM := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "temperature",
			ChannelDataType: ChannelDataTypeNumeric,
			DataScopeName:   "default",
			Buckets:         1000,
			Aggregations:    []string{"MEAN"},
		}

		logReq := newTestQueryExecution(ds, nil).buildComputeRequest(logQM, baseTimeRange, 0)
		numericReq := newTestQueryExecution(ds, nil).buildComputeRequest(numericQM, baseTimeRange, 0)

		if logKind, numericKind := decodePlan(t, logReq.Node).SeriesKind, decodePlan(t, numericReq.Node).SeriesKind; logKind == numericKind {
			t.Errorf("expected different series kinds for log vs numeric, both = %q", logKind)
		}
	})
}
