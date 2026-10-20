package plugin

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	"github.com/palantir/pkg/uuid"
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
		func(computeapi1.EnumSeries) error { kind = "enum"; return nil },
		func(computeapi1.NumericSeries) error { kind = "numeric"; return nil },
		func(computeapi1.LogSeries) error { kind = "log"; return nil },
		func(computeapi1.ArraySeries) error { kind = "array"; return nil },
		func(computeapi1.StructSeries) error { kind = "struct"; return nil },
		func(computeapi.Reference) error { kind = "video"; return nil },
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

// summarizationPageInfo returns the page info when the strategy is a page strategy.
func summarizationPageInfo(t *testing.T, strategy computeapi.SummarizationStrategy) (page computeapi.PageInfo, isPage bool) {
	t.Helper()
	err := strategy.AcceptFuncs(
		func(computeapi.DecimateStrategy) error { return nil },
		func(computeapi.LttbStrategy) error { return nil },
		func(p computeapi.PageStrategy) error {
			return p.AcceptFuncs(
				func(info computeapi.PageInfo) error { page, isPage = info, true; return nil },
				func(string) error { return fmt.Errorf("unknown page strategy type") },
			)
		},
		func(computeapi.TruncateStrategy) error { return nil },
		func(string) error { return fmt.Errorf("unknown summarization strategy type") },
	)
	if err != nil {
		t.Fatalf("inspecting page info: %v", err)
	}
	return page, isPage
}

func isArrowFormat(format *computeapi.OutputFormat, want computeapi.OutputFormat_Value) bool {
	return format != nil && format.Value() == want
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
		func(computeapi1.Spectrogram) error { return fmt.Errorf("expected series node, got spectrogram") },
		func(computeapi1.Histogram) error { return fmt.Errorf("expected series node, got histogram") },
		func(computeapi1.CurveFitV2) error { return fmt.Errorf("expected series node, got curveV2") },
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
	ds := withCatalog(&Datasource{})

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

func TestResolvePointBudget(t *testing.T) {
	tests := []struct {
		name          string
		qm            NominalQueryModel
		maxDataPoints int64
		wantCount     int
		wantNotice    bool
		wantErr       string
	}{
		{"maxDataPoints lowers saved buckets", NominalQueryModel{Buckets: 1000}, 500, 500, false, ""},
		{"maxDataPoints does not raise saved buckets", NominalQueryModel{Buckets: 500}, 1000, 500, false, ""},
		{"zero maxDataPoints keeps saved buckets", NominalQueryModel{Buckets: 1000}, 0, 1000, false, ""},
		{"saved buckets at the API limit are kept", NominalQueryModel{Buckets: 10000}, 0, 10000, false, ""},
		{"saved buckets above the API limit are rejected", NominalQueryModel{Buckets: 10001}, 0, 0, false, "buckets must be at most 10000, got 10001"},
		{"maxDataPoints lowers saved buckets above the API limit", NominalQueryModel{Buckets: 20000}, 1200, 1200, false, ""},
		{"saved buckets above the API limit are rejected when maxDataPoints is too", NominalQueryModel{Buckets: 20000}, 15000, 0, false, "buckets must be at most 10000, got 20000"},
		{"negative buckets are rejected", NominalQueryModel{Buckets: -10}, 300, 0, false, "buckets must be non-negative, got -10"},
		{"maxDataPoints used when buckets is unset", NominalQueryModel{}, 2000, 2000, false, ""},
		{"maxDataPoints at the API limit is not capped", NominalQueryModel{}, 10000, 10000, false, ""},
		{"maxDataPoints above the API limit is capped with a notice", NominalQueryModel{}, 43200, 10000, true, ""},
		{"both unset falls back to the default", NominalQueryModel{}, 0, 1000, false, ""},
		{"string channels use the same rules", NominalQueryModel{ChannelDataType: ChannelDataTypeString}, 43200, 10000, true, ""},
		{"log channels get no budget", NominalQueryModel{ChannelDataType: ChannelDataTypeLog, Buckets: 10001}, 43200, 0, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolvePointBudget(tt.qm, tt.maxDataPoints)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Count != tt.wantCount {
				t.Errorf("Count = %d, want %d", got.Count, tt.wantCount)
			}
			if (got.Notice != "") != tt.wantNotice {
				t.Errorf("Notice = %q, want notice %v", got.Notice, tt.wantNotice)
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

func TestAlignedBucketRange(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		from, to time.Time
		n        int
	}{
		{"unaligned 10 minutes", base.Add(3500 * time.Millisecond), base.Add(603500 * time.Millisecond), 60},
		{"quarter second", base.Add(7), base.Add(250*time.Millisecond + 7), 1000},
		{"two buckets", base.Add(time.Second), base.Add(time.Minute), 2},
		{"range end on the grid", base, base.Add(time.Minute), 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aligned, ok := alignedBucketRange(backend.TimeRange{From: tt.from, To: tt.to}, tt.n)
			start, end, n := aligned.From.UnixNano(), aligned.To.UnixNano(), int64(tt.n)
			width := (end - start) / n
			if !ok || (end-start)%n != 0 || start%width != 0 {
				t.Fatalf("grid not exact: ok=%v start=%d end=%d n=%d", ok, start, end, n)
			}
			from, to := tt.from.UnixNano(), tt.to.UnixNano()
			if start > from || start+width <= from {
				t.Errorf("first bucket [%d, %d) does not contain from %d", start, start+width, from)
			}
			if end < to || (to-start)%width == 0 {
				t.Errorf("to %d is not strictly inside a bucket: start=%d end=%d width=%d", to, start, end, width)
			}
		})
	}

	epoch := time.Unix(0, 0)
	year := func(y int) time.Time { return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC) }
	unchanged := []struct {
		name     string
		from, to time.Time
		n        int
	}{
		{"zero buckets", base.Add(time.Second), base.Add(time.Minute), 0},
		{"one bucket", base.Add(time.Second), base.Add(time.Minute), 1},
		{"reversed range", base.Add(time.Minute), base, 10},
		{"range ends at the epoch", epoch.Add(-time.Hour), epoch, 1000},
		{"aligned end past int64 nanoseconds", epoch, year(2200), 2},
		{"range longer than int64 nanoseconds", year(1700), year(2200), 1000},
	}
	for _, tt := range unchanged {
		t.Run(tt.name, func(t *testing.T) {
			tr := backend.TimeRange{From: tt.from, To: tt.to}
			if got, ok := alignedBucketRange(tr, tt.n); ok || got != tr {
				t.Errorf("got %v ok=%v, want the range unchanged", got, ok)
			}
		})
	}
}

func TestBuildComputeRequestAlignsOnlyBucketedNumeric(t *testing.T) {
	qe := newTestQueryExecution(withCatalog(&Datasource{}), nil)
	tr := backend.TimeRange{
		From: time.Date(2026, 11, 1, 0, 0, 3, 500_000_000, time.UTC),
		To:   time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name        string
		qm          NominalQueryModel
		wantAligned bool
	}{
		{"numeric buckets", NominalQueryModel{ChannelDataType: ChannelDataTypeNumeric, Aggregations: []string{AggMean}, Points: pointBudget{Count: 100}}, true},
		{"enum buckets", NominalQueryModel{ChannelDataType: ChannelDataTypeString, Points: pointBudget{Count: 100}}, false},
		{"LTTB", NominalQueryModel{ChannelDataType: ChannelDataTypeNumeric, Aggregations: []string{AggLTTB}, Points: pointBudget{Count: 100}}, false},
	}
	asTime := func(ts api.Timestamp) time.Time { return time.Unix(int64(ts.Seconds), int64(ts.Nanos)) }
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := qe.buildComputeRequest(tt.qm, tr)
			start, end := asTime(req.Start), asTime(req.End)
			raw, err := json.Marshal(req.Node)
			if err != nil {
				t.Fatal(err)
			}
			var node struct {
				Series struct {
					Input struct {
						Numeric struct {
							TimeRangeFilter *struct {
								StartTime, EndTime struct{ Literal api.Timestamp }
							}
						}
					}
				}
			}
			if err := json.Unmarshal(raw, &node); err != nil {
				t.Fatal(err)
			}
			filter := node.Series.Input.Numeric.TimeRangeFilter

			if !tt.wantAligned {
				if !start.Equal(tr.From) || !end.Equal(tr.To) || filter != nil {
					t.Errorf("request [%v, %v], filter %v; want the panel range and no filter", start, end, filter)
				}
				return
			}
			if start.Equal(tr.From) || filter == nil {
				t.Fatalf("start = %v, filter = %v; want an aligned start and a filter", start, filter)
			}
			if from, to := asTime(filter.StartTime.Literal), asTime(filter.EndTime.Literal); !from.Equal(tr.From) || !to.Equal(tr.To) {
				t.Errorf("filter = [%v, %v], want the panel range [%v, %v]", from, to, tr.From, tr.To)
			}
			if got := *summarizeSeriesFromNode(t, req.Node).Buckets; got != tt.qm.Points.Count {
				t.Errorf("buckets = %d, want %d", got, tt.qm.Points.Count)
			}
		})
	}
}

// seriesPlanFor builds a plan with an empty time range, which no plan aligns.
func seriesPlanFor(qe *NominalQueryExecution, qm NominalQueryModel) computeapi1.SummarizeSeries {
	plan, _ := qe.buildSeriesPlan(qm, backend.TimeRange{})
	return plan
}

func TestBuildAssetChannel(t *testing.T) {
	ds := withCatalog(&Datasource{})

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
	ds := withCatalog(&Datasource{})
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
			plan := seriesPlanFor(qe, qm)
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

		stringKind := seriesKind(t, seriesPlanFor(qe, stringQM).Input)
		numericKind := seriesKind(t, seriesPlanFor(qe, numericQM).Input)
		if stringKind == numericKind {
			t.Errorf("expected different series kinds for string vs numeric, both = %q", stringKind)
		}
	})
}

func TestBuildSeriesPlanUsesResolvedPointCount(t *testing.T) {
	qe := newTestQueryExecution(withCatalog(&Datasource{}), nil)
	for _, channelDataType := range []string{ChannelDataTypeNumeric, ChannelDataTypeString} {
		t.Run(channelDataType, func(t *testing.T) {
			qm := NominalQueryModel{
				AssetRid:        "ri.nominal.asset.test",
				Channel:         "temperature",
				DataScopeName:   "default",
				ChannelDataType: channelDataType,
				Buckets:         1000,
				Points:          pointBudget{Count: 750},
			}
			plan := seriesPlanFor(qe, qm)
			if plan.Buckets == nil || *plan.Buckets != 750 {
				t.Errorf("buckets = %v, want 750", plan.Buckets)
			}
		})
	}
}

func TestBuildSeriesPlanArrowFormat(t *testing.T) {
	ds := withCatalog(&Datasource{})
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
		plan := seriesPlanFor(qe, qm)

		if !isArrowFormat(plan.OutputFormat, computeapi.OutputFormat_ARROW_V3) {
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
		if plan := seriesPlanFor(qe, qm); !isArrowFormat(plan.OutputFormat, computeapi.OutputFormat_ARROW_V3) {
			t.Errorf("outputFormat = %v, want ARROW_V3 for default numeric path", plan.OutputFormat)
		}
	})

	t.Run("string ChannelDataType gets Arrow format without numeric fields", func(t *testing.T) {
		qm := NominalQueryModel{
			AssetRid:        "ri.nominal.asset.123",
			Channel:         "status",
			ChannelDataType: ChannelDataTypeString,
			DataScopeName:   "default",
			Buckets:         1000,
		}
		plan := seriesPlanFor(qe, qm)

		if !isArrowFormat(plan.OutputFormat, computeapi.OutputFormat_ARROW_V4) {
			t.Errorf("outputFormat = %v, want ARROW_V4", plan.OutputFormat)
		}
		if plan.NumericOutputFields != nil {
			t.Errorf("numericOutputFields = %v, want nil for enum path", plan.NumericOutputFields)
		}
	})
}

func TestBuildSeriesPlanEnumKeepsRangeEndInsideABucket(t *testing.T) {
	qe := newTestQueryExecution(withCatalog(&Datasource{}), nil)
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		from, to time.Time
		n, want  int
	}{
		// 300 s / 30 = 10 s and to is a multiple of 10 s; 29 buckets give a width that is not.
		{"to on the grid", base, base.Add(300 * time.Second), 30, 29},
		{"to off the grid", base.Add(3500 * time.Millisecond), base.Add(303500 * time.Millisecond), 30, 30},
		{"one bucket", base, base.Add(time.Minute), 1, 1},
		{"range ends at the epoch", time.Unix(0, 0).Add(-time.Hour), time.Unix(0, 0), 30, 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := NominalQueryModel{ChannelDataType: ChannelDataTypeString, Points: pointBudget{Count: tt.n}}
			plan, _ := qe.buildSeriesPlan(qm, backend.TimeRange{From: tt.from, To: tt.to})
			if got := *plan.Buckets; got != tt.want {
				t.Errorf("buckets = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBuildSeriesPlanLTTBUsesRawNumericPoints(t *testing.T) {
	qe := newTestQueryExecution(withCatalog(&Datasource{}), nil)
	qm := NominalQueryModel{
		AssetRid: "ri.nominal.asset.test", Channel: "temperature", DataScopeName: "default",
		ChannelDataType: ChannelDataTypeNumeric, Aggregations: []string{AggLTTB}, Buckets: 5000,
		Points: pointBudget{Count: 500},
	}
	plan := seriesPlanFor(qe, qm)
	if got := seriesKind(t, plan.Input); got != "numeric" {
		t.Fatalf("series kind = %q, want numeric", got)
	}
	if !isArrowFormat(plan.OutputFormat, computeapi.OutputFormat_ARROW_V3) {
		t.Fatalf("LTTB must request ARROW_V3: %+v", plan.OutputFormat)
	}
	if plan.Buckets != nil || plan.NumericOutputFields != nil {
		t.Fatalf("LTTB must not request buckets or output fields: %+v", plan)
	}
	if plan.SummarizationStrategy == nil {
		t.Fatal("LTTB strategy is missing")
	}
	points := 0
	err := plan.SummarizationStrategy.AcceptFuncs(
		func(computeapi.DecimateStrategy) error { return fmt.Errorf("unexpected decimate strategy") },
		func(strategy computeapi.LttbStrategy) error { points = strategy.MaxPointsPerGroup; return nil },
		func(computeapi.PageStrategy) error { return fmt.Errorf("unexpected page strategy") },
		func(computeapi.TruncateStrategy) error { return fmt.Errorf("unexpected truncate strategy") },
		func(kind string) error { return fmt.Errorf("unknown strategy %s", kind) },
	)
	if err != nil || points != 500 {
		t.Fatalf("LTTB strategy points = %d, error = %v; want 500", points, err)
	}
	encoded, err := json.Marshal(plan.Input)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "timeShift") || !strings.Contains(string(encoded), `"type":"channel"`) {
		t.Fatalf("LTTB input must be the raw channel series: %s", encoded)
	}
}

func TestBuildSeriesPlanLogPath(t *testing.T) {
	ds := withCatalog(&Datasource{})
	qe := newTestQueryExecution(ds, nil)

	qm := NominalQueryModel{
		AssetRid:        "ri.nominal.asset.123",
		Channel:         "app.logs",
		ChannelDataType: ChannelDataTypeLog,
		DataScopeName:   "default",
		Buckets:         1000,
	}
	plan := seriesPlanFor(qe, qm)

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
	page, isPage := summarizationPageInfo(t, *plan.SummarizationStrategy)
	if !isPage {
		t.Error("expected a page strategy for log path")
	}
	if page.PageSize != -logPageSize || page.PageToken != nil {
		t.Errorf("page = %d with token %v, want %d without (newest-first)", page.PageSize, page.PageToken, -logPageSize)
	}
}

func TestBuildComputeRequestLogPage(t *testing.T) {
	from := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	lineID := func(at time.Time) string {
		return strconv.FormatInt(at.UnixNano(), 10) + ":" + uuid.UUID{7}.String() + ":1f"
	}
	for _, tc := range []struct {
		name     string
		id       string
		newer    bool
		wantSize int
	}{
		{name: "forward from the start for newer lines", newer: true, wantSize: logPageSize},
		{name: "back from the end when the line is before the range", id: lineID(from.Add(-1)), wantSize: -logPageSize},
		{name: "forward from the start when the line is after the range", id: lineID(from.Add(2 * time.Minute)), newer: true, wantSize: logPageSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qe := newTestQueryExecution(withCatalog(&Datasource{}), nil)
			qm := NominalQueryModel{Channel: "app.logs", DataScopeName: "ds1", ChannelDataType: ChannelDataTypeLog, ScrollID: tc.id, ScrollNewer: tc.newer}
			req := qe.buildComputeRequest(qm, backend.TimeRange{From: from, To: from.Add(time.Minute)})
			page, _ := summarizationPageInfo(t, *summarizeSeriesFromNode(t, req.Node).SummarizationStrategy)
			if page.PageSize != tc.wantSize || page.PageToken != nil {
				t.Errorf("page = %d with token %v; want %d without a token", page.PageSize, page.PageToken, tc.wantSize)
			}
		})
	}
}

func BenchmarkBuildComputeContext(b *testing.B) {
	ds := withCatalog(&Datasource{})
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
