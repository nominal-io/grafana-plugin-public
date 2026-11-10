package plugin

import (
	"fmt"
	"slices"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	"github.com/palantir/pkg/safelong"
)

const logPageSize = -250

// assetRidVariableName is the compute-context variable that carries the asset RID.
// AssetChannel binds the RID by this variable name; the value is supplied separately
// in buildComputeContext, so the channel builders do not take the RID as a parameter.
const assetRidVariableName computeapi.VariableName = "assetRid"

// buildComputeRequest constructs a ComputeNodeRequest from query model and time range.
func (e *NominalQueryExecution) buildComputeRequest(qm NominalQueryModel, timeRange backend.TimeRange) computeapi1.ComputeNodeRequest {
	seriesPlan, requestRange := e.buildSeriesPlan(qm, timeRange)
	node := computeapi1.NewComputableNodeFromSeries(seriesPlan)

	return computeapi1.ComputeNodeRequest{
		Start:   timestampFromTime(requestRange.From),
		End:     timestampFromTime(requestRange.To),
		Node:    node,
		Context: e.buildComputeContext(qm),
	}
}

// alignedBucketRange returns a range of n buckets that covers timeRange and starts
// on a multiple of the bucket width, so bucket end times sit on a fixed grid.
func alignedBucketRange(timeRange backend.TimeRange, n int) (aligned backend.TimeRange, ok bool) {
	from, to := timeRange.From.UnixNano(), timeRange.To.UnixNano()
	// to-from <= 0 also catches a range too long for int64 nanoseconds. Every width
	// divides 0, so with to at the epoch the loop below would never end.
	if n < 2 || to-from <= 0 || to == 0 {
		return timeRange, false
	}
	// Size n-1 buckets to the range so n still cover it after start rounds down.
	width := (to - from + int64(n) - 2) / int64(n-1)
	alignStart := func(width int64) int64 { return from - ((from%width)+width)%width }
	start := alignStart(width)
	// A bucket that starts exactly at to would hold only a sample at to, so keep to inside a bucket.
	for (to-start)%width == 0 {
		width++
		start = alignStart(width)
	}
	// The aligned end always lies past to, so an end at or before to means it overflowed.
	end := start + int64(n)*width
	if end <= to {
		return timeRange, false
	}
	return backend.TimeRange{From: time.Unix(0, start), To: time.Unix(0, end)}, true
}

// enumBucketCount lowers n until to falls strictly inside a bucket. Enum buckets
// sit at multiples of (to - from) / n since the epoch, and a bucket starting
// exactly at to would hold only a sample at to.
func enumBucketCount(timeRange backend.TimeRange, n int) int {
	from, to := timeRange.From.UnixNano(), timeRange.To.UnixNano()
	// Every width divides 0, so with to at the epoch the loop would drop to one bucket.
	if to == 0 {
		return n
	}
	for ; n > 1; n-- {
		if width := (to - from) / int64(n); width == 0 || to%width != 0 {
			break
		}
	}
	return n
}

// buildSeriesPlan builds the full summarized series for a channel kind: it owns both the
// series shape and its summarization strategy, so adding a new channel kind is a single
// case here rather than coordinated edits across separate series/summarization helpers.
func (e *NominalQueryExecution) buildSeriesPlan(qm NominalQueryModel, timeRange backend.TimeRange) (computeapi1.SummarizeSeries, backend.TimeRange) {
	channelSeries := computeapi.NewChannelSeriesFromAsset(e.buildAssetChannel(qm.Channel, qm.DataScopeName))

	switch qm.ChannelDataType {
	case ChannelDataTypeString:
		enumTimeShiftSeries := computeapi1.EnumTimeShiftSeries{
			Input:    computeapi1.NewEnumSeriesFromChannel(channelSeries),
			Duration: zeroDurationConstant(),
		}
		enumSeries := computeapi1.NewEnumSeriesFromTimeShift(enumTimeShiftSeries)
		series := computeapi1.NewSeriesFromEnum(enumSeries)

		buckets := enumBucketCount(timeRange, qm.Points.Count)
		arrowFormat := computeapi.New_OutputFormat(computeapi.OutputFormat_ARROW_V4)
		return computeapi1.SummarizeSeries{
			Input:        series,
			Buckets:      &buckets,
			OutputFormat: &arrowFormat,
		}, timeRange

	case ChannelDataTypeLog:
		logSeries := computeapi1.NewLogSeriesFromChannel(channelSeries)
		series := computeapi1.NewSeriesFromLog(logSeries)

		pageInfo := computeapi.PageInfo{PageSize: logPageSize}
		pageStrategy := computeapi.NewPageStrategyFromPageInfo(pageInfo)
		summarizationStrategy := computeapi.NewSummarizationStrategyFromPage(pageStrategy)
		return computeapi1.SummarizeSeries{
			Input:                 series,
			SummarizationStrategy: &summarizationStrategy,
		}, timeRange

	default:
		if slices.Contains(qm.Aggregations, AggLTTB) {
			series := computeapi1.NewSeriesFromNumeric(computeapi1.NewNumericSeriesFromChannel(channelSeries))
			strategy := computeapi.NewSummarizationStrategyFromLargestTriangleThreeBuckets(computeapi.LttbStrategy{MaxPointsPerGroup: qm.Points.Count})
			arrowFormat := computeapi.New_OutputFormat(computeapi.OutputFormat_ARROW_V3)
			return computeapi1.SummarizeSeries{Input: series, SummarizationStrategy: &strategy, OutputFormat: &arrowFormat}, timeRange
		}
		numericTimeShiftSeries := computeapi1.NumericTimeShiftSeries{
			Input:    computeapi1.NewNumericSeriesFromChannel(channelSeries),
			Duration: zeroDurationConstant(),
		}
		numericSeries := computeapi1.NewNumericSeriesFromTimeShift(numericTimeShiftSeries)
		buckets := qm.Points.Count
		requestRange, aligned := alignedBucketRange(timeRange, buckets)
		if aligned {
			// The aligned range is wider; keep only panel-range data.
			from, to := computeapi.NewTimestampConstantFromLiteral(timestampFromTime(timeRange.From)), computeapi.NewTimestampConstantFromLiteral(timestampFromTime(timeRange.To))
			numericSeries = computeapi1.NewNumericSeriesFromTimeRangeFilter(computeapi1.NumericTimeRangeFilterSeries{Input: numericSeries, StartTime: &from, EndTime: &to})
		}
		series := computeapi1.NewSeriesFromNumeric(numericSeries)

		arrowFormat := computeapi.New_OutputFormat(computeapi.OutputFormat_ARROW_V3)
		outputFields := numericOutputFields(qm.Aggregations)
		return computeapi1.SummarizeSeries{
			Input:               series,
			Buckets:             &buckets,
			OutputFormat:        &arrowFormat,
			NumericOutputFields: &outputFields,
		}, requestRange
	}
}

// buildAssetChannel constructs the asset-bound AssetChannel shared by every channel kind.
// The asset RID is bound by variable name (see assetRidVariableName); its value is supplied in buildComputeContext.
func (e *NominalQueryExecution) buildAssetChannel(channel, dataScopeName string) computeapi.AssetChannel {
	return computeapi.AssetChannel{
		AssetRid:       computeapi.NewStringConstantFromVariable(assetRidVariableName),
		Channel:        computeapi.NewStringConstantFromLiteral(channel),
		DataScopeName:  computeapi.NewStringConstantFromLiteral(dataScopeName),
		AdditionalTags: map[string]computeapi.StringConstant{},
		TagsToGroupBy:  []string{},
		GroupByTags:    []computeapi.StringConstant{},
	}
}

// buildComputeContext creates the context with variables for the compute request.
func (e *NominalQueryExecution) buildComputeContext(qm NominalQueryModel) computeapi1.Context {
	variables := map[computeapi.VariableName]computeapi1.VariableValue{
		assetRidVariableName: computeapi1.NewVariableValueFromString(qm.AssetRid),
	}

	if qm.TemplateVariables != nil {
		for key, value := range qm.TemplateVariables {
			if strValue, ok := value.(string); ok {
				variables[computeapi.VariableName(key)] = computeapi1.NewVariableValueFromString(strValue)
			}
		}
	}

	return computeapi1.Context{
		Variables:         variables,
		FunctionVariables: nil,
	}
}

const defaultPointCount = 1000

// maxPointsPerQuery is the Nominal API's per-request point limit.
const maxPointsPerQuery = 10000

type pointBudget struct {
	Count int
	// Notice is set when maxDataPoints was capped.
	Notice string
}

// resolvePointBudget caps maxDataPoints, a ceiling, at the API limit with a
// notice, and rejects saved buckets above it. Log queries get no budget.
func resolvePointBudget(qm NominalQueryModel, maxDataPoints int64) (pointBudget, error) {
	if qm.ChannelDataType == ChannelDataTypeLog {
		return pointBudget{}, nil
	}
	if qm.Buckets < 0 {
		return pointBudget{}, fmt.Errorf("buckets must be non-negative, got %d", qm.Buckets)
	}
	if qm.Buckets > 0 {
		count := qm.Buckets
		if maxDataPoints > 0 && maxDataPoints < int64(count) {
			count = int(maxDataPoints)
		}
		if count > maxPointsPerQuery {
			return pointBudget{}, fmt.Errorf("buckets must be at most %d, got %d", maxPointsPerQuery, qm.Buckets)
		}
		return pointBudget{Count: count}, nil
	}
	if maxDataPoints > maxPointsPerQuery {
		return pointBudget{
			Count:  maxPointsPerQuery,
			Notice: fmt.Sprintf("Max data points %d is above the Nominal limit, so %d points were requested", maxDataPoints, maxPointsPerQuery),
		}, nil
	}
	if maxDataPoints > 0 {
		return pointBudget{Count: int(maxDataPoints)}, nil
	}
	return pointBudget{Count: defaultPointCount}, nil
}

func numericOutputFields(aggregations []string) []computeapi.NumericOutputField {
	var outputFields []computeapi.NumericOutputField
	for _, agg := range aggregations {
		outputFields = append(outputFields, computeapi.New_NumericOutputField(
			computeapi.NumericOutputField_Value(agg),
		))
	}
	return outputFields
}

func zeroDurationConstant() computeapi1.DurationConstant {
	return computeapi1.NewDurationConstantFromLiteral(runapi.Duration{
		Seconds: safelong.SafeLong(0),
		Nanos:   safelong.SafeLong(0),
	})
}

func timestampFromTime(value time.Time) api.Timestamp {
	return api.Timestamp{
		Seconds: safelong.SafeLong(value.Unix()),
		Nanos:   safelong.SafeLong(value.Nanosecond()),
	}
}
