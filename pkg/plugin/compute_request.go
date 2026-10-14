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

const logPageSize = 1000

// assetRidVariableName is the compute-context variable that carries the asset RID.
// buildDataset binds the RID by this name; buildComputeContext supplies the value.
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
	// A bare select is grouped by ASSET_RID and DATA_SCOPE; selectTags([]) collapses the
	// result to a single series.
	selectSeries := computeapi1.SelectSeries{
		Name:    new(computeapi.NewStringConstantFromLiteral(qm.Channel)),
		Dataset: new(buildDataset(qm)),
	}
	noTagKeys := []computeapi.StringConstant{}

	switch qm.ChannelDataType {
	case ChannelDataTypeString:
		selected := computeapi1.NewEnumSeriesFromSelectTags(computeapi1.EnumSelectTagsSeries{
			Input:   computeapi1.NewEnumSeriesFromSelectEnum(selectSeries),
			TagKeys: noTagKeys,
		})
		enumTimeShiftSeries := computeapi1.EnumTimeShiftSeries{
			Input:    selected,
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
		logSeries := computeapi1.NewLogSeriesFromSelectTags(computeapi1.LogSelectTagsSeries{
			Input:   computeapi1.NewLogSeriesFromSelectLogs(selectSeries),
			TagKeys: noTagKeys,
		})
		series := computeapi1.NewSeriesFromLog(logSeries)

		pageRange, cursor := logPageRange(timeRange, qm)
		pageStrategy := computeapi.NewPageStrategyFromPageInfo(logPageInfo(qm.ScrollNewer, cursor))
		summarizationStrategy := computeapi.NewSummarizationStrategyFromPage(pageStrategy)
		return computeapi1.SummarizeSeries{
			Input:                 series,
			SummarizationStrategy: &summarizationStrategy,
		}, pageRange

	default:
		selected := computeapi1.NewNumericSeriesFromSelectTags(computeapi1.NumericSelectTagsSeries{
			Input:   computeapi1.NewNumericSeriesFromSelectNumeric(selectSeries),
			TagKeys: noTagKeys,
		})
		if slices.Contains(qm.Aggregations, AggLTTB) {
			series := computeapi1.NewSeriesFromNumeric(selected)
			strategy := computeapi.NewSummarizationStrategyFromLargestTriangleThreeBuckets(computeapi.LttbStrategy{MaxPointsPerGroup: qm.Points.Count})
			arrowFormat := computeapi.New_OutputFormat(computeapi.OutputFormat_ARROW_V3)
			return computeapi1.SummarizeSeries{Input: series, SummarizationStrategy: &strategy, OutputFormat: &arrowFormat}, timeRange
		}
		numericTimeShiftSeries := computeapi1.NumericTimeShiftSeries{
			Input:    selected,
			Duration: zeroDurationConstant(),
		}
		numericSeries := computeapi1.NewNumericSeriesFromTimeShift(numericTimeShiftSeries)
		buckets := qm.Points.Count
		requestRange, aligned := alignedBucketRange(timeRange, buckets)
		if aligned && e.fromAlert {
			// The aligned range is wider. Alerts evaluate exactly their window; dashboards
			// keep whole edge buckets so edge values hold steady as the range moves.
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

// buildDataset selects the asset's series restricted to one data scope.
//
// The scope predicate belongs on the Dataset: outside selectTags([]) the server
// rejects it, and without it an asset with several scopes merges every scope into
// one series.
func buildDataset(qm NominalQueryModel) computeapi1.Dataset {
	asset := computeapi1.NewDatasetFromAsset(computeapi.Asset{
		Rid: computeapi.NewStringConstantFromVariable(assetRidVariableName),
	})
	scope := computeapi.NewTagPredicateFromIn(computeapi.TagIn{
		Key: computeapi.NewStringConstantFromLiteral(string(computeapi.ReservedTagKey_DATA_SCOPE)),
		Values: computeapi.NewStringSetConstantV2FromLiteral([]computeapi.StringConstant{
			computeapi.NewStringConstantFromLiteral(qm.DataScopeName),
		}),
	})
	return computeapi1.NewDatasetFromFilter(computeapi1.FilteredDataset{Input: asset, Predicate: scope})
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

// resolvePointBudget takes Grafana's maxDataPoints as the point count, capped at
// the API limit with a notice. Log queries get no budget.
func resolvePointBudget(channelDataType string, maxDataPoints int64) pointBudget {
	switch {
	case channelDataType == ChannelDataTypeLog:
		return pointBudget{}
	case maxDataPoints > maxPointsPerQuery:
		return pointBudget{
			Count:  maxPointsPerQuery,
			Notice: fmt.Sprintf("Max data points %d is above the Nominal limit, so %d points were requested", maxDataPoints, maxPointsPerQuery),
		}
	case maxDataPoints > 0:
		return pointBudget{Count: int(maxDataPoints)}
	default:
		return pointBudget{Count: defaultPointCount}
	}
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

// logPageInfo pages a log query back from the end of its range, or forward from its start on a scroll
// page that reads newer lines. A scroll page that continues from a line pages from that line.
func logPageInfo(newer bool, cursor *logCursor) computeapi.PageInfo {
	info := computeapi.PageInfo{PageSize: -logPageSize}
	if newer {
		info.PageSize = logPageSize
	}
	if cursor != nil && cursor.serviceID != nil {
		token := computeapi.NewPageTokenFromTimestampAndId(computeapi.TimestampAndId{Timestamp: timestampFromTime(cursor.at), Id: cursor.serviceID})
		info.PageToken = &token
	}
	return info
}
