package plugin

import (
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
func (e *NominalQueryExecution) buildComputeRequest(qm NominalQueryModel, timeRange backend.TimeRange, maxDataPoints int64) computeapi1.ComputeNodeRequest {
	seriesPlan := e.buildSeriesPlan(qm, maxDataPoints)
	node := computeapi1.NewComputableNodeFromSeries(seriesPlan)

	return computeapi1.ComputeNodeRequest{
		Start:   timestampFromTime(timeRange.From),
		End:     timestampFromTime(timeRange.To),
		Node:    node,
		Context: e.buildComputeContext(qm),
	}
}

// buildSeriesPlan builds the full summarized series for a channel kind: it owns both the
// series shape and its summarization strategy, so adding a new channel kind is a single
// case here rather than coordinated edits across separate series/summarization helpers.
func (e *NominalQueryExecution) buildSeriesPlan(qm NominalQueryModel, maxDataPoints int64) computeapi1.SummarizeSeries {
	channelSeries := computeapi.NewChannelSeriesFromAsset(e.buildAssetChannel(qm.Channel, qm.DataScopeName))

	switch qm.ChannelDataType {
	case ChannelDataTypeString:
		enumTimeShiftSeries := computeapi1.EnumTimeShiftSeries{
			Input:    computeapi1.NewEnumSeriesFromChannel(channelSeries),
			Duration: zeroDurationConstant(),
		}
		enumSeries := computeapi1.NewEnumSeriesFromTimeShift(enumTimeShiftSeries)
		series := computeapi1.NewSeriesFromEnum(enumSeries)

		buckets := effectiveBucketCount(qm, maxDataPoints)
		return computeapi1.SummarizeSeries{
			Input:   series,
			Buckets: &buckets,
		}

	case ChannelDataTypeLog:
		logSeries := computeapi1.NewLogSeriesFromChannel(channelSeries)
		series := computeapi1.NewSeriesFromLog(logSeries)

		pageInfo := computeapi.PageInfo{PageSize: logPageSize}
		pageStrategy := computeapi.NewPageStrategyFromPageInfo(pageInfo)
		summarizationStrategy := computeapi.NewSummarizationStrategyFromPage(pageStrategy)
		return computeapi1.SummarizeSeries{
			Input:                 series,
			SummarizationStrategy: &summarizationStrategy,
		}

	default:
		numericTimeShiftSeries := computeapi1.NumericTimeShiftSeries{
			Input:    computeapi1.NewNumericSeriesFromChannel(channelSeries),
			Duration: zeroDurationConstant(),
		}
		numericSeries := computeapi1.NewNumericSeriesFromTimeShift(numericTimeShiftSeries)
		series := computeapi1.NewSeriesFromNumeric(numericSeries)

		buckets := effectiveBucketCount(qm, maxDataPoints)
		arrowFormat := computeapi.New_OutputFormat(computeapi.OutputFormat_ARROW_V3)
		outputFields := numericOutputFields(qm.Aggregations)
		return computeapi1.SummarizeSeries{
			Input:               series,
			Buckets:             &buckets,
			OutputFormat:        &arrowFormat,
			NumericOutputFields: &outputFields,
		}
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

func effectiveBucketCount(qm NominalQueryModel, maxDataPoints int64) int {
	buckets := int(qm.Buckets)
	if maxDataPoints > 0 && (buckets <= 0 || int(maxDataPoints) < buckets) {
		buckets = int(maxDataPoints)
	}
	return buckets
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
		Picos:   nil,
	})
}

func timestampFromTime(value time.Time) api.Timestamp {
	return api.Timestamp{
		Seconds: safelong.SafeLong(value.Unix()),
		Nanos:   safelong.SafeLong(value.Nanosecond()),
		Picos:   nil,
	}
}
