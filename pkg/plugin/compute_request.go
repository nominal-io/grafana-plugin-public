package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	"github.com/palantir/pkg/safelong"
)

const logPageSize = -250

// buildComputeRequest constructs a ComputeNodeRequest from query model and time range.
func (e *NominalQueryExecution) buildComputeRequest(qm NominalQueryModel, timeRange backend.TimeRange, maxDataPoints int64) computeapi1.ComputeNodeRequest {
	startSeconds := timeRange.From.Unix()
	endSeconds := timeRange.To.Unix()

	series := e.buildSeries(qm)
	seriesNode := e.buildSummarizeSeries(qm, series, maxDataPoints)
	node := computeapi1.NewComputableNodeFromSeries(seriesNode)

	return computeapi1.ComputeNodeRequest{
		Start:   timestampFromUnix(startSeconds),
		End:     timestampFromUnix(endSeconds),
		Node:    node,
		Context: e.buildComputeContext(qm, startSeconds, endSeconds),
	}
}

func (e *NominalQueryExecution) buildSeries(qm NominalQueryModel) computeapi1.Series {
	switch qm.ChannelDataType {
	case "string":
		enumTimeShiftSeries := computeapi1.EnumTimeShiftSeries{
			Input:    e.buildEnumChannelSeries(qm.AssetRid, qm.Channel, qm.DataScopeName),
			Duration: zeroDurationConstant(),
		}
		enumSeries := computeapi1.NewEnumSeriesFromTimeShift(enumTimeShiftSeries)
		return computeapi1.NewSeriesFromEnum(enumSeries)
	case "log":
		channelSeries := computeapi.NewChannelSeriesFromAsset(
			e.buildAssetChannel(qm.AssetRid, qm.Channel, qm.DataScopeName),
		)
		logSeries := computeapi1.NewLogSeriesFromChannel(channelSeries)
		return computeapi1.NewSeriesFromLog(logSeries)
	default:
		numericTimeShiftSeries := computeapi1.NumericTimeShiftSeries{
			Input:    e.buildChannelSeries(qm.AssetRid, qm.Channel, qm.DataScopeName),
			Duration: zeroDurationConstant(),
		}
		numericSeries := computeapi1.NewNumericSeriesFromTimeShift(numericTimeShiftSeries)
		return computeapi1.NewSeriesFromNumeric(numericSeries)
	}
}

func (e *NominalQueryExecution) buildSummarizeSeries(qm NominalQueryModel, series computeapi1.Series, maxDataPoints int64) computeapi1.SummarizeSeries {
	if qm.ChannelDataType == "log" {
		pageInfo := computeapi.PageInfo{
			PageSize: logPageSize,
		}
		pageStrategy := computeapi.NewPageStrategyFromPageInfo(pageInfo)
		summarizationStrategy := computeapi.NewSummarizationStrategyFromPage(pageStrategy)
		return computeapi1.SummarizeSeries{
			Input:                 series,
			SummarizationStrategy: &summarizationStrategy,
		}
	}

	buckets := effectiveBucketCount(qm, maxDataPoints)
	if qm.ChannelDataType == "string" {
		return computeapi1.SummarizeSeries{
			Input:   series,
			Buckets: &buckets,
		}
	}

	arrowFormat := computeapi.New_OutputFormat(computeapi.OutputFormat_ARROW_V3)
	outputFields := numericOutputFields(qm.Aggregations)
	return computeapi1.SummarizeSeries{
		Input:               series,
		Buckets:             &buckets,
		OutputFormat:        &arrowFormat,
		NumericOutputFields: &outputFields,
	}
}

// buildAssetChannel constructs the shared AssetChannel used by numeric, enum, and log series builders.
func (e *NominalQueryExecution) buildAssetChannel(assetRid, channel, dataScopeName string) computeapi.AssetChannel {
	return computeapi.AssetChannel{
		AssetRid:       computeapi.NewStringConstantFromVariable(computeapi.VariableName("assetRid")),
		Channel:        computeapi.NewStringConstantFromLiteral(channel),
		DataScopeName:  computeapi.NewStringConstantFromLiteral(dataScopeName),
		AdditionalTags: map[string]computeapi.StringConstant{},
		TagsToGroupBy:  []string{},
		GroupByTags:    []computeapi.StringConstant{},
	}
}

// buildChannelSeries creates a numeric channel series for the given asset/channel.
func (e *NominalQueryExecution) buildChannelSeries(assetRid, channel, dataScopeName string) computeapi1.NumericSeries {
	channelSeries := computeapi.NewChannelSeriesFromAsset(e.buildAssetChannel(assetRid, channel, dataScopeName))
	return computeapi1.NewNumericSeriesFromChannel(channelSeries)
}

// buildEnumChannelSeries creates an enum channel series for the given asset/channel.
func (e *NominalQueryExecution) buildEnumChannelSeries(assetRid, channel, dataScopeName string) computeapi1.EnumSeries {
	channelSeries := computeapi.NewChannelSeriesFromAsset(e.buildAssetChannel(assetRid, channel, dataScopeName))
	return computeapi1.NewEnumSeriesFromChannel(channelSeries)
}

// buildComputeContext creates the context with variables for the compute request.
func (e *NominalQueryExecution) buildComputeContext(qm NominalQueryModel, startSeconds, endSeconds int64) computeapi1.Context {
	variables := map[computeapi.VariableName]computeapi1.VariableValue{
		computeapi.VariableName("assetRid"): computeapi1.NewVariableValueFromString(qm.AssetRid),
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

func timestampFromUnix(seconds int64) api.Timestamp {
	return api.Timestamp{
		Seconds: safelong.SafeLong(seconds),
		Nanos:   safelong.SafeLong(0),
		Picos:   nil,
	}
}
