package plugin

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	"github.com/palantir/pkg/safelong"
)

// transformBatchResult converts a single batch result to a Grafana DataResponse.
func (e *NominalQueryExecution) transformBatchResult(result computeapi.ComputeWithUnitsResult, qm NominalQueryModel) (response backend.DataResponse) {
	// Batch execution runs in its own goroutines, outside the SDK gRPC panic
	// recovery. A malformed result must fail only its own query.
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Error("Recovered panic while transforming query result",
				"channel", qm.Channel,
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()),
			)
			response = backend.ErrDataResponse(
				backend.StatusInternal,
				"Internal error while processing query result",
			)
		}
	}()

	err := result.ComputeResult.AcceptFuncs(
		func(computeResponse computeapi.ComputeNodeResponse) error {
			result, transformErr := e.transformNominalResponseFromClient(computeResponse, qm)
			if transformErr != nil {
				response = backend.ErrDataResponse(
					backend.StatusInternal,
					fmt.Sprintf("Transform failed: %v", transformErr),
				)
				return nil
			}

			if result.IsLog {
				// Newest first. Grafana's infinite scroll derives the next time range
				// from the boundary row, and the compute API's PageInfo fixes selection
				// direction (sign of PageSize), not response order, so this sort is
				// not redundant.
				if !slices.IsSortedFunc(result.LogEntries, compareLogEntriesNewestFirst) {
					slices.SortStableFunc(result.LogEntries, compareLogEntriesNewestFirst)
				}

				frame := data.NewFrame(qm.Channel)
				frame.Meta = &data.FrameMeta{
					Type: data.FrameTypeLogLines,
					// The log-lines dataplane contract is at v0.0, not time-series-wide's 0.1.
					TypeVersion:            data.FrameTypeVersion{0, 0},
					PreferredVisualization: data.VisTypeLogs,
				}

				if len(result.LogEntries) > 0 {
					times := make([]time.Time, len(result.LogEntries))
					bodies := make([]string, len(result.LogEntries))
					ids := make([]string, len(result.LogEntries))
					labels := make([]json.RawMessage, len(result.LogEntries))
					for i, e := range result.LogEntries {
						times[i] = e.Time
						bodies[i] = e.Body
						ids[i] = e.ID
						labels[i] = e.Labels
					}
					frame.Fields = append(frame.Fields,
						data.NewField("timestamp", nil, times),
						data.NewField("body", nil, bodies),
						data.NewField("id", nil, ids),
						data.NewField("labels", nil, labels),
					)
				} else {
					frame.Fields = append(frame.Fields,
						data.NewField("timestamp", nil, []time.Time{}),
						data.NewField("body", nil, []string{}),
						data.NewField("id", nil, []string{}),
						data.NewField("labels", nil, []json.RawMessage{}),
					)
				}

				log.DefaultLogger.Debug("Successfully processed log query",
					"entries", len(result.LogEntries))
				response.Frames = append(response.Frames, frame)
			} else if len(result.AggSeries) > 0 {
				// Multi-aggregation Arrow path: one frame per series
				for _, agg := range result.AggSeries {
					frame := data.NewFrame("response")
					displayName := qm.Channel
					if qm.ExplicitAggregations {
						displayName = fmt.Sprintf("%s (%s)", qm.Channel, agg.Name)
					}
					frame.Name = displayName
					if len(agg.TimePoints) > 0 && len(agg.Values) > 0 {
						valueField := data.NewField("value", nil, agg.Values)
						valueField.Config = fieldConfigForNumeric(&qm, displayName, agg.CarriesChannelUnit)
						frame.Fields = append(frame.Fields,
							data.NewField("time", nil, agg.TimePoints),
							valueField,
						)
					} else {
						valueField := data.NewField("value", nil, []*float64{})
						valueField.Config = fieldConfigForNumeric(&qm, displayName, agg.CarriesChannelUnit)
						frame.Fields = append(frame.Fields,
							data.NewField("time", nil, []time.Time{}),
							valueField,
						)
					}
					response.Frames = append(response.Frames, frame)
				}
				dataPoints := 0
				if len(result.AggSeries) > 0 {
					dataPoints = len(result.AggSeries[0].TimePoints)
				}
				log.DefaultLogger.Debug("Successfully processed multi-agg query",
					"series", len(result.AggSeries),
					"dataPoints", dataPoints)
			} else if result.IsEnum {
				frame := data.NewFrame("response")
				frame.Name = qm.Channel
				// Mark enum frames as table type so panels like Stat can pick up string fields.
				// Time series frames filter to numeric fields only by default.
				frame.Meta = &data.FrameMeta{
					Type:                   data.FrameTypeTable,
					PreferredVisualization: data.VisTypeTable,
				}
				if len(result.TimePoints) > 0 && len(result.StringValues) > 0 {
					valueField := data.NewField("value", nil, result.StringValues)
					valueField.Config = fieldConfigForEnum(&qm)
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, result.TimePoints),
						valueField,
					)
				} else {
					valueField := data.NewField("value", nil, []string{})
					valueField.Config = fieldConfigForEnum(&qm)
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, []time.Time{}),
						valueField,
					)
				}
				log.DefaultLogger.Debug("Successfully processed enum query", "dataPoints", len(result.TimePoints))
				response.Frames = append(response.Frames, frame)
			} else {
				// Legacy numeric path (BucketedNumericPlot, NumericPlot)
				frame := data.NewFrame("response")
				frame.Name = qm.Channel
				if len(result.TimePoints) > 0 && len(result.NumericValues) > 0 {
					valueField := data.NewField("value", nil, result.NumericValues)
					valueField.Config = fieldConfigForNumericWithChannelUnit(&qm, qm.Channel)
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, result.TimePoints),
						valueField,
					)
				} else {
					valueField := data.NewField("value", nil, []*float64{})
					valueField.Config = fieldConfigForNumericWithChannelUnit(&qm, qm.Channel)
					frame.Fields = append(frame.Fields,
						data.NewField("time", nil, []time.Time{}),
						valueField,
					)
				}
				log.DefaultLogger.Debug("Successfully processed query", "dataPoints", len(result.TimePoints))
				response.Frames = append(response.Frames, frame)
			}

			return nil
		},
		func(errorResult computeapi.ErrorResult) error {
			errMsg := fmt.Sprintf("Compute error: %v (code: %v) [channel=%s, dataType=%s]", errorResult.ErrorType, errorResult.Code, qm.Channel, qm.ChannelDataType)

			// The query's stored type and the channel's actual type disagree.
			// Re-selecting the channel in the editor refreshes the stored type.
			errType := string(errorResult.ErrorType)
			if strings.Contains(errType, "ChannelHasWrongType") {
				errMsg += ". Hint: The channel type in this query doesn't match what the API expects. " +
					"Re-select the channel from the dropdown to update its type."
			}

			response = backend.ErrDataResponse(
				backend.StatusInternal,
				errMsg,
			)
			return nil
		},
		func(typeName string) error {
			response = backend.ErrDataResponse(
				backend.StatusInternal,
				fmt.Sprintf("Unknown result type: %s", typeName),
			)
			return nil
		},
	)

	if err != nil {
		return backend.ErrDataResponse(
			backend.StatusInternal,
			fmt.Sprintf("Failed to process result: %v", err),
		)
	}

	return response
}

type TransformResult struct {
	// Numeric aggregation series (Arrow bucketed path, one entry per requested field)
	AggSeries []AggregationSeries

	// Legacy numeric path (non-Arrow), single series only
	TimePoints    []time.Time
	NumericValues []*float64

	// Enum path
	StringValues []string
	IsEnum       bool

	// Log path
	IsLog      bool
	LogEntries []LogEntry
}

func unsupportedComputeResponse[T any](typeName string) func(T) error {
	return func(T) error {
		return unsupportedComputeResponseError(typeName)
	}
}

func unsupportedComputeResponseError(typeName string) error {
	return fmt.Errorf("compute response type %q is not supported by the plugin", typeName)
}

// Variable so a test can inject a panic into the result transform.
var decodeArrowBucketedNumeric = extractArrowBucketedNumericSeries

// transformNominalResponseFromClient converts a compute response into a
// TransformResult. qm names the aggregation columns the Arrow bucketed arm reads.
func (e *NominalQueryExecution) transformNominalResponseFromClient(response computeapi.ComputeNodeResponse, qm NominalQueryModel) (TransformResult, error) {
	log.DefaultLogger.Debug("Transforming conjure client response")

	var result TransformResult

	// AcceptFuncs invokes a selected nil handler, so every arm needs one.
	visitErr := response.AcceptFuncs(
		unsupportedComputeResponse[[]computeapi.Range]("range"),
		unsupportedComputeResponse[computeapi.RangesSummary]("rangesSummary"),
		unsupportedComputeResponse[*computeapi.Range]("rangeValue"),
		func(numeric computeapi.NumericPlot) error {
			timePoints, values, err := e.extractNumericDataFromConjure(numeric)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.NumericValues = values
			result.IsEnum = false
			return nil
		},
		func(bucketed computeapi.BucketedNumericPlot) error {
			timePoints, values, err := e.extractBucketedDataFromConjure(bucketed)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.NumericValues = values
			result.IsEnum = false
			return nil
		},
		unsupportedComputeResponse[*computeapi.NumericPoint]("numericPoint"),
		unsupportedComputeResponse[*computeapi.SinglePoint]("singlePoint"),
		unsupportedComputeResponse[computeapi.ArrowNumericPlot]("arrowNumeric"),
		// arrowBucketedNumericFunc: one AggregationSeries per requested aggregation field.
		func(arrowBucketed computeapi.ArrowBucketedNumericPlot) error {
			var specs []aggColumnSpec
			for _, agg := range qm.Aggregations {
				specs = append(specs, aggColumnSpecFromEnum(agg))
			}
			if len(specs) == 0 {
				return fmt.Errorf("no aggregation fields requested for ArrowBucketedNumericPlot response")
			}
			series, err := decodeArrowBucketedNumeric(arrowBucketed, specs)
			if err != nil {
				return err
			}
			result.AggSeries = series
			result.IsEnum = false
			return nil
		},
		// enumFunc - maps integer indices to category strings
		func(enum computeapi.EnumPlot) error {
			timePoints, values, err := e.extractEnumDataFromConjure(enum)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.StringValues = values
			result.IsEnum = true
			return nil
		},
		// enumPointFunc - single-point enum response (value is already a resolved string)
		func(ep *computeapi.EnumPoint) error {
			if ep == nil {
				result.IsEnum = true
				return nil
			}
			seconds := int64(ep.Timestamp.Seconds)
			nanos := int64(ep.Timestamp.Nanos)
			result.TimePoints = []time.Time{time.Unix(seconds, nanos)}
			result.StringValues = []string{ep.Value}
			result.IsEnum = true
			return nil
		},
		// bucketedEnumFunc - bucketed enum response (returned by SummarizeSeries with buckets)
		func(bucketed computeapi.BucketedEnumPlot) error {
			timePoints, values, err := e.extractBucketedEnumDataFromConjure(bucketed)
			if err != nil {
				return err
			}
			result.TimePoints = timePoints
			result.StringValues = values
			result.IsEnum = true
			return nil
		},
		unsupportedComputeResponse[computeapi.ArrowEnumPlot]("arrowEnum"),
		unsupportedComputeResponse[computeapi.ArrowBucketedEnumPlot]("arrowBucketedEnum"),
		func(paged computeapi.PagedLogPlot) error {
			n := min(len(paged.Timestamps), len(paged.Values))
			if len(paged.Timestamps) != len(paged.Values) {
				log.DefaultLogger.Warn("Paged log response has mismatched timestamp and value counts; truncating",
					"timestamps", len(paged.Timestamps), "values", len(paged.Values))
			}
			result.LogEntries = make([]LogEntry, 0, n)
			labelEncoder := newLogLabelEncoder(qm.Channel)
			for i := 0; i < n; i++ {
				ts := paged.Timestamps[i]
				val := paged.Values[i]

				result.LogEntries = append(result.LogEntries, LogEntry{
					Time:   time.Unix(int64(ts.Seconds), int64(ts.Nanos)),
					Body:   val.Message,
					ID:     val.Id.String(),
					Labels: labelEncoder.encode(val.Args),
				})
			}
			result.IsLog = true
			log.DefaultLogger.Debug("Extracted paged log data",
				"entries", len(result.LogEntries))
			return nil
		},
		func(lp *computeapi.LogPoint) error {
			if lp == nil {
				result.IsLog = true
				return nil
			}
			result.LogEntries = []LogEntry{{
				Time:   time.Unix(int64(lp.Timestamp.Seconds), int64(lp.Timestamp.Nanos)),
				Body:   lp.Value.Message,
				ID:     lp.Value.Id.String(),
				Labels: marshalLogArgs(lp.Value.Args, qm.Channel),
			}}
			result.IsLog = true
			return nil
		},
		unsupportedComputeResponse[computeapi.CartesianPlot]("cartesian"),
		unsupportedComputeResponse[computeapi.BucketedCartesianPlot]("bucketedCartesian"),
		unsupportedComputeResponse[computeapi.BucketedCartesian3dPlot]("bucketedCartesian3d"),
		unsupportedComputeResponse[computeapi.FrequencyDomainPlot]("frequencyDomain"),
		unsupportedComputeResponse[computeapi.FrequencyDomainPlotV2]("frequencyDomainV2"),
		unsupportedComputeResponse[computeapi.BucketedFrequencyDomainPlot]("bucketedFrequencyDomain"),
		unsupportedComputeResponse[computeapi.NumericHistogramPlot]("numericHistogram"),
		unsupportedComputeResponse[computeapi.EnumHistogramPlot]("enumHistogram"),
		unsupportedComputeResponse[computeapi.CurveFitResult]("curveFit"),
		unsupportedComputeResponse[computeapi.GroupedComputeNodeResponses]("grouped"),
		unsupportedComputeResponse[computeapi.ArrowArrayPlot]("array"),
		unsupportedComputeResponse[computeapi.ArrowBucketedStructPlot]("bucketedStruct"),
		unsupportedComputeResponse[computeapi.ArrowFullResolutionPlot]("fullResolution"),
		unsupportedComputeResponse[computeapi.ArrowBucketedMultivariatePlot]("arrowBucketedMultivariate"),
		unsupportedComputeResponse[computeapi.BucketedMultivariatePlot]("multivariate"),
		unsupportedComputeResponseError,
	)

	if visitErr != nil {
		return TransformResult{}, fmt.Errorf("failed to process response: %w", visitErr)
	}

	return result, nil
}

func (e *NominalQueryExecution) extractNumericDataFromConjure(numeric computeapi.NumericPlot) ([]time.Time, []*float64, error) {
	var timePoints []time.Time
	var values []*float64

	for i := 0; i < len(numeric.Timestamps) && i < len(numeric.Values); i++ {
		timestamp := numeric.Timestamps[i]
		value := numeric.Values[i]

		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))
		values = append(values, &value)
	}

	log.DefaultLogger.Debug("Extracted numeric data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

func (e *NominalQueryExecution) extractBucketedDataFromConjure(bucketed computeapi.BucketedNumericPlot) ([]time.Time, []*float64, error) {
	var timePoints []time.Time
	var values []*float64

	for i := 0; i < len(bucketed.Timestamps) && i < len(bucketed.Buckets); i++ {
		timestamp := bucketed.Timestamps[i]
		bucket := bucketed.Buckets[i]

		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))

		mean := bucket.Mean
		values = append(values, &mean)
	}

	log.DefaultLogger.Debug("Extracted bucketed data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

// extractEnumDataFromConjure maps EnumPlot indices to category strings.
// Out-of-range indices become "unknown(N)".
func (e *NominalQueryExecution) extractEnumDataFromConjure(enumPlot computeapi.EnumPlot) ([]time.Time, []string, error) {
	n := min(len(enumPlot.Timestamps), len(enumPlot.Values))
	timePoints := make([]time.Time, 0, n)
	values := make([]string, 0, n)

	for i := 0; i < n; i++ {
		timestamp := enumPlot.Timestamps[i]
		index := enumPlot.Values[i]

		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))

		if index >= 0 && index < len(enumPlot.Categories) {
			values = append(values, enumPlot.Categories[index])
		} else {
			values = append(values, fmt.Sprintf("unknown(%d)", index))
			log.DefaultLogger.Warn("Enum index out of bounds",
				"index", index,
				"categoriesLen", len(enumPlot.Categories),
			)
		}
	}

	log.DefaultLogger.Debug("Extracted enum data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

// extractBucketedEnumDataFromConjure takes each bucket's histogram mode as its
// value, the categorical counterpart of the numeric path's mean.
func (e *NominalQueryExecution) extractBucketedEnumDataFromConjure(bucketed computeapi.BucketedEnumPlot) ([]time.Time, []string, error) {
	n := min(len(bucketed.Timestamps), len(bucketed.Buckets))
	timePoints := make([]time.Time, 0, n)
	values := make([]string, 0, n)

	for i := 0; i < n; i++ {
		timestamp := bucketed.Timestamps[i]
		bucket := bucketed.Buckets[i]

		seconds := int64(timestamp.Seconds)
		nanos := int64(timestamp.Nanos)
		timePoints = append(timePoints, time.Unix(seconds, nanos))

		// Break count ties by lowest category index so Go's random map order
		// cannot change the result. An empty histogram falls back to FirstPoint.
		modeIndex := bucket.FirstPoint.Value
		maxCount := safelong.SafeLong(0)
		for idx, count := range bucket.Histogram {
			if count > maxCount || (count == maxCount && count > 0 && idx < modeIndex) {
				maxCount = count
				modeIndex = idx
			}
		}

		if modeIndex >= 0 && modeIndex < len(bucketed.Categories) {
			values = append(values, bucketed.Categories[modeIndex])
		} else {
			values = append(values, fmt.Sprintf("unknown(%d)", modeIndex))
			log.DefaultLogger.Warn("Bucketed enum index out of bounds",
				"index", modeIndex,
				"categoriesLen", len(bucketed.Categories),
			)
		}
	}

	log.DefaultLogger.Debug("Extracted bucketed enum data from conjure", "timePoints", len(timePoints), "values", len(values))
	return timePoints, values, nil
}

// carriesChannelUnit is false for COUNT (dimensionless) and VARIANCE (unit²).
// Non-aggregated call sites use fieldConfigForNumericWithChannelUnit so the
// true is named at the call site.
func fieldConfigForNumeric(qm *NominalQueryModel, displayName string, carriesChannelUnit bool) *data.FieldConfig {
	cfg := &data.FieldConfig{DisplayNameFromDS: displayName}
	if !carriesChannelUnit {
		return cfg
	}
	cfg.Unit = mapToGrafanaUnit(qm.ChannelUnit)
	return cfg
}

// fieldConfigForNumericWithChannelUnit is for non-aggregated numeric frames,
// which always carry the channel unit.
func fieldConfigForNumericWithChannelUnit(qm *NominalQueryModel, displayName string) *data.FieldConfig {
	return fieldConfigForNumeric(qm, displayName, true)
}

func fieldConfigForEnum(qm *NominalQueryModel) *data.FieldConfig {
	return &data.FieldConfig{DisplayNameFromDS: qm.Channel}
}
