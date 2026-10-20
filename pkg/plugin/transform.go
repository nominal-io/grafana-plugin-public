package plugin

import (
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
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
				frame := logLinesFrame(qm.Channel, &result.LogLines)
				log.DefaultLogger.Debug("Successfully processed log query",
					"entries", result.LogLines.Len())
				response.Frames = append(response.Frames, frame)
			} else if len(result.AggSeries) > 0 {
				// Multi-aggregation Arrow path: one frame per series
				for _, agg := range result.AggSeries {
					capTimes(agg.TimePoints, qm.RangeEnd)
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
				log.DefaultLogger.Debug("Successfully processed multi-agg query",
					"series", len(result.AggSeries),
					"dataPoints", len(result.AggSeries[0].TimePoints))
			} else if result.IsEnum {
				capTimes(result.TimePoints, qm.RangeEnd)
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
				response = backend.ErrDataResponse(
					backend.StatusInternal,
					"Transform failed: compute response produced no series",
				)
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
	// Numeric series from an Arrow response, one entry per requested aggregation
	AggSeries []AggregationSeries

	// Enum path
	TimePoints   []time.Time
	StringValues []string
	IsEnum       bool

	// Log path
	IsLog    bool
	LogLines logLines
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
// TransformResult. qm names the aggregation columns the Arrow numeric arms read.
func (e *NominalQueryExecution) transformNominalResponseFromClient(response computeapi.ComputeNodeResponse, qm NominalQueryModel) (TransformResult, error) {
	log.DefaultLogger.Debug("Transforming conjure client response")

	var result TransformResult

	// Raw (LTTB) and bucketed Arrow responses differ only in which columns the specs name.
	arrowNumeric := func(plot computeapi.ArrowPlot) error {
		var specs []aggColumnSpec
		for _, agg := range qm.Aggregations {
			specs = append(specs, aggColumnSpecFromEnum(agg))
		}
		if len(specs) == 0 {
			return fmt.Errorf("no aggregation fields requested for Arrow numeric response")
		}
		series, err := decodeArrowBucketedNumeric(plot, specs)
		if err != nil {
			return err
		}
		result.AggSeries = series
		return nil
	}

	// AcceptFuncs invokes a selected nil handler, so every arm needs one.
	visitErr := response.AcceptFuncs(
		unsupportedComputeResponse[[]computeapi.Range]("range"),
		unsupportedComputeResponse[computeapi.RangesSummary]("rangesSummary"),
		unsupportedComputeResponse[*computeapi.Range]("rangeValue"),
		unsupportedComputeResponse[computeapi.NumericPlot]("numeric"),
		unsupportedComputeResponse[computeapi.BucketedNumericPlot]("bucketedNumeric"),
		unsupportedComputeResponse[*computeapi.NumericPoint]("numericPoint"),
		unsupportedComputeResponse[*computeapi.SinglePoint]("singlePoint"),
		arrowNumeric,
		arrowNumeric,
		unsupportedComputeResponse[computeapi.EnumPlot]("enum"),
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
		unsupportedComputeResponse[computeapi.BucketedEnumPlot]("bucketedEnum"),
		unsupportedComputeResponse[computeapi.ArrowPlot]("arrowEnum"),
		func(plot computeapi.ArrowPlot) error {
			timePoints, values, err := extractArrowBucketedEnum(plot)
			if err != nil {
				return err
			}
			result.TimePoints, result.StringValues, result.IsEnum = timePoints, values, true
			return nil
		},
		func(paged computeapi.PagedLogPlot) error {
			n := min(len(paged.Timestamps), len(paged.Values))
			if len(paged.Timestamps) != len(paged.Values) {
				log.DefaultLogger.Warn("Paged log response has mismatched timestamp and value counts; truncating",
					"timestamps", len(paged.Timestamps), "values", len(paged.Values))
			}
			result.LogLines = newLogLines(n)
			labelEncoder := newLogLabelEncoder(qm.Channel)
			// Newest first, so lines at one time keep the service's order and the last line is the
			// cursor for the page before.
			for i := n - 1; i >= 0; i-- {
				ts := paged.Timestamps[i]
				val := paged.Values[i]

				t := time.Unix(int64(ts.Seconds), int64(ts.Nanos))
				labels := labelEncoder.encode(val.Args)
				result.LogLines.add(t, val.Message, labels, &val.Id)
			}
			result.IsLog = true
			log.DefaultLogger.Debug("Extracted paged log data",
				"entries", result.LogLines.Len())
			return nil
		},
		func(lp *computeapi.LogPoint) error {
			if lp == nil {
				result.IsLog = true
				return nil
			}
			result.LogLines = newLogLines(1)
			t := time.Unix(int64(lp.Timestamp.Seconds), int64(lp.Timestamp.Nanos))
			labels := marshalLogArgs(lp.Value.Args, qm.Channel)
			result.LogLines.add(t, lp.Value.Message, labels, &lp.Value.Id)
			result.IsLog = true
			return nil
		},
		unsupportedComputeResponse[computeapi.ArrowPlot]("arrowLog"),
		unsupportedComputeResponse[computeapi.CartesianPlot]("cartesian"),
		unsupportedComputeResponse[computeapi.BucketedCartesianPlot]("bucketedCartesian"),
		unsupportedComputeResponse[computeapi.BucketedCartesian3dPlot]("bucketedCartesian3d"),
		unsupportedComputeResponse[computeapi.FrequencyDomainPlot]("frequencyDomain"),
		unsupportedComputeResponse[computeapi.FrequencyDomainPlotV2]("frequencyDomainV2"),
		unsupportedComputeResponse[computeapi.SpectrogramPlot]("spectrogram"),
		unsupportedComputeResponse[computeapi.BucketedFrequencyDomainPlot]("bucketedFrequencyDomain"),
		unsupportedComputeResponse[computeapi.NumericHistogramPlot]("numericHistogram"),
		unsupportedComputeResponse[computeapi.EnumHistogramPlot]("enumHistogram"),
		unsupportedComputeResponse[computeapi.CurveFitResult]("curveFit"),
		unsupportedComputeResponse[computeapi.CurveFitResultV2]("curveFitV2"),
		unsupportedComputeResponse[computeapi.GroupedComputeNodeResponses]("grouped"),
		unsupportedComputeResponse[computeapi.ArrowArrayPlot]("array"),
		unsupportedComputeResponse[computeapi.ArrowPlot]("bucketedStruct"),
		unsupportedComputeResponse[computeapi.ArrowPlot]("arrowStruct"),
		unsupportedComputeResponse[computeapi.ArrowPlot]("fullResolution"),
		unsupportedComputeResponse[computeapi.ArrowPlot]("arrowBucketedMultivariate"),
		unsupportedComputeResponse[computeapi.BucketedMultivariatePlot]("multivariate"),
		unsupportedComputeResponseError,
	)

	if visitErr != nil {
		return TransformResult{}, fmt.Errorf("failed to process response: %w", visitErr)
	}

	return result, nil
}

// extractArrowBucketedEnum takes each bucket's most frequent category as its
// value. Count ties go to the bucket's last value when it is among them, and
// otherwise to the category seen first in the response.
func extractArrowBucketedEnum(plot computeapi.ArrowPlot) ([]time.Time, []string, error) {
	var tsIdx, histIdx, lastIdx []int
	open := func(schema *arrow.Schema) error {
		tsIdx, histIdx, lastIdx = schema.FieldIndices("end_bucket_timestamp"), schema.FieldIndices("histogram"), schema.FieldIndices("last_value")
		if len(tsIdx) == 0 || len(histIdx) == 0 || len(lastIdx) == 0 {
			return fmt.Errorf("enum Arrow schema missing end_bucket_timestamp, histogram or last_value: have %v", schema.Fields())
		}
		return nil
	}

	times, values := []time.Time{}, []string{}
	// Category names are cloned once so values do not alias Arrow buffers.
	rank := map[string]int{}
	var names []string
	each := func(rec arrow.RecordBatch) error {
		ts, tsOK := rec.Column(tsIdx[0]).(*array.Int64)
		hist, histOK := rec.Column(histIdx[0]).(*array.Map)
		last, lastOK := rec.Column(lastIdx[0]).(*array.String)
		if !tsOK || !histOK || !lastOK {
			return fmt.Errorf("unexpected Arrow enum column types %T, %T, %T", rec.Column(tsIdx[0]), rec.Column(histIdx[0]), rec.Column(lastIdx[0]))
		}
		keys, keysOK := hist.Keys().(*array.String)
		counts, countsOK := hist.Items().(*array.Uint64)
		if !keysOK || !countsOK {
			return fmt.Errorf("unexpected Arrow histogram types %T, %T", hist.Keys(), hist.Items())
		}
		for i := 0; i < ts.Len(); i++ {
			start, end := hist.ValueOffsets(i)
			mode, best := -1, uint64(0)
			lastRank, lastCount := -1, uint64(0)
			for j := int(start); j < int(end); j++ {
				key := keys.Value(j)
				r, ok := rank[key]
				if !ok {
					r = len(names)
					name := strings.Clone(key)
					rank[name] = r
					names = append(names, name)
				}
				c := counts.Value(j)
				if key == last.Value(i) {
					lastRank, lastCount = r, c
				}
				if mode < 0 || c > best || (c == best && r < mode) {
					mode, best = r, c
				}
			}
			if lastRank >= 0 && lastCount == best {
				mode = lastRank
			}
			if mode < 0 {
				return fmt.Errorf("enum Arrow bucket at %d has an empty histogram", ts.Value(i))
			}
			times = append(times, time.Unix(0, ts.Value(i)))
			values = append(values, names[mode])
		}
		return nil
	}
	if err := readArrowBatches(plot.ArrowBinary, open, each); err != nil {
		return nil, nil, err
	}
	return times, values, nil
}

// carriesChannelUnit is false for COUNT (dimensionless) and VARIANCE (unit²).
func fieldConfigForNumeric(qm *NominalQueryModel, displayName string, carriesChannelUnit bool) *data.FieldConfig {
	cfg := &data.FieldConfig{DisplayNameFromDS: displayName}
	if !carriesChannelUnit {
		return cfg
	}
	cfg.Unit = mapToGrafanaUnit(qm.ChannelUnit)
	return cfg
}

func fieldConfigForEnum(qm *NominalQueryModel) *data.FieldConfig {
	return &data.FieldConfig{DisplayNameFromDS: qm.Channel}
}

// capTimes caps times at end: a last bucket that runs past the range carries its
// own end time, though its data stops at end. Times ascend, so only the tail is scanned.
func capTimes(times []time.Time, end time.Time) {
	for i := len(times) - 1; i >= 0 && !end.IsZero() && times[i].After(end); i-- {
		times[i] = end
	}
}
