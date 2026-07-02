package plugin

import (
	"fmt"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdklog "github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	"github.com/palantir/pkg/safelong"
)

var benchmarkSinkTransformResult TransformResult
var benchmarkSinkDataResponse backend.DataResponse

func suppressBenchmarkLogs() {
	sdklog.DefaultLogger = sdklog.NewNullLogger()
}

type benchmarkLogOrder int

const (
	benchmarkLogDescending benchmarkLogOrder = iota
	benchmarkLogAscending
)

type benchmarkLogArgsKind int

const (
	benchmarkLogArgsNil benchmarkLogArgsKind = iota
	benchmarkLogArgsPopulated
	benchmarkLogArgsWithNominalChannel
)

func benchmarkEnumPlot(rows int) computeapi.EnumPlot {
	timestamps := make([]api.Timestamp, rows)
	values := make([]int, rows)
	categories := []string{"idle", "ready", "running", "fault"}
	for i := 0; i < rows; i++ {
		timestamps[i] = testTimestamp(1704067200 + int64(i))
		values[i] = i % len(categories)
	}
	return computeapi.EnumPlot{
		Timestamps: timestamps,
		Values:     values,
		Categories: categories,
	}
}

func benchmarkBucketedEnumPlot(rows int) computeapi.BucketedEnumPlot {
	timestamps := make([]api.Timestamp, rows)
	buckets := make([]computeapi.EnumBucket, rows)
	categories := []string{"idle", "ready", "running", "fault"}
	for i := 0; i < rows; i++ {
		category := i % len(categories)
		timestamps[i] = testTimestamp(1704067200 + int64(i))
		buckets[i] = computeapi.EnumBucket{
			Histogram: map[int]safelong.SafeLong{
				category:                         safelong.SafeLong(5),
				(category + 1) % len(categories): safelong.SafeLong(1),
			},
			FirstPoint: computeapi.CompactEnumPoint{
				Timestamp: timestamps[i],
				Value:     category,
			},
		}
	}
	return computeapi.BucketedEnumPlot{
		Timestamps: timestamps,
		Buckets:    buckets,
		Categories: categories,
	}
}

func benchmarkLogArgs(kind benchmarkLogArgsKind, i int) map[string]string {
	switch kind {
	case benchmarkLogArgsNil:
		return nil
	case benchmarkLogArgsWithNominalChannel:
		return map[string]string{
			"host":              fmt.Sprintf("srv-%02d", i%16),
			nominalChannelLabel: "source-channel",
		}
	case benchmarkLogArgsPopulated:
		return map[string]string{
			"PRIORITY":          fmt.Sprintf("%d", i%8),
			"_HOSTNAME":         fmt.Sprintf("srv-%02d", i%16),
			"_SYSTEMD_UNIT":     "nominal-agent.service",
			"_PID":              fmt.Sprintf("%d", 1000+i),
			"MESSAGE_ID":        fmt.Sprintf("message-%04d", i),
			"SYSLOG_IDENTIFIER": "nominal-agent",
			"CODE_FILE":         "collector.go",
			"CODE_LINE":         fmt.Sprintf("%d", 100+i),
		}
	default:
		return map[string]string{}
	}
}

func benchmarkPagedLogResult(rows int, order benchmarkLogOrder, argsKind benchmarkLogArgsKind) computeapi.ComputeWithUnitsResult {
	timestamps := make([]api.Timestamp, rows)
	messages := make([]string, rows)
	args := make([]map[string]string, rows)
	base := int64(1704067200)
	for i := 0; i < rows; i++ {
		offset := int64(i)
		if order == benchmarkLogDescending {
			offset = int64(rows - i)
		}
		timestamps[i] = testTimestamp(base + offset)
		messages[i] = fmt.Sprintf("log entry %04d", i)
		args[i] = benchmarkLogArgs(argsKind, i)
	}
	return createMockPagedLogResult(messages, args, timestamps)
}

func BenchmarkExtractEnumDataFromConjure(b *testing.B) {
	suppressBenchmarkLogs()
	exec := newTestQueryExecution(&Datasource{}, nil)
	for _, rows := range []int{250, 1000, 10000} {
		plot := benchmarkEnumPlot(rows)
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			gotTimes, gotValues, err := exec.extractEnumDataFromConjure(plot)
			if err != nil {
				b.Fatalf("extract enum data: %v", err)
			}
			if len(gotTimes) != rows || len(gotValues) != rows {
				b.Fatalf("got %d times and %d values, want %d", len(gotTimes), len(gotValues), rows)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				gotTimes, gotValues, err = exec.extractEnumDataFromConjure(plot)
				if err != nil {
					b.Fatalf("extract enum data: %v", err)
				}
			}
			benchmarkSinkTransformResult = TransformResult{TimePoints: gotTimes, StringValues: gotValues, IsEnum: true}
		})
	}
}

func BenchmarkExtractBucketedEnumDataFromConjure(b *testing.B) {
	suppressBenchmarkLogs()
	exec := newTestQueryExecution(&Datasource{}, nil)
	for _, rows := range []int{250, 1000, 10000} {
		plot := benchmarkBucketedEnumPlot(rows)
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			gotTimes, gotValues, err := exec.extractBucketedEnumDataFromConjure(plot)
			if err != nil {
				b.Fatalf("extract bucketed enum data: %v", err)
			}
			if len(gotTimes) != rows || len(gotValues) != rows {
				b.Fatalf("got %d times and %d values, want %d", len(gotTimes), len(gotValues), rows)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				gotTimes, gotValues, err = exec.extractBucketedEnumDataFromConjure(plot)
				if err != nil {
					b.Fatalf("extract bucketed enum data: %v", err)
				}
			}
			benchmarkSinkTransformResult = TransformResult{TimePoints: gotTimes, StringValues: gotValues, IsEnum: true}
		})
	}
}

func BenchmarkPagedLogTransform(b *testing.B) {
	suppressBenchmarkLogs()
	exec := newTestQueryExecution(&Datasource{}, nil)
	qm := NominalQueryModel{
		Channel:         "app.logs",
		ChannelDataType: "log",
	}
	cases := []struct {
		name     string
		order    benchmarkLogOrder
		argsKind benchmarkLogArgsKind
	}{
		{name: "rows_250_nil_args_descending", order: benchmarkLogDescending, argsKind: benchmarkLogArgsNil},
		{name: "rows_250_nil_args_ascending", order: benchmarkLogAscending, argsKind: benchmarkLogArgsNil},
		{name: "rows_250_populated_args_descending", order: benchmarkLogDescending, argsKind: benchmarkLogArgsPopulated},
		{name: "rows_250_populated_args_ascending", order: benchmarkLogAscending, argsKind: benchmarkLogArgsPopulated},
		{name: "rows_250_preexisting_nominal_channel_descending", order: benchmarkLogDescending, argsKind: benchmarkLogArgsWithNominalChannel},
		{name: "rows_250_preexisting_nominal_channel_ascending", order: benchmarkLogAscending, argsKind: benchmarkLogArgsWithNominalChannel},
	}

	for _, tc := range cases {
		result := benchmarkPagedLogResult(250, tc.order, tc.argsKind)
		b.Run(tc.name, func(b *testing.B) {
			resp := exec.transformBatchResult(result, qm)
			if resp.Error != nil {
				b.Fatalf("transform batch result: %v", resp.Error)
			}
			if len(resp.Frames) != 1 {
				b.Fatalf("got %d frames, want 1", len(resp.Frames))
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkSinkDataResponse = exec.transformBatchResult(result, qm)
			}
		})
	}
}
