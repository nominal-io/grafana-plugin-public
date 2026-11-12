package plugin

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	sdklog "github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	"github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/codecs"
)

var benchmarkSinkTransformResult TransformResult
var benchmarkSinkDataResponse backend.DataResponse
var benchmarkSinkLogLabels json.RawMessage

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

func BenchmarkMarshalLogArgsPopulated(b *testing.B) {
	args := benchmarkLogArgs(benchmarkLogArgsPopulated, 0)
	cases := []struct {
		name    string
		channel string
	}{
		{name: "standard_channel", channel: "app.logs"},
		{
			name:    "long_channel",
			channel: "organizations/nominal/production/telemetry/platforms/primary/agents/collector/services/journald/channels/application/system/runtime/logs",
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			warmup := marshalLogArgs(args, tc.channel)
			if !json.Valid(warmup) {
				b.Fatalf("marshal log args returned invalid JSON: %q", string(warmup))
			}
			var labels map[string]string
			if err := json.Unmarshal(warmup, &labels); err != nil {
				b.Fatalf("unmarshal log labels: %v", err)
			}
			if len(labels) != len(args)+1 || labels[nominalChannelLabel] != tc.channel {
				b.Fatalf("unexpected log labels: %v", labels)
			}
			for key, value := range args {
				if labels[key] != value {
					b.Fatalf("log label %q = %q, want %q", key, labels[key], value)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkSinkLogLabels = marshalLogArgs(args, tc.channel)
			}
		})
	}
}

func BenchmarkEnumResponse(b *testing.B) {
	suppressBenchmarkLogs()
	exec := newTestQueryExecution(withCatalog(&Datasource{}), nil)
	categories := []string{"idle", "ready", "running", "fault", "standby", "boot", "shutdown", "error", "warn", "ok"}
	for _, n := range []int{1000, 10000} {
		rows := make([]enumRow, n)
		for i := range rows {
			rows[i].ts = 1704067200_000000000 + int64(i)*1_000_000_000
			for j := 0; j < 4; j++ {
				rows[i].hist = append(rows[i].hist, enumCount{categories[(i+j)%len(categories)], uint64(1 + (i*7+j*3)%5)})
			}
		}
		arrowBytes := createTestArrowBucketedEnum(rows)
		// ArrowPlot.arrowBinary is base64 on the wire; build the wire JSON directly.
		body := []byte(fmt.Sprintf(`{"type":"arrowBucketedEnum","arrowBucketedEnum":{"arrowBinary":%q}}`, base64.StdEncoding.EncodeToString(arrowBytes)))
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var resp computeapi.ComputeNodeResponse
				if err := codecs.JSON.Decode(bytes.NewReader(body), &resp); err != nil {
					b.Fatal(err)
				}
				res, err := exec.transformNominalResponseFromClient(resp, NominalQueryModel{})
				if err != nil || len(res.StringValues) != n {
					b.Fatalf("got %d values, err %v; want %d", len(res.StringValues), err, n)
				}
				benchmarkSinkTransformResult = res
			}
			b.ReportMetric(float64(len(body)), "wire-B/op")
		})
	}
}

func BenchmarkPagedLogTransform(b *testing.B) {
	suppressBenchmarkLogs()
	exec := newTestQueryExecution(withCatalog(&Datasource{}), nil)
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
