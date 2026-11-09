package plugin

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	"github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/codecs"
)

// lttbBenchPoints returns deterministic irregular nanosecond timestamps and
// random-walk doubles.
func lttbBenchPoints(rows int) ([]int64, []float64) {
	rng := rand.New(rand.NewSource(1))
	ts := make([]int64, rows)
	vals := make([]float64, rows)
	t, v := int64(1730000000_123456789), 20.0
	for i := range ts {
		t += 1_000_000 + rng.Int63n(9_000_000)
		v += rng.NormFloat64() * 0.37
		ts[i], vals[i] = t, v
	}
	return ts, vals
}

var lttbBenchSink TransformResult

func BenchmarkLTTBResponse(b *testing.B) {
	suppressBenchmarkLogs()
	exec := newTestQueryExecution(withCatalog(&Datasource{}), nil)
	qm := NominalQueryModel{Aggregations: []string{AggLTTB}}
	for _, rows := range []int{1000, 10000} {
		ts, vals := lttbBenchPoints(rows)
		body := lttbBenchBody(b, ts, vals)
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var resp computeapi.ComputeNodeResponse
				if err := codecs.JSON.Decode(bytes.NewReader(body), &resp); err != nil {
					b.Fatal(err)
				}
				res, err := exec.transformNominalResponseFromClient(resp, qm)
				if err != nil {
					b.Fatal(err)
				}
				lttbBenchSink = res
			}
			b.ReportMetric(float64(len(body)), "wire-B/op")
		})
	}
}

// lttbBenchBody builds the zstd-compressed Arrow IPC response the compute API
// returns for LTTB.
func lttbBenchBody(tb testing.TB, ts []int64, vals []float64) []byte {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rb.Field(0).(*array.Int64Builder).AppendValues(ts, nil)
	rb.Field(1).(*array.Float64Builder).AppendValues(vals, nil)
	rec := rb.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithZstd())
	if err := w.Write(rec); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	// ArrowPlot.arrowBinary is base64 on the wire; build the wire JSON directly.
	body := fmt.Sprintf(`{"type":"arrowNumeric","arrowNumeric":{"arrowBinary":%q}}`, base64.StdEncoding.EncodeToString(buf.Bytes()))
	return []byte(body)
}
