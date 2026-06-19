package plugin

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

var benchmarkExtractArrowBucketedNumericSeriesSink []AggregationSeries

type benchmarkArrowNullPattern int

const (
	benchmarkArrowDense benchmarkArrowNullPattern = iota
	benchmarkArrowAllNull
	benchmarkArrowOnePercentNonNull
)

func appendBenchmarkFloat64Value(builder *array.Float64Builder, row, column int, pattern benchmarkArrowNullPattern) {
	switch pattern {
	case benchmarkArrowDense:
		builder.Append(float64(row) + float64(column)/10)
	case benchmarkArrowAllNull:
		builder.AppendNull()
	case benchmarkArrowOnePercentNonNull:
		if row%100 == 0 {
			builder.Append(float64(row) + float64(column)/10)
		} else {
			builder.AppendNull()
		}
	}
}

func createBenchmarkArrowBucketedNumeric(
	b testing.TB,
	rows int,
	specs []aggColumnSpec,
	pattern benchmarkArrowNullPattern,
) []byte {
	b.Helper()

	pool := memory.DefaultAllocator
	fields := []arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
	}
	for _, spec := range specs {
		fields = append(fields, arrow.Field{Name: spec.ValueCol, Type: arrow.PrimitiveTypes.Float64, Nullable: true})
	}
	schema := arrow.NewSchema(fields, nil)

	tsBuilder := array.NewInt64Builder(pool)
	for i := 0; i < rows; i++ {
		tsBuilder.Append(int64(i) * int64(time.Second))
	}
	tsArr := tsBuilder.NewArray()
	tsBuilder.Release()

	arrays := []arrow.Array{tsArr}
	valueArrays := make([]arrow.Array, 0, len(specs))
	for column := range specs {
		valueBuilder := array.NewFloat64Builder(pool)
		for row := 0; row < rows; row++ {
			appendBenchmarkFloat64Value(valueBuilder, row, column, pattern)
		}
		valueArr := valueBuilder.NewArray()
		valueBuilder.Release()
		arrays = append(arrays, valueArr)
		valueArrays = append(valueArrays, valueArr)
	}

	rec := array.NewRecord(schema, arrays, int64(rows))
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		b.Fatalf("write benchmark Arrow record: %v", err)
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("close benchmark Arrow writer: %v", err)
	}

	rec.Release()
	tsArr.Release()
	for _, valueArr := range valueArrays {
		valueArr.Release()
	}

	return buf.Bytes()
}

func BenchmarkExtractArrowBucketedNumericSeries(b *testing.B) {
	oneAgg := []aggColumnSpec{aggColumnSpecFromEnum(AggMean)}
	threeAggs := []aggColumnSpec{
		aggColumnSpecFromEnum(AggMean),
		aggColumnSpecFromEnum(AggMin),
		aggColumnSpecFromEnum(AggMax),
	}

	cases := []struct {
		name    string
		rows    int
		specs   []aggColumnSpec
		pattern benchmarkArrowNullPattern
	}{
		{name: "rows_1000_aggs_1_dense", rows: 1000, specs: oneAgg, pattern: benchmarkArrowDense},
		{name: "rows_1000_aggs_3_dense", rows: 1000, specs: threeAggs, pattern: benchmarkArrowDense},
		{name: "rows_10000_aggs_1_dense", rows: 10000, specs: oneAgg, pattern: benchmarkArrowDense},
		{name: "rows_10000_aggs_3_dense", rows: 10000, specs: threeAggs, pattern: benchmarkArrowDense},
		{name: "rows_10000_aggs_3_all_null", rows: 10000, specs: threeAggs, pattern: benchmarkArrowAllNull},
		{name: "rows_10000_aggs_3_one_percent_non_null", rows: 10000, specs: threeAggs, pattern: benchmarkArrowOnePercentNonNull},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			arrowBytes := createBenchmarkArrowBucketedNumeric(b, tc.rows, tc.specs, tc.pattern)
			arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				series, err := extractArrowBucketedNumericSeries(arrowPlot, tc.specs)
				if err != nil {
					b.Fatalf("extract Arrow bucketed numeric series: %v", err)
				}
				benchmarkExtractArrowBucketedNumericSeriesSink = series
			}
		})
	}
}
