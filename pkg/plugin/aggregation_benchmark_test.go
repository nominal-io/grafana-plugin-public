package plugin

import (
	"strconv"
	"testing"
	"time"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

var benchmarkExtractArrowBucketedNumericSeriesSink []AggregationSeries

type benchmarkArrowNullPattern int

const (
	benchmarkArrowDense benchmarkArrowNullPattern = iota
	benchmarkArrowAllNull
	benchmarkArrowOnePercentNonNull
)

func benchmarkArrowNullPatternFunc(pattern benchmarkArrowNullPattern) testArrowMultiAggNullPattern {
	switch pattern {
	case benchmarkArrowDense:
		return nil
	case benchmarkArrowAllNull:
		return func(row int, column int) bool {
			return true
		}
	case benchmarkArrowOnePercentNonNull:
		return func(row int, column int) bool {
			return row%100 != 0
		}
	}
	return nil
}

func createBenchmarkArrowBucketedNumeric(
	b testing.TB,
	rows int,
	specs []aggColumnSpec,
	pattern benchmarkArrowNullPattern,
) []byte {
	b.Helper()

	timestamps := make([]int64, rows)
	for row := 0; row < rows; row++ {
		timestamps[row] = int64(row) * int64(time.Second)
	}
	columns := make(map[string][]float64, len(specs))
	for column := range specs {
		values := make([]float64, rows)
		for row := 0; row < rows; row++ {
			values[row] = float64(row) + float64(column)/10
		}
		columns[specs[column].ValueCol] = values
	}

	return createTestArrowMultiAggWithNullPattern(b, timestamps, columns, benchmarkArrowNullPatternFunc(pattern))
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

func BenchmarkExtractArrowBucketedNumericSeriesFirstLast(b *testing.B) {
	specs := []aggColumnSpec{
		aggColumnSpecFromEnum(AggFirstPoint),
		aggColumnSpecFromEnum(AggLastPoint),
	}

	for _, rows := range []int{1000, 10000} {
		rows := rows
		b.Run("rows_"+strconv.Itoa(rows)+"_aggs_2_dense", func(b *testing.B) {
			arrowBytes := createFirstLastArrow(b, rows, 0)
			arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				series, err := extractArrowBucketedNumericSeries(arrowPlot, specs)
				if err != nil {
					b.Fatalf("extract Arrow FIRST/LAST series: %v", err)
				}
				benchmarkExtractArrowBucketedNumericSeriesSink = series
			}
		})
		b.Run("rows_"+strconv.Itoa(rows)+"_aggs_2_sparse_timestamps", func(b *testing.B) {
			arrowBytes := createFirstLastArrow(b, rows, 10)
			arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				series, err := extractArrowBucketedNumericSeries(arrowPlot, specs)
				if err != nil {
					b.Fatalf("extract Arrow FIRST/LAST series: %v", err)
				}
				benchmarkExtractArrowBucketedNumericSeriesSink = series
			}
		})
	}
}
