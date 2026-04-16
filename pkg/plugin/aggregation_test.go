package plugin

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

// createTestArrowBucketedNumeric builds an Arrow IPC stream buffer for testing.
// Column order is intentionally reversed from production (timestamp first, mean second)
// to validate name-based column lookup.
func createTestArrowBucketedNumeric(timestamps []int64, means []float64, nullMask []bool) []byte {
	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "mean", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	tsBuilder := array.NewInt64Builder(pool)
	meanBuilder := array.NewFloat64Builder(pool)
	defer tsBuilder.Release()
	defer meanBuilder.Release()

	for i, ts := range timestamps {
		tsBuilder.Append(ts)
		if nullMask != nil && nullMask[i] {
			meanBuilder.AppendNull()
		} else {
			meanBuilder.Append(means[i])
		}
	}

	tsArr := tsBuilder.NewArray()
	meanArr := meanBuilder.NewArray()
	defer tsArr.Release()
	defer meanArr.Release()

	rec := array.NewRecord(schema, []arrow.Array{tsArr, meanArr}, int64(len(timestamps)))
	defer rec.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		panic(err)
	}
	writer.Close()
	return buf.Bytes()
}

func TestExtractArrowBucketedNumericSeries(t *testing.T) {
	t.Run("normal case with valid data", func(t *testing.T) {
		timestamps := []int64{1773975408000000000, 1773975414000000000, 1773975420000000000}
		means := []float64{0.71, -0.40, 0.53}
		arrowBytes := createTestArrowBucketedNumeric(timestamps, means, nil)

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
		series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "mean", ValueCol: "mean"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(series) != 1 {
			t.Fatalf("expected 1 series, got %d", len(series))
		}
		s := series[0]
		if len(s.TimePoints) != 3 {
			t.Fatalf("expected 3 time points, got %d", len(s.TimePoints))
		}
		if len(s.Values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(s.Values))
		}
		for i, ts := range timestamps {
			expected := time.Unix(0, ts)
			if !s.TimePoints[i].Equal(expected) {
				t.Errorf("timePoints[%d] = %v, want %v", i, s.TimePoints[i], expected)
			}
		}
		for i, m := range means {
			if s.Values[i] == nil || *s.Values[i] != m {
				t.Errorf("values[%d] = %v, want %f", i, s.Values[i], m)
			}
		}
	})

	t.Run("nullable means produce nil", func(t *testing.T) {
		timestamps := []int64{1000000000, 2000000000, 3000000000}
		means := []float64{1.5, 0, 3.5} // index 1 will be null
		nullMask := []bool{false, true, false}
		arrowBytes := createTestArrowBucketedNumeric(timestamps, means, nullMask)

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
		series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "mean", ValueCol: "mean"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		s := series[0]
		if len(s.Values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(s.Values))
		}
		if s.Values[0] == nil || *s.Values[0] != 1.5 {
			t.Errorf("values[0] = %v, want 1.5", s.Values[0])
		}
		if s.Values[1] != nil {
			t.Errorf("values[1] = %v, want nil", s.Values[1])
		}
		if s.Values[2] == nil || *s.Values[2] != 3.5 {
			t.Errorf("values[2] = %v, want 3.5", s.Values[2])
		}
		if len(s.TimePoints) != 3 {
			t.Fatalf("expected 3 time points, got %d", len(s.TimePoints))
		}
	})

	t.Run("empty response returns empty series", func(t *testing.T) {
		arrowBytes := createTestArrowBucketedNumeric(nil, nil, nil)

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
		series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "mean", ValueCol: "mean"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(series) != 1 {
			t.Fatalf("expected 1 series, got %d", len(series))
		}
		if len(series[0].TimePoints) != 0 {
			t.Errorf("expected 0 time points, got %d", len(series[0].TimePoints))
		}
		if len(series[0].Values) != 0 {
			t.Errorf("expected 0 values, got %d", len(series[0].Values))
		}
	})

	t.Run("single row", func(t *testing.T) {
		arrowBytes := createTestArrowBucketedNumeric(
			[]int64{1773975408000000000},
			[]float64{42.0},
			nil,
		)

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: arrowBytes}
		series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "mean", ValueCol: "mean"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		s := series[0]
		if len(s.TimePoints) != 1 || len(s.Values) != 1 {
			t.Fatalf("expected 1 row, got %d timePoints and %d values", len(s.TimePoints), len(s.Values))
		}
		if s.Values[0] == nil || *s.Values[0] != 42.0 {
			t.Errorf("values[0] = %v, want 42.0", s.Values[0])
		}
	})

	t.Run("missing requested column returns error", func(t *testing.T) {
		pool := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "not_mean", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, nil)
		tsBuilder := array.NewInt64Builder(pool)
		otherBuilder := array.NewFloat64Builder(pool)
		tsBuilder.Append(1000)
		otherBuilder.Append(1.0)
		tsArr := tsBuilder.NewArray()
		otherArr := otherBuilder.NewArray()
		defer tsBuilder.Release()
		defer otherBuilder.Release()
		defer tsArr.Release()
		defer otherArr.Release()

		rec := array.NewRecord(schema, []arrow.Array{tsArr, otherArr}, 1)
		defer rec.Release()

		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		writer.Write(rec)
		writer.Close()

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
		_, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "mean", ValueCol: "mean"}})
		if err == nil {
			t.Fatal("expected error for missing mean column, got nil")
		}
		if !strings.Contains(err.Error(), "missing requested column") {
			t.Errorf("error should mention missing column, got: %v", err)
		}
	})

	t.Run("wrong column type returns error", func(t *testing.T) {
		pool := memory.DefaultAllocator
		// mean as Int64 instead of Float64
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "mean", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		tsBuilder := array.NewInt64Builder(pool)
		meanBuilder := array.NewInt64Builder(pool)
		tsBuilder.Append(1000)
		meanBuilder.Append(42)
		tsArr := tsBuilder.NewArray()
		meanArr := meanBuilder.NewArray()
		defer tsBuilder.Release()
		defer meanBuilder.Release()
		defer tsArr.Release()
		defer meanArr.Release()

		rec := array.NewRecord(schema, []arrow.Array{tsArr, meanArr}, 1)
		defer rec.Release()

		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		writer.Write(rec)
		writer.Close()

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
		_, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "mean", ValueCol: "mean"}})
		if err == nil {
			t.Fatal("expected error for wrong column type, got nil")
		}
		if !strings.Contains(err.Error(), "unsupported column type for mean") {
			t.Errorf("error should mention unsupported column type, got: %v", err)
		}
	})

	t.Run("Uint32 count column is converted to float64", func(t *testing.T) {
		pool := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "count", Type: arrow.PrimitiveTypes.Uint32},
		}, nil)
		tsBuilder := array.NewInt64Builder(pool)
		countBuilder := array.NewUint32Builder(pool)
		tsBuilder.Append(1773975408000000000)
		tsBuilder.Append(1773975414000000000)
		countBuilder.Append(5)
		countBuilder.Append(12)
		tsArr := tsBuilder.NewArray()
		countArr := countBuilder.NewArray()
		defer tsBuilder.Release()
		defer countBuilder.Release()
		defer tsArr.Release()
		defer countArr.Release()

		rec := array.NewRecord(schema, []arrow.Array{tsArr, countArr}, 2)
		defer rec.Release()

		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		writer.Write(rec)
		writer.Close()

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
		series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "count", ValueCol: "count"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(series) != 1 {
			t.Fatalf("expected 1 series, got %d", len(series))
		}
		s := series[0]
		if len(s.Values) != 2 {
			t.Fatalf("expected 2 values, got %d", len(s.Values))
		}
		if *s.Values[0] != 5.0 {
			t.Errorf("values[0] = %v, want 5.0", *s.Values[0])
		}
		if *s.Values[1] != 12.0 {
			t.Errorf("values[1] = %v, want 12.0", *s.Values[1])
		}
	})

	t.Run("ZSTD compressed Arrow is handled transparently", func(t *testing.T) {
		pool := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "mean", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, nil)
		tsBuilder := array.NewInt64Builder(pool)
		meanBuilder := array.NewFloat64Builder(pool)
		defer tsBuilder.Release()
		defer meanBuilder.Release()

		for i := 0; i < 100; i++ {
			tsBuilder.Append(int64(i) * 1000000000)
			meanBuilder.Append(float64(i) * 0.1)
		}
		tsArr := tsBuilder.NewArray()
		meanArr := meanBuilder.NewArray()
		defer tsArr.Release()
		defer meanArr.Release()

		rec := array.NewRecord(schema, []arrow.Array{tsArr, meanArr}, 100)
		defer rec.Release()

		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithZstd())
		writer.Write(rec)
		writer.Close()

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
		series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "mean", ValueCol: "mean"}})
		if err != nil {
			t.Fatalf("unexpected error with ZSTD compressed Arrow: %v", err)
		}
		s := series[0]
		if len(s.TimePoints) != 100 || len(s.Values) != 100 {
			t.Fatalf("expected 100 rows, got %d timePoints and %d values", len(s.TimePoints), len(s.Values))
		}
		if s.Values[50] == nil || *s.Values[50] != 5.0 {
			t.Errorf("values[50] = %v, want 5.0", s.Values[50])
		}
	})

	t.Run("multi-batch stream is concatenated", func(t *testing.T) {
		pool := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "mean", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, nil)

		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))

		// Write two separate record batches to simulate server flush at 1 MB.
		for batch := 0; batch < 2; batch++ {
			tsBuilder := array.NewInt64Builder(pool)
			meanBuilder := array.NewFloat64Builder(pool)
			for i := 0; i < 3; i++ {
				tsBuilder.Append(int64(batch*3+i) * 1000000000)
				meanBuilder.Append(float64(batch*3+i) * 1.1)
			}
			tsArr := tsBuilder.NewArray()
			meanArr := meanBuilder.NewArray()
			rec := array.NewRecord(schema, []arrow.Array{tsArr, meanArr}, 3)
			if err := writer.Write(rec); err != nil {
				t.Fatalf("failed to write batch %d: %v", batch, err)
			}
			rec.Release()
			tsArr.Release()
			meanArr.Release()
			tsBuilder.Release()
			meanBuilder.Release()
		}
		writer.Close()

		arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
		series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{{Name: "mean", ValueCol: "mean"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		s := series[0]
		if len(s.TimePoints) != 6 {
			t.Fatalf("expected 6 timePoints from 2 batches, got %d", len(s.TimePoints))
		}
		if len(s.Values) != 6 {
			t.Fatalf("expected 6 values from 2 batches, got %d", len(s.Values))
		}
		// Verify data spans both batches: first batch [0,1,2], second batch [3,4,5]
		for i := 0; i < 6; i++ {
			wantTs := int64(i) * 1000000000
			gotTs := s.TimePoints[i].UnixNano()
			if gotTs != wantTs {
				t.Errorf("timePoints[%d] = %d ns, want %d ns", i, gotTs, wantTs)
			}
			wantVal := float64(i) * 1.1
			if s.Values[i] == nil || *s.Values[i] != wantVal {
				t.Errorf("values[%d] = %v, want %f", i, s.Values[i], wantVal)
			}
		}
	})
}

func TestTransformArrowFirstLastWithNulls(t *testing.T) {
	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "first_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "first_timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "last_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "last_timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	// 3 buckets: bucket 0 has data, bucket 1 is empty (null first/last), bucket 2 has data.
	endBucketTs := []int64{1000000000000, 2000000000000, 3000000000000}

	tsBuilder := array.NewInt64Builder(pool)
	for _, v := range endBucketTs {
		tsBuilder.Append(v)
	}
	tsArr := tsBuilder.NewArray()
	defer tsArr.Release()
	tsBuilder.Release()

	// first_value: 10.0, null, 30.0
	firstValBuilder := array.NewFloat64Builder(pool)
	firstValBuilder.Append(10.0)
	firstValBuilder.AppendNull()
	firstValBuilder.Append(30.0)
	firstValArr := firstValBuilder.NewArray()
	defer firstValArr.Release()
	firstValBuilder.Release()

	// first_timestamp: 900ns, null, 2900ns
	firstTsBuilder := array.NewInt64Builder(pool)
	firstTsBuilder.Append(900000000000)
	firstTsBuilder.AppendNull()
	firstTsBuilder.Append(2900000000000)
	firstTsArr := firstTsBuilder.NewArray()
	defer firstTsArr.Release()
	firstTsBuilder.Release()

	// last_value: null, 25.0, 35.0
	lastValBuilder := array.NewFloat64Builder(pool)
	lastValBuilder.AppendNull()
	lastValBuilder.Append(25.0)
	lastValBuilder.Append(35.0)
	lastValArr := lastValBuilder.NewArray()
	defer lastValArr.Release()
	lastValBuilder.Release()

	// last_timestamp: null, 1999ns, 2999ns
	lastTsBuilder := array.NewInt64Builder(pool)
	lastTsBuilder.AppendNull()
	lastTsBuilder.Append(1999000000000)
	lastTsBuilder.Append(2999000000000)
	lastTsArr := lastTsBuilder.NewArray()
	defer lastTsArr.Release()
	lastTsBuilder.Release()

	rec := array.NewRecord(schema, []arrow.Array{tsArr, firstValArr, firstTsArr, lastValArr, lastTsArr}, 3)
	defer rec.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		panic(err)
	}
	writer.Close()

	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
	series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{
		{Name: "first", ValueCol: "first_value", TimestampCol: "first_timestamp"},
		{Name: "last", ValueCol: "last_value", TimestampCol: "last_timestamp"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("expected 2 series, got %d", len(series))
	}

	first := series[0]
	last := series[1]

	// first: buckets 0 and 2 have data, bucket 1 has null timestamp and is dropped.
	if len(first.Values) != 2 {
		t.Fatalf("first: expected 2 values (null-ts row dropped), got %d", len(first.Values))
	}
	if len(first.TimePoints) != 2 {
		t.Fatalf("first: expected 2 timepoints, got %d", len(first.TimePoints))
	}
	if first.Values[0] == nil || *first.Values[0] != 10.0 {
		t.Errorf("first.Values[0] = %v, want 10.0", first.Values[0])
	}
	if first.Values[1] == nil || *first.Values[1] != 30.0 {
		t.Errorf("first.Values[1] = %v, want 30.0", first.Values[1])
	}
	if first.TimePoints[0] != time.Unix(0, 900000000000) {
		t.Errorf("first.TimePoints[0] = %v, want %v", first.TimePoints[0], time.Unix(0, 900000000000))
	}
	if first.TimePoints[1] != time.Unix(0, 2900000000000) {
		t.Errorf("first.TimePoints[1] = %v, want %v", first.TimePoints[1], time.Unix(0, 2900000000000))
	}

	// last: bucket 0 has null timestamp and is dropped, buckets 1 and 2 have data.
	if len(last.Values) != 2 {
		t.Fatalf("last: expected 2 values (null-ts row dropped), got %d", len(last.Values))
	}
	if len(last.TimePoints) != 2 {
		t.Fatalf("last: expected 2 timepoints, got %d", len(last.TimePoints))
	}
	if last.Values[0] == nil || *last.Values[0] != 25.0 {
		t.Errorf("last.Values[0] = %v, want 25.0", last.Values[0])
	}
	if last.Values[1] == nil || *last.Values[1] != 35.0 {
		t.Errorf("last.Values[1] = %v, want 35.0", last.Values[1])
	}
	if last.TimePoints[0] != time.Unix(0, 1999000000000) {
		t.Errorf("last.TimePoints[0] = %v, want %v", last.TimePoints[0], time.Unix(0, 1999000000000))
	}
	if last.TimePoints[1] != time.Unix(0, 2999000000000) {
		t.Errorf("last.TimePoints[1] = %v, want %v", last.TimePoints[1], time.Unix(0, 2999000000000))
	}

}

// TestTransformArrowFirstLastWithNullsMultiBatch exercises the multi-record-batch
// path: rowOffset and perSeriesValid must track state correctly across batch
// boundaries when null timestamps appear in both batches.
func TestTransformArrowFirstLastWithNullsMultiBatch(t *testing.T) {
	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "end_bucket_timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "first_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "first_timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "last_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "last_timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	// Batch 1: 2 rows
	//   Row 0: first has data, last has null timestamp
	//   Row 1: first has null timestamp, last has data
	// Batch 2: 2 rows
	//   Row 2: first has null timestamp, last has data
	//   Row 3: first has data, last has null timestamp
	type row struct {
		endTs    int64
		firstVal float64
		firstTs  int64
		firstOk  bool
		lastVal  float64
		lastTs   int64
		lastOk   bool
	}
	batches := [][]row{
		{
			{endTs: 1_000_000_000_000, firstVal: 10.0, firstTs: 900_000_000_000, firstOk: true, lastVal: 0, lastTs: 0, lastOk: false},
			{endTs: 2_000_000_000_000, firstVal: 0, firstTs: 0, firstOk: false, lastVal: 25.0, lastTs: 1_999_000_000_000, lastOk: true},
		},
		{
			{endTs: 3_000_000_000_000, firstVal: 0, firstTs: 0, firstOk: false, lastVal: 35.0, lastTs: 2_999_000_000_000, lastOk: true},
			{endTs: 4_000_000_000_000, firstVal: 40.0, firstTs: 3_900_000_000_000, firstOk: true, lastVal: 0, lastTs: 0, lastOk: false},
		},
	}

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	for _, batch := range batches {
		n := len(batch)
		tsB := array.NewInt64Builder(pool)
		fvB := array.NewFloat64Builder(pool)
		ftB := array.NewInt64Builder(pool)
		lvB := array.NewFloat64Builder(pool)
		ltB := array.NewInt64Builder(pool)
		for _, r := range batch {
			tsB.Append(r.endTs)
			if r.firstOk {
				fvB.Append(r.firstVal)
				ftB.Append(r.firstTs)
			} else {
				fvB.AppendNull()
				ftB.AppendNull()
			}
			if r.lastOk {
				lvB.Append(r.lastVal)
				ltB.Append(r.lastTs)
			} else {
				lvB.AppendNull()
				ltB.AppendNull()
			}
		}
		cols := make([]arrow.Array, 5)
		cols[0] = tsB.NewArray()
		cols[1] = fvB.NewArray()
		cols[2] = ftB.NewArray()
		cols[3] = lvB.NewArray()
		cols[4] = ltB.NewArray()
		rec := array.NewRecord(schema, cols, int64(n))
		if err := writer.Write(rec); err != nil {
			t.Fatalf("write batch: %v", err)
		}
		rec.Release()
		for _, c := range cols {
			c.Release()
		}
		tsB.Release()
		fvB.Release()
		ftB.Release()
		lvB.Release()
		ltB.Release()
	}
	writer.Close()

	arrowPlot := computeapi.ArrowBucketedNumericPlot{ArrowBinary: buf.Bytes()}
	series, err := extractArrowBucketedNumericSeries(arrowPlot, []aggColumnSpec{
		{Name: "first", ValueCol: "first_value", TimestampCol: "first_timestamp"},
		{Name: "last", ValueCol: "last_value", TimestampCol: "last_timestamp"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("expected 2 series, got %d", len(series))
	}

	first := series[0]
	last := series[1]

	// first: rows 0 and 3 have data, rows 1 and 2 have null timestamps → dropped.
	if len(first.Values) != 2 {
		t.Fatalf("first: expected 2 values, got %d", len(first.Values))
	}
	if len(first.TimePoints) != 2 {
		t.Fatalf("first: expected 2 timepoints, got %d", len(first.TimePoints))
	}
	if first.Values[0] == nil || *first.Values[0] != 10.0 {
		t.Errorf("first.Values[0] = %v, want 10.0", first.Values[0])
	}
	if first.Values[1] == nil || *first.Values[1] != 40.0 {
		t.Errorf("first.Values[1] = %v, want 40.0", first.Values[1])
	}
	if first.TimePoints[0] != time.Unix(0, 900_000_000_000) {
		t.Errorf("first.TimePoints[0] = %v, want %v", first.TimePoints[0], time.Unix(0, 900_000_000_000))
	}
	if first.TimePoints[1] != time.Unix(0, 3_900_000_000_000) {
		t.Errorf("first.TimePoints[1] = %v, want %v", first.TimePoints[1], time.Unix(0, 3_900_000_000_000))
	}

	// last: rows 1 and 2 have data, rows 0 and 3 have null timestamps → dropped.
	if len(last.Values) != 2 {
		t.Fatalf("last: expected 2 values, got %d", len(last.Values))
	}
	if len(last.TimePoints) != 2 {
		t.Fatalf("last: expected 2 timepoints, got %d", len(last.TimePoints))
	}
	if last.Values[0] == nil || *last.Values[0] != 25.0 {
		t.Errorf("last.Values[0] = %v, want 25.0", last.Values[0])
	}
	if last.Values[1] == nil || *last.Values[1] != 35.0 {
		t.Errorf("last.Values[1] = %v, want 35.0", last.Values[1])
	}
	if last.TimePoints[0] != time.Unix(0, 1_999_000_000_000) {
		t.Errorf("last.TimePoints[0] = %v, want %v", last.TimePoints[0], time.Unix(0, 1_999_000_000_000))
	}
	if last.TimePoints[1] != time.Unix(0, 2_999_000_000_000) {
		t.Errorf("last.TimePoints[1] = %v, want %v", last.TimePoints[1], time.Unix(0, 2_999_000_000_000))
	}
}

func TestValidateAndDedup(t *testing.T) {
	// All valid, no duplicates
	deduped, bad := validateAndDedup([]string{"MEAN", "MIN", "MAX", "COUNT", "VARIANCE", "FIRST_POINT", "LAST_POINT"})
	if bad != "" {
		t.Errorf("expected valid, got bad=%q", bad)
	}
	if len(deduped) != 7 {
		t.Errorf("expected 7, got %d", len(deduped))
	}

	// Invalid entry
	_, bad = validateAndDedup([]string{"MEAN", "BOGUS"})
	if bad != "BOGUS" {
		t.Errorf("expected BOGUS, got bad=%q", bad)
	}

	// Empty input
	deduped, bad = validateAndDedup([]string{})
	if bad != "" {
		t.Errorf("expected valid for empty, got bad=%q", bad)
	}
	if len(deduped) != 0 {
		t.Errorf("expected 0, got %d", len(deduped))
	}

	// Duplicates removed
	deduped, bad = validateAndDedup([]string{"MEAN", "MEAN", "MIN", "MIN", "MAX"})
	if bad != "" {
		t.Errorf("expected valid, got bad=%q", bad)
	}
	if len(deduped) != 3 {
		t.Errorf("expected 3 after dedup, got %d: %v", len(deduped), deduped)
	}
	if deduped[0] != "MEAN" || deduped[1] != "MIN" || deduped[2] != "MAX" {
		t.Errorf("unexpected order: %v", deduped)
	}
}
