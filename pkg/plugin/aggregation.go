package plugin

import (
	"bytes"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

// Aggregation enum constants matching the API's expected values.
const (
	AggMean       = "MEAN"
	AggMin        = "MIN"
	AggMax        = "MAX"
	AggCount      = "COUNT"
	AggVariance   = "VARIANCE"
	AggFirstPoint = "FIRST_POINT"
	AggLastPoint  = "LAST_POINT"
)

// AggregationSeries holds one aggregation's worth of data (e.g. "mean", "min").
// Each series carries its own timestamps. Most aggregations share end_bucket_timestamp,
// but FIRST_POINT/LAST_POINT use their own timestamp columns (first_timestamp, last_timestamp).
type AggregationSeries struct {
	Name       string      // display name: "mean", "min", "max", "first", "last"
	TimePoints []time.Time
	Values     []*float64
}

// aggColumnSpec describes how an aggregation maps to Arrow columns.
// Standard aggregations (MEAN, MIN, etc.) have a single value column and use shared timestamps.
// FIRST_POINT/LAST_POINT have a value column plus their own timestamp column.
type aggColumnSpec struct {
	Name         string // display name for the series (e.g. "mean", "first")
	ValueCol     string // Arrow column name for values (e.g. "mean", "first_value")
	TimestampCol string // Arrow column name for timestamps; empty means use shared end_bucket_timestamp
}

// aggSpecs is the single source of truth for all supported aggregations.
// To add a new aggregation: add a constant above, then add one entry here.
// Validation (validateAndDedup) and column mapping (aggColumnSpecFromEnum)
// both derive from this table.
var aggSpecs = map[string]aggColumnSpec{
	AggMean:       {Name: "mean", ValueCol: "mean"},
	AggMin:        {Name: "min", ValueCol: "min"},
	AggMax:        {Name: "max", ValueCol: "max"},
	AggCount:      {Name: "count", ValueCol: "count"},
	AggVariance:   {Name: "variance", ValueCol: "variance"},
	AggFirstPoint: {Name: "first", ValueCol: "first_value", TimestampCol: "first_timestamp"},
	AggLastPoint:  {Name: "last", ValueCol: "last_value", TimestampCol: "last_timestamp"},
}

// aggColumnSpecFromEnum looks up the Arrow column spec for an aggregation enum value.
func aggColumnSpecFromEnum(agg string) aggColumnSpec {
	return aggSpecs[agg]
}

// validateAndDedup checks that all aggregation names are known and removes duplicates,
// preserving order. Returns the deduped list and an empty string on success, or nil
// and the first unrecognised name on failure.
func validateAndDedup(aggs []string) ([]string, string) {
	seen := make(map[string]bool, len(aggs))
	deduped := make([]string, 0, len(aggs))
	for _, a := range aggs {
		if _, ok := aggSpecs[a]; ok {
			if !seen[a] {
				seen[a] = true
				deduped = append(deduped, a)
			}
		} else {
			return nil, a
		}
	}
	return deduped, ""
}

// resolvedSpec maps an aggColumnSpec to concrete column indices within an Arrow schema.
type resolvedSpec struct {
	valueIdx int
	tsIdx    int // -1 means use shared end_bucket_timestamp
}

// resolveArrowSchema locates the shared timestamp column and each spec's value/timestamp
// columns in the Arrow schema. Returns the shared timestamp column index and one
// resolvedSpec per input spec.
func resolveArrowSchema(schema *arrow.Schema, specs []aggColumnSpec) (sharedTsIdx int, resolved []resolvedSpec, err error) {
	tsIdx := schema.FieldIndices("end_bucket_timestamp")
	if len(tsIdx) == 0 {
		return 0, nil, fmt.Errorf("Arrow schema missing end_bucket_timestamp: have %v", schema.Fields())
	}

	resolved = make([]resolvedSpec, len(specs))
	for i, spec := range specs {
		idx := schema.FieldIndices(spec.ValueCol)
		if len(idx) == 0 {
			return 0, nil, fmt.Errorf("Arrow schema missing requested column %q: have %v", spec.ValueCol, schema.Fields())
		}
		resolved[i].valueIdx = idx[0]
		if spec.TimestampCol != "" {
			tsColIdx := schema.FieldIndices(spec.TimestampCol)
			if len(tsColIdx) == 0 {
				return 0, nil, fmt.Errorf("Arrow schema missing timestamp column %q: have %v", spec.TimestampCol, schema.Fields())
			}
			resolved[i].tsIdx = tsColIdx[0]
		} else {
			resolved[i].tsIdx = -1
		}
	}

	return tsIdx[0], resolved, nil
}

// extractColumnValues reads nRows from an Arrow column (Float64 or Uint32) and
// appends the values to dest. Rows where validMask[offset+i] is false are skipped
// (used to drop null-timestamp rows for FIRST/LAST). Pass nil validMask to include all rows.
func extractColumnValues(dest *[]*float64, rawCol arrow.Array, validMask []bool, offset, nRows int) error {
	switch col := rawCol.(type) {
	case *array.Float64:
		for i := 0; i < nRows; i++ {
			if validMask != nil && !validMask[offset+i] {
				continue
			}
			if col.IsNull(i) {
				*dest = append(*dest, nil)
			} else {
				v := col.Value(i)
				*dest = append(*dest, &v)
			}
		}
	case *array.Uint32:
		for i := 0; i < nRows; i++ {
			if validMask != nil && !validMask[offset+i] {
				continue
			}
			if col.IsNull(i) {
				*dest = append(*dest, nil)
			} else {
				v := float64(col.Value(i))
				*dest = append(*dest, &v)
			}
		}
	default:
		return fmt.Errorf("%T (expected Float64 or Uint32)", rawCol)
	}
	return nil
}

// extractArrowBucketedNumericSeries parses an Arrow IPC stream and extracts
// one AggregationSeries per aggColumnSpec. Standard aggregations share the
// end_bucket_timestamp column. FIRST_POINT/LAST_POINT use their own timestamp
// columns (first_timestamp, last_timestamp) so each series can have independent time axes.
func extractArrowBucketedNumericSeries(
	arrowPlot computeapi.ArrowBucketedNumericPlot,
	specs []aggColumnSpec,
) ([]AggregationSeries, error) {
	buf := bytes.NewReader(arrowPlot.ArrowBinary)
	reader, err := ipc.NewReader(buf, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer reader.Release()

	sharedTsIdx, resolved, err := resolveArrowSchema(reader.Schema(), specs)
	if err != nil {
		return nil, err
	}

	// Initialize result slices — always non-nil so callers don't depend on nil semantics.
	seriesData := make([]AggregationSeries, len(specs))
	for i, spec := range specs {
		seriesData[i].Name = spec.Name
		seriesData[i].Values = []*float64{}
	}
	sharedTimePoints := []time.Time{}
	// Per-series timestamps for specs that have their own timestamp column.
	// Indexed by spec position; nil for specs using shared timestamps.
	perSeriesTime := make([][]time.Time, len(specs))
	for i, spec := range specs {
		if spec.TimestampCol != "" {
			perSeriesTime[i] = []time.Time{}
		}
	}
	// rowOffset tracks how many Arrow rows we've seen per spec across records,
	// used to index into perSeriesValid which is built across all records.
	rowOffset := make([]int, len(specs))
	// perSeriesValid tracks which Arrow rows have non-null per-series timestamps.
	// Rows with null timestamps (empty buckets) are dropped entirely from that
	// series so that no zero-time entries appear in table panels. nil for specs
	// that use the shared timestamp.
	perSeriesValid := make([][]bool, len(specs))

	for reader.Next() {
		rec := reader.Record()
		nRows := int(rec.NumRows())

		// Extract shared timestamps
		tsCol, ok := rec.Column(sharedTsIdx).(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("expected Int64 for end_bucket_timestamp, got %T", rec.Column(sharedTsIdx))
		}
		for i := 0; i < nRows; i++ {
			sharedTimePoints = append(sharedTimePoints, time.Unix(0, tsCol.Value(i)))
		}

		// Extract per-series timestamps for FIRST_POINT/LAST_POINT.
		// Rows with null timestamps (empty buckets) are skipped entirely so the
		// series has fewer rows rather than containing zero-time placeholders.
		for si, rs := range resolved {
			if rs.tsIdx < 0 {
				continue
			}
			perTsCol, ok := rec.Column(rs.tsIdx).(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("expected Int64 for %s, got %T", specs[si].TimestampCol, rec.Column(rs.tsIdx))
			}
			for i := 0; i < nRows; i++ {
				valid := !perTsCol.IsNull(i)
				perSeriesValid[si] = append(perSeriesValid[si], valid)
				if valid {
					perSeriesTime[si] = append(perSeriesTime[si], time.Unix(0, perTsCol.Value(i)))
				}
			}
		}

		// Extract each field's values.
		// For series with per-series timestamps, rows where the timestamp was null are
		// skipped so that TimePoints and Values stay the same length.
		for fi, rs := range resolved {
			if err := extractColumnValues(&seriesData[fi].Values, rec.Column(rs.valueIdx), perSeriesValid[fi], rowOffset[fi], nRows); err != nil {
				return nil, fmt.Errorf("unsupported column type for %s: %w", specs[fi].ValueCol, err)
			}
			if perSeriesValid[fi] != nil {
				rowOffset[fi] += nRows
			}
		}
	}

	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("Arrow IPC read error: %w", err)
	}

	// Assign timestamps: per-series for FIRST/LAST, shared for everything else.
	for i := range seriesData {
		if perSeriesTime[i] != nil {
			seriesData[i].TimePoints = perSeriesTime[i]
		} else {
			seriesData[i].TimePoints = sharedTimePoints
		}
	}

	return seriesData, nil
}
