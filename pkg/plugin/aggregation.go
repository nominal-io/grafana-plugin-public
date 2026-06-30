package plugin

import (
	"bytes"
	"fmt"
	"slices"
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
//
// CarriesChannelUnit mirrors the source aggColumnSpec.CarriesChannelUnit flag so
// downstream frame builders (fieldConfigForNumeric) can decide whether to attach
// the channel unit without re-looking up the spec by name.
type AggregationSeries struct {
	Name               string // display name: "mean", "min", "max", "first", "last"
	TimePoints         []time.Time
	Values             []*float64
	CarriesChannelUnit bool
}

// aggColumnSpec describes how an aggregation maps to Arrow columns.
// Standard aggregations (MEAN, MIN, etc.) have a single value column and use shared timestamps.
// FIRST_POINT/LAST_POINT have a value column plus their own timestamp column.
type aggColumnSpec struct {
	Name         string // display name for the series (e.g. "mean", "first")
	ValueCol     string // Arrow column name for values (e.g. "mean", "first_value")
	TimestampCol string // Arrow column name for timestamps; empty means use shared end_bucket_timestamp
	// CarriesChannelUnit is true when the aggregation's output is expressed in the
	// base channel unit (MEAN/MIN/MAX/FIRST/LAST). It is false when the output is
	// in a different unit-space: COUNT is dimensionless, and VARIANCE is unit²
	// (rendering it with the channel unit would mislead). When false, the channel
	// unit MUST NOT be attached to the resulting frame's FieldConfig.
	CarriesChannelUnit bool
}

// aggSpecs is the single source of truth for all supported aggregations.
// To add a new aggregation: add a constant above, then add one entry here.
// Validation (validateAndDedup) and column mapping (aggColumnSpecFromEnum)
// both derive from this table.
var aggSpecs = map[string]aggColumnSpec{
	AggMean:       {Name: "mean", ValueCol: "mean", CarriesChannelUnit: true},
	AggMin:        {Name: "min", ValueCol: "min", CarriesChannelUnit: true},
	AggMax:        {Name: "max", ValueCol: "max", CarriesChannelUnit: true},
	AggCount:      {Name: "count", ValueCol: "count"},       // dimensionless
	AggVariance:   {Name: "variance", ValueCol: "variance"}, // unit² — channel unit would mislead
	AggFirstPoint: {Name: "first", ValueCol: "first_value", TimestampCol: "first_timestamp", CarriesChannelUnit: true},
	AggLastPoint:  {Name: "last", ValueCol: "last_value", TimestampCol: "last_timestamp", CarriesChannelUnit: true},
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

func validateRecordColumnLengths(rec arrow.Record) error {
	nRows := int(rec.NumRows())
	for i := 0; i < int(rec.NumCols()); i++ {
		col := rec.Column(i)
		if col.Len() != nRows {
			return fmt.Errorf("Arrow record column %q length mismatch: got %d, want %d", rec.ColumnName(i), col.Len(), nRows)
		}
	}
	return nil
}

type rowSelection struct {
	mask         []bool
	includedRows int
}

func allRows(nRows int) rowSelection {
	return rowSelection{includedRows: nRows}
}

func (s rowSelection) includes(row int) bool {
	return s.mask == nil || s.mask[row]
}

func appendUnixNanos(dst []time.Time, nanos int64) []time.Time {
	return append(dst, time.Unix(0, nanos))
}

func appendNonNullTimestamps(dst []time.Time, col *array.Int64) ([]time.Time, rowSelection) {
	nRows := col.Len()
	// Keep dense timestamp columns on a separate path so the common FIRST/LAST
	// case avoids a per-row mask branch; appendUnixNanos keeps conversion shared.
	if col.NullN() == 0 {
		dst = slices.Grow(dst, nRows)
		for i := 0; i < nRows; i++ {
			dst = appendUnixNanos(dst, col.Value(i))
		}
		return dst, allRows(nRows)
	}

	selection := rowSelection{
		mask:         make([]bool, nRows),
		includedRows: nRows - col.NullN(),
	}
	dst = slices.Grow(dst, selection.includedRows)
	for i := 0; i < nRows; i++ {
		if col.IsNull(i) {
			continue
		}
		selection.mask[i] = true
		dst = appendUnixNanos(dst, col.Value(i))
	}
	return dst, selection
}

// valueBackingLen sizes extractColumnValues' shared float backing slice.
// Unmasked columns use Arrow's O(1) null counter. Masked FIRST/LAST columns use
// the selected-row count as an upper bound to avoid a second pass over the same
// rows; null-valued selected rows leave a few unused float64 slots.
func valueBackingLen(col arrow.Array, selection rowSelection) int {
	if selection.mask == nil {
		return selection.includedRows - col.NullN()
	}
	return selection.includedRows
}

// extractColumnValues reads nRows from an Arrow column (Float64 or Uint32) and
// appends values to series.Values. Rows excluded by selection are skipped (used
// to drop null-timestamp rows for FIRST/LAST).
//
// Non-null values share one backing slice per call, avoiding one heap allocation
// per value while keeping pointers stable.
func extractColumnValues(series *AggregationSeries, rawCol arrow.Array, selection rowSelection, nRows int) error {
	// Dense columns are the common throughput path and can avoid per-row null
	// checks plus indirect value dispatch.
	switch col := rawCol.(type) {
	case *array.Float64:
		if selection.mask == nil && col.NullN() == 0 {
			extractDenseFloat64ColumnValues(series, col, nRows)
			return nil
		}
	case *array.Uint32:
		if selection.mask == nil && col.NullN() == 0 {
			extractDenseUint32ColumnValues(series, col, nRows)
			return nil
		}
	default:
		return fmt.Errorf("%T (expected Float64 or Uint32)", rawCol)
	}

	// Resolve the accessor first so unsupported columns fail before mutating series.
	var valueAt func(int) float64
	switch col := rawCol.(type) {
	case *array.Float64:
		valueAt = col.Value
	case *array.Uint32:
		valueAt = func(i int) float64 { return float64(col.Value(i)) }
	}

	series.Values = slices.Grow(series.Values, selection.includedRows)

	backing := make([]float64, valueBackingLen(rawCol, selection))
	bi := 0
	for i := 0; i < nRows; i++ {
		if !selection.includes(i) {
			continue
		}
		if rawCol.IsNull(i) {
			series.Values = append(series.Values, nil)
			continue
		}
		backing[bi] = valueAt(i)
		series.Values = append(series.Values, &backing[bi])
		bi++
	}
	return nil
}

func extractDenseFloat64ColumnValues(series *AggregationSeries, col *array.Float64, nRows int) {
	series.Values = slices.Grow(series.Values, nRows)

	backing := make([]float64, nRows)
	for i := 0; i < nRows; i++ {
		backing[i] = col.Value(i)
		series.Values = append(series.Values, &backing[i])
	}
}

func extractDenseUint32ColumnValues(series *AggregationSeries, col *array.Uint32, nRows int) {
	series.Values = slices.Grow(series.Values, nRows)

	backing := make([]float64, nRows)
	for i := 0; i < nRows; i++ {
		backing[i] = float64(col.Value(i))
		series.Values = append(series.Values, &backing[i])
	}
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
		seriesData[i].CarriesChannelUnit = spec.CarriesChannelUnit
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
	recordSelections := make([]rowSelection, len(specs))

	for reader.Next() {
		rec := reader.Record()
		nRows := int(rec.NumRows())
		if err := validateRecordColumnLengths(rec); err != nil {
			return nil, err
		}
		for i := range recordSelections {
			recordSelections[i] = allRows(nRows)
		}

		// Extract shared timestamps
		tsCol, ok := rec.Column(sharedTsIdx).(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("expected Int64 for end_bucket_timestamp, got %T", rec.Column(sharedTsIdx))
		}
		// Shared timestamps include every row; grow once per record.
		sharedTimePoints = slices.Grow(sharedTimePoints, nRows)
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
			perSeriesTime[si], recordSelections[si] = appendNonNullTimestamps(perSeriesTime[si], perTsCol)
		}

		// Extract each field's values.
		// For series with per-series timestamps, rows where the timestamp was null are
		// skipped so that TimePoints and Values stay the same length.
		for fi, rs := range resolved {
			if err := extractColumnValues(&seriesData[fi], rec.Column(rs.valueIdx), recordSelections[fi], nRows); err != nil {
				return nil, fmt.Errorf("unsupported column type for %s: %w", specs[fi].ValueCol, err)
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
