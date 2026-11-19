package plugin

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
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
	AggLTTB       = "LTTB"
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
	Values             []float64
	CarriesChannelUnit bool
	missing            []bool // nil unless a value is missing; then parallel to Values
	ownTimes           bool   // TimePoints are sample times, not shared bucket ends
}

// dropMissingBuckets omits Arrow nulls while preserving numeric values, including
// zero and NaN. Timestamps may be shared with other series, so only the values
// can be compacted in place.
func (s *AggregationSeries) dropMissingBuckets() {
	if s.missing == nil {
		return
	}
	times := make([]time.Time, 0, len(s.Values))
	kept := 0
	for i, missing := range s.missing {
		if !missing {
			times = append(times, s.TimePoints[i])
			s.Values[kept] = s.Values[i]
			kept++
		}
	}
	s.missing = nil
	if kept == 0 {
		s.TimePoints, s.Values = nil, nil
		return
	}
	s.TimePoints, s.Values = times, s.Values[:kept]
}

// appendValue adds a value, or a missing placeholder, to the series.
func (s *AggregationSeries) appendValue(v float64, missing bool) {
	if missing && s.missing == nil {
		s.missing = make([]bool, len(s.Values), cap(s.Values))
	}
	s.Values = append(s.Values, v)
	if s.missing != nil {
		s.missing = append(s.missing, missing)
	}
}

// fieldValues returns the values for a frame field: the []float64 itself when none is missing or
// NaN, else []*float64 with nil for the missing ones. NaN keeps the field nullable for the reason
// given on columnValues.
func (s *AggregationSeries) fieldValues() any {
	if s.missing == nil && !slices.ContainsFunc(s.Values, math.IsNaN) {
		return s.Values
	}
	ptrs := make([]*float64, len(s.Values))
	for i := range s.Values {
		if s.missing == nil || !s.missing[i] {
			ptrs[i] = &s.Values[i]
		}
	}
	return ptrs
}

// dropOutsideRange removes points sampled outside r. Edge buckets can extend past
// the query range, so their first or last sample may lie outside it. Only call it
// on a series with its own timestamps; it compacts TimePoints in place.
func (s *AggregationSeries) dropOutsideRange(r backend.TimeRange) {
	kept := 0
	for i, t := range s.TimePoints {
		if !t.Before(r.From) && !t.After(r.To) {
			s.TimePoints[kept], s.Values[kept] = t, s.Values[i]
			kept++
		}
	}
	s.TimePoints, s.Values = s.TimePoints[:kept], s.Values[:kept]
}

// aggColumnSpec describes how an aggregation maps to Arrow columns.
// Standard aggregations (MEAN, MIN, etc.) have a single value column and use shared timestamps.
// FIRST_POINT/LAST_POINT and LTTB have a value column plus their own timestamp column.
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
	AggLTTB:       {Name: "lttb", ValueCol: "value", TimestampCol: "timestamp", CarriesChannelUnit: true},
}

// validAggregationNames lists aggSpecs for error messages.
func validAggregationNames() string {
	return strings.Join(slices.Sorted(maps.Keys(aggSpecs)), ", ")
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
// columns in the Arrow schema. Returns the shared timestamp column index (-1 when
// every spec has its own timestamp column) and one resolvedSpec per input spec.
func resolveArrowSchema(schema *arrow.Schema, specs []aggColumnSpec) (sharedTsIdx int, resolved []resolvedSpec, err error) {
	sharedTsIdx = -1
	if slices.ContainsFunc(specs, func(s aggColumnSpec) bool { return s.TimestampCol == "" }) {
		tsIdx := schema.FieldIndices("end_bucket_timestamp")
		if len(tsIdx) == 0 {
			return 0, nil, fmt.Errorf("Arrow schema missing end_bucket_timestamp: have %v", schema.Fields())
		}
		sharedTsIdx = tsIdx[0]
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

	return sharedTsIdx, resolved, nil
}

func validateRecordColumnLengths(rec arrow.Record, sharedTsIdx int, resolved []resolvedSpec) error {
	nRows := int(rec.NumRows())
	seen := make(map[int]struct{}, 1+len(resolved)*2)
	checkColumn := func(i int) error {
		if i < 0 {
			return nil
		}
		if _, ok := seen[i]; ok {
			return nil
		}
		seen[i] = struct{}{}
		col := rec.Column(i)
		if col.Len() != nRows {
			return fmt.Errorf("Arrow record column %q length mismatch: got %d, want %d", rec.ColumnName(i), col.Len(), nRows)
		}
		return nil
	}
	if err := checkColumn(sharedTsIdx); err != nil {
		return err
	}
	for _, rs := range resolved {
		if err := checkColumn(rs.valueIdx); err != nil {
			return err
		}
		if err := checkColumn(rs.tsIdx); err != nil {
			return err
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

func appendNonNullTimestamps(dst []time.Time, col *array.Int64) ([]time.Time, rowSelection, error) {
	nRows := col.Len()
	// Keep dense timestamp columns on a separate path so the common FIRST/LAST
	// case avoids a per-row mask branch; appendUnixNanos keeps conversion shared.
	if col.NullN() == 0 {
		dst = slices.Grow(dst, nRows)
		for i := 0; i < nRows; i++ {
			dst = appendUnixNanos(dst, col.Value(i))
		}
		return dst, allRows(nRows), nil
	}

	includedRows := nRows - col.NullN()
	if includedRows < 0 {
		return dst, rowSelection{}, fmt.Errorf("Arrow timestamp column null metadata inconsistent: null count %d exceeds rows %d", col.NullN(), nRows)
	}
	selection := rowSelection{
		mask:         make([]bool, nRows),
		includedRows: includedRows,
	}
	dst = slices.Grow(dst, selection.includedRows)
	actualIncludedRows := 0
	for i := 0; i < nRows; i++ {
		if col.IsNull(i) {
			continue
		}
		selection.mask[i] = true
		actualIncludedRows++
		dst = appendUnixNanos(dst, col.Value(i))
	}
	selection.includedRows = actualIncludedRows
	return dst, selection, nil
}

// extractColumnValues reads nRows from an Arrow column (Float64 or Uint32) and
// appends values to series.Values. Rows excluded by selection are skipped (used
// to drop null-timestamp rows for FIRST/LAST). Null rows are recorded in series.missing.
func extractColumnValues(series *AggregationSeries, rawCol arrow.Array, selection rowSelection, nRows int) error {
	// Dense columns are the common throughput path and can avoid per-row null
	// checks plus indirect value dispatch.
	var valueAt func(int) float64
	switch col := rawCol.(type) {
	case *array.Float64:
		if selection.mask == nil && col.NullN() == 0 {
			series.Values = append(slices.Grow(series.Values, nRows), col.Float64Values()[:nRows]...)
			series.extendMissing(nRows)
			return nil
		}
		valueAt = col.Value
	case *array.Uint32:
		if selection.mask == nil && col.NullN() == 0 {
			series.Values = slices.Grow(series.Values, nRows)
			for _, v := range col.Uint32Values()[:nRows] {
				series.Values = append(series.Values, float64(v))
			}
			series.extendMissing(nRows)
			return nil
		}
		valueAt = func(i int) float64 { return float64(col.Value(i)) }
	default:
		return fmt.Errorf("%T (expected Float64 or Uint32)", rawCol)
	}

	series.Values = slices.Grow(series.Values, selection.includedRows)
	// An unmasked column's null count must agree with its validity bitmap.
	wantValid := selection.includedRows - rawCol.NullN()
	if selection.mask == nil && wantValid < 0 {
		return fmt.Errorf("Arrow column null metadata inconsistent: null count %d exceeds selected rows %d", rawCol.NullN(), selection.includedRows)
	}
	valid := 0
	for i := 0; i < nRows; i++ {
		if !selection.includes(i) {
			continue
		}
		if rawCol.IsNull(i) {
			series.appendValue(0, true)
			continue
		}
		if valid++; selection.mask == nil && valid > wantValid {
			return fmt.Errorf("Arrow column null metadata inconsistent: more non-null values than declared by null count")
		}
		series.appendValue(valueAt(i), false)
	}
	return nil
}

// extendMissing marks n newly appended values as present.
func (s *AggregationSeries) extendMissing(n int) {
	if s.missing != nil {
		s.missing = append(s.missing, make([]bool, n)...)
	}
}

// extractArrowBucketedNumericSeries parses an Arrow IPC stream into one
// AggregationSeries per aggColumnSpec. Despite the name, it also decodes raw
// LTTB responses. Specs without a TimestampCol share end_bucket_timestamp.
func extractArrowBucketedNumericSeries(
	arrowPlot computeapi.ArrowPlot,
	specs []aggColumnSpec,
) ([]AggregationSeries, error) {
	// Initialize result slices — always non-nil so callers don't depend on nil semantics.
	seriesData := make([]AggregationSeries, len(specs))
	for i, spec := range specs {
		seriesData[i].Name = spec.Name
		seriesData[i].CarriesChannelUnit = spec.CarriesChannelUnit
		seriesData[i].Values = []float64{}
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

	var sharedTsIdx int
	var resolved []resolvedSpec
	open := func(schema *arrow.Schema) (err error) {
		sharedTsIdx, resolved, err = resolveArrowSchema(schema, specs)
		return err
	}
	each := func(rec arrow.RecordBatch) error {
		nRows := int(rec.NumRows())
		if err := validateRecordColumnLengths(rec, sharedTsIdx, resolved); err != nil {
			return err
		}
		for i := range recordSelections {
			recordSelections[i] = allRows(nRows)
		}

		if sharedTsIdx >= 0 {
			tsCol, ok := rec.Column(sharedTsIdx).(*array.Int64)
			if !ok {
				return fmt.Errorf("expected Int64 for end_bucket_timestamp, got %T", rec.Column(sharedTsIdx))
			}
			// Shared timestamps include every row; grow once per record.
			sharedTimePoints = slices.Grow(sharedTimePoints, nRows)
			for i := 0; i < nRows; i++ {
				sharedTimePoints = append(sharedTimePoints, time.Unix(0, tsCol.Value(i)))
			}
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
				return fmt.Errorf("expected Int64 for %s, got %T", specs[si].TimestampCol, rec.Column(rs.tsIdx))
			}
			var err error
			perSeriesTime[si], recordSelections[si], err = appendNonNullTimestamps(perSeriesTime[si], perTsCol)
			if err != nil {
				return fmt.Errorf("invalid timestamp column %s: %w", specs[si].TimestampCol, err)
			}
		}

		// Extract each field's values.
		// For series with per-series timestamps, rows where the timestamp was null are
		// skipped so that TimePoints and Values stay the same length.
		for fi, rs := range resolved {
			if err := extractColumnValues(&seriesData[fi], rec.Column(rs.valueIdx), recordSelections[fi], nRows); err != nil {
				return fmt.Errorf("unsupported column type for %s: %w", specs[fi].ValueCol, err)
			}
		}
		return nil
	}
	if err := readArrowBatches(arrowPlot.ArrowBinary, open, each); err != nil {
		return nil, err
	}

	// Assign timestamps: per-series for FIRST/LAST, shared for everything else.
	for i := range seriesData {
		if perSeriesTime[i] != nil {
			seriesData[i].TimePoints = perSeriesTime[i]
			seriesData[i].ownTimes = true
		} else {
			seriesData[i].TimePoints = sharedTimePoints
		}
	}

	return seriesData, nil
}

// readArrowBatches opens an Arrow IPC stream, passes its schema to open, then
// passes each record batch to each.
func readArrowBatches(binary []byte, open func(*arrow.Schema) error, each func(arrow.RecordBatch) error) error {
	reader, err := ipc.NewReader(bytes.NewReader(binary), ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer reader.Release()
	if err := open(reader.Schema()); err != nil {
		return err
	}
	for reader.Next() {
		if err := each(reader.RecordBatch()); err != nil {
			return err
		}
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("failed to read Arrow IPC stream: %w", err)
	}
	return nil
}
