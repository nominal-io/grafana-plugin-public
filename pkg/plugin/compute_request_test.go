package plugin

import (
	"testing"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

func TestEffectiveBucketCount(t *testing.T) {
	tests := []struct {
		name          string
		buckets       int
		maxDataPoints int64
		want          int
	}{
		{"maxDataPoints caps buckets when smaller", 1000, 500, 500},
		{"maxDataPoints does not increase buckets", 500, 1000, 500},
		{"maxDataPoints used when buckets is zero", 0, 800, 800},
		{"maxDataPoints used when buckets is negative", -10, 300, 300},
		{"zero maxDataPoints uses saved buckets", 1000, 0, 1000},
		{"negative maxDataPoints uses saved buckets", 1000, -1, 1000},
		{"zero buckets and zero maxDataPoints stays zero", 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := NominalQueryModel{Buckets: tt.buckets}
			got := effectiveBucketCount(qm, tt.maxDataPoints)
			if got != tt.want {
				t.Errorf("effectiveBucketCount(Buckets=%d, maxDataPoints=%d) = %d, want %d", tt.buckets, tt.maxDataPoints, got, tt.want)
			}
		})
	}
}

func TestNumericOutputFields(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []computeapi.NumericOutputField_Value
	}{
		{
			name: "preserves aggregation order",
			in:   []string{AggMean, AggMin, AggMax, AggCount, AggVariance, AggFirstPoint, AggLastPoint},
			want: []computeapi.NumericOutputField_Value{
				computeapi.NumericOutputField_MEAN,
				computeapi.NumericOutputField_MIN,
				computeapi.NumericOutputField_MAX,
				computeapi.NumericOutputField_COUNT,
				computeapi.NumericOutputField_VARIANCE,
				computeapi.NumericOutputField_FIRST_POINT,
				computeapi.NumericOutputField_LAST_POINT,
			},
		},
		{
			name: "returns empty slice for empty aggregations",
			in:   nil,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := numericOutputFields(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("len(numericOutputFields) = %d, want %d; got %v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].Value() != tt.want[i] {
					t.Errorf("field[%d] = %s, want %s", i, got[i].Value(), tt.want[i])
				}
			}
		})
	}
}
