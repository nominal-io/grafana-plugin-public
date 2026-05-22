package plugin

import computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"

func effectiveBucketCount(qm NominalQueryModel, maxDataPoints int64) int {
	buckets := int(qm.Buckets)
	if maxDataPoints > 0 && (buckets <= 0 || int(maxDataPoints) < buckets) {
		buckets = int(maxDataPoints)
	}
	return buckets
}

func numericOutputFields(aggregations []string) []computeapi.NumericOutputField {
	outputFields := make([]computeapi.NumericOutputField, 0, len(aggregations))
	for _, agg := range aggregations {
		outputFields = append(outputFields, computeapi.New_NumericOutputField(
			computeapi.NumericOutputField_Value(agg),
		))
	}
	return outputFields
}
