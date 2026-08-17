package plugin

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

func arm[T any](ctor func(T) computeapi.ComputeNodeResponse) func() computeapi.ComputeNodeResponse {
	return func() computeapi.ComputeNodeResponse { return ctor(*new(T)) }
}

func TestUnsupportedComputeResponseArmsReturnErrors(t *testing.T) {
	tests := map[string]func() computeapi.ComputeNodeResponse{
		"range":                     arm(computeapi.NewComputeNodeResponseFromRange),
		"rangesSummary":             arm(computeapi.NewComputeNodeResponseFromRangesSummary),
		"rangeValue":                arm(computeapi.NewComputeNodeResponseFromRangeValue),
		"numericPoint":              arm(computeapi.NewComputeNodeResponseFromNumericPoint),
		"singlePoint":               arm(computeapi.NewComputeNodeResponseFromSinglePoint),
		"arrowNumeric":              arm(computeapi.NewComputeNodeResponseFromArrowNumeric),
		"arrowEnum":                 arm(computeapi.NewComputeNodeResponseFromArrowEnum),
		"arrowBucketedEnum":         arm(computeapi.NewComputeNodeResponseFromArrowBucketedEnum),
		"cartesian":                 arm(computeapi.NewComputeNodeResponseFromCartesian),
		"bucketedCartesian":         arm(computeapi.NewComputeNodeResponseFromBucketedCartesian),
		"bucketedCartesian3d":       arm(computeapi.NewComputeNodeResponseFromBucketedCartesian3d),
		"frequencyDomain":           arm(computeapi.NewComputeNodeResponseFromFrequencyDomain),
		"frequencyDomainV2":         arm(computeapi.NewComputeNodeResponseFromFrequencyDomainV2),
		"bucketedFrequencyDomain":   arm(computeapi.NewComputeNodeResponseFromBucketedFrequencyDomain),
		"numericHistogram":          arm(computeapi.NewComputeNodeResponseFromNumericHistogram),
		"enumHistogram":             arm(computeapi.NewComputeNodeResponseFromEnumHistogram),
		"curveFit":                  arm(computeapi.NewComputeNodeResponseFromCurveFit),
		"grouped":                   arm(computeapi.NewComputeNodeResponseFromGrouped),
		"array":                     arm(computeapi.NewComputeNodeResponseFromArray),
		"bucketedStruct":            arm(computeapi.NewComputeNodeResponseFromBucketedStruct),
		"fullResolution":            arm(computeapi.NewComputeNodeResponseFromFullResolution),
		"arrowBucketedMultivariate": arm(computeapi.NewComputeNodeResponseFromArrowBucketedMultivariate),
		"multivariate":              arm(computeapi.NewComputeNodeResponseFromMultivariate),
	}

	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			e := &NominalQueryExecution{}
			_, err := e.transformNominalResponseFromClient(build(), NominalQueryModel{})
			if err == nil {
				t.Fatalf("expected an error for unsupported arm %q, got nil", name)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("compute response type %q is not supported by the plugin", name)) {
				t.Fatalf("error for arm %q should use the standard unsupported message, got: %v", name, err)
			}
		})
	}
}

func TestUnknownComputeResponseReturnsError(t *testing.T) {
	var response computeapi.ComputeNodeResponse
	if err := json.Unmarshal([]byte(`{"type":"futureResponse"}`), &response); err != nil {
		t.Fatalf("decode future compute response: %v", err)
	}

	e := &NominalQueryExecution{}
	_, err := e.transformNominalResponseFromClient(response, NominalQueryModel{})
	if err == nil {
		t.Fatal("expected an error for an unknown compute response, got nil")
	}
	if !strings.Contains(err.Error(), `compute response type "futureResponse" is not supported by the plugin`) {
		t.Fatalf("unknown response should use the standard unsupported message, got: %v", err)
	}
}
