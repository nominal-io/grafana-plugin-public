package plugin

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

func arm[T any](ctor func(T) computeapi.ComputeNodeResponse) computeapi.ComputeNodeResponse {
	var zero T
	return ctor(zero)
}

func TestUnsupportedComputeResponseArmsReturnErrors(t *testing.T) {
	arms := map[string]computeapi.ComputeNodeResponse{
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
	for name, response := range arms {
		t.Run(name, func(t *testing.T) {
			e := newTestQueryExecution(&Datasource{}, nil)
			_, err := e.transformNominalResponseFromClient(response, NominalQueryModel{})
			want := fmt.Sprintf("compute response type %q is not supported by the plugin", name)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("arm %q: want error containing %s, got: %v", name, want, err)
			}
		})
	}
}

func TestUnknownComputeResponseReturnsError(t *testing.T) {
	var response computeapi.ComputeNodeResponse
	if err := json.Unmarshal([]byte(`{"type":"futureResponse"}`), &response); err != nil {
		t.Fatalf("decode future compute response: %v", err)
	}

	e := newTestQueryExecution(&Datasource{}, nil)
	_, err := e.transformNominalResponseFromClient(response, NominalQueryModel{})
	const want = `compute response type "futureResponse" is not supported by the plugin`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("want error containing %s, got: %v", want, err)
	}
}
