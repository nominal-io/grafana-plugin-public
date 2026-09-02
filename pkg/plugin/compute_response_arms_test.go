package plugin

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

func arm[T any](ctor func(T) computeapi.ComputeNodeResponse) func() computeapi.ComputeNodeResponse {
	return func() computeapi.ComputeNodeResponse { return ctor(*new(T)) }
}

// unsupportedComputeResponseArms maps every arm without a renderer to a
// builder producing a valid instance of that arm.
var unsupportedComputeResponseArms = map[string]func() computeapi.ComputeNodeResponse{
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

// supportedComputeResponseArms maps every arm with a renderer to a builder so
// the test executes the handler instead of trusting a parallel count.
var supportedComputeResponseArms = map[string]func() computeapi.ComputeNodeResponse{
	"numeric":              arm(computeapi.NewComputeNodeResponseFromNumeric),
	"bucketedNumeric":      arm(computeapi.NewComputeNodeResponseFromBucketedNumeric),
	"arrowBucketedNumeric": arm(computeapi.NewComputeNodeResponseFromArrowBucketedNumeric),
	"enum":                 arm(computeapi.NewComputeNodeResponseFromEnum),
	"enumPoint":            arm(computeapi.NewComputeNodeResponseFromEnumPoint),
	"bucketedEnum":         arm(computeapi.NewComputeNodeResponseFromBucketedEnum),
	"pagedLog":             arm(computeapi.NewComputeNodeResponseFromPagedLog),
	"logPoint":             arm(computeapi.NewComputeNodeResponseFromLogPoint),
}

// A nominal-api-go bump that adds union arms must classify every actual struct
// field as supported or unsupported. Comparing names catches duplicate or
// invented list entries that a count-only check would miss.
func TestComputeResponseArmsAreExhaustive(t *testing.T) {
	responseType := reflect.TypeOf(computeapi.ComputeNodeResponse{})
	actual := make(map[string]bool, responseType.NumField()-1)
	for i := 0; i < responseType.NumField(); i++ {
		// Conjure suffixes Go keywords such as the "range" arm with an underscore.
		name := strings.TrimSuffix(responseType.Field(i).Name, "_")
		if name != "typ" {
			actual[name] = true
		}
	}

	for name := range supportedComputeResponseArms {
		if !actual[name] {
			t.Errorf("supported response arm %q is not a ComputeNodeResponse field", name)
		}
		delete(actual, name)
	}
	for name := range unsupportedComputeResponseArms {
		if !actual[name] {
			t.Errorf("unsupported response arm %q is duplicated or is not a ComputeNodeResponse field", name)
		}
		delete(actual, name)
	}
	if len(actual) != 0 {
		t.Fatalf("ComputeNodeResponse arms are unclassified: %v", reflect.ValueOf(actual).MapKeys())
	}
}

func TestSupportedComputeResponseArmsInvokeRenderers(t *testing.T) {
	for name, build := range supportedComputeResponseArms {
		t.Run(name, func(t *testing.T) {
			e := newTestQueryExecution(&Datasource{}, nil)
			_, err := e.transformNominalResponseFromClient(build(), NominalQueryModel{})
			if err != nil && strings.Contains(err.Error(), "is not supported by the plugin") {
				t.Fatalf("supported arm %q reached the unsupported handler: %v", name, err)
			}
		})
	}
}

func TestUnsupportedComputeResponseArmsReturnErrors(t *testing.T) {
	for name, build := range unsupportedComputeResponseArms {
		t.Run(name, func(t *testing.T) {
			e := newTestQueryExecution(&Datasource{}, nil)
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

	e := newTestQueryExecution(&Datasource{}, nil)
	_, err := e.transformNominalResponseFromClient(response, NominalQueryModel{})
	if err == nil {
		t.Fatal("expected an error for an unknown compute response, got nil")
	}
	if !strings.Contains(err.Error(), `compute response type "futureResponse" is not supported by the plugin`) {
		t.Fatalf("unknown response should use the standard unsupported message, got: %v", err)
	}
}
