package plugin

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

// arm adapts a union constructor into a zero-value builder; *new(T) is nil
// for pointer and slice payloads, which the dispatcher must tolerate.
func arm[T any](ctor func(T) computeapi.ComputeNodeResponse) func() computeapi.ComputeNodeResponse {
	return func() computeapi.ComputeNodeResponse { return ctor(*new(T)) }
}

// computeResponseArms maps every known ComputeNodeResponse arm to a builder
// producing a valid instance of that arm.
var computeResponseArms = map[string]func() computeapi.ComputeNodeResponse{
	"range":                     arm(computeapi.NewComputeNodeResponseFromRange),
	"rangesSummary":             arm(computeapi.NewComputeNodeResponseFromRangesSummary),
	"rangeValue":                arm(computeapi.NewComputeNodeResponseFromRangeValue),
	"numeric":                   arm(computeapi.NewComputeNodeResponseFromNumeric),
	"bucketedNumeric":           arm(computeapi.NewComputeNodeResponseFromBucketedNumeric),
	"numericPoint":              arm(computeapi.NewComputeNodeResponseFromNumericPoint),
	"singlePoint":               arm(computeapi.NewComputeNodeResponseFromSinglePoint),
	"arrowNumeric":              arm(computeapi.NewComputeNodeResponseFromArrowNumeric),
	"arrowBucketedNumeric":      arm(computeapi.NewComputeNodeResponseFromArrowBucketedNumeric),
	"enum":                      arm(computeapi.NewComputeNodeResponseFromEnum),
	"enumPoint":                 arm(computeapi.NewComputeNodeResponseFromEnumPoint),
	"bucketedEnum":              arm(computeapi.NewComputeNodeResponseFromBucketedEnum),
	"arrowEnum":                 arm(computeapi.NewComputeNodeResponseFromArrowEnum),
	"arrowBucketedEnum":         arm(computeapi.NewComputeNodeResponseFromArrowBucketedEnum),
	"pagedLog":                  arm(computeapi.NewComputeNodeResponseFromPagedLog),
	"logPoint":                  arm(computeapi.NewComputeNodeResponseFromLogPoint),
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

// unsupportedComputeResponseArms marks the arms that must return a
// descriptive error. Update when an arm gains a real renderer.
var unsupportedComputeResponseArms = map[string]bool{
	"range": true, "rangesSummary": true, "rangeValue": true,
	"numericPoint": true, "singlePoint": true,
	"arrowEnum": true, "arrowBucketedEnum": true,
	"cartesian": true, "bucketedCartesian": true, "bucketedCartesian3d": true,
	"frequencyDomain": true, "frequencyDomainV2": true, "bucketedFrequencyDomain": true,
	"numericHistogram": true, "enumHistogram": true, "curveFit": true,
	"grouped": true, "array": true, "bucketedStruct": true, "fullResolution": true,
	"arrowBucketedMultivariate": true, "multivariate": true,
}

// Guard: a nominal-api-go bump that adds union arms must not ship with a nil
// handler. The union struct has one field per arm plus the "typ" discriminator.
func TestComputeResponseArmTableIsExhaustive(t *testing.T) {
	armCount := reflect.TypeOf(computeapi.ComputeNodeResponse{}).NumField() - 1
	if armCount != len(computeResponseArms) {
		t.Fatalf(
			"ComputeNodeResponse has %d arms but computeResponseArms covers %d. "+
				"A nominal-api-go bump added new arms: add non-nil handlers in "+
				"transformNominalResponseFromClient (never nil), then extend this table.",
			armCount, len(computeResponseArms),
		)
	}

	for name := range unsupportedComputeResponseArms {
		if _, ok := computeResponseArms[name]; !ok {
			t.Errorf("unsupported arm %q is missing from computeResponseArms; a misspelled key silently drops that arm's error assertion", name)
		}
	}
}

func TestComputeResponseArmHandling(t *testing.T) {
	for name, build := range computeResponseArms {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("arm %q panicked: %v (every AcceptFuncs handler must be non-nil)", name, r)
				}
			}()
			e := &NominalQueryExecution{}
			_, err := e.transformNominalResponseFromClient(build(), NominalQueryModel{})
			if !unsupportedComputeResponseArms[name] {
				return
			}
			if err == nil {
				t.Fatalf("expected an error for unsupported arm %q, got nil", name)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("compute response type %q is not supported by the plugin", name)) {
				t.Fatalf("error for arm %q should use the standard unsupported message, got: %v", name, err)
			}
		})
	}
}
