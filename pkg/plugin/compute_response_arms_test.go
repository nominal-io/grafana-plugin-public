package plugin

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

// computeResponseArms maps every known ComputeNodeResponse arm to a builder
// producing a valid instance of that arm. Optional-payload arms use nil on
// purpose: the dispatcher invokes their handlers even for nil payloads.
var computeResponseArms = map[string]func() computeapi.ComputeNodeResponse{
	"range": func() computeapi.ComputeNodeResponse { return computeapi.NewComputeNodeResponseFromRange(nil) },
	"rangesSummary": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromRangesSummary(computeapi.RangesSummary{})
	},
	"rangeValue": func() computeapi.ComputeNodeResponse { return computeapi.NewComputeNodeResponseFromRangeValue(nil) },
	"numeric": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromNumeric(computeapi.NumericPlot{})
	},
	"bucketedNumeric": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromBucketedNumeric(computeapi.BucketedNumericPlot{})
	},
	"numericPoint": func() computeapi.ComputeNodeResponse { return computeapi.NewComputeNodeResponseFromNumericPoint(nil) },
	"singlePoint":  func() computeapi.ComputeNodeResponse { return computeapi.NewComputeNodeResponseFromSinglePoint(nil) },
	"arrowNumeric": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromArrowNumeric(computeapi.ArrowNumericPlot{})
	},
	"arrowBucketedNumeric": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(computeapi.ArrowBucketedNumericPlot{})
	},
	"enum": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromEnum(computeapi.EnumPlot{})
	},
	"enumPoint": func() computeapi.ComputeNodeResponse { return computeapi.NewComputeNodeResponseFromEnumPoint(nil) },
	"bucketedEnum": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromBucketedEnum(computeapi.BucketedEnumPlot{})
	},
	"arrowEnum": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromArrowEnum(computeapi.ArrowEnumPlot{})
	},
	"arrowBucketedEnum": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromArrowBucketedEnum(computeapi.ArrowBucketedEnumPlot{})
	},
	"pagedLog": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromPagedLog(computeapi.PagedLogPlot{})
	},
	"logPoint": func() computeapi.ComputeNodeResponse { return computeapi.NewComputeNodeResponseFromLogPoint(nil) },
	"cartesian": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromCartesian(computeapi.CartesianPlot{})
	},
	"bucketedCartesian": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromBucketedCartesian(computeapi.BucketedCartesianPlot{})
	},
	"bucketedCartesian3d": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromBucketedCartesian3d(computeapi.BucketedCartesian3dPlot{})
	},
	"frequencyDomain": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromFrequencyDomain(computeapi.FrequencyDomainPlot{})
	},
	"frequencyDomainV2": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromFrequencyDomainV2(computeapi.FrequencyDomainPlotV2{})
	},
	"bucketedFrequencyDomain": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromBucketedFrequencyDomain(computeapi.BucketedFrequencyDomainPlot{})
	},
	"numericHistogram": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromNumericHistogram(computeapi.NumericHistogramPlot{})
	},
	"enumHistogram": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromEnumHistogram(computeapi.EnumHistogramPlot{})
	},
	"curveFit": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromCurveFit(computeapi.CurveFitResult{})
	},
	"grouped": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromGrouped(computeapi.GroupedComputeNodeResponses{})
	},
	"array": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromArray(computeapi.ArrowArrayPlot{})
	},
	"bucketedStruct": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromBucketedStruct(computeapi.ArrowBucketedStructPlot{})
	},
	"fullResolution": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromFullResolution(computeapi.ArrowFullResolutionPlot{})
	},
	"arrowBucketedMultivariate": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromArrowBucketedMultivariate(computeapi.ArrowBucketedMultivariatePlot{})
	},
	"multivariate": func() computeapi.ComputeNodeResponse {
		return computeapi.NewComputeNodeResponseFromMultivariate(computeapi.BucketedMultivariatePlot{})
	},
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
