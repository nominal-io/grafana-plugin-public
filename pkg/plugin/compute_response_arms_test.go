package plugin

import (
	"strings"
	"testing"

	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
)

// A valid grouped response must produce an unsupported-type error, not a
// nil-handler panic that kills the plugin process.
func TestTransformNominalResponseGroupedReturnsErrorNotPanic(t *testing.T) {
	e := &NominalQueryExecution{}
	response := computeapi.NewComputeNodeResponseFromGrouped(
		computeapi.GroupedComputeNodeResponses{},
	)

	_, err := e.transformNominalResponseFromClient(response, NominalQueryModel{})

	if err == nil {
		t.Fatal("expected an error for a grouped response, got nil")
	}
	if !strings.Contains(err.Error(), "grouped") {
		t.Fatalf("error should name the response type, got: %v", err)
	}
}
