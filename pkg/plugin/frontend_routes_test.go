package plugin

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

// Check the frontend's route manifest against the real backend router.
func TestFrontendResourceRoutesAreRegistered(t *testing.T) {
	body, err := os.ReadFile("../../src/resourceRoutes.json")
	if err != nil {
		t.Fatal(err)
	}
	var routes map[string]string
	if err := json.Unmarshal(body, &routes); err != nil {
		t.Fatal(err)
	}
	if len(routes) == 0 {
		t.Fatal("frontend resource route manifest is empty")
	}
	for name, path := range routes {
		t.Run(name, func(t *testing.T) {
			// GET returns 405 for registered POST routes, without calling Nominal.
			resp := callResourceAndCapture(t, &Datasource{}, &backend.CallResourceRequest{
				Path: path, Method: http.MethodGet,
			})
			if resp.Status != http.StatusMethodNotAllowed {
				t.Fatalf("frontend route %q returned %d, want 405 from a registered POST handler; add or fix its CallResource registration", path, resp.Status)
			}
		})
	}
}
