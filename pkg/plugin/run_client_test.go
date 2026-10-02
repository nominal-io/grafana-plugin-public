package plugin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/rid"
)

// The Nominal API answers a deleted or inaccessible run with a RunNotFound
// error rather than leaving it out of the map.
func TestGetRunsTreatsRunNotFoundAsMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errorCode":"NOT_FOUND","errorName":"Scout:RunNotFound","errorInstanceId":"00000000-0000-0000-0000-000000000000","parameters":{"runRid":"ri.scout.main.run.1"}}`))
	}))
	defer server.Close()
	client, err := conjurehttpclient.NewClient(conjurehttpclient.WithBaseURLs([]string{server.URL}))
	if err != nil {
		t.Fatal(err)
	}
	runRid := runapi.RunRid(rid.MustNew("scout", "main", "run", "1"))

	runs, err := newRunAPIClient(client).GetRuns(context.Background(), "token", []runapi.RunRid{runRid})
	if err != nil || len(runs) != 0 {
		t.Fatalf("GetRuns = %v, %v; want an empty map and no error", runs, err)
	}
}
