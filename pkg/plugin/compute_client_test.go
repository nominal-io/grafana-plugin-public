package plugin

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/safejson"
)

// computeResponseBody marshals results as the server would send them. Each
// result gets a unit result, because a zero ComputeUnitResult does not marshal.
func computeResponseBody(tb testing.TB, results ...computeapi.ComputeWithUnitsResult) []byte {
	tb.Helper()
	for i := range results {
		results[i].UnitResult = computeapi.NewComputeUnitResultFromSingle(computeapi.NewUnitResultFromNoUnitAvailable([]computeapi.UnitComputationError{}))
	}
	body, err := safejson.Marshal(computeapi.BatchComputeWithUnitsResponse{Results: results})
	if err != nil {
		tb.Fatal(err)
	}
	return body
}

func TestComputeClientMatchesGenerated(t *testing.T) {
	const instanceID = "11111111-2222-3333-4444-555555555555"
	conjureError := func(parameters string) []byte {
		return []byte(`{"errorCode":"INVALID_ARGUMENT","errorName":"Compute:ChannelHasWrongType","errorInstanceId":"` + instanceID + `","parameters":` + parameters + `}`)
	}
	success := computeResponseBody(t, createMockArrowComputeResult([]float64{1, 2, 3}))
	cases := []struct {
		name   string
		status int
		body   []byte
		// userMessage is the plugin client's message where it differs from the
		// generated client's.
		userMessage string
	}{
		{name: "success", status: http.StatusOK, body: success},
		{name: "registered Conjure error", status: http.StatusBadRequest, body: conjureError(`{}`)},
		// The generated client cannot decode these parameters into the typed
		// error, so it reports only the status.
		{name: "registered Conjure error with string parameters", status: http.StatusBadRequest,
			body:        conjureError(`{"seriesType":"STRING","validTypes":"[DOUBLE, ENUM]"}`),
			userMessage: "Batch compute failed: INVALID_ARGUMENT Compute:ChannelHasWrongType (errorInstanceId: " + instanceID + ")"},
	}
	request := computeapi1.BatchComputeWithUnitsRequest{Requests: []computeapi1.ComputeNodeRequest{
		newTestQueryExecution(withCatalog(&Datasource{}), nil).buildComputeRequest(
			NominalQueryModel{AssetRid: "ri.nominal.asset.12345", Channel: "temperature", DataScopeName: "default", Buckets: 100},
			backend.TimeRange{From: time.Unix(1704067200, 0), To: time.Unix(1704070800, 0)}),
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var requests [][]byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				dump, err := httputil.DumpRequest(r, true)
				if err != nil {
					t.Error(err)
				}
				mu.Lock()
				requests = append(requests, dump)
				mu.Unlock()

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write(tc.body)
			}))
			defer srv.Close()
			client, err := conjurehttpclient.NewClient(conjurehttpclient.WithBaseURLs([]string{srv.URL}))
			if err != nil {
				t.Fatal(err)
			}
			call := func(svc computeapi1.ComputeServiceClient) (computeapi.BatchComputeWithUnitsResponse, error) {
				return svc.BatchComputeWithUnits(context.Background(), "token", request)
			}

			want, wantErr := call(computeapi1.NewComputeServiceClient(client))
			got, gotErr := call(newComputeClient(client))

			mu.Lock()
			if len(requests) != 2 || !bytes.Equal(requests[0], requests[1]) {
				t.Errorf("requests = %q, want one identical request from each client", requests)
			}
			mu.Unlock()
			if !reflect.DeepEqual(got, want) {
				t.Errorf("response = %+v, want %+v", got, want)
			}
			if (gotErr == nil) != (wantErr == nil) {
				t.Fatalf("error = %v, want %v", gotErr, wantErr)
			}
			if tc.userMessage != "" {
				if g := formatUserError("Batch compute failed", gotErr); g != tc.userMessage {
					t.Errorf("user message = %q, want %q", g, tc.userMessage)
				}
				return
			}
			if wantErr == nil {
				return
			}
			if g, w := gotErr.Error(), wantErr.Error(); g != w {
				t.Errorf("error text = %q, want %q", g, w)
			}
			if g, w := extractErrorDetails(gotErr), extractErrorDetails(wantErr); g != w {
				t.Errorf("error details = %+v, want %+v", g, w)
			}
			if g, w := formatUserError("Batch compute failed", gotErr), formatUserError("Batch compute failed", wantErr); g != w {
				t.Errorf("user message = %q, want %q", g, w)
			}
		})
	}
}
