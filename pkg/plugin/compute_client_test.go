package plugin

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	computeapi "github.com/nominal-io/nominal-api-go/scout/compute/api"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	conjurehttpclient "github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/codecs"
	"github.com/palantir/pkg/safejson"
)

var testNoUnit = computeapi.NewComputeUnitResultFromSingle(computeapi.NewUnitResultFromNoUnitAvailable([]computeapi.UnitComputationError{}))

// computeResponseBody marshals results as the server would send them. Each
// result gets a unit result, because a zero ComputeUnitResult does not marshal.
func computeResponseBody(tb testing.TB, results ...computeapi.ComputeWithUnitsResult) []byte {
	tb.Helper()
	for i := range results {
		results[i].UnitResult = testNoUnit
	}
	body, err := safejson.Marshal(computeapi.BatchComputeWithUnitsResponse{Results: results})
	if err != nil {
		tb.Fatal(err)
	}
	return body
}

func successResult(response computeapi.ComputeNodeResponse) computeapi.ComputeWithUnitsResult {
	return computeapi.ComputeWithUnitsResult{ComputeResult: computeapi.NewComputeNodeResultFromSuccess(response)}
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
		// cut drops the final chunk, so reading past the body fails.
		cut bool
		// userMessage is the plugin client's message where it differs from the
		// generated client's.
		userMessage string
	}{
		{name: "success", status: http.StatusOK, body: success},
		{name: "stream breaks after the body", status: http.StatusOK, body: success, cut: true},
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
				if tc.cut {
					conn, buf, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n", len(tc.body), tc.body)
					_ = buf.Flush()
					return
				}
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

type computeDecoderCase struct {
	name string
	body []byte
	// fast requires every result to decode without the generated decoder.
	fast bool
}

func computeDecoderCases(tb testing.TB) []computeDecoderCase {
	tb.Helper()
	unit, err := safejson.Marshal(testNoUnit)
	if err != nil {
		tb.Fatal(err)
	}
	success := func(response string) string {
		return `{"computeResult":{"type":"success","success":` + response + `},"unitResult":` + string(unit) + `}`
	}
	batch := func(results ...string) []byte {
		return []byte(`{"results":[` + strings.Join(results, ",") + `]}`)
	}
	arrow := success(`{"type":"arrowBucketedNumeric","arrowBucketedNumeric":{"arrowBinary":"AQID"}}`)

	token := computeapi.NewPageTokenFromTimestampAndId(computeapi.TimestampAndId{Timestamp: testTimestamp(1704067200)})
	paged := successResult(computeapi.NewComputeNodeResponseFromArrowBucketedNumeric(
		computeapi.ArrowPlot{ArrowBinary: []byte{1, 2, 3}, GroupByKeys: &[]string{"vehicle"}, NextPageToken: &token}))
	fixture := computeResponseBody(tb, createMockArrowComputeResult([]float64{1, 2, 3}))

	return []computeDecoderCase{
		{"arrow", fixture, true},
		{"arrow with group keys and page token", computeResponseBody(tb, paged), true},
		{"mixed with error results", computeResponseBody(tb,
			createMockArrowComputeResult([]float64{1}), createMockErrorResult(500, "INTERNAL"), createMockArrowComputeResult([]float64{2})), false},
		{"arm null", batch(success(`{"type":"arrowBucketedNumeric","arrowBucketedNumeric":null}`)), false},
		{"result type is not success", batch(`{"computeResult":{"type":"error","success":{"type":"arrowBucketedNumeric","arrowBucketedNumeric":{"arrowBinary":"AQID"}}},"unitResult":` + string(unit) + `}`), false},
		{"type names another arm", batch(success(`{"type":"enum","arrowBucketedNumeric":{"arrowBinary":"AQID"}}`)), false},
		{"two arms", batch(success(`{"type":"pagedLog","arrowBucketedNumeric":{"arrowBinary":"AQID"},"pagedLog":{"timestamps":[],"values":[]}}`)), false},
		{"unhandled second arm", batch(success(`{"type":"arrowBucketedNumeric","arrowBucketedNumeric":{"arrowBinary":"AQID"},"numericPoint":{"timestamp":{"seconds":1,"nanos":0},"value":1}}`)), false},
		{"key differs in case", batch(success(`{"type":"arrowBucketedNumeric","ArrowBucketedNumeric":{"arrowBinary":"AQID"}}`)), false},
		{"duplicate key", batch(success(`{"type":"arrowBucketedNumeric","arrowBucketedNumeric":{"arrowBinary":"AQID","arrowBinary":"BAUG"}}`)), false},
		{"missing unit result", batch(`{"computeResult":{"type":"success","success":{"type":"arrowBucketedNumeric","arrowBucketedNumeric":{"arrowBinary":"AQID"}}}}`), false},
		{"null results", []byte(`{"results":null}`), false},
		{"null body", []byte(`null`), false},
		{"truncated body", fixture[:len(fixture)/2], false},
		{"trailing bytes", append(append([]byte{}, fixture...), `{}`...), false},
		{"result that fails to decode", batch(arrow, `5`), false},
	}
}

// assertSameDecode requires the new decoder to return what the generated
// decoder returns for body.
func assertSameDecode(t *testing.T, body []byte) {
	t.Helper()
	var want, got *computeapi.BatchComputeWithUnitsResponse
	wantErr := codecs.JSON.Decode(bytes.NewReader(body), &want)
	gotErr := computeResponseDecoder{codecs.JSON}.Decode(bytes.NewReader(body), &got)
	if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
		t.Fatalf("error = %v, want %v", gotErr, wantErr)
	}
	if wantErr == nil && !reflect.DeepEqual(got, want) {
		t.Fatalf("response = %+v, want %+v", got, want)
	}
}

func TestComputeResponseDecoderMatchesGenerated(t *testing.T) {
	for _, tc := range computeDecoderCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			assertSameDecode(t, tc.body)
			if !tc.fast {
				return
			}
			var top struct {
				Results []jsontext.Value `json:"results"`
			}
			if err := jsonv2.Unmarshal(tc.body, &top); err != nil {
				t.Fatal(err)
			}
			for i, raw := range top.Results {
				if _, err := decodeComputeResult(raw, computeDecodeOptions); err != nil {
					t.Errorf("result %d left to the generated decoder: %v", i, err)
				}
			}
		})
	}
}

func TestRedactDecodeError(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		kept bool
	}{
		{"unknown member", `{"computeResult":{"type":"success","success":{"type":"grouped","grouped":{}}}}`, true},
		{"duplicate key", `{"computeResult":{"type":"success","success":{"type":"x","type":"secret"}}}`, false},
	} {
		_, err := decodeComputeResult([]byte(tc.raw), computeDecodeOptions)
		if got := redactDecodeError(err); (got == err) != tc.kept {
			t.Errorf("%s: redactDecodeError(%v) = %v", tc.name, err, got)
		}
	}
}

func FuzzComputeResponseDecoder(f *testing.F) {
	for _, tc := range computeDecoderCases(f) {
		f.Add(tc.body)
	}
	f.Fuzz(assertSameDecode)
}
