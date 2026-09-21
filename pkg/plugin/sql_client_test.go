package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestSqlClientQueryContract(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/sql/v1/query" || r.Method != http.MethodPost {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("Accept") != "application/octet-stream" {
			t.Error("missing contract headers")
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte("stream"))
	}))
	defer srv.Close()
	r, err := newSqlClient(srv.URL+"/api", http.DefaultTransport).Query(context.Background(), "token", "workspace", "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, _ := io.ReadAll(r)
	if string(got) != "stream" || body["query"] != "SELECT 1" || body["workspace_rid"] != "workspace" || body["result_format"] != sqlArrowStreamFormat {
		t.Fatalf("body=%v stream=%q", body, got)
	}
	if _, ok := body["max_rows"]; ok {
		t.Fatal("max_rows sent")
	}
}

func TestSqlClientEndpointErrors(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   backend.Status
	}{{400, `{"errorName":"bad","errorInstanceId":"i","parameters":{"detail":"detail","sqlQueryId":"q"}}`, backend.StatusBadRequest}, {502, "not json", backend.StatusInternal}, {429, `{}`, backend.StatusTooManyRequests}} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		_, err := newSqlClient(srv.URL, nil).Query(context.Background(), "k", "w", "select")
		srv.Close()
		var endpoint *sqlEndpointError
		if !errors.As(err, &endpoint) || endpoint.backendStatus() != tc.want {
			t.Fatalf("%d: %v", tc.status, err)
		}
		if tc.status == 400 && endpoint.Error() != "detail (sqlQueryId: q)" {
			t.Fatal(endpoint.Error())
		}
	}
}

func TestSqlClientCancelsRequests(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-release }))
	defer srv.Close()
	defer close(release)
	client := newSqlClient(srv.URL, nil)
	if client.http.Timeout == 0 {
		t.Fatal("SQL requests need a finite timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.Query(ctx, "key", "workspace", "SELECT 1"); result <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not cancel")
	}
}
