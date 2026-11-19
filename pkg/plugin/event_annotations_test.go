package plugin

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/event"
	rids "github.com/nominal-io/nominal-api-go/scout/rids/api"
	runapi "github.com/nominal-io/nominal-api-go/scout/run/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
	"github.com/palantir/pkg/safelong"
)

const (
	testEventAssetA = "ri.scout.main.asset.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	testEventAssetB = "ri.scout.main.asset.bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
)

type fakeEventService struct {
	event.EventServiceClient
	events []event.Event
	err    error
}

func (f *fakeEventService) SearchEvents(context.Context, bearertoken.Token, event.SearchEventsRequest) (event.SearchEventsResponse, error) {
	return event.SearchEventsResponse{Results: f.events}, f.err
}

func testEventAssetRid(t *testing.T, s string) rids.AssetRid {
	t.Helper()
	parsed, err := rid.ParseRID(s)
	if err != nil {
		t.Fatal(err)
	}
	return rids.AssetRid(parsed)
}

func testEvent(t *testing.T, name, description string, durationSeconds int64, assets ...string) event.Event {
	t.Helper()
	ev := event.Event{
		Name:        name,
		Description: description,
		Timestamp:   testTimestamp(1_700_000_000),
		Duration:    runapi.Duration{Seconds: safelong.SafeLong(durationSeconds)},
		Type:        event.New_EventType(event.EventType_FLAG),
		Labels:      []api.Label{"a,b", "c"},
	}
	for _, a := range assets {
		ev.AssetRids = append(ev.AssetRids, testEventAssetRid(t, a))
	}
	return ev
}

func TestEventAnnotationFrame(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	titles := map[string]string{testEventAssetA: "Asset <A>", testEventAssetB: "Asset B"}
	for _, tc := range []struct {
		name       string
		event      event.Event
		wantTitle  string
		titles     map[string]string
		wantEnd    time.Time
		wantText   string
		wantTagsJS string
	}{
		{name: "fields and duration", event: testEvent(t, "Launch", "plain", 30), wantEnd: start.Add(30 * time.Second), wantText: "plain", wantTagsJS: `["a,b","c","FLAG"]`},
		{name: "unknown type keeps its name", event: func() event.Event {
			ev := testEvent(t, "Launch", "plain", 0)
			ev.Type = event.New_EventType(event.EventType_Value("NEW_KIND"))
			return ev
		}(), wantEnd: start, wantText: "plain", wantTagsJS: `["a,b","c","NEW_KIND"]`},
		{name: "escaping", event: testEvent(t, "<i>Launch</i>", "<b>x</b> & y", 0), wantTitle: "&lt;i&gt;Launch&lt;/i&gt;", wantEnd: start, wantText: "&lt;b&gt;x&lt;/b&gt; &amp; y", wantTagsJS: `["a,b","c","FLAG"]`},
		{name: "newlines", event: testEvent(t, "Launch", "one\ntwo", 0), wantEnd: start, wantText: "one<br />two", wantTagsJS: `["a,b","c","FLAG"]`},
		{name: "no titles", event: testEvent(t, "Launch", "d", 0, testEventAssetA), wantEnd: start, wantText: "d", wantTagsJS: `["a,b","c","FLAG"]`},
		{name: "asset line", event: testEvent(t, "Launch", "d", 0, testEventAssetA), titles: titles, wantEnd: start, wantText: "Asset: Asset &lt;A&gt;<br />d", wantTagsJS: `["a,b","c","FLAG"]`},
		{name: "line per matching asset", event: testEvent(t, "Launch", "d", 0, testEventAssetA, "ri.scout.main.asset.cccccccc-cccc-cccc-cccc-cccccccccccc", testEventAssetB), titles: titles, wantEnd: start, wantText: "Asset: Asset &lt;A&gt;<br />Asset: Asset B<br />d", wantTagsJS: `["a,b","c","FLAG"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := eventAnnotationFrame([]event.Event{tc.event}, tc.titles)
			if len(frame.Fields) != 5 {
				t.Fatalf("got %d fields, want 5", len(frame.Fields))
			}
			if got := frame.Fields[0].At(0).(time.Time); !got.Equal(start) {
				t.Errorf("time = %v, want %v", got, start)
			}
			if got := frame.Fields[1].At(0).(time.Time); !got.Equal(tc.wantEnd) {
				t.Errorf("timeEnd = %v, want %v", got, tc.wantEnd)
			}
			wantTitle := cmp.Or(tc.wantTitle, "Launch")
			if got := frame.Fields[2].At(0); got != wantTitle {
				t.Errorf("title = %v, want %v", got, wantTitle)
			}
			if got := frame.Fields[3].At(0); got != tc.wantText {
				t.Errorf("text = %q, want %q", got, tc.wantText)
			}
			if got := string(frame.Fields[4].At(0).(json.RawMessage)); got != tc.wantTagsJS {
				t.Errorf("tags = %s, want %s", got, tc.wantTagsJS)
			}
		})
	}
}

func TestBuildEventSearch(t *testing.T) {
	timeRange := backend.TimeRange{From: time.Unix(1000, 0), To: time.Unix(2000, 0)}
	const (
		assetsFilter = `{"type":"assets","assets":{"operator":"OR","assets":["` + testEventAssetA + `","` + testEventAssetB + `"]}}`
		timeFilters  = `{"type":"advancedTimeFilter","advancedTimeFilter":{"timestamp":{"seconds":2000,"nanos":0},"timestampCondition":"START_TIME_BEFORE_INCLUSIVE"}},` +
			`{"type":"advancedTimeFilter","advancedTimeFilter":{"timestamp":{"seconds":1000,"nanos":0},"timestampCondition":"END_TIME_AFTER_INCLUSIVE"}}`
	)
	for _, tc := range []struct {
		name    string
		model   NominalQueryModel
		wantAnd string
	}{
		{name: "no optional filters", model: NominalQueryModel{AssetRids: []string{testEventAssetA, testEventAssetB}}, wantAnd: assetsFilter + "," + timeFilters},
		{
			name:    "types and labels",
			model:   NominalQueryModel{AssetRids: []string{testEventAssetA, testEventAssetB}, EventTypes: []string{"FLAG", "ERROR"}, EventLabels: []string{"x", "y,z"}},
			wantAnd: assetsFilter + "," + timeFilters + `,{"type":"eventTypes","eventTypes":["FLAG","ERROR"]},{"type":"labels","labels":{"operator":"OR","labels":["x","y,z"]}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assets := []rids.AssetRid{testEventAssetRid(t, testEventAssetA), testEventAssetRid(t, testEventAssetB)}
			request := buildEventSearch(tc.model, assets, timeRange)
			if request.PageSize == 0 || !request.Sort.IsDescending || request.Sort.Field.Value() != event.SortField_START_TIME {
				t.Errorf("page size %d, sort %+v", request.PageSize, request.Sort)
			}
			raw, err := json.Marshal(request.Query)
			if err != nil {
				t.Fatal(err)
			}
			if want := `{"type":"and","and":[` + tc.wantAnd + `]}`; string(raw) != want {
				t.Errorf("query = %s\nwant    %s", raw, want)
			}
		})
	}
}

func TestExecuteEventQueries(t *testing.T) {
	assetServer := newTestAssetServer(t, map[string]SingleAssetResponse{testEventAssetA: {Rid: testEventAssetA, Title: "Asset A"}}, nil)
	defer assetServer.Close()
	events := &fakeEventService{events: []event.Event{testEvent(t, "Launch", "d", 0, testEventAssetA)}}
	ds := newTestDatasource(assetServer.URL, nil, nil)
	ds.eventService = events
	e := newTestQueryExecution(ds, nil)
	e.config.BaseUrl = assetServer.URL
	workspace := sqlTestWorkspaceRid(t)
	ds.workspaceRid = &workspace
	ds.sqlService = sqlQueryService(t, sqlPayloadServer(sqlValueStream(t, 1)))

	timeRange := backend.TimeRange{From: time.Unix(1000, 0), To: time.Unix(2000, 0)}
	query := func(refID, js string) backend.DataQuery {
		return backend.DataQuery{RefID: refID, TimeRange: timeRange, JSON: []byte(js)}
	}
	response := e.Execute(context.Background(), []backend.DataQuery{
		query("E", `{"queryType":"events","assetRids":["`+testEventAssetA+`","`+testEventAssetB+`"]}`),
		query("S", `{"queryType":"sql","rawSql":"SELECT 1","format":"table"}`),
		query("N", `{"queryType":"events","assetRids":[]}`),
		query("I", `{"queryType":"events","assetRids":["not a rid"]}`),
	})

	if r := response.Responses["E"]; r.Error != nil || len(r.Frames) != 1 || r.Frames[0].Fields[3].At(0) != "Asset: Asset A<br />d" {
		t.Errorf("events response = %+v", r)
	}
	if r := response.Responses["S"]; r.Error != nil || len(r.Frames) == 0 {
		t.Errorf("sql response = %+v", r)
	}
	if r := response.Responses["N"]; r.Status != backend.StatusBadRequest || r.ErrorSource != backend.ErrorSourceDownstream {
		t.Errorf("empty asset list response = %+v", r)
	}
	if r := response.Responses["I"]; r.Status != backend.StatusBadRequest || r.ErrorSource != backend.ErrorSourceDownstream {
		t.Errorf("invalid asset RID response = %+v", r)
	}

	events.err = errors.New("boom")
	failed := e.Execute(context.Background(), []backend.DataQuery{query("E", `{"queryType":"events","assetRids":["`+testEventAssetA+`"]}`)})
	if r := failed.Responses["E"]; r.Error == nil || r.ErrorSource != backend.ErrorSourceDownstream {
		t.Errorf("failed search response = %+v", r)
	}
}
