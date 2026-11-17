package plugin

import (
	"context"
	"encoding/json"
	"html"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	"github.com/nominal-io/nominal-api-go/io/nominal/event"
	rids "github.com/nominal-io/nominal-api-go/scout/rids/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

const eventPageSize = 1000

// buildEventSearch matches events that overlap the time range, on any of the model's assets.
func buildEventSearch(qm NominalQueryModel, assets []rids.AssetRid, timeRange backend.TimeRange) event.SearchEventsRequest {
	filters := []event.SearchQuery{
		event.NewSearchQueryFromAssets(event.AssetsFilter{Operator: api.New_SetOperator(api.SetOperator_OR), Assets: assets}),
		event.NewSearchQueryFromAdvancedTimeFilter(event.EventTimeFilter{
			Timestamp:          timestampFromTime(timeRange.To),
			TimestampCondition: event.New_EventTimeFilterCondition(event.EventTimeFilterCondition_START_TIME_BEFORE_INCLUSIVE),
		}),
		event.NewSearchQueryFromAdvancedTimeFilter(event.EventTimeFilter{
			Timestamp:          timestampFromTime(timeRange.From),
			TimestampCondition: event.New_EventTimeFilterCondition(event.EventTimeFilterCondition_END_TIME_AFTER_INCLUSIVE),
		}),
	}
	if len(qm.EventTypes) > 0 {
		types := make([]event.EventType, len(qm.EventTypes))
		for i, t := range qm.EventTypes {
			types[i] = event.New_EventType(event.EventType_Value(t))
		}
		filters = append(filters, event.NewSearchQueryFromEventTypes(types))
	}
	if len(qm.EventLabels) > 0 {
		labels := make([]api.Label, len(qm.EventLabels))
		for i, l := range qm.EventLabels {
			labels[i] = api.Label(l)
		}
		filters = append(filters, event.NewSearchQueryFromLabels(rids.LabelsFilter{Operator: api.New_SetOperator(api.SetOperator_OR), Labels: labels}))
	}
	return event.SearchEventsRequest{
		Sort:     event.SortOptions{IsDescending: true, Field: event.New_SortField(event.SortField_START_TIME)},
		PageSize: eventPageSize,
		Query:    event.NewSearchQueryFromAnd(filters),
	}
}

// eventAnnotationFrame shapes events as a Grafana annotation frame. Text is rendered as HTML, so
// event text is escaped. A line naming each event asset found in assetTitles leads the text.
func eventAnnotationFrame(events []event.Event, assetTitles map[rids.AssetRid]string) *data.Frame {
	n := len(events)
	times, timeEnds := make([]time.Time, n), make([]time.Time, n)
	titles, texts, tags := make([]string, n), make([]string, n), make([]json.RawMessage, n)
	for i, ev := range events {
		times[i] = time.Unix(int64(ev.Timestamp.Seconds), int64(ev.Timestamp.Nanos))
		timeEnds[i] = times[i].Add(time.Duration(ev.Duration.Seconds)*time.Second + time.Duration(ev.Duration.Nanos))
		titles[i] = html.EscapeString(ev.Name)

		var text strings.Builder
		for _, assetRid := range ev.AssetRids {
			if title, ok := assetTitles[assetRid]; ok {
				text.WriteString("Asset: " + html.EscapeString(title) + "<br />")
			}
		}
		text.WriteString(strings.ReplaceAll(html.EscapeString(ev.Description), "\n", "<br />"))
		texts[i] = text.String()

		eventTags := make([]string, 0, len(ev.Labels)+1)
		for _, label := range ev.Labels {
			eventTags = append(eventTags, string(label))
		}
		eventTags = append(eventTags, ev.Type.String())
		tags[i], _ = json.Marshal(eventTags)
	}
	return data.NewFrame("events",
		data.NewField("time", nil, times),
		data.NewField("timeEnd", nil, timeEnds),
		data.NewField("title", nil, titles),
		data.NewField("text", nil, texts),
		data.NewField("tags", nil, tags),
	)
}

// eventAssetTitles looks up the title of each asset in one request. If the lookup fails, no titles
// are returned.
func (e *NominalQueryExecution) eventAssetTitles(ctx context.Context, assetRids []rids.AssetRid) map[rids.AssetRid]string {
	ridStrings := make([]string, len(assetRids))
	for i, assetRid := range assetRids {
		ridStrings[i] = rid.ResourceIdentifier(assetRid).String()
	}
	assets, err := e.datasource.nominalCatalog.fetchAssetsByRidUncached(ctx, e.config, ridStrings)
	titles := make(map[rids.AssetRid]string, len(assetRids))
	if err != nil {
		return titles
	}
	for i, assetRid := range assetRids {
		if asset, ok := assets[ridStrings[i]]; ok {
			titles[assetRid] = asset.Title
		}
	}
	return titles
}

func (e *NominalQueryExecution) executeEventQuery(ctx context.Context, prepared preparedQuery) (response backend.DataResponse) {
	defer recoverQuery(ctx, "Event query", &response)
	request := buildEventSearch(prepared.Model, prepared.Assets, prepared.Query.TimeRange)
	token := bearertoken.Token(e.config.Secrets.AuthToken)
	result, err := e.datasource.eventService.SearchEvents(ctx, token, request)
	if err != nil {
		logErrorWithConjureFields("Event search failed", err)
		return backend.ErrDataResponseWithSource(backend.Status(upstreamStatus(err)), backend.ErrorSourceDownstream, formatUserError("Event search failed", err))
	}
	var assetTitles map[rids.AssetRid]string
	if len(prepared.Assets) > 1 {
		assetTitles = e.eventAssetTitles(ctx, prepared.Assets)
	}
	frame := eventAnnotationFrame(result.Results, assetTitles)
	frame.RefID = prepared.Query.RefID
	return backend.DataResponse{Frames: data.Frames{frame}}
}
