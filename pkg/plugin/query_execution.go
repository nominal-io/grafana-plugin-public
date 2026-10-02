package plugin

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nominal-io/grafana-plugin-public/pkg/models"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/uuid"
	"golang.org/x/sync/errgroup"
)

type NominalQueryExecution struct {
	datasource  *Datasource
	config      *models.PluginSettings
	skipShading bool
}

func newNominalQueryExecution(datasource *Datasource, config *models.PluginSettings) *NominalQueryExecution {
	return &NominalQueryExecution{
		datasource: datasource,
		config:     config,
	}
}

// Execute owns the Nominal query path after Grafana settings are loaded:
// preparation, planning, batch execution, and response rendering by RefID.
func (e *NominalQueryExecution) Execute(ctx context.Context, queries []backend.DataQuery) *backend.QueryDataResponse {
	response := backend.NewQueryDataResponse()

	var batchable, sqlQueries, shadeable []preparedQuery
	for _, q := range queries {
		prepared, prepErr := e.prepareQuery(ctx, q)
		if prepErr != nil {
			response.Responses[q.RefID] = *prepErr
			continue
		}

		switch prepared.Kind {
		case preparedQueryConnectionTest:
			response.Responses[q.RefID] = e.handleConnectionTestQuery(ctx)
		case preparedQueryBatchable:
			batchable = append(batchable, prepared)
			shadeable = append(shadeable, prepared)
		case preparedQueryAnswered:
			response.Responses[q.RefID] = *prepared.Response
			shadeable = append(shadeable, prepared)
		case preparedQuerySQL:
			sqlQueries = append(sqlQueries, prepared)
		case preparedQueryLegacy:
			response.Responses[q.RefID] = e.handleLegacyQuery(prepared.Model, q.TimeRange)
		}
	}

	var sqlResponses map[string]backend.DataResponse
	var wg sync.WaitGroup
	wg.Go(func() { sqlResponses = e.executeSQLQueries(ctx, sqlQueries) })
	maps.Copy(response.Responses, e.executePreparedBatches(ctx, batchable))
	if !e.skipShading {
		appendOutsideRunFrames(response, shadeable)
	}
	wg.Wait()
	maps.Copy(response.Responses, sqlResponses)

	return response
}

// maxConcurrentSQLQueries bounds the SQL requests that one Grafana request runs at a time.
const maxConcurrentSQLQueries = 8

func (e *NominalQueryExecution) executeSQLQueries(ctx context.Context, queries []preparedQuery) map[string]backend.DataResponse {
	results := make([]backend.DataResponse, len(queries))
	var g errgroup.Group
	g.SetLimit(maxConcurrentSQLQueries)
	for i, query := range queries {
		g.Go(func() error {
			results[i] = e.executeSQLQuery(ctx, query)
			return nil
		})
	}
	_ = g.Wait()
	responses := make(map[string]backend.DataResponse, len(queries))
	for i, query := range queries {
		responses[query.Query.RefID] = results[i]
	}
	return responses
}

type queryBatch struct {
	queries []backend.DataQuery
	models  []NominalQueryModel
}

func (b *queryBatch) add(prepared preparedQuery) {
	b.queries = append(b.queries, prepared.Query)
	b.models = append(b.models, prepared.Model)
}

func (e *NominalQueryExecution) executePreparedBatches(ctx context.Context, prepared []preparedQuery) map[string]backend.DataResponse {
	if len(prepared) == 0 {
		return nil
	}

	logBatch, otherBatch := partitionPreparedQueries(prepared)

	runBatch := func(label string, batch queryBatch) map[string]backend.DataResponse {
		if len(batch.queries) == 0 {
			return nil
		}
		log.DefaultLogger.Debug("Executing batch query", "partition", label, "count", len(batch.queries))
		return e.executeBatchQuery(ctx, batch)
	}

	var wg sync.WaitGroup
	var logResults, otherResults map[string]backend.DataResponse
	wg.Add(2)
	go func() {
		defer wg.Done()
		logResults = runBatch("log", logBatch)
	}()
	go func() {
		defer wg.Done()
		otherResults = runBatch("other", otherBatch)
	}()
	wg.Wait()

	results := make(map[string]backend.DataResponse, len(logResults)+len(otherResults))
	for refID, res := range logResults {
		results[refID] = res
	}
	for refID, res := range otherResults {
		results[refID] = res
	}
	return results
}

func partitionPreparedQueries(prepared []preparedQuery) (queryBatch, queryBatch) {
	var logBatch, otherBatch queryBatch
	for _, query := range prepared {
		if query.Model.ChannelDataType == ChannelDataTypeLog {
			logBatch.add(query)
		} else {
			otherBatch.add(query)
		}
	}
	return logBatch, otherBatch
}

func (e *NominalQueryExecution) executeBatchQuery(ctx context.Context, batch queryBatch) map[string]backend.DataResponse {
	results := make(map[string]backend.DataResponse)
	bearerToken := bearertoken.Token(e.config.Secrets.ApiKey)

	if len(batch.queries) != len(batch.models) {
		for _, q := range batch.queries {
			results[q.RefID] = backend.ErrDataResponse(
				backend.StatusInternal,
				"Batch query internal error: query/model count mismatch",
			)
		}
		return results
	}

	for chunkStart := 0; chunkStart < len(batch.queries); chunkStart += maxBatchComputeSubrequests {
		if ctx.Err() != nil {
			// The caller is gone; stop minting requestIDs for work that would
			// only fail client-side and enqueue kills the server never saw.
			errMsg := fmt.Sprintf("Batch compute cancelled: %v", ctx.Err())
			for _, q := range batch.queries[chunkStart:] {
				results[q.RefID] = backend.ErrDataResponse(backend.StatusInternal, errMsg)
			}
			break
		}

		chunkEnd := chunkStart + maxBatchComputeSubrequests
		if chunkEnd > len(batch.queries) {
			chunkEnd = len(batch.queries)
		}

		chunkQueries := batch.queries[chunkStart:chunkEnd]
		chunkModels := batch.models[chunkStart:chunkEnd]
		computeRequests := make([]computeapi1.ComputeNodeRequest, len(chunkModels))
		for i, qm := range chunkModels {
			computeRequests[i] = e.buildComputeRequest(qm, chunkQueries[i].TimeRange)
		}

		// A shared request ID lets one kill cancel the entire batch.
		requestID := uuid.NewUUID()
		for i := range computeRequests {
			computeRequests[i].RequestId = &requestID
		}

		batchRequest := computeapi1.BatchComputeWithUnitsRequest{
			Requests: computeRequests,
		}

		log.DefaultLogger.Debug(
			"Making batch compute API call",
			"chunkStart", chunkStart,
			"chunkEnd", chunkEnd,
			"queryCount", len(computeRequests),
		)

		batchResponse, err := e.datasource.computeService.BatchComputeWithUnits(ctx, bearerToken, batchRequest)
		// Kill only when nothing answered: Grafana cancelled the query or the connection dropped.
		if ctx.Err() != nil || (err != nil && extractErrorDetails(err).Status == 0) {
			ua, _ := userAgentComponentsFromContext(ctx)
			e.datasource.enqueueKill(requestID, killTarget{token: bearerToken, ua: ua})
		}
		if err != nil {
			logErrorWithConjureFields("Batch compute API call failed", err,
				"chunkStart", chunkStart, "chunkEnd", chunkEnd)
			errMsg := formatUserError("Batch compute failed", err)
			for _, q := range chunkQueries {
				results[q.RefID] = backend.ErrDataResponse(backend.StatusInternal, errMsg)
			}
			continue
		}

		log.DefaultLogger.Debug(
			"Batch compute successful",
			"chunkStart", chunkStart,
			"chunkEnd", chunkEnd,
			"resultCount", len(batchResponse.Results),
		)

		for i, q := range chunkQueries {
			if i >= len(batchResponse.Results) {
				results[q.RefID] = backend.ErrDataResponse(
					backend.StatusInternal,
					"Missing result in batch response",
				)
				continue
			}

			response := e.transformBatchResult(batchResponse.Results[i], chunkModels[i])
			if notice := chunkModels[i].Points.Notice; notice != "" && len(response.Frames) > 0 {
				response.Frames[0].AppendNotices(data.Notice{Severity: data.NoticeSeverityInfo, Text: notice})
			}
			results[q.RefID] = response
		}
	}

	return results
}

func (e *NominalQueryExecution) handleConnectionTestQuery(ctx context.Context) backend.DataResponse {
	var response backend.DataResponse

	log.DefaultLogger.Debug("Processing connectionTest query")

	bearerToken := bearertoken.Token(e.config.Secrets.ApiKey)
	profile, err := e.datasource.authService.GetMyProfile(ctx, bearerToken)
	if err != nil {
		logErrorWithConjureFields("Connection test failed", err)
		message, _ := classifyConnectionError(err)
		return backend.ErrDataResponse(backend.StatusInternal, message)
	}

	log.DefaultLogger.Debug("Connection test successful", "profileRid", profile.Rid)

	frame := data.NewFrame("connectionTest")
	frame.Fields = append(frame.Fields,
		data.NewField("status", nil, []string{"success"}),
		data.NewField("message", nil, []string{"Successfully connected to Nominal API"}),
	)

	response.Frames = append(response.Frames, frame)
	return response
}

// handleLegacyQuery handles legacy queries that don't have asset/channel
func (e *NominalQueryExecution) handleLegacyQuery(qm NominalQueryModel, timeRange backend.TimeRange) backend.DataResponse {
	var response backend.DataResponse

	log.DefaultLogger.Debug("Using legacy query support")

	frame := data.NewFrame("response")
	frame.Fields = append(frame.Fields,
		data.NewField("time", nil, []time.Time{timeRange.From, timeRange.To}),
		data.NewField("values", nil, []float64{qm.Constant, qm.Constant + 10}),
	)

	response.Frames = append(response.Frames, frame)
	return response
}

const runNoticeTimeFormat = "2006-01-02 15:04:05"

// runWindowNotice explains why a run query outside the run is not sent to compute.
func runWindowNotice(run *RunResponse, timeRange backend.TimeRange) string {
	start := run.StartTime
	if run.EndTime == nil {
		if timeRange.To.After(start) {
			return ""
		}
		return fmt.Sprintf("The time range ends before run %s starts at %s UTC. Use Snap to run on the shaded strip (Grafana 12.3 and later) or in the query editor.", run.Title, start.Format(runNoticeTimeFormat))
	}
	end := *run.EndTime
	if timeRange.To.After(start) && timeRange.From.Before(end) {
		return ""
	}
	return fmt.Sprintf("The time range does not overlap run %s (%s to %s UTC). Use Snap to run on the shaded strip (Grafana 12.3 and later) or in the query editor.", run.Title, start.Format(runNoticeTimeFormat), end.Format(runNoticeTimeFormat))
}

const outsideRunColor = "rgba(128, 128, 128, 0.15)"

// appendOutsideRunFrames adds one shading frame per run, on the first
// successful query that uses it, so shared runs are not shaded twice.
func appendOutsideRunFrames(response *backend.QueryDataResponse, prepared []preparedQuery) {
	seen := map[string]bool{}
	for _, p := range prepared {
		run := p.Model.Run
		res, ok := response.Responses[p.Query.RefID]
		if run == nil || seen[run.Rid] || !ok || res.Error != nil {
			continue
		}
		if frame := outsideRunFrame(run, p.Query.TimeRange); frame != nil {
			res.Frames = append(res.Frames, frame)
			response.Responses[p.Query.RefID] = res
		}
		seen[run.Rid] = true
	}
}

// outsideRunFrame shades the parts of the time range outside the run. Grafana
// draws annotation-topic regions on time series panels without dashboard setup.
func outsideRunFrame(run *RunResponse, timeRange backend.TimeRange) *data.Frame {
	var from, to []time.Time
	if timeRange.From.Before(run.StartTime) {
		from, to = append(from, timeRange.From), append(to, minTime(run.StartTime, timeRange.To))
	}
	if run.EndTime != nil && timeRange.To.After(*run.EndTime) {
		from, to = append(from, maxTime(*run.EndTime, timeRange.From)), append(to, timeRange.To)
	}
	if len(from) == 0 {
		return nil
	}
	text := make([]string, len(from))
	color := make([]string, len(from))
	isRegion := make([]bool, len(from))
	for i := range from {
		text[i], color[i], isRegion[i] = "Outside run "+run.Title, outsideRunColor, true
	}
	frame := data.NewFrame("outside-run",
		data.NewField("time", nil, from),
		data.NewField("timeEnd", nil, to),
		data.NewField("isRegion", nil, isRegion),
		data.NewField("text", nil, text).SetConfig(&data.FieldConfig{
			Links: []data.DataLink{{Title: "Snap to run " + run.Title, URL: snapToRunURL(run)}},
		}),
		data.NewField("color", nil, color),
	)
	frame.Meta = &data.FrameMeta{DataTopic: data.DataTopicAnnotations}
	return frame
}

// snapToRunURL keeps the dashboard and every parameter except the time range.
// With no other parameters ${__url.params:exclude:...} is "?", so the result
// reads "?&from=...", which Grafana parses.
func snapToRunURL(run *RunResponse) string {
	to := "now"
	if run.EndTime != nil {
		to = strconv.FormatInt(run.EndTime.UnixMilli(), 10)
	}
	return fmt.Sprintf("${__url.path}${__url.params:exclude:from,to}&from=%d&to=%s", run.StartTime.UnixMilli(), to)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
