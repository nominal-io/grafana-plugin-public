package plugin

import (
	"context"
	"fmt"
	"sync"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nominal-inc/nominal-ds/pkg/models"
	computeapi1 "github.com/nominal-io/nominal-api-go/scout/compute/api1"
	"github.com/palantir/pkg/bearertoken"
)

type NominalQueryExecution struct {
	datasource *Datasource
	config     *models.PluginSettings
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

	var batchable []preparedQuery
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
		case preparedQueryLegacy:
			response.Responses[q.RefID] = e.handleLegacyQuery(prepared.Model, q.TimeRange)
		}
	}

	for refID, res := range e.executePreparedBatches(ctx, batchable) {
		response.Responses[refID] = res
	}

	return response
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
		if query.Model.ChannelDataType == "log" {
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
		chunkEnd := chunkStart + maxBatchComputeSubrequests
		if chunkEnd > len(batch.queries) {
			chunkEnd = len(batch.queries)
		}

		chunkQueries := batch.queries[chunkStart:chunkEnd]
		chunkModels := batch.models[chunkStart:chunkEnd]
		computeRequests := make([]computeapi1.ComputeNodeRequest, len(chunkModels))
		for i, qm := range chunkModels {
			computeRequests[i] = e.buildComputeRequest(qm, chunkQueries[i].TimeRange, chunkQueries[i].MaxDataPoints)
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
		if err != nil {
			log.DefaultLogger.Error("Batch compute API call failed", "error", err, "chunkStart", chunkStart, "chunkEnd", chunkEnd)
			for _, q := range chunkQueries {
				results[q.RefID] = backend.ErrDataResponse(
					backend.StatusInternal,
					fmt.Sprintf("Batch compute failed: %v", err),
				)
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

			results[q.RefID] = e.transformBatchResult(batchResponse.Results[i], chunkModels[i])
		}
	}

	return results
}
