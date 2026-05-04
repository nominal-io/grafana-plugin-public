package plugin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

type preparedQueryKind int

const (
	preparedQueryConnectionTest preparedQueryKind = iota
	preparedQueryLegacy
	preparedQueryBatchable
)

type preparedQuery struct {
	Query backend.DataQuery
	Model NominalQueryModel
	Kind  preparedQueryKind
}

type queryPreparationError struct {
	response backend.DataResponse
}

func (e *queryPreparationError) dataResponse() backend.DataResponse {
	return e.response
}

// prepareQuery turns one raw Grafana query into the runtime shape used by query execution.
func (e *NominalQueryExecution) prepareQuery(ctx context.Context, q backend.DataQuery) (preparedQuery, *queryPreparationError) {
	var qm NominalQueryModel
	if err := json.Unmarshal(q.JSON, &qm); err != nil {
		return preparedQuery{}, &queryPreparationError{
			response: backend.ErrDataResponse(
				backend.StatusBadRequest,
				fmt.Sprintf("json unmarshal: %v", err),
			),
		}
	}

	e.applyTemplateVariables(&qm)

	if qm.QueryType == "connectionTest" {
		return preparedQuery{Query: q, Model: qm, Kind: preparedQueryConnectionTest}, nil
	}

	if err := e.validateQuery(qm); err != nil {
		log.DefaultLogger.Error("Query validation failed", "error", err)
		return preparedQuery{}, &queryPreparationError{
			response: backend.ErrDataResponse(
				backend.StatusBadRequest,
				fmt.Sprintf("Query validation failed: %v", err),
			),
		}
	}

	e.inferChannelDataType(ctx, &qm)
	if prepErr := normalizeAggregations(&qm); prepErr != nil {
		return preparedQuery{}, prepErr
	}

	if qm.AssetRid != "" && qm.Channel != "" {
		return preparedQuery{Query: q, Model: qm, Kind: preparedQueryBatchable}, nil
	}

	return preparedQuery{Query: q, Model: qm, Kind: preparedQueryLegacy}, nil
}

func normalizeAggregations(qm *NominalQueryModel) *queryPreparationError {
	qm.ExplicitAggregations = len(qm.Aggregations) > 0
	if qm.ChannelDataType == "string" || qm.ChannelDataType == "log" {
		return nil
	}

	if !qm.ExplicitAggregations {
		qm.Aggregations = []string{AggMean}
		return nil
	}

	deduped, badAgg := validateAndDedup(qm.Aggregations)
	if badAgg != "" {
		return &queryPreparationError{
			response: backend.ErrDataResponse(
				backend.StatusBadRequest,
				fmt.Sprintf("unsupported aggregation %q; valid options are MEAN, MIN, MAX, COUNT, VARIANCE, FIRST_POINT, LAST_POINT", badAgg),
			),
		}
	}
	qm.Aggregations = deduped
	return nil
}
