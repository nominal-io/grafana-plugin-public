package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

// NominalQueryModel represents a query to the Nominal API
type NominalQueryModel struct {
	// Asset information
	AssetRid        string `json:"assetRid"`
	Channel         string `json:"channel"`
	DataScopeName   string `json:"dataScopeName"`
	ChannelDataType string `json:"channelDataType"`
	// ComputeBy is "run" for a run query. Empty or "asset" is an asset query.
	ComputeBy string `json:"computeBy,omitempty"`
	RunRid    string `json:"runRid,omitempty"`
	// TemplateSources is set by the frontend for templated RID fields so errors can name the variable.
	TemplateSources templateSources `json:"templateSources,omitempty"`
	// Run is runtime-only; resolveRun fetches it and points AssetRid at its asset.
	Run *RunResponse `json:"-"`

	// Bucket aggregations, or LTTB alone. Empty means MEAN. Numeric channels only.
	Aggregations         []string `json:"aggregations,omitempty"`
	ExplicitAggregations bool     `json:"-"` // true when aggregations were set by the frontend (not defaulted)
	// RawLTTB is runtime-only; normalizeAggregations sets it and clears Aggregations.
	RawLTTB bool `json:"-"`

	// Query parameters
	Buckets   int    `json:"buckets"`
	QueryType string `json:"queryType"`

	// Template variables support
	TemplateVariables map[string]interface{} `json:"templateVariables,omitempty"`

	// Legacy support
	QueryText string  `json:"queryText"`
	Constant  float64 `json:"constant"`

	// ChannelUnit is runtime-only; populated by inferChannelMetadata at QueryData time.
	// json:"-" prevents inferred values from persisting into saved dashboards.
	ChannelUnit string `json:"-"`

	// Points is runtime-only, set by resolvePointBudget.
	Points pointBudget `json:"-"`
}

// ChannelDataType values. These are produced by getChannelDataType (normalizing the
// API's SeriesDataType) and consumed by the compute-request and query-execution layers.
// An empty ChannelDataType (searched-but-not-found, or DataType nil) is treated as numeric.
const (
	ChannelDataTypeNumeric = "numeric"
	ChannelDataTypeString  = "string"
	ChannelDataTypeLog     = "log"
)

// queryTypeSQL marks a query that runs SQL instead of a Compute request.
const queryTypeSQL = "sql"

const computeByRun = "run"

type templateSource struct {
	Raw  string `json:"raw"`
	Name string `json:"name"`
}

// templateSources names the variable behind each templated RID field.
type templateSources struct {
	AssetRid *templateSource `json:"assetRid,omitempty"`
	RunRid   *templateSource `json:"runRid,omitempty"`
}

// ridFieldError explains a templated RID field that did not resolve to a RID
// of the wanted type. Fields without a template source are not checked here.
func ridFieldError(label, ridType, noun, value string, src *templateSource) error {
	if src == nil {
		return nil
	}
	switch {
	case strings.Contains(value, "$"):
		return fmt.Errorf("No variable named `%s`.", src.Name) //nolint:staticcheck // user-facing message
	case strings.HasPrefix(value, "{") && strings.Contains(value, ","):
		return fmt.Errorf("%s is `%s`, currently %d values. The %s field needs one %s.", label, src.Raw, strings.Count(value, ",")+1, label, ridType) //nolint:staticcheck // user-facing message
	case ridHasType(value, ridType):
		return nil
	default:
		return fmt.Errorf("%s is `%s`, currently `%s`. That is not %s. Check that variable's query.", label, src.Raw, value, noun) //nolint:staticcheck // user-facing message
	}
}

type preparedQueryKind int

const (
	preparedQueryConnectionTest preparedQueryKind = iota
	preparedQueryLegacy
	preparedQueryBatchable
	preparedQuerySQL
	// preparedQueryAnswered queries already have their Response and skip compute.
	preparedQueryAnswered
)

type preparedQuery struct {
	Query backend.DataQuery
	Model NominalQueryModel
	Kind  preparedQueryKind
	// SQL is set for preparedQuerySQL queries.
	SQL *sqlutil.Query
	// Response is set for preparedQueryAnswered queries.
	Response *backend.DataResponse
}

// prepareQuery turns one raw Grafana query into the runtime shape used by query execution.
func (e *NominalQueryExecution) prepareQuery(ctx context.Context, q backend.DataQuery) (preparedQuery, *backend.DataResponse) {
	var qm NominalQueryModel
	if err := json.Unmarshal(q.JSON, &qm); err != nil {
		response := backend.ErrDataResponse(
			backend.StatusBadRequest,
			fmt.Sprintf("json unmarshal: %v", err),
		)
		return preparedQuery{}, &response
	}

	e.applyTemplateVariables(&qm)

	if qm.QueryType == "connectionTest" {
		return preparedQuery{Query: q, Model: qm, Kind: preparedQueryConnectionTest}, nil
	}
	if qm.QueryType == queryTypeSQL {
		sql, err := sqlutil.GetQuery(q)
		if err == nil && strings.TrimSpace(sql.RawSQL) == "" {
			err = errors.New("SQL query is empty")
		}
		if err != nil {
			response := backend.ErrDataResponseWithSource(backend.StatusBadRequest, backend.ErrorSourceDownstream, err.Error())
			return preparedQuery{}, &response
		}
		return preparedQuery{Query: q, Model: qm, Kind: preparedQuerySQL, SQL: sql}, nil
	}

	if err := e.validateQuery(qm); err != nil {
		log.DefaultLogger.Error("Query validation failed", "error", err)
		response := backend.ErrDataResponse(
			backend.StatusBadRequest,
			fmt.Sprintf("Query validation failed: %v", err),
		)
		return preparedQuery{}, &response
	}

	if qm.ComputeBy == computeByRun {
		if prepErr := e.resolveRun(ctx, &qm); prepErr != nil {
			return preparedQuery{}, prepErr
		}
	}

	e.inferChannelMetadata(ctx, &qm)
	if prepErr := normalizeAggregations(&qm); prepErr != nil {
		return preparedQuery{}, prepErr
	}

	// The compute service rejects a run query whose range misses the run, so
	// answer it here with the frames an empty result would have, plus the reason.
	if qm.ComputeBy == computeByRun {
		if notice := runWindowNotice(qm.Run, q.TimeRange); notice != "" {
			frames := framesFromResult(emptyTransformResult(qm), qm)
			frames[0].AppendNotices(data.Notice{Severity: data.NoticeSeverityWarning, Text: notice})
			return preparedQuery{Query: q, Model: qm, Kind: preparedQueryAnswered, Response: &backend.DataResponse{Frames: frames}}, nil
		}
	}

	if qm.AssetRid != "" && qm.Channel != "" {
		points, err := resolvePointBudget(qm, q.MaxDataPoints)
		if err != nil {
			log.DefaultLogger.Error("Query validation failed", "error", err)
			response := backend.ErrDataResponse(
				backend.StatusBadRequest,
				fmt.Sprintf("Query validation failed: %v", err),
			)
			return preparedQuery{}, &response
		}
		qm.Points = points
		return preparedQuery{Query: q, Model: qm, Kind: preparedQueryBatchable}, nil
	}

	return preparedQuery{Query: q, Model: qm, Kind: preparedQueryLegacy}, nil
}

func normalizeAggregations(qm *NominalQueryModel) *backend.DataResponse {
	qm.ExplicitAggregations = len(qm.Aggregations) > 0
	if qm.ChannelDataType == ChannelDataTypeString || qm.ChannelDataType == ChannelDataTypeLog {
		return nil
	}

	if !qm.ExplicitAggregations {
		qm.Aggregations = []string{AggMean}
		return nil
	}
	if slices.Contains(qm.Aggregations, AggLTTB) {
		if slices.ContainsFunc(qm.Aggregations, func(agg string) bool { return agg != AggLTTB }) {
			response := backend.ErrDataResponse(backend.StatusBadRequest, "LTTB cannot be combined with bucket aggregations")
			return &response
		}
		qm.RawLTTB = true
		qm.Aggregations = nil
		return nil
	}

	deduped, badAgg := validateAndDedup(qm.Aggregations)
	if badAgg != "" {
		response := backend.ErrDataResponse(
			backend.StatusBadRequest,
			fmt.Sprintf("unsupported aggregation %q; valid options are %s", badAgg, validAggregationNames()),
		)
		return &response
	}
	qm.Aggregations = deduped
	return nil
}

// interpolateTemplateVariables replaces template variables in strings.
// It supports both ${var} and $var syntax. The ${var} form is processed first
// so that a bare $var replacement cannot accidentally corrupt a ${othervar}
// token that happens to share a prefix (e.g. key "o" must not match inside
// "${othervar}"). The bare $var form uses a word-boundary regex so it only
// matches when the key name ends at a non-word character (or end-of-string).
func interpolateTemplateVariables(input string, variables map[string]interface{}) string {
	if variables == nil {
		return input
	}

	result := input
	for key, value := range variables {
		valueStr := fmt.Sprintf("%v", value)

		// Replace ${var} form first (unambiguous).
		result = strings.ReplaceAll(result, fmt.Sprintf("${%s}", key), valueStr)

		// Replace bare $var form only as a whole token: must not be immediately
		// followed by a word character so that $foo does not match inside $foobar.
		bareRe := regexp.MustCompile(`\$` + regexp.QuoteMeta(key) + `(\W|$)`)
		result = bareRe.ReplaceAllStringFunc(result, func(match string) string {
			// Preserve any trailing non-word character that was part of the match.
			suffix := match[len("$"+key):]
			return valueStr + suffix
		})
	}

	return result
}

// applyTemplateVariables applies template variable interpolation to query fields.
//
// Defense-in-depth: Grafana's SDK resolves dashboard template variables before
// the query JSON reaches the backend in most panel flows, so by the time
// QueryData is called the variables in qm.AssetRid / qm.Channel etc. are
// usually already substituted. However, variables passed explicitly via the
// TemplateVariables field of the query model (populated by the frontend for
// variable-panel queries and programmatic calls) are NOT resolved by the SDK,
// so this server-side pass is still needed for those paths.
func (e *NominalQueryExecution) applyTemplateVariables(qm *NominalQueryModel) {
	if qm.TemplateVariables == nil {
		return
	}

	qm.AssetRid = interpolateTemplateVariables(qm.AssetRid, qm.TemplateVariables)
	qm.RunRid = interpolateTemplateVariables(qm.RunRid, qm.TemplateVariables)
	qm.Channel = interpolateTemplateVariables(qm.Channel, qm.TemplateVariables)
	qm.DataScopeName = interpolateTemplateVariables(qm.DataScopeName, qm.TemplateVariables)
	qm.QueryText = interpolateTemplateVariables(qm.QueryText, qm.TemplateVariables)
}

// resolveRun fetches the query's run and points the in-memory AssetRid at the
// run's only asset, so metadata inference, the point budget and batching treat
// it like an asset query. It runs before batching because a failure inside a
// batch fails every query in the chunk.
func (e *NominalQueryExecution) resolveRun(ctx context.Context, qm *NominalQueryModel) *backend.DataResponse {
	fail := func(status backend.Status, msg string) *backend.DataResponse {
		response := backend.ErrDataResponse(status, msg)
		return &response
	}
	run, err := e.datasource.nominalCatalog.FetchRunByRid(ctx, e.config, qm.RunRid)
	if err != nil {
		logErrorWithConjureFields("Failed to fetch run", err, "runRid", qm.RunRid)
		return fail(backend.StatusInternal, formatUserError("Failed to load run", err))
	}
	if run == nil {
		return fail(backend.StatusBadRequest, fmt.Sprintf("run %s not found", qm.RunRid))
	}
	if len(run.Assets) != 1 {
		return fail(backend.StatusBadRequest, fmt.Sprintf("run %s spans %d assets; multi-asset runs are not supported yet", run.Title, len(run.Assets)))
	}
	qm.Run = run
	qm.AssetRid = run.Assets[0]
	return nil
}

// validateQuery validates query parameters similar to pure-ts implementation
func (e *NominalQueryExecution) validateQuery(qm NominalQueryModel) error {
	if qm.ComputeBy == computeByRun {
		if strings.TrimSpace(qm.RunRid) == "" {
			return fmt.Errorf("runRid is required for run queries")
		}
		if qm.TemplateSources.RunRid == nil && !ridHasType(qm.RunRid, "run") {
			return fmt.Errorf("`%s` is not a run RID.", qm.RunRid) //nolint:staticcheck // user-facing message
		}
		if err := ridFieldError("Run", "run", "a run RID", qm.RunRid, qm.TemplateSources.RunRid); err != nil {
			return err
		}
		switch {
		case strings.TrimSpace(qm.Channel) == "":
			return fmt.Errorf("channel cannot be empty")
		case strings.TrimSpace(qm.DataScopeName) == "":
			return fmt.Errorf("dataScopeName is required for run queries")
		}
		return nil
	}

	if err := ridFieldError("Asset", "asset", "an asset RID", qm.AssetRid, qm.TemplateSources.AssetRid); err != nil {
		return err
	}

	// Check if we have either Nominal-specific fields or legacy fields
	hasNominalQuery := qm.AssetRid != "" && qm.Channel != ""
	hasLegacyQuery := qm.QueryText != ""
	hasConstantQuery := qm.Constant != 0

	if !hasNominalQuery && !hasLegacyQuery && !hasConstantQuery {
		return fmt.Errorf("query must have either asset/channel parameters, query text, or constant value")
	}

	// Validate Nominal query fields
	if hasNominalQuery {
		if strings.TrimSpace(qm.AssetRid) == "" {
			return fmt.Errorf("assetRid cannot be empty")
		}
		if strings.TrimSpace(qm.Channel) == "" {
			return fmt.Errorf("channel cannot be empty")
		}
		// DataScopeName is required — the compute API needs it to locate the channel.
		// The frontend filterQuery also enforces this; this is defense-in-depth.
		if strings.TrimSpace(qm.DataScopeName) == "" {
			return fmt.Errorf("dataScopeName is required for asset/channel queries")
		}
	}

	return nil
}

// inferChannelMetadata verifies (or backfills) channel metadata — both data type
// and unit symbol — against the actual ChannelMetadata returned by SearchChannels.
//
// Why this exists:
//   - ChannelDataType: the frontend-supplied value may be stale when a multi-select
//     template variable expands $channel to a mix of numeric and string channels;
//     every expanded query inherits the same saved type.
//   - ChannelUnit: never persisted on the query (transient runtime field); resolved
//     here so FieldConfig.Unit can be set at frame-construction time without an
//     extra round trip.
//
// Both lookups ride on the same cached SearchChannels exact-match call.
func (e *NominalQueryExecution) inferChannelMetadata(ctx context.Context, qm *NominalQueryModel) {
	if e == nil || e.datasource == nil {
		return
	}
	e.datasource.nominalCatalog.InferChannelMetadata(ctx, e.config, qm)
}

func applyChannelMetadata(qm *NominalQueryModel, entry channelMetadataCacheEntry) {
	if entry.channelDataType != "" {
		qm.ChannelDataType = entry.channelDataType
	}
	if entry.unit != "" {
		qm.ChannelUnit = entry.unit
	}
}
