package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	datasourceapi "github.com/nominal-io/nominal-api-go/datasource/api"
	"github.com/palantir/pkg/bearertoken"
)

// NominalQueryModel represents a query to the Nominal API
type NominalQueryModel struct {
	// Asset information
	AssetRid        string `json:"assetRid"`
	Channel         string `json:"channel"`
	DataScopeName   string `json:"dataScopeName"`
	ChannelDataType string `json:"channelDataType"`

	// Aggregation functions for numeric channels (e.g. "MEAN", "MIN", "MAX").
	// Empty/missing defaults to ["MEAN"]. Ignored for enum channels.
	Aggregations         []string `json:"aggregations,omitempty"`
	ExplicitAggregations bool     `json:"-"` // true when aggregations were set by the frontend (not defaulted)

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
}

// ChannelDataType values. These are produced by getChannelDataType (normalizing the
// API's SeriesDataType) and consumed by the compute-request and query-execution layers.
// An empty ChannelDataType (searched-but-not-found, or DataType nil) is treated as numeric.
const (
	ChannelDataTypeNumeric = "numeric"
	ChannelDataTypeString  = "string"
	ChannelDataTypeLog     = "log"
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

	if err := e.validateQuery(qm); err != nil {
		log.DefaultLogger.Error("Query validation failed", "error", err)
		response := backend.ErrDataResponse(
			backend.StatusBadRequest,
			fmt.Sprintf("Query validation failed: %v", err),
		)
		return preparedQuery{}, &response
	}

	e.inferChannelMetadata(ctx, &qm)
	if prepErr := normalizeAggregations(&qm); prepErr != nil {
		return preparedQuery{}, prepErr
	}

	if qm.AssetRid != "" && qm.Channel != "" {
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

	deduped, badAgg := validateAndDedup(qm.Aggregations)
	if badAgg != "" {
		response := backend.ErrDataResponse(
			backend.StatusBadRequest,
			fmt.Sprintf("unsupported aggregation %q; valid options are MEAN, MIN, MAX, COUNT, VARIANCE, FIRST_POINT, LAST_POINT", badAgg),
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
	qm.Channel = interpolateTemplateVariables(qm.Channel, qm.TemplateVariables)
	qm.DataScopeName = interpolateTemplateVariables(qm.DataScopeName, qm.TemplateVariables)
	qm.QueryText = interpolateTemplateVariables(qm.QueryText, qm.TemplateVariables)
}

// validateQuery validates query parameters similar to pure-ts implementation
func (e *NominalQueryExecution) validateQuery(qm NominalQueryModel) error {
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
		// Validate bucket count
		if qm.Buckets < 0 {
			return fmt.Errorf("buckets must be non-negative, got %d", qm.Buckets)
		}
		if qm.Buckets > 10000 {
			log.DefaultLogger.Warn("Large bucket count may impact performance", "buckets", qm.Buckets)
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
	if qm == nil || e.datasource.datasourceService == nil {
		return
	}
	if strings.TrimSpace(qm.AssetRid) == "" || strings.TrimSpace(qm.Channel) == "" || strings.TrimSpace(qm.DataScopeName) == "" {
		return
	}

	cacheKey := qm.AssetRid + "|" + qm.DataScopeName + "|" + qm.Channel

	if entry, hit := e.datasource.lookupChannelMetadata(cacheKey); hit {
		applyChannelMetadata(qm, entry)
		return
	}

	asset, err := e.datasource.fetchAssetByRid(ctx, e.config, qm.AssetRid)
	if err != nil {
		log.DefaultLogger.Warn("Failed to fetch asset for channel metadata inference", "assetRid", qm.AssetRid, "error", err)
		return
	}
	if asset == nil {
		return
	}

	dataSourceRids := collectDataSourceRidsForScope(asset, qm.DataScopeName)
	if len(dataSourceRids) == 0 {
		return
	}

	bearerToken := bearertoken.Token(e.config.Secrets.ApiKey)
	searchRequest := datasourceapi.SearchChannelsRequest{
		ExactMatch:  []string{qm.Channel},
		DataSources: dataSourceRids,
	}
	channelsResponse, err := e.datasource.datasourceService.SearchChannels(ctx, bearerToken, searchRequest)
	if err != nil {
		log.DefaultLogger.Warn("Failed to search channels for channel metadata inference", "assetRid", qm.AssetRid, "error", err)
		return
	}

	if entry, ok := channelMetadataEntryForExactMatch(channelsResponse.Results, qm.Channel); ok {
		applyChannelMetadata(qm, entry)
		entry.fetchedAt = time.Now()
		e.datasource.storeChannelMetadata(cacheKey, entry)
		return
	}

	// No usable metadata — cache the miss so a re-query doesn't re-search.
	e.datasource.storeChannelMetadata(cacheKey, channelMetadataCacheEntry{fetchedAt: time.Now()})
}

func applyChannelMetadata(qm *NominalQueryModel, entry channelMetadataCacheEntry) {
	if entry.channelDataType != "" {
		qm.ChannelDataType = entry.channelDataType
	}
	if entry.unit != "" {
		qm.ChannelUnit = entry.unit
	}
}

func channelMetadataEntryForExactMatch(channels []datasourceapi.ChannelMetadata, channelName string) (channelMetadataCacheEntry, bool) {
	// Nominal enforces unique DataScopeName per asset (CreateAssetDataScope conjure
	// doc + DuplicateDataScopeNames error), so SearchChannels-exact-match returns
	// at most one case-exact result. Pick the first match with usable metadata.
	for _, channel := range channels {
		if string(channel.Name) != channelName {
			continue
		}
		entry := channelMetadataCacheEntry{
			channelDataType: getChannelDataType(channel), // "" if ChannelMetadata.DataType is nil
			unit:            getChannelUnit(channel),     // "" if Unit is nil
		}
		if entry.channelDataType == "" && entry.unit == "" {
			continue
		}
		return entry, true
	}
	return channelMetadataCacheEntry{}, false
}

// lookupChannelMetadata returns a cached channel metadata entry if present and
// not yet expired. Lazily initializes the cache map on first call. Caller must
// apply the entry to its query model on hit.
func (d *Datasource) lookupChannelMetadata(cacheKey string) (channelMetadataCacheEntry, bool) {
	d.channelMetadataCacheMu.Lock()
	defer d.channelMetadataCacheMu.Unlock()
	if d.channelMetadataCache == nil {
		d.channelMetadataCache = make(map[string]channelMetadataCacheEntry)
	}
	entry, ok := d.channelMetadataCache[cacheKey]
	if !ok || time.Since(entry.fetchedAt) >= assetCacheTTL {
		return channelMetadataCacheEntry{}, false
	}
	return entry, true
}

// storeChannelMetadata writes (or overwrites) a cache entry. Lazily initializes
// the cache map so this is safe even when called before any lookup.
func (d *Datasource) storeChannelMetadata(cacheKey string, entry channelMetadataCacheEntry) {
	d.channelMetadataCacheMu.Lock()
	defer d.channelMetadataCacheMu.Unlock()
	if d.channelMetadataCache == nil {
		d.channelMetadataCache = make(map[string]channelMetadataCacheEntry)
	}
	d.channelMetadataCache[cacheKey] = entry
}
