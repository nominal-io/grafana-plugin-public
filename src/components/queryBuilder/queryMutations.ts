import { AggregationType, DEFAULT_AGGREGATIONS, QUERY_TYPE_COMPUTE, type NominalQuery } from '../../types';

const QUERY_BUILDER_EXECUTION_DEFAULTS = {
  queryType: QUERY_TYPE_COMPUTE,
};

// Keys older dashboards persisted that NominalQuery no longer has. Without this,
// every write would re-persist them because the helpers spread the incoming query.
const LEGACY_KEYS = ['assetInputMethod', 'buckets'];

export function normalizeLegacyQuery(query: NominalQuery): NominalQuery {
  if (!LEGACY_KEYS.some((key) => Object.prototype.hasOwnProperty.call(query, key))) {
    return query;
  }
  return Object.fromEntries(Object.entries(query).filter(([key]) => !LEGACY_KEYS.includes(key))) as NominalQuery;
}

export function changeAssetRidQuery(query: NominalQuery, assetRid: string): NominalQuery {
  return { ...normalizeLegacyQuery(query), assetRid };
}

export function changeSelectedDataScopeQuery(query: NominalQuery, dataScopeName: string): NominalQuery {
  return {
    ...normalizeLegacyQuery(query),
    dataScopeName,
    ...QUERY_BUILDER_EXECUTION_DEFAULTS,
  };
}

export function changeSelectedChannelQuery(
  query: NominalQuery,
  {
    channel,
    dataType,
  }: {
    channel: string;
    dataType: string;
  }
): NominalQuery {
  return {
    ...normalizeLegacyQuery(query),
    channel,
    channelDataType: dataType,
    dataScopeName: query?.dataScopeName || '',
    ...QUERY_BUILDER_EXECUTION_DEFAULTS,
  };
}

export function inferChannelDataTypeQuery(query: NominalQuery, channelDataType: string): NominalQuery {
  return { ...normalizeLegacyQuery(query), channelDataType };
}

export function changeAggregationsQuery(query: NominalQuery, aggregations: string[]): NominalQuery {
  let selected = aggregations;
  if (aggregations.includes(AggregationType.Lttb)) {
    selected =
      aggregations.length > 1 && query.aggregations?.includes(AggregationType.Lttb)
        ? aggregations.filter((aggregation) => aggregation !== AggregationType.Lttb)
        : [AggregationType.Lttb];
  }
  return {
    ...normalizeLegacyQuery(query),
    aggregations: selected.length > 0 ? selected : [...DEFAULT_AGGREGATIONS],
  };
}
