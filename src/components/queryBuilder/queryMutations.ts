import { AggregationType, DEFAULT_AGGREGATIONS, type NominalQuery } from '../../types';

const QUERY_BUILDER_EXECUTION_DEFAULTS = {
  queryType: 'decimation' as const,
  buckets: 1000,
};

// Removes the legacy `assetInputMethod` key that older dashboards persisted.
// The field was removed from NominalQuery; without this, every write would
// re-persist the stale key because the helpers spread the incoming query.
export function normalizeLegacyQuery(query: NominalQuery): NominalQuery {
  if (!Object.prototype.hasOwnProperty.call(query, 'assetInputMethod')) {
    return query;
  }
  const { assetInputMethod: _assetInputMethod, ...rest } = query as NominalQuery & { assetInputMethod?: unknown };
  return rest;
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
