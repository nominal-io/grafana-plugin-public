import { DEFAULT_AGGREGATIONS, type NominalQuery } from '../../types';

const QUERY_BUILDER_EXECUTION_DEFAULTS = {
  queryType: 'decimation' as const,
  buckets: 1000,
};

export function changeAssetRidQuery(query: NominalQuery, assetRid: string): NominalQuery {
  return { ...query, assetRid };
}

export function changeSelectedDataScopeQuery(query: NominalQuery, dataScopeName: string): NominalQuery {
  return {
    ...query,
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
    ...query,
    channel,
    channelDataType: dataType,
    dataScopeName: query?.dataScopeName || '',
    ...QUERY_BUILDER_EXECUTION_DEFAULTS,
  };
}

export function inferChannelDataTypeQuery(query: NominalQuery, channelDataType: string): NominalQuery {
  return { ...query, channelDataType };
}

export function changeAggregationsQuery(query: NominalQuery, aggregations: string[]): NominalQuery {
  return {
    ...query,
    aggregations: aggregations.length > 0 ? aggregations : [...DEFAULT_AGGREGATIONS],
  };
}
