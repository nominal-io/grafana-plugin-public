import { DEFAULT_AGGREGATIONS, type NominalQuery } from '../../types';
import type { AssetInputMethod } from './queryBuilderTypes';

const QUERY_BUILDER_EXECUTION_DEFAULTS = {
  queryType: 'decimation' as const,
  buckets: 1000,
};

export function changeAssetInputMethodQuery(query: NominalQuery, assetInputMethod: AssetInputMethod): NominalQuery {
  return { ...query, assetInputMethod };
}

export function changeSearchAssetRidQuery(query: NominalQuery, assetRid: string): NominalQuery {
  return { ...query, assetRid, assetInputMethod: 'search' };
}

export function changeDirectAssetRidQuery(query: NominalQuery, assetRid: string): NominalQuery {
  if (!assetRid.trim()) {
    return { ...query, assetRid: '', assetInputMethod: 'direct' };
  }

  return {
    ...query,
    assetRid,
    assetInputMethod: 'direct',
    ...QUERY_BUILDER_EXECUTION_DEFAULTS,
  };
}

export function changeSelectedDataScopeQuery(
  query: NominalQuery,
  dataScopeName: string,
  assetInputMethod: AssetInputMethod
): NominalQuery {
  return {
    ...query,
    dataScopeName,
    assetInputMethod,
    ...QUERY_BUILDER_EXECUTION_DEFAULTS,
  };
}

export function changeSelectedChannelQuery(
  query: NominalQuery,
  {
    channel,
    dataType,
    assetInputMethod,
  }: {
    channel: string;
    dataType: string;
    assetInputMethod: AssetInputMethod;
  }
): NominalQuery {
  return {
    ...query,
    channel,
    channelDataType: dataType,
    dataScopeName: query?.dataScopeName || '',
    assetInputMethod,
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
