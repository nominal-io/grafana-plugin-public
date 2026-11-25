import {
  AggregationType,
  DEFAULT_AGGREGATIONS,
  isDatasetQuery,
  QUERY_TYPE_COMPUTE,
  type NominalQuery,
} from '../../types';

export function changeAssetRidQuery(query: NominalQuery, assetRid: string): NominalQuery {
  const { datasetRid: _datasetRid, ...rest } = query;
  return { ...rest, assetRid };
}

export function changeDatasetRidQuery(query: NominalQuery, datasetRid: string): NominalQuery {
  const { assetRid: _assetRid, dataScopeName: _dataScopeName, ...rest } = query;
  return { ...rest, datasetRid };
}

export function changeSelectedDataScopeQuery(query: NominalQuery, dataScopeName: string): NominalQuery {
  return {
    ...query,
    dataScopeName,
    queryType: QUERY_TYPE_COMPUTE,
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
    // A dataset query has no data scope; do not add an empty one.
    ...(!isDatasetQuery(query) && { dataScopeName: query?.dataScopeName || '' }),
    queryType: QUERY_TYPE_COMPUTE,
  };
}

export function inferChannelDataTypeQuery(query: NominalQuery, channelDataType: string): NominalQuery {
  return { ...query, channelDataType };
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
    ...query,
    aggregations: selected.length > 0 ? selected : [...DEFAULT_AGGREGATIONS],
  };
}
