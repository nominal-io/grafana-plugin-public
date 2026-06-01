import { AggregationType, DEFAULT_AGGREGATIONS, type NominalQuery } from '../../types';
import {
  changeAggregationsQuery,
  changeAssetInputMethodQuery,
  changeDirectAssetRidQuery,
  changeSearchAssetRidQuery,
  changeSelectedChannelQuery,
  changeSelectedDataScopeQuery,
  inferChannelDataTypeQuery,
} from './queryMutations';

const baseQuery: NominalQuery = {
  refId: 'A',
  assetRid: 'asset-a',
  dataScopeName: 'scope-a',
  channel: 'temp',
};

describe('queryMutations', () => {
  it('persists direct RID changes with query-builder defaults', () => {
    expect(changeDirectAssetRidQuery(baseQuery, 'asset-b')).toEqual(
      expect.objectContaining({
        assetRid: 'asset-b',
        assetInputMethod: 'direct',
        queryType: 'decimation',
        buckets: 1000,
      })
    );
  });

  it('clears direct RID without forcing execution defaults', () => {
    expect(changeDirectAssetRidQuery(baseQuery, '')).toEqual(
      expect.objectContaining({
        assetRid: '',
        assetInputMethod: 'direct',
      })
    );
    expect(changeDirectAssetRidQuery(baseQuery, '').queryType).toBeUndefined();
  });

  it('persists selected data scope and channel with query-builder defaults', () => {
    expect(changeSelectedDataScopeQuery(baseQuery, 'scope-b', 'search')).toEqual(
      expect.objectContaining({
        dataScopeName: 'scope-b',
        assetInputMethod: 'search',
        queryType: 'decimation',
        buckets: 1000,
      })
    );
    expect(
      changeSelectedChannelQuery(baseQuery, {
        channel: 'pressure',
        dataType: 'numeric',
        assetInputMethod: 'direct',
      })
    ).toEqual(
      expect.objectContaining({
        channel: 'pressure',
        channelDataType: 'numeric',
        dataScopeName: 'scope-a',
        assetInputMethod: 'direct',
        queryType: 'decimation',
        buckets: 1000,
      })
    );
  });

  it('updates asset input method and search asset RID without unrelated defaults', () => {
    expect(changeAssetInputMethodQuery(baseQuery, 'direct')).toEqual(
      expect.objectContaining({ assetInputMethod: 'direct' })
    );
    expect(changeAssetInputMethodQuery(baseQuery, 'direct').queryType).toBeUndefined();
    expect(changeSearchAssetRidQuery(baseQuery, 'asset-b')).toEqual(
      expect.objectContaining({ assetRid: 'asset-b', assetInputMethod: 'search' })
    );
    expect(changeSearchAssetRidQuery(baseQuery, 'asset-b').queryType).toBeUndefined();
  });

  it('normalizes aggregation changes through one mutation helper', () => {
    expect(changeAggregationsQuery(baseQuery, [])).toEqual(
      expect.objectContaining({ aggregations: [...DEFAULT_AGGREGATIONS] })
    );
    expect(changeAggregationsQuery(baseQuery, [AggregationType.Min])).toEqual(
      expect.objectContaining({ aggregations: [AggregationType.Min] })
    );
  });

  it('updates inferred channel data type without changing other query fields', () => {
    expect(inferChannelDataTypeQuery(baseQuery, 'string')).toEqual(
      expect.objectContaining({ assetRid: 'asset-a', channel: 'temp', channelDataType: 'string' })
    );
  });
});
