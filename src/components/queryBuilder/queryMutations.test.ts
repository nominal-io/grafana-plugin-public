import { AggregationType, DEFAULT_AGGREGATIONS, type NominalQuery } from '../../types';
import {
  changeAggregationsQuery,
  changeAssetRidQuery,
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
  it('persists selected data scope and channel with query-builder defaults', () => {
    expect(changeSelectedDataScopeQuery(baseQuery, 'scope-b')).toEqual(
      expect.objectContaining({
        dataScopeName: 'scope-b',
        queryType: 'compute',
      })
    );
    expect(
      changeSelectedChannelQuery(baseQuery, {
        channel: 'pressure',
        dataType: 'numeric',
      })
    ).toEqual(
      expect.objectContaining({
        channel: 'pressure',
        channelDataType: 'numeric',
        dataScopeName: 'scope-a',
        queryType: 'compute',
      })
    );
  });

  it('updates the asset RID without unrelated defaults', () => {
    expect(changeAssetRidQuery(baseQuery, 'asset-b')).toEqual(expect.objectContaining({ assetRid: 'asset-b' }));
    expect(changeAssetRidQuery(baseQuery, 'asset-b').queryType).toBeUndefined();
  });

  it('normalizes aggregation changes through one mutation helper', () => {
    expect(changeAggregationsQuery(baseQuery, [])).toEqual(
      expect.objectContaining({ aggregations: [...DEFAULT_AGGREGATIONS] })
    );
    expect(changeAggregationsQuery(baseQuery, [AggregationType.Min])).toEqual(
      expect.objectContaining({ aggregations: [AggregationType.Min] })
    );
  });

  it.each([
    { current: [AggregationType.Mean], selected: [AggregationType.Mean, AggregationType.Lttb], want: [AggregationType.Lttb] },
    { current: [AggregationType.Lttb], selected: [AggregationType.Lttb, AggregationType.Min], want: [AggregationType.Min] },
  ])('keeps LTTB exclusive when selection changes from $current to $selected', ({ current, selected, want }) => {
    expect(changeAggregationsQuery({ ...baseQuery, aggregations: current }, selected).aggregations).toEqual(want);
  });

  it('updates inferred channel data type without changing other query fields', () => {
    expect(inferChannelDataTypeQuery(baseQuery, 'string')).toEqual(
      expect.objectContaining({ assetRid: 'asset-a', channel: 'temp', channelDataType: 'string' })
    );
  });
});
