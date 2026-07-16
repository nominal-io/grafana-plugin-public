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

// Old dashboards persisted assetInputMethod, which has since been removed from
// NominalQuery. The cast mimics untyped saved JSON that still carries the key.
function legacyQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return { refId: 'A', assetInputMethod: 'direct', ...overrides } as NominalQuery;
}

function hasLegacyKey(query: NominalQuery): boolean {
  return Object.prototype.hasOwnProperty.call(query, 'assetInputMethod');
}

type LegacyMutationCase = {
  name: string;
  mutate: () => NominalQuery;
  assertResult: (query: NominalQuery) => void;
};

const legacyMutationCases: LegacyMutationCase[] = [
  {
    name: 'changeAssetRidQuery',
    mutate: () => changeAssetRidQuery(legacyQuery({ queryText: 'q', constant: 3 }), 'ri.scout.main.asset.a'),
    assertResult: (result) => {
      expect(result.assetRid).toBe('ri.scout.main.asset.a');
      expect(result.queryText).toBe('q');
      expect(result.constant).toBe(3);
    },
  },
  {
    name: 'changeSelectedDataScopeQuery',
    mutate: () => changeSelectedDataScopeQuery(legacyQuery({ assetRid: 'ri.x' }), 'default'),
    assertResult: (result) => {
      expect(result.dataScopeName).toBe('default');
      expect(result.assetRid).toBe('ri.x');
    },
  },
  {
    name: 'changeSelectedChannelQuery',
    mutate: () => changeSelectedChannelQuery(legacyQuery(), { channel: 'temp', dataType: 'double' }),
    assertResult: (result) => {
      expect(result.channel).toBe('temp');
    },
  },
  {
    name: 'inferChannelDataTypeQuery',
    mutate: () => inferChannelDataTypeQuery(legacyQuery(), 'double'),
    assertResult: (result) => {
      expect(result.channelDataType).toBe('double');
    },
  },
  {
    name: 'changeAggregationsQuery',
    mutate: () => changeAggregationsQuery(legacyQuery(), ['MEAN']),
    assertResult: (result) => {
      expect(result.aggregations).toEqual(['MEAN']);
    },
  },
];

describe('queryMutations', () => {
  it('persists selected data scope and channel with query-builder defaults', () => {
    expect(changeSelectedDataScopeQuery(baseQuery, 'scope-b')).toEqual(
      expect.objectContaining({
        dataScopeName: 'scope-b',
        queryType: 'decimation',
        buckets: 1000,
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
        queryType: 'decimation',
        buckets: 1000,
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

  it('updates inferred channel data type without changing other query fields', () => {
    expect(inferChannelDataTypeQuery(baseQuery, 'string')).toEqual(
      expect.objectContaining({ assetRid: 'asset-a', channel: 'temp', channelDataType: 'string' })
    );
  });

  describe('legacy assetInputMethod stripping', () => {
    it.each(legacyMutationCases)('$name strips the legacy key while applying its write', ({ mutate, assertResult }) => {
      const result = mutate();
      expect(hasLegacyKey(result)).toBe(false);
      assertResult(result);
    });
  });
});
