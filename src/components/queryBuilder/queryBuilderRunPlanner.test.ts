import { NominalQuery } from '../../types';
import {
  AGGREGATION_RUN_DELAY_MS,
  shouldDebounceAggregationRun,
  shouldRunCompleteQuery,
} from './queryBuilderRunPlanner';

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return {
    refId: 'A',
    assetRid: 'ri.scout.main.asset.abc123',
    dataScopeName: 'default',
    channel: 'temperature',
    queryType: 'decimation',
    buckets: 1000,
    ...overrides,
  } as NominalQuery;
}

describe('queryBuilderRunPlanner', () => {
  it('treats asset, scope, and channel as complete-query eligibility', () => {
    expect(shouldRunCompleteQuery(makeQuery())).toBe(true);
    expect(shouldDebounceAggregationRun(makeQuery())).toBe(true);
  });

  it('skips complete-query and aggregation runs when required fields are missing', () => {
    for (const query of [
      undefined,
      makeQuery({ assetRid: '' }),
      makeQuery({ dataScopeName: '' }),
      makeQuery({ channel: '' }),
    ]) {
      expect(shouldRunCompleteQuery(query)).toBe(false);
      expect(shouldDebounceAggregationRun(query)).toBe(false);
    }
  });

  it('keeps the current aggregation rerun debounce delay', () => {
    expect(AGGREGATION_RUN_DELAY_MS).toBe(400);
  });
});
