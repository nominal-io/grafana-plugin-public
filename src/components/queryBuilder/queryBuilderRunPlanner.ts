import type { NominalQuery } from '../../types';

type QueryCompletenessInput = Pick<NominalQuery, 'assetRid' | 'channel' | 'dataScopeName'> | undefined;

export const AGGREGATION_RUN_DELAY_MS = 400;

export function shouldRunCompleteQuery(query: QueryCompletenessInput): boolean {
  return Boolean(query?.assetRid && query.channel && query.dataScopeName);
}

export function shouldDebounceAggregationRun(query: QueryCompletenessInput): boolean {
  return shouldRunCompleteQuery(query);
}
