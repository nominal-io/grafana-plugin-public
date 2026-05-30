import { useCallback, useEffect, useMemo, useRef } from 'react';
import type { SelectableValue } from '@grafana/data';
import type { NominalQuery } from '../../types';
import { getAggregationValue, NUMERIC_AGG_OPTIONS } from './queryBuilderOptions';
import type { AggregationState } from './queryBuilderTypes';

export const AGGREGATION_RUN_DELAY_MS = 400;

type QueryCompletenessInput = Pick<NominalQuery, 'assetRid' | 'channel' | 'dataScopeName'> | undefined;

function shouldRunCompleteQuery(query: QueryCompletenessInput): boolean {
  return Boolean(query?.assetRid && query.channel && query.dataScopeName);
}

function shouldDebounceAggregationRun(query: QueryCompletenessInput): boolean {
  return shouldRunCompleteQuery(query);
}

interface UseAggregationRunArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  onRunQuery: () => void;
}

export interface AggregationRunModel {
  aggregationState: AggregationState;
  changeAggregations: (selected: Array<SelectableValue<string>>) => void;
}

export function useAggregationRun({ query, onChange, onRunQuery }: UseAggregationRunArgs): AggregationRunModel {
  const queryRef = useRef(query);
  queryRef.current = query;
  const onRunQueryRef = useRef(onRunQuery);
  onRunQueryRef.current = onRunQuery;

  // Track aggregations by normalized value (not array identity) so a content-identical
  // array arriving as a new reference (e.g. Grafana re-cloning query targets) doesn't
  // schedule a redundant rerun. Empty/undefined normalizes to DEFAULT_AGGREGATIONS.
  const aggregationsKey = getAggregationValue(query?.aggregations).join('|');
  const lastDebouncedAggregationsKeyRef = useRef(aggregationsKey);

  // Trigger graph update when query is complete
  useEffect(() => {
    if (shouldRunCompleteQuery(queryRef.current)) {
      onRunQuery();
    }
  }, [query?.assetRid, query?.channel, query?.dataScopeName, onRunQuery]);

  // Debounced re-run on aggregation changes - coalesces rapid toggles into a single requery.
  // Compared by normalized value so logically identical aggregations in a new array
  // reference don't schedule a redundant rerun.
  useEffect(() => {
    if (aggregationsKey === lastDebouncedAggregationsKeyRef.current) {
      return;
    }
    lastDebouncedAggregationsKeyRef.current = aggregationsKey;

    if (!shouldDebounceAggregationRun(queryRef.current)) {
      return;
    }
    const timer = setTimeout(() => onRunQueryRef.current(), AGGREGATION_RUN_DELAY_MS);
    return () => clearTimeout(timer);
  }, [aggregationsKey]);

  const changeAggregations = useCallback(
    (selected: Array<SelectableValue<string>>) => {
      const values = selected.map((selection) => selection.value).filter((value): value is string => value != null);
      const aggregations = values.length > 0 ? values : getAggregationValue(undefined);
      onChange({ ...query, aggregations });
    },
    [onChange, query]
  );

  const aggregationState = useMemo<AggregationState>(() => {
    if (query?.channelDataType === 'string') {
      return {
        kind: 'string',
        tooltip: 'String channels only support Mode (most frequent value per time bucket)',
        value: ['Mode'],
        options: NUMERIC_AGG_OPTIONS,
      };
    }
    if (query?.channelDataType === 'log') {
      return {
        kind: 'log',
        tooltip: 'Log channels return raw entries without aggregation',
        value: ['Logs (raw)'],
        options: NUMERIC_AGG_OPTIONS,
      };
    }
    return {
      kind: 'numeric',
      tooltip: 'Aggregation functions to apply per time bucket',
      value: getAggregationValue(query?.aggregations),
      options: NUMERIC_AGG_OPTIONS,
    };
  }, [query?.aggregations, query?.channelDataType]);

  return { aggregationState, changeAggregations };
}
