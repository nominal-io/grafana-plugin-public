// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, renderHook } from '@testing-library/react';
import { AggregationType, DEFAULT_AGGREGATIONS, type NominalQuery } from '../../types';
import { AGGREGATION_RUN_DELAY_MS, useAggregationRun } from './useAggregationRun';

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return {
    refId: 'A',
    assetRid: 'ri.scout.main.asset.abc',
    dataScopeName: 'default',
    channel: 'temp',
    queryType: 'decimation',
    buckets: 1000,
    ...overrides,
  } as NominalQuery;
}

describe('useAggregationRun', () => {
  afterEach(() => jest.useRealTimers());

  it('runs the query immediately when a complete query mounts', () => {
    const onRunQuery = jest.fn();
    renderHook(() => useAggregationRun({ query: makeQuery(), onChange: jest.fn(), onRunQuery }));
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it('does not run an incomplete query', () => {
    const onRunQuery = jest.fn();
    renderHook(() => useAggregationRun({ query: makeQuery({ channel: '' }), onChange: jest.fn(), onRunQuery }));
    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('changeAggregations normalizes empty selection to the default and calls onChange', () => {
    const onChange = jest.fn();
    const { result } = renderHook(() => useAggregationRun({ query: makeQuery(), onChange, onRunQuery: jest.fn() }));
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeAggregations([]);
    });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ aggregations: [...DEFAULT_AGGREGATIONS] }));
  });

  it('debounces a rerun when the aggregations value changes', () => {
    jest.useFakeTimers();
    const onRunQuery = jest.fn();
    const { rerender } = renderHook(
      ({ query }) => useAggregationRun({ query, onChange: jest.fn(), onRunQuery }),
      { initialProps: { query: makeQuery({ aggregations: [AggregationType.Mean] }) } }
    );
    onRunQuery.mockClear();

    rerender({ query: makeQuery({ aggregations: [AggregationType.Min, AggregationType.Max] }) });
    expect(onRunQuery).not.toHaveBeenCalled();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(AGGREGATION_RUN_DELAY_MS);
    });
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it('does not reschedule when aggregations arrive as a new array with identical values', () => {
    jest.useFakeTimers();
    const onRunQuery = jest.fn();
    const { rerender } = renderHook(
      ({ query }) => useAggregationRun({ query, onChange: jest.fn(), onRunQuery }),
      { initialProps: { query: makeQuery({ aggregations: [AggregationType.Min, AggregationType.Max] }) } }
    );
    onRunQuery.mockClear();

    rerender({ query: makeQuery({ aggregations: [AggregationType.Min, AggregationType.Max] }) });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(AGGREGATION_RUN_DELAY_MS);
    });
    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('exposes numeric aggregation state by default', () => {
    const { result } = renderHook(() =>
      useAggregationRun({
        query: makeQuery({ aggregations: [AggregationType.Min] }),
        onChange: jest.fn(),
        onRunQuery: jest.fn(),
      })
    );
    expect(result.current.aggregationState.kind).toBe('numeric');
    expect(result.current.aggregationState.value).toEqual([AggregationType.Min]);
  });

  it('exposes string aggregation state for string channels', () => {
    const { result } = renderHook(() =>
      useAggregationRun({ query: makeQuery({ channelDataType: 'string' }), onChange: jest.fn(), onRunQuery: jest.fn() })
    );
    expect(result.current.aggregationState.kind).toBe('string');
    expect(result.current.aggregationState.value).toEqual(['Mode']);
  });
});
