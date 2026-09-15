import React from 'react';
// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, render, screen, fireEvent, within, waitFor } from '@testing-library/react';
import { QueryEditor } from './QueryEditor';
import { NominalQuery, AggregationType, DEFAULT_AGGREGATIONS } from '../types';
import { DataSource } from '../datasource';
import { AGGREGATION_RUN_DELAY_MS } from './queryBuilder/useNominalQueryBuilder';

// Mock Grafana runtime
const post = jest.fn();
const publish = jest.fn();

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getBackendSrv: jest.fn(() => ({ post })),
  getAppEvents: jest.fn(() => ({ publish })),
  getTemplateSrv: jest.fn(() => ({ replace: (v: string) => v })),
}));

const mockDatasource = { url: '/api/datasources/uid/test/resources' } as unknown as DataSource;

const BASE_QUERY: Partial<NominalQuery> = {
  refId: 'A',
  assetRid: 'ri.scout.main.asset.abc123',
  dataScopeName: 'default',
  queryType: 'decimation',
  buckets: 1000,
};

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return { ...BASE_QUERY, ...overrides } as NominalQuery;
}

/** Find the Aggregation(s) InlineField wrapper containing both the label and field. */
function getAggregationSection() {
  const label = screen.getByText('Aggregation(s)');
  // The Grafana InlineField renders as: <div class="..."><label>Aggregation(s)</label><div>...field...</div></div>
  // label.parentElement is the <label>, label.parentElement.parentElement is the InlineField div.
  // But getByText returns the innermost text node container, which is the <label> itself.
  return label.closest('label')!.parentElement!;
}

async function settleInitialEffects() {
  await waitFor(() => {
    expect(post.mock.calls.length).toBeGreaterThan(0);
  });
}

describe('Aggregation widget', () => {
  beforeEach(() => {
    post.mockReset();
    publish.mockReset();
    post.mockResolvedValue({});
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it.each([
    { channel: 'state', channelDataType: 'string', value: 'Mode' },
    { channel: 'app.logs', channelDataType: 'log', value: 'Logs (raw)' },
  ])('renders a fixed-width read-only $value box for $channelDataType channels', async ({ channel, channelDataType, value }) => {
    render(
      <QueryEditor
        query={makeQuery({ channel, channelDataType })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );
    await settleInitialEffects();

    expect(screen.getByDisplayValue(value)).toBeInTheDocument();
    const aggSection = getAggregationSection();
    expect(within(aggSection).queryByRole('combobox')).not.toBeInTheDocument();
    expect(window.getComputedStyle(within(aggSection).getByTestId('input-wrapper')).width).toMatch(/px$/);
  });

  it('aggregation query changes trigger the current rerun path', async () => {
    jest.useFakeTimers();
    const onRunQuery = jest.fn();
    const onChange = jest.fn();

    const { rerender } = render(
      <QueryEditor
        query={makeQuery({ channel: 'temp', channelDataType: 'numeric' })}
        onChange={onChange}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );
    await settleInitialEffects();
    onRunQuery.mockClear();

    // Rerender with changed aggregations (simulating user selection via onChange callback)
    rerender(
      <QueryEditor
        query={makeQuery({ channel: 'temp', channelDataType: 'numeric', aggregations: [AggregationType.Min, AggregationType.Max] })}
        onChange={onChange}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );

    // Blur the aggregation combobox (not the channel Select's)
    const aggSection = getAggregationSection();
    const combobox = within(aggSection).getByRole('combobox');
    fireEvent.blur(combobox);

    expect(onRunQuery).not.toHaveBeenCalled();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(AGGREGATION_RUN_DELAY_MS);
      await Promise.resolve();
    });

    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it('does not schedule an aggregation rerun on initial complete query mount', async () => {
    jest.useFakeTimers();
    const onRunQuery = jest.fn();

    render(
      <QueryEditor
        query={makeQuery({
          channel: 'temp',
          channelDataType: 'numeric',
          aggregations: [AggregationType.Mean],
        })}
        onChange={jest.fn()}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );
    await settleInitialEffects();

    expect(onRunQuery).toHaveBeenCalledTimes(1);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(AGGREGATION_RUN_DELAY_MS);
      await Promise.resolve();
    });

    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it('aggregation blur without a query change does not add a rerun', async () => {
    const onRunQuery = jest.fn();

    render(
      <QueryEditor
        query={makeQuery({
          channel: 'temp',
          channelDataType: 'numeric',
          aggregations: [...DEFAULT_AGGREGATIONS],
        })}
        onChange={jest.fn()}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );
    await settleInitialEffects();

    // Clear calls from the "query complete" auto-run effect
    onRunQuery.mockClear();

    // Blur the aggregation combobox (same value as initial -> no additional onRunQuery)
    const aggSection = getAggregationSection();
    const combobox = within(aggSection).getByRole('combobox');
    fireEvent.blur(combobox);

    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('empty aggregations falls back to MEAN', async () => {
    jest.useFakeTimers();
    const onRunQuery = jest.fn();

    const { rerender } = render(
      <QueryEditor
        query={makeQuery({ channel: 'temp', channelDataType: 'numeric', aggregations: [] })}
        onChange={jest.fn()}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );
    await settleInitialEffects();

    // Empty aggregations should display Mean (the DEFAULT_AGGREGATIONS fallback)
    expect(screen.getByText('Mean')).toBeInTheDocument();

    onRunQuery.mockClear();

    // Rerender with explicit ['MEAN'] — the component should treat this as identical
    // to the empty-fallback state (both normalize to DEFAULT_AGGREGATIONS), so even
    // after the debounce window no rerun should be scheduled.
    rerender(
      <QueryEditor
        query={makeQuery({ channel: 'temp', channelDataType: 'numeric', aggregations: [AggregationType.Mean] })}
        onChange={jest.fn()}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );

    const aggSection = getAggregationSection();
    const combobox = within(aggSection).getByRole('combobox');
    fireEvent.blur(combobox);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(AGGREGATION_RUN_DELAY_MS);
      await Promise.resolve();
    });

    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('does not reschedule a rerun when aggregations arrive as a new array with identical values', async () => {
    jest.useFakeTimers();
    const onRunQuery = jest.fn();

    const { rerender } = render(
      <QueryEditor
        query={makeQuery({ channel: 'temp', channelDataType: 'numeric', aggregations: [AggregationType.Min, AggregationType.Max] })}
        onChange={jest.fn()}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );
    await settleInitialEffects();
    onRunQuery.mockClear();

    // Simulate Grafana re-cloning the query target: same values, brand-new array reference.
    rerender(
      <QueryEditor
        query={makeQuery({ channel: 'temp', channelDataType: 'numeric', aggregations: [AggregationType.Min, AggregationType.Max] })}
        onChange={jest.fn()}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(AGGREGATION_RUN_DELAY_MS);
      await Promise.resolve();
    });

    expect(onRunQuery).not.toHaveBeenCalled();
  });
});
