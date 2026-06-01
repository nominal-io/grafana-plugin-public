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
  assetInputMethod: 'direct',
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

  it('renders disabled Mode input for string channels', async () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'state', channelDataType: 'string' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );
    await settleInitialEffects();

    // String channels render a read-only "Mode" input instead of a multi-value picker.
    const modeInput = screen.getByDisplayValue('Mode');
    expect(modeInput).toBeInTheDocument();
    // The Grafana Input component with disabled prop renders as a visually
    // disabled field. Verify there's no combobox in the aggregation section.
    const aggSection = getAggregationSection();
    expect(within(aggSection).queryByRole('combobox')).not.toBeInTheDocument();
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

  it('renders disabled Logs (raw) input for log channels', async () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'app.logs', channelDataType: 'log' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );
    await settleInitialEffects();

    const logsInput = screen.getByDisplayValue('Logs (raw)');
    expect(logsInput).toBeInTheDocument();
    const aggSection = getAggregationSection();
    expect(within(aggSection).queryByRole('combobox')).not.toBeInTheDocument();
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
