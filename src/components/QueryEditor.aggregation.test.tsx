import React from 'react';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { QueryEditor } from './QueryEditor';
import { NominalQuery, AggregationType, DEFAULT_AGGREGATIONS } from '../types';
import { DataSource } from '../datasource';

// Mock Grafana runtime
jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getBackendSrv: jest.fn(() => ({ post: jest.fn().mockResolvedValue({}) })),
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

describe('Aggregation widget', () => {
  it('renders disabled Mode input for string channels', () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'state', channelDataType: 'string' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    // String channels render a read-only "Mode" input instead of the MultiSelect
    const modeInput = screen.getByDisplayValue('Mode');
    expect(modeInput).toBeInTheDocument();
    // The Grafana Input component with disabled prop renders as a visually
    // disabled field. Verify there's no MultiSelect combobox in the aggregation section.
    const aggSection = getAggregationSection();
    expect(within(aggSection).queryByRole('combobox')).not.toBeInTheDocument();
  });

  it('blur with changed aggregations calls onRunQuery', () => {
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

    // Rerender with changed aggregations (simulating user selection via onChange callback)
    rerender(
      <QueryEditor
        query={makeQuery({ channel: 'temp', channelDataType: 'numeric', aggregations: [AggregationType.Min, AggregationType.Max] })}
        onChange={onChange}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );

    // Blur the aggregation MultiSelect's combobox (not the channel Select's)
    const aggSection = getAggregationSection();
    const combobox = within(aggSection).getByRole('combobox');
    fireEvent.blur(combobox);

    expect(onRunQuery).toHaveBeenCalled();
  });

  it('blur without change does not call onRunQuery', () => {
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

    // Clear calls from the "query complete" auto-run effect
    onRunQuery.mockClear();

    // Blur the aggregation MultiSelect (same value as initial → no additional onRunQuery)
    const aggSection = getAggregationSection();
    const combobox = within(aggSection).getByRole('combobox');
    fireEvent.blur(combobox);

    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('empty aggregations falls back to MEAN', () => {
    const onRunQuery = jest.fn();

    const { rerender } = render(
      <QueryEditor
        query={makeQuery({ channel: 'temp', channelDataType: 'numeric', aggregations: [] })}
        onChange={jest.fn()}
        onRunQuery={onRunQuery}
        datasource={mockDatasource}
      />
    );

    // Empty aggregations should display Mean (the DEFAULT_AGGREGATIONS fallback)
    expect(screen.getByText('Mean')).toBeInTheDocument();

    onRunQuery.mockClear();

    // Rerender with explicit ['MEAN'] — the component should treat this as identical
    // to the empty-fallback state, so blur should NOT trigger onRunQuery.
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

    expect(onRunQuery).not.toHaveBeenCalled();
  });
});
