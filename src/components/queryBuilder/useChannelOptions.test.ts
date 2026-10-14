// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, renderHook } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { searchChannels, type Channel } from '../../utils/api';
import type { ChannelOption } from './queryBuilderTypes';
import { buildChannelOptions, getChannelSelectValue } from './queryBuilderOptions';
import { resolveTemplateValue, type TemplateValueResolution } from './templateResolution';
import { useChannelOptions } from './useChannelOptions';

const publish = jest.fn();
jest.mock('@grafana/runtime', () => ({
  getTemplateSrv: jest.fn(() => ({ replace: (v: string) => v })),
  getAppEvents: jest.fn(() => ({ publish })),
}));

jest.mock('../../utils/api', () => ({
  ...jest.requireActual('../../utils/api'),
  searchChannels: jest.fn(),
}));

jest.mock('./queryBuilderOptions', () => {
  const actual = jest.requireActual('./queryBuilderOptions');
  return {
    ...actual,
    buildChannelOptions: jest.fn(actual.buildChannelOptions),
    getChannelSelectValue: jest.fn(actual.getChannelSelectValue),
  };
});

const mockSearchChannels = searchChannels as jest.MockedFunction<typeof searchChannels>;
const mockBuildChannelOptions = buildChannelOptions as jest.MockedFunction<typeof buildChannelOptions>;
const mockGetChannelSelectValue = getChannelSelectValue as jest.MockedFunction<typeof getChannelSelectValue>;

const DATASET_A = 'ri.scout.main.dataset.a';
const DATASET_B = 'ri.scout.main.dataset.b';

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return { refId: 'A', dataScopeName: 'default', ...overrides } as NominalQuery;
}

function args(overrides: Record<string, unknown> = {}) {
  const query = (overrides.query as NominalQuery | undefined) || makeQuery();
  const replace = (value: string) => value;
  return {
    query,
    onChange: jest.fn(),
    dataSourceRids: [DATASET_A],
    channelResolution: resolveTemplateValue(query.channel, replace),
    datasourceUrl: '/api/x',
    hasUserInteracted: false,
    markInteracted: jest.fn(),
    ...overrides,
  };
}

function unresolvedResolution(raw: string, overrides: Partial<TemplateValueResolution> = {}): TemplateValueResolution {
  return {
    raw,
    resolved: '',
    hasTemplate: true,
    isResolved: false,
    ...overrides,
  };
}

function renderMemoTestHook() {
  const query = makeQuery({ channel: '$channel', dataScopeName: '$scope' });
  const hookArgs = args({
    query,
    channelResolution: unresolvedResolution('$channel'),
  });
  const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useChannelOptions(nextArgs), {
    initialProps: hookArgs,
  });

  mockBuildChannelOptions.mockClear();
  mockGetChannelSelectValue.mockClear();

  return { hookArgs, result, rerender };
}

describe('useChannelOptions', () => {
  beforeEach(() => {
    publish.mockReset();
    mockSearchChannels.mockReset();
    mockBuildChannelOptions.mockClear();
    mockGetChannelSelectValue.mockClear();
  });

  it('channelOptions loads typed channel options through the backend', async () => {
    mockSearchChannels.mockResolvedValue([{ name: 'temp', dataSource: 'ds', description: '', dataType: 'numeric' }]);
    const { result } = renderHook(() => useChannelOptions(args()));

    let options: ChannelOption[] = [];
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      options = await result.current.channelOptions('temp');
    });

    expect(mockSearchChannels).toHaveBeenCalledWith(
      '/api/x',
      ['ri.scout.main.dataset.a'],
      'temp',
      { requestId: expect.stringMatching(/^nominal-channel-options-\d+$/) }
    );
    expect(options).toEqual([expect.objectContaining({ label: 'temp', value: 'temp', dataType: 'numeric' })]);
  });

  it('uses one stable backend request id for channel option loads from the same hook instance', async () => {
    mockSearchChannels.mockResolvedValue([]);
    const { result } = renderHook(() => useChannelOptions(args()));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await result.current.channelOptions('temp');
      await result.current.channelOptions('pressure');
    });

    const calls = mockSearchChannels.mock.calls as unknown as Array<
      [string, string[], string, { requestId?: string }]
    >;
    const firstRequestId = calls[0][3]?.requestId;
    expect(firstRequestId).toEqual(expect.stringMatching(/^nominal-channel-options-\d+$/));
    expect(calls[1][3]).toEqual({ requestId: firstRequestId });
  });

  it('selectChannel marks interaction and emits channel + dataType', () => {
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const { result } = renderHook(() => useChannelOptions(args({ onChange, markInteracted })));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectChannel({ label: 'temp', value: 'temp', dataType: 'numeric' });
    });

    expect(markInteracted).toHaveBeenCalled();
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ channel: 'temp', channelDataType: 'numeric' }));

    onChange.mockClear();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectChannel({ label: 'manual.channel', value: 'manual.channel' });
    });

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ channel: 'manual.channel', channelDataType: '' }));
  });

  it('offers the template channel when the source has no data source RIDs', async () => {
    mockSearchChannels.mockResolvedValue([]);
    const query = makeQuery({ channel: '$chan' });
    const { result } = renderHook(() =>
      useChannelOptions(args({ query, dataSourceRids: [], channelResolution: unresolvedResolution('$chan') }))
    );

    let options: ChannelOption[] = [];
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      options = await result.current.channelOptions('');
    });

    expect(options.map((o) => o.value)).toEqual(['$chan']);
  });

  it.each([
    {
      name: 'keeps a saved type while the data source RIDs change before any edit',
      hasUserInteracted: false,
      inferred: false,
    },
    {
      name: 're-checks a saved type when the data source RIDs change after an edit',
      hasUserInteracted: true,
      inferred: true,
    },
  ])('$name', async ({ hasUserInteracted, inferred }) => {
    mockSearchChannels.mockResolvedValue([{ name: 'temp', dataSource: 'ds', description: '', dataType: 'string' }]);
    const query = makeQuery({ channel: 'temp', channelDataType: 'numeric' });
    const onChange = jest.fn();
    const { rerender } = renderHook(
      ({ dataSourceRids }) => useChannelOptions(args({ query, onChange, dataSourceRids, hasUserInteracted })),
      { initialProps: { dataSourceRids: [] as string[] } }
    );

    // A dashboard variable that switches the scope changes the RIDs after the first list.
    for (const dataSourceRids of [[DATASET_A], [DATASET_B]]) {
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      await act(async () => {
        rerender({ dataSourceRids });
      });
    }

    expect(onChange.mock.calls.some(([q]) => q.channelDataType === 'string')).toBe(inferred);
  });

  it('publishes a Grafana alert and returns empty options when channel loading fails', async () => {
    mockSearchChannels.mockRejectedValue(new Error('simulated failure'));
    const { result } = renderHook(() => useChannelOptions(args()));

    let options: ChannelOption[] = [];
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      options = await result.current.channelOptions('temp');
    });

    expect(options).toEqual([]);
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: ['Unable to load Nominal channels', 'Check the selected source and data source configuration.'],
      })
    );
  });

  it('does not publish an alert when a stale channel loading request fails', async () => {
    const calls: Array<{
      resolve: (channels: Channel[]) => void;
      reject: (error: Error) => void;
    }> = [];
    mockSearchChannels.mockImplementation(
      () =>
        new Promise<Channel[]>((resolve, reject) => {
          calls.push({ resolve, reject });
        })
    );
    const { result } = renderHook(() => useChannelOptions(args()));

    const olderRequest = result.current.channelOptions('older');
    const newerRequest = result.current.channelOptions('newer');

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      calls[1].resolve([{ name: 'newer', dataSource: 'ds', description: '', dataType: 'numeric' }]);
      await newerRequest;
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      calls[0].reject(new Error('older failure'));
      await olderRequest;
    });

    expect(publish).not.toHaveBeenCalled();
  });

  it('does not publish an alert when a request for earlier data source RIDs fails', async () => {
    const calls: Array<{
      reject: (error: Error) => void;
    }> = [];
    mockSearchChannels.mockImplementation(
      () =>
        new Promise<Channel[]>((_resolve, reject) => {
          calls.push({ reject });
        })
    );
    const { result, rerender } = renderHook(({ dataSourceRids }) => useChannelOptions(args({ dataSourceRids })), {
      initialProps: { dataSourceRids: [DATASET_A] },
    });

    const olderRequest = result.current.channelOptions('old-asset-search');
    rerender({ dataSourceRids: [DATASET_B] });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      calls[0].reject(new Error('old asset failure'));
      await olderRequest;
    });

    expect(publish).not.toHaveBeenCalled();
  });

  it('reports an inferred data type through the latest onChange', async () => {
    let resolveSearch: (channels: Channel[]) => void = () => undefined;
    mockSearchChannels.mockReturnValue(new Promise((resolve) => (resolveSearch = resolve)));
    const query = makeQuery({ channel: '$chan' });
    const channelResolution = { raw: '$chan', resolved: 'temp', hasTemplate: true, isResolved: true };
    const staleOnChange = jest.fn();
    const latestOnChange = jest.fn();
    const { rerender } = renderHook(({ onChange }) => useChannelOptions(args({ query, channelResolution, onChange })), {
      initialProps: { onChange: staleOnChange },
    });
    rerender({ onChange: latestOnChange });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      resolveSearch([{ name: 'temp', dataSource: 'ds', description: '', dataType: 'numeric' }]);
    });

    expect(staleOnChange).not.toHaveBeenCalled();
    expect(latestOnChange).toHaveBeenCalledWith(expect.objectContaining({ channelDataType: 'numeric' }));
  });

  it('loads channel options against the current data source RIDs', async () => {
    mockSearchChannels.mockResolvedValue([]);
    const { result, rerender } = renderHook(({ dataSourceRids }) => useChannelOptions(args({ dataSourceRids })), {
      initialProps: { dataSourceRids: [DATASET_A] },
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await result.current.channelOptions('');
    });
    expect(mockSearchChannels).toHaveBeenLastCalledWith('/api/x', [DATASET_A], '', expect.anything());

    rerender({ dataSourceRids: [DATASET_B] });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await result.current.channelOptions('');
    });
    expect(mockSearchChannels).toHaveBeenLastCalledWith('/api/x', [DATASET_B], '', expect.anything());
  });

  it('does not rebuild options when resolution references change but primitive fields do not', () => {
    const { hookArgs, rerender } = renderMemoTestHook();
    const nextChannelResolution = unresolvedResolution('$channel');
    expect(nextChannelResolution).not.toBe(hookArgs.channelResolution);

    rerender({
      ...hookArgs,
      channelResolution: nextChannelResolution,
    });

    expect(mockBuildChannelOptions).not.toHaveBeenCalled();
    expect(mockGetChannelSelectValue).not.toHaveBeenCalled();
  });

  it.each([
    ['raw', { raw: '$channel2' }],
    ['resolved', { resolved: 'temperature' }],
    ['hasTemplate', { hasTemplate: false }],
    ['isResolved', { isResolved: true }],
  ] satisfies Array<[string, Partial<TemplateValueResolution>]>)(
    'rebuilds options when channel %s changes',
    async (_field, channelChange) => {
      mockSearchChannels.mockResolvedValue([]);
      const { hookArgs, result, rerender } = renderMemoTestHook();

      rerender({
        ...hookArgs,
        channelResolution: {
          ...hookArgs.channelResolution,
          ...channelChange,
        },
      });

      // eslint-disable-next-line @typescript-eslint/no-deprecated
      await act(async () => {
        await result.current.channelOptions('');
      });

      expect(mockBuildChannelOptions).toHaveBeenCalledTimes(1);
      expect(mockGetChannelSelectValue).toHaveBeenCalledTimes(1);
    }
  );

  it('reorders server results so an exact channel name match is first', async () => {
    const exact = 'vehicle.telemetry.engine_metrics.rpm_rx_count';
    const variant = 'vehicle_telemetry_engine_metrics_rpm_rx_count';
    mockSearchChannels.mockResolvedValue([
      { name: variant, dataSource: 'ds', description: '', dataType: 'numeric' },
      { name: exact, dataSource: 'ds', description: '', dataType: 'numeric' },
    ]);
    const { result } = renderHook(() => useChannelOptions(args()));

    let options: ChannelOption[] = [];
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      options = await result.current.channelOptions(exact);
    });

    expect(options.map((option) => option.value)).toEqual([exact, variant]);
  });

  it('appends the template channel entry during a literal search instead of pinning it first', async () => {
    const exact = 'vehicle.telemetry.engine_metrics.rpm_rx_count';
    mockSearchChannels.mockResolvedValue([{ name: exact, dataSource: 'ds', description: '', dataType: 'numeric' }]);
    const query = makeQuery({ channel: '$chan' });
    const { result } = renderHook(() =>
      useChannelOptions(args({ query, channelResolution: resolveTemplateValue('$chan', (value) => value) }))
    );

    let options: ChannelOption[] = [];
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      options = await result.current.channelOptions(exact);
    });

    expect(options.map((option) => option.value)).toEqual([exact, '$chan']);
  });
});
