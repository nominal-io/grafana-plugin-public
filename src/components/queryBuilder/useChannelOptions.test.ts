// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, renderHook } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { searchChannels, type Asset, type Channel } from '../../utils/api';
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

const ASSET: Asset = {
  rid: 'ri.scout.main.asset.a',
  title: 'Asset A',
  labels: [],
  dataScopes: [
    {
      dataScopeName: 'default',
      dataSource: { type: 'dataset', dataset: 'ri.scout.main.dataset.a' },
      timestampType: 'ABSOLUTE',
      seriesTags: {},
    },
  ],
  properties: {},
};

const MULTI_SCOPE_ASSET: Asset = {
  ...ASSET,
  dataScopes: [
    {
      dataScopeName: 'scope-a',
      dataSource: { type: 'dataset', dataset: 'ri.scout.main.dataset.a' },
      timestampType: 'ABSOLUTE',
      seriesTags: {},
    },
    {
      dataScopeName: 'scope-b',
      dataSource: { type: 'dataset', dataset: 'ri.scout.main.dataset.b' },
      timestampType: 'ABSOLUTE',
      seriesTags: {},
    },
  ],
};

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return { refId: 'A', dataScopeName: 'default', ...overrides } as NominalQuery;
}

function args(overrides: Record<string, unknown> = {}) {
  const query = (overrides.query as NominalQuery | undefined) || makeQuery();
  const replace = (value: string) => value;
  return {
    query,
    onChange: jest.fn(),
    selectedAsset: ASSET as Asset | null,
    channelResolution: resolveTemplateValue(query.channel, replace),
    dataScopeResolution: resolveTemplateValue(query.dataScopeName, replace),
    datasourceUrl: '/api/x',
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
    dataScopeResolution: unresolvedResolution('$scope'),
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

  it('returns no channel options and does not call the API without a selected asset', async () => {
    const { result } = renderHook(() => useChannelOptions(args({ selectedAsset: null })));

    let options: ChannelOption[] = [];
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      options = await result.current.channelOptions('temp');
    });

    expect(options).toEqual([]);
    expect(mockSearchChannels).not.toHaveBeenCalled();
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
        payload: ['Unable to load Nominal channels', 'Check the selected asset, data scope, and data source configuration.'],
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

  it('does not publish an alert when a previous asset request fails after switching to another asset with the same data source', async () => {
    const calls: Array<{
      reject: (error: Error) => void;
    }> = [];
    mockSearchChannels.mockImplementation(
      () =>
        new Promise<Channel[]>((_resolve, reject) => {
          calls.push({ reject });
        })
    );
    const nextAsset: Asset = {
      ...ASSET,
      rid: 'ri.scout.main.asset.b',
    };
    const { result, rerender } = renderHook(
      ({ selectedAsset }) => useChannelOptions(args({ selectedAsset })),
      { initialProps: { selectedAsset: ASSET } }
    );

    const olderRequest = result.current.channelOptions('old-asset-search');
    rerender({ selectedAsset: nextAsset });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      calls[0].reject(new Error('old asset failure'));
      await olderRequest;
    });

    expect(publish).not.toHaveBeenCalled();
  });

  it('loads channel options against the currently resolved template data scope', async () => {
    mockSearchChannels.mockResolvedValue([]);
    const query = makeQuery({ dataScopeName: '$scope' });
    const { result, rerender } = renderHook(
      ({ dataScopeName }) =>
        useChannelOptions(
          args({
            query,
            selectedAsset: MULTI_SCOPE_ASSET,
            dataScopeResolution: resolveTemplateValue(query.dataScopeName, () => dataScopeName),
          })
        ),
      { initialProps: { dataScopeName: 'scope-a' } }
    );

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await result.current.channelOptions('');
    });
    expect(mockSearchChannels).toHaveBeenCalledWith(
      '/api/x',
      ['ri.scout.main.dataset.a'],
      '',
      { requestId: expect.stringMatching(/^nominal-channel-options-\d+$/) }
    );

    mockSearchChannels.mockClear();
    rerender({ dataScopeName: 'scope-b' });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await result.current.channelOptions('');
    });
    expect(mockSearchChannels).toHaveBeenCalledWith(
      '/api/x',
      ['ri.scout.main.dataset.b'],
      '',
      { requestId: expect.stringMatching(/^nominal-channel-options-\d+$/) }
    );
  });

  it('does not rebuild options when resolution references change but primitive fields do not', () => {
    const { hookArgs, rerender } = renderMemoTestHook();
    const nextChannelResolution = unresolvedResolution('$channel');
    const nextDataScopeResolution = unresolvedResolution('$scope');
    expect(nextChannelResolution).not.toBe(hookArgs.channelResolution);
    expect(nextDataScopeResolution).not.toBe(hookArgs.dataScopeResolution);

    rerender({
      ...hookArgs,
      channelResolution: nextChannelResolution,
      dataScopeResolution: nextDataScopeResolution,
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
});
