// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, renderHook, waitFor } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { searchChannels, type Asset, type Channel } from '../../utils/api';
import type { AssetInputMethod } from './queryBuilderTypes';
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

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

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
    assetInputMethod: 'search' as AssetInputMethod,
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
    selectedAsset: null,
    channelResolution: unresolvedResolution('$channel'),
    dataScopeResolution: unresolvedResolution('$scope'),
  });
  const { rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useChannelOptions(nextArgs), {
    initialProps: hookArgs,
  });

  mockBuildChannelOptions.mockClear();
  mockGetChannelSelectValue.mockClear();

  return { hookArgs, rerender };
}

describe('useChannelOptions', () => {
  beforeEach(() => {
    publish.mockReset();
    mockSearchChannels.mockReset();
    mockBuildChannelOptions.mockClear();
    mockGetChannelSelectValue.mockClear();
    jest.useFakeTimers();
  });
  afterEach(() => jest.useRealTimers());

  it('openChannelMenu loads channel options into state', async () => {
    mockSearchChannels.mockResolvedValue([{ name: 'temp', dataSource: 'ds', description: '', dataType: 'numeric' }]);
    const { result } = renderHook(() => useChannelOptions(args()));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.openChannelMenu();
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.channelOptions.some((o) => o.value === 'temp')).toBe(true);
    });
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
  });

  it('returns no channel options and does not call the API without a selected asset', async () => {
    const { result } = renderHook(() => useChannelOptions(args({ selectedAsset: null })));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.openChannelMenu();
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.channelOptions).toEqual([]);
    });
    expect(mockSearchChannels).not.toHaveBeenCalled();
  });

  it('preloads channel options again when a template data scope resolves to a different scope', async () => {
    mockSearchChannels.mockResolvedValue([]);
    const query = makeQuery({ dataScopeName: '$scope' });
    const { rerender } = renderHook(
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
      jest.advanceTimersByTime(300);
      await Promise.resolve();
    });
    expect(mockSearchChannels).toHaveBeenCalledWith('/api/x', ['ri.scout.main.dataset.a'], '');

    mockSearchChannels.mockClear();
    rerender({ dataScopeName: 'scope-b' });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
    });
    expect(mockSearchChannels).toHaveBeenCalledWith('/api/x', ['ri.scout.main.dataset.b'], '');
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
    (_field, channelChange) => {
      const { hookArgs, rerender } = renderMemoTestHook();

      rerender({
        ...hookArgs,
        channelResolution: {
          ...hookArgs.channelResolution,
          ...channelChange,
        },
      });

      expect(mockBuildChannelOptions).toHaveBeenCalledTimes(1);
      expect(mockGetChannelSelectValue).toHaveBeenCalledTimes(1);
    }
  );

  // The channelSearchId counter must discard a slow earlier response so it can't overwrite the
  // results of a newer search that already resolved.
  it('discards a stale channel search response when a newer search resolves first', async () => {
    const calls: Array<ReturnType<typeof deferred<Channel[]>>> = [];
    mockSearchChannels.mockImplementation(() => {
      const d = deferred<Channel[]>();
      calls.push(d);
      return d.promise;
    });
    const { result } = renderHook(() => useChannelOptions(args()));

    // Mount preload issues the first debounced search (id=1).
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
    });
    // A newer search (id=2).
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.searchChannels('newer');
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
    });

    expect(calls.length).toBe(2);

    // Newer search (id=2) resolves first and is applied.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      calls[1].resolve([{ name: 'newer-chan', dataSource: 'ds', description: '', dataType: 'numeric' }]);
      await Promise.resolve();
      await Promise.resolve();
    });
    // Stale search (id=1) resolves afterwards and must be discarded by the counter guard.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      calls[0].resolve([{ name: 'stale-chan', dataSource: 'ds', description: '', dataType: 'numeric' }]);
      await Promise.resolve();
      await Promise.resolve();
    });

    const values = result.current.channelOptions.map((o) => o.value);
    expect(values).toContain('newer-chan');
    expect(values).not.toContain('stale-chan');
  });
});
