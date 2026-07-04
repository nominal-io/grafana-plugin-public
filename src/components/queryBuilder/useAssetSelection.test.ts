import { StrictMode } from 'react';
// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, renderHook, waitFor } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { fetchAssetByRid, searchAssets, type Asset } from '../../utils/api';
import { resolveTemplateValue } from './templateResolution';
import { useAssetSelection } from './useAssetSelection';

const publish = jest.fn();
jest.mock('@grafana/runtime', () => ({
  getTemplateSrv: jest.fn(() => ({ replace: (v: string) => v })),
  getAppEvents: jest.fn(() => ({ publish })),
}));

jest.mock('../../utils/api', () => ({
  ...jest.requireActual('../../utils/api'),
  searchAssets: jest.fn(),
  fetchAssetByRid: jest.fn(),
}));

const mockSearchAssets = searchAssets as jest.MockedFunction<typeof searchAssets>;
const mockFetchAssetByRid = fetchAssetByRid as jest.MockedFunction<typeof fetchAssetByRid>;

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

const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return { refId: 'A', ...overrides } as NominalQuery;
}

function args(overrides: Record<string, unknown> = {}) {
  const query = (overrides.query as NominalQuery | undefined) || makeQuery();
  const replace = (value: string) => value;
  return {
    query,
    onChange: jest.fn(),
    datasourceUrl: '/api/x',
    assetRidResolution: resolveTemplateValue(query.assetRid, replace),
    dataScopeResolution: resolveTemplateValue(query.dataScopeName, replace),
    resolveTemplateText: (value: string) => resolveTemplateValue(value, replace),
    hasUserInteracted: false,
    markInteracted: jest.fn(),
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

type HookArgs = Parameters<typeof useAssetSelection>[0];

function renderAssetSelectionHarness({
  initialQuery = makeQuery(),
  hookOverrides = {},
  hasUserInteracted = false,
}: {
  initialQuery?: NominalQuery;
  hookOverrides?: Partial<HookArgs>;
  hasUserInteracted?: boolean;
} = {}) {
  let currentQuery = initialQuery;
  // Mirrors the parent's interaction latch: markInteracted flips it, and every
  // rerender feeds it back as the hasUserInteracted prop unless overridden.
  let interacted = hasUserInteracted;
  const onChange = jest.fn((nextQuery: NominalQuery) => {
    currentQuery = nextQuery;
  });
  const markInteracted = jest.fn(() => {
    interacted = true;
  });
  const renderResult = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
    initialProps: args({ query: currentQuery, onChange, markInteracted, hasUserInteracted, ...hookOverrides }),
  });

  const rerenderArgs = (query: NominalQuery, overrides: Partial<HookArgs>) =>
    args({ query, onChange, markInteracted, hasUserInteracted: interacted, ...hookOverrides, ...overrides });

  return {
    ...renderResult,
    onChange,
    markInteracted,
    get currentQuery() {
      return currentQuery;
    },
    rerenderCurrent(overrides: Partial<HookArgs> = {}) {
      renderResult.rerender(rerenderArgs(currentQuery, overrides));
    },
    rerenderQuery(query: NominalQuery, overrides: Partial<HookArgs> = {}) {
      renderResult.rerender(rerenderArgs(query, overrides));
    },
  };
}

async function renderSavedDirectAsset() {
  mockFetchAssetByRid.mockResolvedValue(ASSET);
  const query = makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'direct' });
  const view = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
    initialProps: args({ query }),
  });

  await waitFor(() => {
    expect(view.result.current.directRID).toBe(ASSET.rid);
    expect(view.result.current.selectedAsset?.rid).toBe(ASSET.rid);
  });
  return view;
}

describe('useAssetSelection', () => {
  let consoleErrorSpy: jest.SpyInstance;

  beforeEach(() => {
    publish.mockReset();
    mockSearchAssets.mockReset().mockResolvedValue([]);
    mockFetchAssetByRid.mockReset().mockResolvedValue(null);
    consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    const consoleErrorCalls = consoleErrorSpy.mock.calls;
    jest.useRealTimers();
    consoleErrorSpy.mockRestore();
    expect(consoleErrorCalls).toEqual([]);
  });

  it('does not search on mount; Combobox loader drives searching', () => {
    const hookArgs = args();
    renderHook(() => useAssetSelection(hookArgs));

    expect(mockSearchAssets).not.toHaveBeenCalled();
  });

  it('assetOptions loader searches and returns built options', async () => {
    mockSearchAssets.mockResolvedValue([ASSET]);
    const hookArgs = args({
      query: makeQuery(),
      assetRidResolution: resolveTemplateValue(ASSET.rid, (value) => value),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    const options = await result.current.assetOptions('asset a');

    expect(mockSearchAssets).toHaveBeenCalledWith('/api/x', 'asset a');
    expect(options).toEqual([
      {
        label: 'Asset A',
        value: ASSET.rid,
        description: 'No labels - 1 data scope(s)',
      },
    ]);
  });

  it('loader suppresses alert for superseded failed request', async () => {
    const first = deferred<Asset[]>();
    const second = deferred<Asset[]>();
    mockSearchAssets.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { result } = renderHook(() => useAssetSelection(args()));

    const firstOptions = result.current.assetOptions('first');
    const secondOptions = result.current.assetOptions('second');

    first.reject(new Error('first failed'));
    second.resolve([ASSET]);
    await expect(firstOptions).resolves.toEqual([]);
    await expect(secondOptions).resolves.toEqual(expect.arrayContaining([expect.objectContaining({ value: ASSET.rid })]));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });

    expect(publish).not.toHaveBeenCalled();
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
  });

  it('loader alerts when latest request fails', async () => {
    mockSearchAssets.mockRejectedValue(new Error('latest failed'));
    const { result } = renderHook(() => useAssetSelection(args()));

    await expect(result.current.assetOptions('asset')).resolves.toEqual([]);

    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: ['Unable to load Nominal assets', 'Check the data source configuration and try again.'],
      })
    );
  });

  it('loader suppresses alert after switching away from search mode', async () => {
    const pending = deferred<Asset[]>();
    mockSearchAssets.mockReturnValue(pending.promise);
    const { result } = renderHook(() => useAssetSelection(args()));

    const options = result.current.assetOptions('asset');
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeAssetInputMethod('direct');
    });
    pending.reject(new Error('stale failure'));

    await expect(options).resolves.toEqual([]);
    expect(publish).not.toHaveBeenCalled();
  });

  it('selectAsset uses the displayed asset without fetching and marks interaction', async () => {
    mockSearchAssets.mockResolvedValue([ASSET]);
    mockFetchAssetByRid.mockRejectedValue(new Error('transient fetch failure'));
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    const options = await result.current.assetOptions('asset a');
    expect(options).toEqual(expect.arrayContaining([expect.objectContaining({ value: ASSET.rid })]));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });

    expect(markInteracted).toHaveBeenCalledTimes(1);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: ASSET.rid, assetInputMethod: 'search' }));
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
    expect(result.current.selectedAsset).toEqual(ASSET);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
  });

  it('selectAsset ignores non-RID custom search text', () => {
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset('asset a');
    });

    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
    expect(onChange).not.toHaveBeenCalled();
  });

  it('restores saved direct-mode RID by fetching asset', async () => {
    await renderSavedDirectAsset();
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('mirrors saved direct-mode RID changes into the direct input', async () => {
    const ASSET_A = ASSET;
    mockFetchAssetByRid.mockImplementation(async (_url: string, rid: string) => {
      return rid === ASSET_B.rid ? ASSET_B : ASSET_A;
    });

    const queryA = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'direct' });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryA }),
    });

    await waitFor(() => {
      expect(result.current.directRID).toBe(ASSET_A.rid);
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });

    const queryB = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'direct' });
    rerender(args({ query: queryB }));

    await waitFor(() => {
      expect(result.current.directRID).toBe(ASSET_B.rid);
    });
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    });
  });

  it('clears selected asset and direct input when a saved direct RID is removed', async () => {
    const { result, rerender } = await renderSavedDirectAsset();

    rerender(args({ query: makeQuery({ assetRid: '', assetInputMethod: 'direct' }) }));

    await waitFor(() => {
      expect(result.current.directRID).toBe('');
      expect(result.current.selectedAsset).toBeNull();
      expect(result.current.dataScopeOptions).toEqual([]);
    });
  });

  it('restores saved search-mode RID via single by-RID fetch', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'search' }),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockSearchAssets).not.toHaveBeenCalled();
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('infers direct mode for saved RID without explicit input method', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid }),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(result.current.assetInputMethod).toBe('direct');
    expect(result.current.directRID).toBe(ASSET.rid);
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('changeAssetInputMethod preserves the selected asset and scope options without refetching the unchanged RID', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const harness = renderAssetSelectionHarness();
    const { result } = harness;

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
      expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    });
    harness.rerenderCurrent();
    mockFetchAssetByRid.mockClear();
    harness.onChange.mockClear();
    harness.markInteracted.mockClear();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeAssetInputMethod('direct');
    });

    expect(harness.markInteracted).toHaveBeenCalledTimes(1);
    expect(result.current.assetInputMethod).toBe('direct');
    expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    expect(harness.currentQuery).toEqual(expect.objectContaining({ assetRid: ASSET.rid, assetInputMethod: 'direct' }));

    harness.rerenderCurrent();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await Promise.resolve();
    });

    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
    expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);

    harness.onChange.mockClear();
    harness.markInteracted.mockClear();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeAssetInputMethod('search');
    });

    expect(harness.markInteracted).toHaveBeenCalledTimes(1);
    expect(result.current.assetInputMethod).toBe('search');
    expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    expect(harness.currentQuery).toEqual(expect.objectContaining({ assetRid: ASSET.rid, assetInputMethod: 'search' }));

    harness.rerenderCurrent();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await Promise.resolve();
    });

    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
    expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
  });

  it('user-entered concrete direct RID fetch stays with debounced direct handler, not query reconcile', async () => {
    jest.useFakeTimers();
    mockFetchAssetByRid.mockResolvedValue(ASSET_B);

    const harness = renderAssetSelectionHarness();
    const { result } = harness;

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });

    expect(harness.currentQuery).toEqual(
      expect.objectContaining({ assetRid: ASSET_B.rid, assetInputMethod: 'direct' })
    );
    harness.rerenderCurrent();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await Promise.resolve();
    });
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });

    await waitFor(() => {
      expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET_B.rid);
  });

  it('selecting concrete search RID fetches once and keeps it on resolution', async () => {
    const pendingFetch = deferred<Asset | null>();
    mockFetchAssetByRid.mockReturnValue(pendingFetch.promise);

    const harness = renderAssetSelectionHarness();
    const { result } = harness;

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_B.rid);
    });

    expect(harness.currentQuery).toEqual(
      expect.objectContaining({ assetRid: ASSET_B.rid, assetInputMethod: 'search' })
    );
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);

    harness.rerenderCurrent();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await Promise.resolve();
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingFetch.resolve(ASSET_B);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
  });

  it('hides stale asset controls while a picked RID is loading', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = {
      ...ASSET,
      rid: 'ri.scout.main.asset.bbb',
      title: 'Asset BBB',
      dataScopes: [{ ...ASSET.dataScopes[0], dataScopeName: 'new-scope' }],
    };
    const pendingFetch = deferred<Asset | null>();
    mockFetchAssetByRid.mockResolvedValueOnce(ASSET_A).mockReturnValueOnce(pendingFetch.promise);

    const hookArgs = args();
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_A.rid);
    });
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
      expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_B.rid);
    });

    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);
    // Hidden-state shape (null asset, 0 scopes, empty scope options) is covered by the
    // assetIdentity reducer unit tests; here we only assert this path enters that state.
    expect(result.current.selectedAsset).toBeNull();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingFetch.resolve(ASSET_B);
      await pendingFetch.promise;
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
  });

  it.each([
    {
      method: 'search' as const,
      actWithTemplate: (result: ReturnType<typeof renderAssetSelectionHarness>['result']) => {
        // eslint-disable-next-line @typescript-eslint/no-deprecated
        act(() => {
          result.current.selectAsset('$asset');
        });
      },
    },
    {
      method: 'direct' as const,
      actWithTemplate: (result: ReturnType<typeof renderAssetSelectionHarness>['result']) => {
        // eslint-disable-next-line @typescript-eslint/no-deprecated
        act(() => {
          result.current.changeDirectRID('$asset');
        });
      },
    },
  ])('clears an empty-resolved $method template so a later matching RID can show the selected asset', async ({ method, actWithTemplate }) => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replaceEmpty = (value: string) => (value === '$asset' ? '' : value);
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const queryA = makeQuery({ assetRid: ASSET.rid, assetInputMethod: method });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({
        query: queryA,
        onChange,
        markInteracted,
        resolveTemplateText: (value: string) => resolveTemplateValue(value, replaceEmpty),
      }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });

    actWithTemplate(result);

    // The parent renders the committed '$asset' (which resolves empty)...
    rerender(
      args({
        query: makeQuery({ assetRid: '$asset', assetInputMethod: method }),
        onChange,
        markInteracted,
        hasUserInteracted: true,
        assetRidResolution: resolveTemplateValue('$asset', replaceEmpty),
        resolveTemplateText: (value: string) => resolveTemplateValue(value, replaceEmpty),
      })
    );
    expect(result.current.selectedAsset).toBeNull();

    // ...then a later external change back to the concrete RID restores the asset:
    // the selection must not be wedged behind pendingAssetRid = ''.
    rerender(args({ query: queryA, onChange, markInteracted, hasUserInteracted: true }));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockFetchAssetByRid).not.toHaveBeenCalledWith('/api/x', '');

    // The empty resolver really did resolve the selection to an empty RID.
    expect(resolveTemplateValue('$asset', replaceEmpty).resolved).toBe('');
  });

  it('hides stale asset controls and skips stale scope writes while a query-driven RID restore is loading', async () => {
    // Asset A's scope set does NOT validate asset B's saved scope, so without the
    // stale guard the effect would auto-select A's single scope onto B's query.
    const ASSET_A_SINGLE_SCOPE: Asset = {
      ...ASSET,
      dataScopes: [{ ...ASSET.dataScopes[0], dataScopeName: 'a-scope' }],
    };
    const ASSET_B: Asset = {
      ...ASSET,
      rid: 'ri.scout.main.asset.bbb',
      title: 'Asset BBB',
      dataScopes: [{ ...ASSET.dataScopes[0], dataScopeName: 'b-scope' }],
    };
    const pendingB = deferred<Asset | null>();
    mockFetchAssetByRid.mockImplementation((_url: string, rid: string) =>
      rid === ASSET_B.rid ? pendingB.promise : Promise.resolve(ASSET_A_SINGLE_SCOPE)
    );

    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const queryA = makeQuery({
      assetRid: ASSET_A_SINGLE_SCOPE.rid,
      assetInputMethod: 'search',
      dataScopeName: 'a-scope',
    });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryA, onChange, markInteracted }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A_SINGLE_SCOPE.rid);
    });

    const queryB = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'search', dataScopeName: 'b-scope' });
    rerender(args({ query: queryB, onChange, markInteracted, hasUserInteracted: true }));

    // Controls hide as soon as the different RID starts resolving (shape asserted
    // in the reducer unit tests), and the stale asset A must not drive a scope write.
    expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET_B.rid);
    expect(result.current.selectedAsset).toBeNull();
    expect(onChange).not.toHaveBeenCalled();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingB.resolve(ASSET_B);
      await pendingB.promise;
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    });
    expect(onChange).not.toHaveBeenCalled();
  });

  it('does not let a stale event-owned fetch overwrite a newer query-driven restore', async () => {
    const ASSET_A = ASSET;
    const deferredByRid = new Map<string, ReturnType<typeof deferred<Asset | null>>>();
    mockFetchAssetByRid.mockImplementation((_url: string, rid: string) => {
      const d = deferred<Asset | null>();
      deferredByRid.set(rid, d);
      return d.promise;
    });

    const harness = renderAssetSelectionHarness();
    const { result } = harness;

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_A.rid);
    });

    const queryB = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'search' });
    harness.rerenderQuery(queryB);
    await waitFor(() => {
      expect(deferredByRid.has(ASSET_B.rid)).toBe(true);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      deferredByRid.get(ASSET_B.rid)!.resolve(ASSET_B);
      await deferredByRid.get(ASSET_B.rid)!.promise;
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      deferredByRid.get(ASSET_A.rid)!.resolve(ASSET_A);
      await deferredByRid.get(ASSET_A.rid)!.promise;
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
  });

  it('user-entered direct template RID reconciles through query-driven path', async () => {
    jest.useFakeTimers();
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replace = (value: string) => (value === '$asset' ? ASSET.rid : value);

    const harness = renderAssetSelectionHarness();
    const { result } = harness;

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID('$asset');
    });

    harness.rerenderCurrent({
      assetRidResolution: resolveTemplateValue(harness.currentQuery.assetRid, replace),
      resolveTemplateText: (value: string) => resolveTemplateValue(value, replace),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('saved direct-mode template RID fetches once', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replace = (v: string) => (v === '$asset' ? ASSET.rid : v);
    const query = makeQuery({ assetRid: '$asset', assetInputMethod: 'direct' });
    const hookArgs = args({
      query,
      assetRidResolution: resolveTemplateValue(query.assetRid, replace),
      resolveTemplateText: (value: string) => resolveTemplateValue(value, replace),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('saved search-mode RID under StrictMode restores', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'search' }),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs), { wrapper: StrictMode });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
  });

  it.each([
    {
      name: 'auto-selects the only data scope when none is saved',
      scopes: ['default'],
      savedScope: undefined,
      expected: { dataScopeName: 'default', assetInputMethod: 'search' },
    },
    {
      name: 'clears a saved data scope the asset does not offer',
      scopes: ['scope-1', 'scope-2'],
      savedScope: 'bogus',
      expected: { dataScopeName: '' },
    },
  ])('selected-asset effect reconciles data scope: $name', async ({ scopes, savedScope, expected }) => {
    const asset: Asset = {
      ...ASSET,
      dataScopes: scopes.map((name) => ({ ...ASSET.dataScopes[0], dataScopeName: name })),
    };
    mockFetchAssetByRid.mockResolvedValue(asset);

    const harness = renderAssetSelectionHarness({
      initialQuery: makeQuery({ assetRid: asset.rid, assetInputMethod: 'search', dataScopeName: savedScope }),
    });
    const { result } = harness;

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(asset.rid);
    });
    harness.onChange.mockClear();

    // No user event fires in this test; simulate the parent's latch directly.
    harness.rerenderCurrent({ hasUserInteracted: true });

    await waitFor(() => {
      expect(harness.onChange).toHaveBeenCalledWith(expect.objectContaining(expected));
    });
  });

  it('keeps a valid saved data scope while a different asset is still resolving', async () => {
    const ASSET_A: Asset = {
      ...ASSET,
      dataScopes: ['scope-1', 'scope-2'].map((name) => ({ ...ASSET.dataScopes[0], dataScopeName: name })),
    };
    const pendingB = deferred<Asset | null>();
    mockFetchAssetByRid.mockImplementation((_url: string, rid: string) =>
      rid === ASSET_B.rid ? pendingB.promise : Promise.resolve(ASSET_A)
    );

    const queryA = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'search', dataScopeName: 'scope-1' });
    const harness = renderAssetSelectionHarness({ initialQuery: queryA });
    const { result } = harness;

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_B.rid);
    });
    harness.onChange.mockClear();

    // The parent still renders asset A and its valid saved scope
    // while B's fetch is in flight. The masked visible identity (empty scopes)
    // must not lead the scope-defaulting effect to clear the saved scope or
    // recommit the stale query.
    harness.rerenderQuery(queryA);

    expect(harness.onChange).not.toHaveBeenCalled();
  });

  it('keeps the RID with a fallback asset and alerts when the by-RID fetch fails', async () => {
    mockFetchAssetByRid.mockRejectedValue(new Error('fetch failed'));
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'direct' }),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(result.current.selectedAsset?.dataScopes).toEqual([]);
    expect(publish).toHaveBeenCalledTimes(1);
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: [
          'Unable to load Nominal asset',
          'The RID was kept, but data scopes could not be loaded automatically.',
        ],
      })
    );
  });

  it('selectAsset ignores a whitespace-only value and clears the selection', () => {
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset('   ');
    });

    expect(markInteracted).toHaveBeenCalledTimes(1);
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    expect(result.current.selectedAsset).toBeNull();
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: '' }));
  });

  it('re-picking the already selected asset does not refetch or hide the selection', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    onChange.mockClear();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });

    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: ASSET.rid }));
  });

  it('re-picking a fallback asset after a failed fetch retries the lookup', async () => {
    mockFetchAssetByRid.mockRejectedValueOnce(new Error('transient')).mockResolvedValue(ASSET);
    const hookArgs = args();
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    // Failed fetch left a fallback asset without data scopes.
    expect(result.current.selectedAsset?.dataScopes).toEqual([]);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });

    // The re-pick must not be swallowed by the same-RID guard: it retries and
    // recovers the real asset.
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);
    await waitFor(() => {
      expect(result.current.selectedAsset?.dataScopes).toEqual(ASSET.dataScopes);
    });
    expect(publish).toHaveBeenCalledTimes(1);
  });
});
