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

// Installs a fetch-by-RID mock that defers every lookup and hands back per-RID
// settle handles, so a test can stage several by-RID fetches in flight at once and
// resolve them out of order (newer first, stale last) to exercise the abort guards.
function deferByRidFetches() {
  const pending = new Map<string, ReturnType<typeof deferred<Asset | null>>>();
  mockFetchAssetByRid.mockImplementation((_url: string, rid: string) => {
    const entry = deferred<Asset | null>();
    pending.set(rid, entry);
    return entry.promise;
  });
  const settle = async (asset: Asset) => {
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      const entry = pending.get(asset.rid)!;
      entry.resolve(asset);
      await entry.promise;
      await Promise.resolve();
      await Promise.resolve();
    });
  };
  return { pending, settle };
}

type HookArgs = Parameters<typeof useAssetSelection>[0];
type AssetSelectionResult = { current: Pick<ReturnType<typeof useAssetSelection>, 'selectAsset'> };

function selectAsset(result: AssetSelectionResult, assetRid: string): void {
  // eslint-disable-next-line @typescript-eslint/no-deprecated
  act(() => {
    result.current.selectAsset(assetRid);
  });
}

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

async function renderSavedAsset() {
  mockFetchAssetByRid.mockResolvedValue(ASSET);
  const query = makeQuery({ assetRid: ASSET.rid });
  const view = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
    initialProps: args({ query }),
  });

  await waitFor(() => {
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
    await expect(secondOptions).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ value: ASSET.rid })])
    );
    selectAsset(result, ASSET.rid);

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

  it('selectAsset uses the displayed asset without fetching and marks interaction', async () => {
    mockSearchAssets.mockResolvedValue([ASSET]);
    mockFetchAssetByRid.mockRejectedValue(new Error('transient fetch failure'));
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    const options = await result.current.assetOptions('asset a');
    expect(options).toEqual(expect.arrayContaining([expect.objectContaining({ value: ASSET.rid })]));
    selectAsset(result, ASSET.rid);

    expect(markInteracted).toHaveBeenCalledTimes(1);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: ASSET.rid }));
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
    expect(result.current.selectedAsset).toEqual(ASSET);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
  });

  it('selectAsset ignores non-RID custom search text', () => {
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    selectAsset(result, 'asset a');

    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
    expect(onChange).not.toHaveBeenCalled();
  });

  it('commits an unresolved template variable and clears the resolved selection', async () => {
    // The harness's default resolver leaves any "$var" unresolved (identity replace).
    mockSearchAssets.mockResolvedValue([ASSET]);
    const onChange = jest.fn();
    const hookArgs = args({ onChange });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // Establish a resolved selection via a displayed option (no by-RID fetch).
    await result.current.assetOptions('asset a');
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });
    expect(result.current.selectedAsset).toEqual(ASSET);

    // Selecting an unresolved template variable clears the visible selection...
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset('$missing');
    });
    expect(result.current.selectedAsset).toBeNull();
    // ...but persists the raw variable so it resolves once the variable is defined.
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ assetRid: '$missing' }));
    // No by-RID fetch is attempted for the unresolved variable itself.
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
  });

  it('clears the selected asset when a saved RID is removed', async () => {
    const { result, rerender } = await renderSavedAsset();

    rerender(args({ query: makeQuery({ assetRid: '' }) }));

    await waitFor(() => {
      expect(result.current.selectedAsset).toBeNull();
      expect(result.current.dataScopeOptions).toEqual([]);
    });
  });

  it('restores a saved concrete RID via a single by-RID fetch', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid }),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockSearchAssets).not.toHaveBeenCalled();
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('restores a legacy saved query that still carries the removed input-method key', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    // Old dashboards persisted assetInputMethod; the stale JSON key must be
    // ignored by the single reconcile path. The cast mimics untyped saved JSON.
    const legacyQuery = { ...makeQuery({ assetRid: ASSET.rid }), assetInputMethod: 'direct' } as NominalQuery;
    const hookArgs = args({ query: legacyQuery });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('selecting a new concrete RID resolves it through the reconcile effect', async () => {
    const pendingFetch = deferred<Asset | null>();
    mockFetchAssetByRid.mockReturnValue(pendingFetch.promise);

    const harness = renderAssetSelectionHarness();
    const { result } = harness;
    selectAsset(result, ASSET_B.rid);

    expect(harness.currentQuery).toEqual(expect.objectContaining({ assetRid: ASSET_B.rid }));
    // No immediate fetch: the reconcile effect is the single by-RID fetch trigger.
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();

    harness.rerenderCurrent();
    await waitFor(() => {
      expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    });

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
    const ASSET_B_LOADING: Asset = {
      ...ASSET,
      rid: 'ri.scout.main.asset.bbb',
      title: 'Asset BBB',
      dataScopes: [{ ...ASSET.dataScopes[0], dataScopeName: 'new-scope' }],
    };
    const pendingFetch = deferred<Asset | null>();
    mockFetchAssetByRid.mockResolvedValueOnce(ASSET_A).mockReturnValueOnce(pendingFetch.promise);

    const harness = renderAssetSelectionHarness();
    const { result } = harness;
    selectAsset(result, ASSET_A.rid);
    harness.rerenderCurrent();
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
      expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    });
    selectAsset(result, ASSET_B_LOADING.rid);

    // beginResolving masks the stale asset synchronously, before the reconcile fetch.
    expect(result.current.selectedAsset).toBeNull();

    harness.rerenderCurrent();
    await waitFor(() => {
      expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingFetch.resolve(ASSET_B_LOADING);
      await pendingFetch.promise;
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B_LOADING.rid);
  });

  it('clears an empty-resolved template so a later matching RID can show the selected asset', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replaceEmpty = (value: string) => (value === '$asset' ? '' : value);
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const queryA = makeQuery({ assetRid: ASSET.rid });
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
    selectAsset(result, '$asset');

    rerender(
      args({
        query: makeQuery({ assetRid: '$asset' }),
        onChange,
        markInteracted,
        hasUserInteracted: true,
        assetRidResolution: resolveTemplateValue('$asset', replaceEmpty),
        resolveTemplateText: (value: string) => resolveTemplateValue(value, replaceEmpty),
      })
    );
    expect(result.current.selectedAsset).toBeNull();

    rerender(args({ query: queryA, onChange, markInteracted, hasUserInteracted: true }));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockFetchAssetByRid).not.toHaveBeenCalledWith('/api/x', '');
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
      dataScopeName: 'a-scope',
    });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryA, onChange, markInteracted }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A_SINGLE_SCOPE.rid);
    });

    const queryB = makeQuery({ assetRid: ASSET_B.rid, dataScopeName: 'b-scope' });
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

  it('drops a stale query-driven asset fetch when a newer restore resolves first', async () => {
    const { pending, settle } = deferByRidFetches();

    // Saved query restores asset A, whose by-RID fetch is left in flight.
    const harness = renderAssetSelectionHarness({ initialQuery: makeQuery({ assetRid: ASSET.rid }) });
    const { result } = harness;
    await waitFor(() => {
      expect(pending.has(ASSET.rid)).toBe(true);
    });

    // The query changes to asset B before A resolves; reconcile's beginFetch must
    // supersede A's controller so both fetches are in flight at once.
    harness.rerenderQuery(makeQuery({ assetRid: ASSET_B.rid }));
    await waitFor(() => {
      expect(pending.has(ASSET_B.rid)).toBe(true);
    });

    // Newer restore (B) resolves first and wins.
    await settle(ASSET_B);
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);

    // Stale restore (A) resolves afterwards; its controller was aborted, so the late
    // response must not clobber B.
    await settle(ASSET);
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
  });

  it('user-selected template RID reconciles through the query-driven path', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replace = (value: string) => (value === '$asset' ? ASSET.rid : value);

    const harness = renderAssetSelectionHarness({
      hookOverrides: { resolveTemplateText: (value: string) => resolveTemplateValue(value, replace) },
    });
    const { result } = harness;
    selectAsset(result, '$asset');
    expect(harness.currentQuery).toEqual(expect.objectContaining({ assetRid: '$asset' }));

    harness.rerenderCurrent({
      assetRidResolution: resolveTemplateValue(harness.currentQuery.assetRid, replace),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('saved template RID fetches once', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replace = (v: string) => (v === '$asset' ? ASSET.rid : v);
    const query = makeQuery({ assetRid: '$asset' });
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

  it('saved RID under StrictMode restores', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid }),
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
      expected: { dataScopeName: 'default' },
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
      initialQuery: makeQuery({ assetRid: asset.rid, dataScopeName: savedScope }),
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

    const queryA = makeQuery({ assetRid: ASSET_A.rid, dataScopeName: 'scope-1' });
    const harness = renderAssetSelectionHarness({ initialQuery: queryA });
    const { result } = harness;

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });
    selectAsset(result, ASSET_B.rid);
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
      query: makeQuery({ assetRid: ASSET.rid }),
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
    selectAsset(result, '   ');

    expect(markInteracted).toHaveBeenCalledTimes(1);
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    expect(result.current.selectedAsset).toBeNull();
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: '' }));
  });

  it('re-picking the already selected asset does not refetch or hide the selection', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const harness = renderAssetSelectionHarness();
    const { result } = harness;
    selectAsset(result, ASSET.rid);
    harness.rerenderCurrent();
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    harness.onChange.mockClear();
    selectAsset(result, ASSET.rid);

    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    expect(harness.onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: ASSET.rid }));
  });

  it('re-picking a fallback asset after a failed fetch retries the lookup', async () => {
    mockFetchAssetByRid.mockRejectedValueOnce(new Error('transient')).mockResolvedValue(ASSET);
    const harness = renderAssetSelectionHarness();
    const { result } = harness;
    selectAsset(result, ASSET.rid);
    harness.rerenderCurrent();
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    // Failed fetch left a fallback asset without data scopes.
    expect(result.current.selectedAsset?.dataScopes).toEqual([]);
    selectAsset(result, ASSET.rid);

    // Same-RID retry fetches immediately (reconcile cannot serve an unchanged RID)
    // and must not be swallowed by the same-RID guard: it recovers the real asset.
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);
    await waitFor(() => {
      expect(result.current.selectedAsset?.dataScopes).toEqual(ASSET.dataScopes);
    });
    expect(publish).toHaveBeenCalledTimes(1);
  });
});
