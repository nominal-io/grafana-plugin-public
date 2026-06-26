import { StrictMode } from 'react';
// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, renderHook, waitFor } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { fetchAssetByRid, searchAssets, type Asset } from '../../utils/api';
import { buildAssetOptions, buildDataScopeOptions, getAssetPickerValue } from './queryBuilderOptions';
import { resolveTemplateValue, type TemplateValueResolution } from './templateResolution';
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

jest.mock('./queryBuilderOptions', () => {
  const actual = jest.requireActual('./queryBuilderOptions');
  return {
    ...actual,
    buildAssetOptions: jest.fn(actual.buildAssetOptions),
    buildDataScopeOptions: jest.fn(actual.buildDataScopeOptions),
    getAssetPickerValue: jest.fn(actual.getAssetPickerValue),
  };
});

const mockSearchAssets = searchAssets as jest.MockedFunction<typeof searchAssets>;
const mockFetchAssetByRid = fetchAssetByRid as jest.MockedFunction<typeof fetchAssetByRid>;
const mockBuildAssetOptions = buildAssetOptions as jest.MockedFunction<typeof buildAssetOptions>;
const mockBuildDataScopeOptions = buildDataScopeOptions as jest.MockedFunction<typeof buildDataScopeOptions>;
const mockGetAssetPickerValue = getAssetPickerValue as jest.MockedFunction<typeof getAssetPickerValue>;

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

const OTHER_ASSET: Asset = {
  ...ASSET,
  rid: 'ri.scout.main.asset.other',
  title: 'Other Asset',
};

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

function unresolvedResolution(raw: string, overrides: Partial<TemplateValueResolution> = {}): TemplateValueResolution {
  return {
    raw,
    resolved: '',
    hasTemplate: true,
    isResolved: false,
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

async function waitForAssetSearchToSettle(result: { current: ReturnType<typeof useAssetSelection> }) {
  await waitFor(() => {
    expect(mockSearchAssets).toHaveBeenCalled();
    expect(result.current.isLoadingAssets).toBe(false);
  });
}

async function renderMemoTestHook() {
  const query = makeQuery({ assetRid: '$asset', dataScopeName: '$scope' });
  const hookArgs = args({
    query,
    assetRidResolution: unresolvedResolution('$asset'),
    dataScopeResolution: unresolvedResolution('$scope'),
  });
  const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
    initialProps: hookArgs,
  });
  await waitForAssetSearchToSettle(result);

  mockBuildAssetOptions.mockClear();
  mockBuildDataScopeOptions.mockClear();
  mockGetAssetPickerValue.mockClear();

  return { hookArgs, rerender };
}

describe('useAssetSelection', () => {
  let consoleErrorSpy: jest.SpyInstance;

  beforeEach(() => {
    publish.mockReset();
    mockSearchAssets.mockReset().mockResolvedValue([]);
    mockFetchAssetByRid.mockReset().mockResolvedValue(null);
    mockBuildAssetOptions.mockClear();
    mockBuildDataScopeOptions.mockClear();
    mockGetAssetPickerValue.mockClear();
    consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    const consoleErrorCalls = consoleErrorSpy.mock.calls;
    jest.useRealTimers();
    consoleErrorSpy.mockRestore();
    expect(consoleErrorCalls).toEqual([]);
  });

  it('loads assets immediately on search-mode mount', async () => {
    mockSearchAssets.mockResolvedValue([ASSET]);
    const hookArgs = args({ query: makeQuery({ assetInputMethod: 'search' }) });
    jest.useFakeTimers();

    const { result } = renderHook(() => useAssetSelection(hookArgs));
    expect(mockSearchAssets).toHaveBeenCalledWith('/api/x', '');

    await waitFor(() => {
      expect(result.current.assetSearchResultCount).toBe(1);
    });
    await waitForAssetSearchToSettle(result);
  });

  it('skips asset search on direct-mode mount without a RID', async () => {
    const hookArgs = args({ query: makeQuery({ assetInputMethod: 'direct' }) });
    jest.useFakeTimers();

    renderHook(() => useAssetSelection(hookArgs));

    // Let mount effects run. Direct mode should not call the search-list API.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await Promise.resolve();
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(301);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(mockSearchAssets).not.toHaveBeenCalled();
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
  });

  it('debounces asset searches while typing in search mode', async () => {
    const hookArgs = args({ query: makeQuery({ assetInputMethod: 'search' }) });
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitForAssetSearchToSettle(result);

    mockSearchAssets.mockClear();
    jest.useFakeTimers();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetSearchQuery('a');
      await Promise.resolve();
      await Promise.resolve();
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetSearchQuery('ab');
      await Promise.resolve();
      await Promise.resolve();
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetSearchQuery('abc');
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(mockSearchAssets).not.toHaveBeenCalled();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(299);
    });
    expect(mockSearchAssets).not.toHaveBeenCalled();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(1);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(mockSearchAssets).toHaveBeenCalledTimes(1);
    expect(mockSearchAssets).toHaveBeenCalledWith('/api/x', 'abc');
  });

  it('runs a pending debounced asset search immediately when requested', async () => {
    const hookArgs = args({ query: makeQuery({ assetInputMethod: 'search' }) });
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitForAssetSearchToSettle(result);

    mockSearchAssets.mockClear();
    jest.useFakeTimers();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetSearchQuery('alpha');
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(mockSearchAssets).not.toHaveBeenCalled();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.runAssetSearch();
      await Promise.resolve();
    });

    expect(mockSearchAssets).toHaveBeenCalledTimes(1);
    expect(mockSearchAssets).toHaveBeenCalledWith('/api/x', 'alpha');

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(mockSearchAssets).toHaveBeenCalledTimes(1);
  });

  it('drops an in-flight asset search when the search query changes before the next debounce fires', async () => {
    const hookArgs = args({ query: makeQuery({ assetInputMethod: 'search' }) });
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitForAssetSearchToSettle(result);

    const firstSearch = deferred<Asset[]>();
    const secondSearch = deferred<Asset[]>();
    mockSearchAssets.mockReset();
    mockSearchAssets.mockImplementation((_datasourceUrl, searchText) => {
      if (searchText === 'alpha') {
        return firstSearch.promise;
      }
      if (searchText === 'beta') {
        return secondSearch.promise;
      }
      return Promise.resolve([]);
    });
    jest.useFakeTimers();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetSearchQuery('alpha');
      await Promise.resolve();
      await Promise.resolve();
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(mockSearchAssets).toHaveBeenCalledTimes(1);
    expect(mockSearchAssets).toHaveBeenCalledWith('/api/x', 'alpha');

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetSearchQuery('beta');
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(mockSearchAssets).toHaveBeenCalledTimes(1);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      firstSearch.resolve([ASSET]);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.assetSearchResultCount).toBe(0);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(mockSearchAssets).toHaveBeenCalledTimes(2);
    expect(mockSearchAssets).toHaveBeenNthCalledWith(2, '/api/x', 'beta');

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      secondSearch.resolve([OTHER_ASSET]);
      await Promise.resolve();
    });
    await waitFor(() => {
      expect(result.current.assetOptions[0]?.value).toBe(OTHER_ASSET.rid);
    });
  });

  it('keeps newer forced asset search results when an older same-query search resolves late', async () => {
    const hookArgs = args({ query: makeQuery({ assetInputMethod: 'search' }) });
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitForAssetSearchToSettle(result);

    const firstSearch = deferred<Asset[]>();
    const secondSearch = deferred<Asset[]>();
    mockSearchAssets.mockReset();
    mockSearchAssets.mockImplementationOnce(() => firstSearch.promise).mockImplementationOnce(() => secondSearch.promise);
    jest.useFakeTimers();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetSearchQuery('alpha');
      await Promise.resolve();
      await Promise.resolve();
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
      await Promise.resolve();
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.runAssetSearch();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(mockSearchAssets).toHaveBeenNthCalledWith(1, '/api/x', 'alpha');
    expect(mockSearchAssets).toHaveBeenNthCalledWith(2, '/api/x', 'alpha');

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      secondSearch.resolve([OTHER_ASSET]);
      await Promise.resolve();
    });
    await waitFor(() => {
      expect(result.current.assetOptions[0]?.value).toBe(OTHER_ASSET.rid);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      firstSearch.resolve([ASSET]);
      await Promise.resolve();
    });

    expect(result.current.assetOptions[0]?.value).toBe(OTHER_ASSET.rid);
  });

  it('cancels a pending debounced asset search synchronously when switching to direct mode', async () => {
    const onChange = jest.fn();
    const hookArgs = args({ query: makeQuery({ assetInputMethod: 'search' }), onChange });
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitForAssetSearchToSettle(result);

    mockSearchAssets.mockClear();
    jest.useFakeTimers();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetSearchQuery('alpha');
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(mockSearchAssets).not.toHaveBeenCalled();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      result.current.changeAssetInputMethod('direct');
      jest.advanceTimersByTime(300);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(mockSearchAssets).not.toHaveBeenCalled();
    expect(result.current.assetInputMethod).toBe('direct');
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetInputMethod: 'direct' }));
  });

  it('selectAsset selects a known asset and marks interaction', async () => {
    mockSearchAssets.mockResolvedValue([ASSET]);
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitFor(() => expect(result.current.assetSearchResultCount).toBe(1));
    await waitForAssetSearchToSettle(result);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });

    expect(markInteracted).toHaveBeenCalled();
    expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: ASSET.rid, assetInputMethod: 'search' }));
  });

  it('restores a saved direct-mode RID by fetching the asset', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'direct' }),
    });
    jest.useFakeTimers();

    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitFor(() => {
      expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET.rid);
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(301);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(result.current.directRID).toBe(ASSET.rid);
    expect(mockSearchAssets).not.toHaveBeenCalled();
  });

  it('restores a saved search-mode RID from search results without a by-RID fetch', async () => {
    mockSearchAssets.mockResolvedValue([ASSET]);
    mockFetchAssetByRid.mockResolvedValue(OTHER_ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'search' }),
    });

    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    await waitForAssetSearchToSettle(result);

    expect(result.current.assetInputMethod).toBe('search');
    expect(result.current.selectedAsset?.title).toBe('Asset A');
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();
  });

  it('infers direct mode for a saved RID without an explicit input method when search results do not contain it', async () => {
    mockSearchAssets.mockResolvedValue([OTHER_ASSET]);
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid }),
    });

    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    await waitForAssetSearchToSettle(result);

    expect(result.current.assetInputMethod).toBe('direct');
    expect(result.current.directRID).toBe(ASSET.rid);
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET.rid);
  });

  it('changeAssetInputMethod clears the selection and updates the query', async () => {
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitForAssetSearchToSettle(result);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeAssetInputMethod('direct');
    });

    expect(markInteracted).toHaveBeenCalled();
    expect(result.current.assetInputMethod).toBe('direct');
    expect(result.current.selectedAsset).toBeNull();
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetInputMethod: 'direct' }));
  });

  it('leaves a user-entered concrete direct RID fetch to the debounced direct handler', async () => {
    jest.useFakeTimers();
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    mockFetchAssetByRid.mockResolvedValue(ASSET_B);

    let currentQuery = makeQuery();
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const initialArgs = args({ query: currentQuery, onChange, markInteracted });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: initialArgs,
    });
    await waitForAssetSearchToSettle(result);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });

    expect(markInteracted).toHaveBeenCalled();
    expect(currentQuery).toEqual(expect.objectContaining({ assetRid: ASSET_B.rid, assetInputMethod: 'direct' }));

    rerender(args({ query: currentQuery, onChange, markInteracted, hasUserInteracted: true }));

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

  it('leaves a custom concrete search RID fetch to the select handler', async () => {
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    const pendingFetch = deferred<Asset | null>();
    mockSearchAssets.mockResolvedValue([]);
    mockFetchAssetByRid.mockReturnValue(pendingFetch.promise);

    let currentQuery = makeQuery();
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const initialArgs = args({ query: currentQuery, onChange, markInteracted });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: initialArgs,
    });
    await waitForAssetSearchToSettle(result);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_B.rid);
    });

    expect(markInteracted).toHaveBeenCalled();
    expect(currentQuery).toEqual(expect.objectContaining({ assetRid: ASSET_B.rid, assetInputMethod: 'search' }));
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET_B.rid);

    rerender(args({ query: currentQuery, onChange, markInteracted, hasUserInteracted: true }));

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

  it('reconciles a user-entered direct template RID through the query-driven path', async () => {
    jest.useFakeTimers();
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replace = (value: string) => (value === '$asset' ? ASSET.rid : value);

    let currentQuery = makeQuery();
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const initialArgs = args({ query: currentQuery, onChange, markInteracted });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: initialArgs,
    });
    await waitForAssetSearchToSettle(result);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID('$asset');
    });

    expect(currentQuery).toEqual(expect.objectContaining({ assetRid: '$asset', assetInputMethod: 'direct' }));

    rerender(
      args({
        query: currentQuery,
        onChange,
        markInteracted,
        hasUserInteracted: true,
        assetRidResolution: resolveTemplateValue(currentQuery.assetRid, replace),
        resolveTemplateText: (value: string) => resolveTemplateValue(value, replace),
      })
    );

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });

    // The template-backed path is not owned by the direct RID debounce.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET.rid);
  });

  // Caveat: rapid reselection aborts the prior fetch. A stale response that arrives after a
  // newer selection has already resolved must not overwrite the newer asset.
  it('drops a stale asset fetch when a newer selection resolves first', async () => {
    const ASSET_A: Asset = { ...ASSET, rid: 'ri.scout.main.asset.aaa', title: 'Asset AAA' };
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    // RIDs absent from the (empty) search results force the direct fetch-by-RID path.
    const deferredByRid = new Map<string, ReturnType<typeof deferred<Asset | null>>>();
    mockFetchAssetByRid.mockImplementation((_url: string, rid: string) => {
      const d = deferred<Asset | null>();
      deferredByRid.set(rid, d);
      return d.promise;
    });
    // Build args once: a stable onChange/resolution identity is required, otherwise the
    // dependent-fields effect (which depends on onChange) re-runs every render and loops.
    const hookArgs = args();
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitForAssetSearchToSettle(result);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_A.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_B.rid);
    });

    // Newer selection (B) resolves first and wins.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      deferredByRid.get(ASSET_B.rid)!.resolve(ASSET_B);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);

    // Stale selection (A) resolves afterwards; its controller was aborted by B, so it is ignored.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      deferredByRid.get(ASSET_A.rid)!.resolve(ASSET_A);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
  });

  // A saved direct-mode query whose RID is a template variable is restored by the
  // single query-driven reconcile effect, so it must issue only one by-RID fetch.
  it('fetches a saved direct-mode template RID only once', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replace = (v: string) => (v === '$asset' ? ASSET.rid : v);
    const query = makeQuery({ assetRid: '$asset', assetInputMethod: 'direct' });
    const hookArgs = args({
      query,
      assetRidResolution: resolveTemplateValue(query.assetRid, replace),
      resolveTemplateText: (value: string) => resolveTemplateValue(value, replace),
    });
    jest.useFakeTimers();

    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(301);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET.rid);
    expect(mockSearchAssets).not.toHaveBeenCalled();
  });

  it('does not rebuild options when resolution references change but primitive fields do not', async () => {
    const { hookArgs, rerender } = await renderMemoTestHook();
    const nextAssetRidResolution = unresolvedResolution('$asset');
    const nextDataScopeResolution = unresolvedResolution('$scope');
    expect(nextAssetRidResolution).not.toBe(hookArgs.assetRidResolution);
    expect(nextDataScopeResolution).not.toBe(hookArgs.dataScopeResolution);

    rerender({
      ...hookArgs,
      assetRidResolution: nextAssetRidResolution,
      dataScopeResolution: nextDataScopeResolution,
    });

    expect(mockBuildAssetOptions).not.toHaveBeenCalled();
    expect(mockGetAssetPickerValue).not.toHaveBeenCalled();
    expect(mockBuildDataScopeOptions).not.toHaveBeenCalled();
  });

  it.each([
    ['raw', { raw: '$asset2' }],
    ['resolved', { resolved: 'ri.scout.main.asset.a' }],
    ['hasTemplate', { hasTemplate: false }],
    ['isResolved', { isResolved: true }],
  ] satisfies Array<[string, Partial<TemplateValueResolution>]>)(
    'rebuilds asset options when asset %s changes',
    async (_field, assetChange) => {
      const { hookArgs, rerender } = await renderMemoTestHook();

      rerender({
        ...hookArgs,
        assetRidResolution: {
          ...hookArgs.assetRidResolution,
          ...assetChange,
        },
      });

      expect(mockBuildAssetOptions).toHaveBeenCalledTimes(1);
      expect(mockGetAssetPickerValue).toHaveBeenCalledTimes(1);
      expect(mockBuildDataScopeOptions).not.toHaveBeenCalled();
    }
  );

  it.each([
    ['raw', { raw: '$scope2' }],
    ['resolved', { resolved: 'primary' }],
    ['hasTemplate', { hasTemplate: false }],
    ['isResolved', { isResolved: true }],
  ] satisfies Array<[string, Partial<TemplateValueResolution>]>)(
    'rebuilds data scope options when data scope %s changes',
    async (_field, dataScopeChange) => {
      const { hookArgs, rerender } = await renderMemoTestHook();

      rerender({
        ...hookArgs,
        dataScopeResolution: {
          ...hookArgs.dataScopeResolution,
          ...dataScopeChange,
        },
      });

      expect(mockBuildAssetOptions).not.toHaveBeenCalled();
      expect(mockGetAssetPickerValue).not.toHaveBeenCalled();
      expect(mockBuildDataScopeOptions).toHaveBeenCalledTimes(1);
    }
  );

  // A saved SEARCH-mode RID absent from the search results can only be restored via the
  // query-driven reconcile effect's search fallback by-RID fetch. Under React 18 StrictMode
  // the mount runs setup -> cleanup -> setup; an aborted first setup must not permanently
  // suppress the re-run, or selectedAsset never gets set and the channel picker stays hidden.
  it('restores a saved search-mode RID absent from results under StrictMode', async () => {
    mockSearchAssets.mockResolvedValue([]); // saved RID is NOT in the search list
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'search' }),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs), { wrapper: StrictMode });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    await waitForAssetSearchToSettle(result);
  });
});
