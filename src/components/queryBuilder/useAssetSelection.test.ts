import { StrictMode } from 'react';
// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, renderHook, waitFor } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { fetchAssetByRid, searchAssets, type Asset } from '../../utils/api';
import { buildAssetOptions, buildDataScopeOptions, getAssetSelectValue } from './queryBuilderOptions';
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
    getAssetSelectValue: jest.fn(actual.getAssetSelectValue),
  };
});

const mockSearchAssets = searchAssets as jest.MockedFunction<typeof searchAssets>;
const mockFetchAssetByRid = fetchAssetByRid as jest.MockedFunction<typeof fetchAssetByRid>;
const mockBuildAssetOptions = buildAssetOptions as jest.MockedFunction<typeof buildAssetOptions>;
const mockBuildDataScopeOptions = buildDataScopeOptions as jest.MockedFunction<typeof buildDataScopeOptions>;
const mockGetAssetSelectValue = getAssetSelectValue as jest.MockedFunction<typeof getAssetSelectValue>;

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
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('useAssetSelection', () => {
  let consoleErrorSpy: jest.SpyInstance;

  beforeEach(() => {
    publish.mockReset();
    mockSearchAssets.mockReset().mockResolvedValue([]);
    mockFetchAssetByRid.mockReset().mockResolvedValue(null);
    mockBuildAssetOptions.mockClear();
    mockBuildDataScopeOptions.mockClear();
    mockGetAssetSelectValue.mockClear();
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
    expect(mockBuildAssetOptions).toHaveBeenCalledWith({
      assets: [ASSET],
      selectedAsset: null,
      assetRid: expect.objectContaining({ raw: ASSET.rid, resolved: ASSET.rid }),
    });
    expect(options).toEqual(expect.arrayContaining([expect.objectContaining({ value: ASSET.rid })]));
  });

  it('loader suppresses alert for superseded failed request', async () => {
    const first = deferred<Asset[]>();
    const second = deferred<Asset[]>();
    mockSearchAssets.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { result } = renderHook(() => useAssetSelection(args()));

    const firstOptions = result.current.assetOptions('first');
    const secondOptions = result.current.assetOptions('second');

    first.reject(new Error('first failed'));
    second.resolve([]);
    await expect(firstOptions).resolves.toEqual([]);
    await expect(secondOptions).resolves.toEqual([]);

    expect(publish).not.toHaveBeenCalled();
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

  it('loader suppresses alert when request context changes before an old failure', async () => {
    const pending = deferred<Asset[]>();
    mockSearchAssets.mockReturnValue(pending.promise);
    const query = makeQuery();
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query }),
    });

    const options = result.current.assetOptions('old');
    rerender(
      args({
        query,
        assetRidResolution: resolveTemplateValue('ri.scout.main.asset.bbb', (value) => value),
      })
    );

    pending.reject(new Error('old failure'));

    await expect(options).resolves.toEqual([]);
    expect(publish).not.toHaveBeenCalled();
  });

  it('selectAsset resolves a picked asset by RID once and marks interaction', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });

    expect(markInteracted).toHaveBeenCalledTimes(1);
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: ASSET.rid, assetInputMethod: 'search' }));
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET.rid);
  });

  it('restores saved direct-mode RID by fetching asset', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const hookArgs = args({
      query: makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'direct' }),
    });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    expect(result.current.directRID).toBe(ASSET.rid);
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('mirrors saved direct-mode RID changes into the direct input', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
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

  it('changeAssetInputMethod clears selection and updates query', async () => {
    const pendingFetch = deferred<Asset | null>();
    mockFetchAssetByRid.mockReturnValue(pendingFetch.promise);
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const hookArgs = args({ onChange, markInteracted });
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingFetch.resolve(ASSET);
      await pendingFetch.promise;
      await Promise.resolve();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
      expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await Promise.resolve();
    });
    onChange.mockClear();
    markInteracted.mockClear();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeAssetInputMethod('direct');
    });

    expect(markInteracted).toHaveBeenCalled();
    expect(result.current.assetInputMethod).toBe('direct');
    expect(result.current.selectedAsset).toBeNull();
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetInputMethod: 'direct' }));
  });

  it('user-entered concrete direct RID fetch stays with debounced direct handler, not query reconcile', async () => {
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

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });

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

  it('selecting concrete search RID fetches once and keeps it on resolution', async () => {
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    const pendingFetch = deferred<Asset | null>();
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

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_B.rid);
    });

    expect(currentQuery).toEqual(expect.objectContaining({ assetRid: ASSET_B.rid, assetInputMethod: 'search' }));
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);

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
    expect(result.current.selectedAsset).toBeNull();
    expect(result.current.selectedAssetSupportedScopeCount).toBe(0);
    expect(result.current.dataScopeOptions).toEqual([]);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingFetch.resolve(ASSET_B);
      await pendingFetch.promise;
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'new-scope' })]);
  });

  it('hides stale asset controls while a query-driven RID restore is loading', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = {
      ...ASSET,
      rid: 'ri.scout.main.asset.bbb',
      title: 'Asset BBB',
      dataScopes: [{ ...ASSET.dataScopes[0], dataScopeName: 'new-scope' }],
    };
    const pendingFetch = deferred<Asset | null>();
    mockFetchAssetByRid.mockResolvedValueOnce(ASSET_A).mockReturnValueOnce(pendingFetch.promise);

    const queryA = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'search' });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryA }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
      expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    });

    const queryB = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'search' });
    rerender(args({ query: queryB }));

    await waitFor(() => {
      expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);
      expect(result.current.selectedAsset).toBeNull();
    });
    expect(result.current.selectedAssetSupportedScopeCount).toBe(0);
    expect(result.current.dataScopeOptions).toEqual([]);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingFetch.resolve(ASSET_B);
      await pendingFetch.promise;
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'new-scope' })]);
  });

  it('hides stale asset controls as soon as a direct RID changes', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = {
      ...ASSET,
      rid: 'ri.scout.main.asset.bbb',
      title: 'Asset BBB',
      dataScopes: [{ ...ASSET.dataScopes[0], dataScopeName: 'new-scope' }],
    };
    const pendingFetch = deferred<Asset | null>();
    mockFetchAssetByRid.mockResolvedValueOnce(ASSET_A).mockReturnValueOnce(pendingFetch.promise);

    let currentQuery = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'direct' });
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: currentQuery, onChange }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
      expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    });

    jest.useFakeTimers();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });

    expect(currentQuery).toEqual(expect.objectContaining({ assetRid: ASSET_B.rid, assetInputMethod: 'direct' }));
    expect(result.current.selectedAsset).toBeNull();
    expect(result.current.selectedAssetSupportedScopeCount).toBe(0);
    expect(result.current.dataScopeOptions).toEqual([]);

    rerender(args({ query: currentQuery, onChange, hasUserInteracted: true }));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });
    await waitFor(() => {
      expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingFetch.resolve(ASSET_B);
      await pendingFetch.promise;
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'new-scope' })]);
  });

  it('does not let an old event-owned RID suppress a later query-driven restore', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    mockFetchAssetByRid.mockImplementation(async (_url: string, rid: string) => {
      return rid === ASSET_B.rid ? ASSET_B : ASSET_A;
    });

    let currentQuery = makeQuery();
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const initialArgs = args({ query: currentQuery, onChange, markInteracted });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: initialArgs,
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_A.rid);
    });

    rerender(args({ query: currentQuery, onChange, markInteracted, hasUserInteracted: true }));
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);

    const queryB = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'search' });
    rerender(args({ query: queryB, onChange, markInteracted, hasUserInteracted: true }));
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);

    const queryA = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'search' });
    rerender(args({ query: queryA, onChange, markInteracted, hasUserInteracted: true }));
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(3);
  });

  it('does not let a stale event-owned fetch overwrite a newer query-driven restore', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    const deferredByRid = new Map<string, ReturnType<typeof deferred<Asset | null>>>();
    mockFetchAssetByRid.mockImplementation((_url: string, rid: string) => {
      const d = deferred<Asset | null>();
      deferredByRid.set(rid, d);
      return d.promise;
    });

    let currentQuery = makeQuery();
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const initialArgs = args({ query: currentQuery, onChange, markInteracted });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: initialArgs,
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_A.rid);
    });

    const queryB = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'search' });
    rerender(args({ query: queryB, onChange, markInteracted, hasUserInteracted: true }));
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

    let currentQuery = makeQuery();
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const initialArgs = args({ query: currentQuery, onChange, markInteracted });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: initialArgs,
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID('$asset');
    });

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
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('stale asset fetch is dropped when newer selection resolves first', async () => {
    const ASSET_A: Asset = { ...ASSET, rid: 'ri.scout.main.asset.aaa', title: 'Asset AAA' };
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    const deferredByRid = new Map<string, ReturnType<typeof deferred<Asset | null>>>();
    mockFetchAssetByRid.mockImplementation((_url: string, rid: string) => {
      const d = deferred<Asset | null>();
      deferredByRid.set(rid, d);
      return d.promise;
    });
    const hookArgs = args();
    const { result } = renderHook(() => useAssetSelection(hookArgs));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_A.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_B.rid);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      deferredByRid.get(ASSET_B.rid)!.resolve(ASSET_B);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      deferredByRid.get(ASSET_A.rid)!.resolve(ASSET_A);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
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

  it('assetSelectValue rebuilds only when an asset resolution primitive changes; data scope options do not rebuild for identical data-scope primitives', () => {
    const query = makeQuery({ assetRid: '$asset', dataScopeName: '$scope' });
    const hookArgs = args({
      query,
      assetRidResolution: unresolvedResolution('$asset'),
      dataScopeResolution: unresolvedResolution('$scope'),
    });
    const { rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: hookArgs,
    });

    mockBuildDataScopeOptions.mockClear();
    mockGetAssetSelectValue.mockClear();

    rerender({
      ...hookArgs,
      assetRidResolution: unresolvedResolution('$asset'),
      dataScopeResolution: unresolvedResolution('$scope'),
    });

    expect(mockGetAssetSelectValue).not.toHaveBeenCalled();
    expect(mockBuildDataScopeOptions).not.toHaveBeenCalled();

    rerender({
      ...hookArgs,
      assetRidResolution: unresolvedResolution('$asset2'),
      dataScopeResolution: unresolvedResolution('$scope'),
    });

    expect(mockGetAssetSelectValue).toHaveBeenCalledTimes(1);
    expect(mockBuildDataScopeOptions).not.toHaveBeenCalled();
  });
});
