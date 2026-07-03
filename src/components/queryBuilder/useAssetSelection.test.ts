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

  it('loader alerts for a latest-request failure even after the request context changes', async () => {
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

    pending.reject(new Error('latest failure'));

    await expect(options).resolves.toEqual([]);
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: ['Unable to load Nominal assets', 'Check the data source configuration and try again.'],
      })
    );
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

  it('clears an empty-resolved search template so a later matching RID can show the selected asset', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replaceEmpty = (value: string) => (value === '$asset' ? '' : value);
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const queryA = makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'search' });
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

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset('$asset');
    });

    expect(result.current.selectedAsset).toBeNull();

    // The parent echoes the committed '$asset' (which resolves empty)...
    rerender(
      args({
        query: makeQuery({ assetRid: '$asset', assetInputMethod: 'search' }),
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

  it('clears an empty-resolved direct template so a later matching RID can show the selected asset', async () => {
    mockFetchAssetByRid.mockResolvedValue(ASSET);
    const replaceEmpty = (value: string) => (value === '$asset' ? '' : value);
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const queryA = makeQuery({ assetRid: ASSET.rid, assetInputMethod: 'direct' });
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

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID('$asset');
    });

    rerender(
      args({
        query: makeQuery({ assetRid: '$asset', assetInputMethod: 'direct' }),
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
  });

  it('shows the current selected asset when a superseded query-driven fetch is aborted', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = {
      ...ASSET,
      rid: 'ri.scout.main.asset.bbb',
      title: 'Asset BBB',
      dataScopes: [{ ...ASSET.dataScopes[0], dataScopeName: 'new-scope' }],
    };
    const pendingB = deferred<Asset | null>();
    mockFetchAssetByRid.mockImplementation((_url: string, rid: string) => {
      return rid === ASSET_B.rid ? pendingB.promise : Promise.resolve(ASSET_A);
    });

    const onChange = jest.fn();
    const queryA = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'search' });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryA, onChange, hasUserInteracted: false }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });

    const queryB = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'search' });
    rerender(args({ query: queryB, onChange, hasUserInteracted: true }));

    await waitFor(() => {
      expect(result.current.selectedAsset).toBeNull();
      expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET_B.rid);
    });

    rerender(args({ query: queryA, onChange, hasUserInteracted: true }));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
      expect(result.current.dataScopeOptions).toEqual([expect.objectContaining({ value: 'default' })]);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingB.resolve(ASSET_B);
      await pendingB.promise;
      await Promise.resolve();
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
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
    // Hidden-state shape is covered by the reducer unit tests; assert only that a direct-RID
    // change enters that state synchronously.
    expect(result.current.selectedAsset).toBeNull();

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
  });

  it('keeps a typed RID when the query prop still holds the pre-edit value as the fetch resolves', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    mockFetchAssetByRid.mockImplementation(async (_url: string, rid: string) =>
      rid === ASSET_B.rid ? ASSET_B : ASSET_A
    );

    let currentQuery = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'direct' });
    const queryA = currentQuery;
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryA, onChange, markInteracted }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });

    jest.useFakeTimers();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });

    // B resolves while the prop still holds the pre-edit A. That value is parent
    // lag, not an external change, so the typed B must survive.
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    });
    expect(result.current.directRID).toBe(ASSET_B.rid);

    // Re-delivering the pre-edit value is still lag.
    rerender(args({ query: queryA, onChange, markInteracted, hasUserInteracted: true }));
    expect(result.current.directRID).toBe(ASSET_B.rid);
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);

    // The real echo then lands and confirms the typed value.
    rerender(args({ query: currentQuery, onChange, markInteracted, hasUserInteracted: true }));
    expect(result.current.directRID).toBe(ASSET_B.rid);
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);
  });

  it('cancels a pending direct RID when the query switches to an unresolved search template before the fetch starts', async () => {
    jest.useFakeTimers();
    const ASSET_A = ASSET;
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    mockFetchAssetByRid.mockResolvedValue(ASSET_A);

    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const queryA = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'direct' });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryA, onChange, markInteracted }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });
    expect(result.current.selectedAsset).toBeNull();

    // External switch to search mode with an unresolved template while the
    // debounced fetch for the typed RID has not started yet: the pending RID
    // must be cancelled (no abort listener exists to do it), restoring the
    // resolved selection instead of hiding it behind a fetch that never runs.
    const templateQuery = makeQuery({ assetRid: '$asset', assetInputMethod: 'search' });
    rerender(
      args({
        query: templateQuery,
        onChange,
        markInteracted,
        hasUserInteracted: true,
        assetRidResolution: unresolvedResolution('$asset'),
      })
    );

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });
    expect(mockFetchAssetByRid).not.toHaveBeenCalledWith('/api/x', ASSET_B.rid);
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

    let currentQuery = makeQuery({ assetRid: asset.rid, assetInputMethod: 'search', dataScopeName: savedScope });
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: currentQuery, onChange, hasUserInteracted: false }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(asset.rid);
    });
    onChange.mockClear();

    rerender(args({ query: currentQuery, onChange, hasUserInteracted: true }));

    await waitFor(() => {
      expect(onChange).toHaveBeenCalledWith(expect.objectContaining(expected));
    });
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
    expect(result.current.selectedAssetSupportedScopeCount).toBe(0);
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

  it('applies an external direct-mode query change that arrives while a re-typed RID fetch is in flight', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    const pendingB = deferred<Asset | null>();
    // First B fetch (saved-query restore) resolves; the re-typed B fetch stays pending.
    mockFetchAssetByRid
      .mockResolvedValueOnce(ASSET_B)
      .mockImplementation((_url: string, rid: string) =>
        rid === ASSET_B.rid ? pendingB.promise : Promise.resolve(ASSET_A)
      );

    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const queryB = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'direct' });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryB, onChange, markInteracted }),
    });

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    });

    jest.useFakeTimers();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(2);

    // A value this editor never committed is a genuine external change, not parent
    // lag: it must win immediately, even though the re-typed RID's fetch is pending
    // and its settlement would not change selectedAsset.rid.
    const externalQuery = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'direct' });
    rerender(args({ query: externalQuery, onChange, markInteracted, hasUserInteracted: true }));

    await waitFor(() => {
      expect(result.current.directRID).toBe(ASSET_A.rid);
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });

    // The superseded re-typed fetch must not resurrect B.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      pendingB.resolve(ASSET_B);
      await pendingB.promise;
    });
    expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
  });

  it('ignores a lagging echo of an older committed RID after the newer fetch settles', async () => {
    jest.useFakeTimers();
    const ASSET_A = ASSET;
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    mockFetchAssetByRid.mockImplementation(async (_url: string, rid: string) =>
      rid === ASSET_B.rid ? ASSET_B : ASSET_A
    );

    let currentQuery = makeQuery({ assetInputMethod: 'direct' });
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: currentQuery, onChange, markInteracted }),
    });

    // Two quick edits: A's debounce is superseded by B before it fires.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_A.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    });

    // The parent now echoes the older committed A. That is lag, not an external
    // change: the typed B and its resolved asset must survive.
    const staleEcho = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'direct' });
    rerender(args({ query: staleEcho, onChange, markInteracted, hasUserInteracted: true }));

    expect(result.current.directRID).toBe(ASSET_B.rid);
    expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    expect(mockFetchAssetByRid).not.toHaveBeenCalledWith('/api/x', ASSET_A.rid);

    // The newest echo then lands and everything stays consistent.
    rerender(args({ query: currentQuery, onChange, markInteracted, hasUserInteracted: true }));
    expect(result.current.directRID).toBe(ASSET_B.rid);
    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
  });

  it('does not revert in-progress template typing when an older committed RID echoes back', async () => {
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    mockFetchAssetByRid.mockResolvedValue(ASSET_B);

    let currentQuery = makeQuery({ assetInputMethod: 'direct' });
    const onChange = jest.fn((nextQuery: NominalQuery) => {
      currentQuery = nextQuery;
    });
    const markInteracted = jest.fn();
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: currentQuery, onChange, markInteracted }),
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID(ASSET_B.rid);
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeDirectRID('$asset');
    });
    expect(result.current.directRID).toBe('$asset');

    // The parent echoes the older committed concrete RID while the user's newer
    // template text is still in flight — the mirror must not clobber it.
    const staleEcho = makeQuery({ assetRid: ASSET_B.rid, assetInputMethod: 'direct' });
    rerender(args({ query: staleEcho, onChange, markInteracted, hasUserInteracted: true }));

    expect(result.current.directRID).toBe('$asset');
    expect(mockFetchAssetByRid).not.toHaveBeenCalled();

    // The newest echo then lands and the local text is confirmed.
    rerender(args({ query: currentQuery, onChange, markInteracted, hasUserInteracted: true }));
    expect(result.current.directRID).toBe('$asset');
  });

  it('reconciles the mode-switch echo while an earlier selection is still un-echoed', async () => {
    const ASSET_A = ASSET;
    const ASSET_B: Asset = { ...ASSET, rid: 'ri.scout.main.asset.bbb', title: 'Asset BBB' };
    mockFetchAssetByRid.mockImplementation(async (_url: string, rid: string) =>
      rid === ASSET_B.rid ? ASSET_B : ASSET_A
    );

    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const queryA = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'search' });
    const { result, rerender } = renderHook((nextArgs: ReturnType<typeof args>) => useAssetSelection(nextArgs), {
      initialProps: args({ query: queryA, onChange, markInteracted }),
    });
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });

    // The user picks B, but the parent lags: B's commit never echoes back.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset(ASSET_B.rid);
    });
    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_B.rid);
    });

    // Switching to direct mode commits the stale prop's assetRid (A) with the new
    // method. That commit overwrites the un-echoed B, so B's echo never arrives.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.changeAssetInputMethod('direct');
    });
    expect(result.current.directRID).toBe(ASSET_A.rid);

    // When the mode-switch echo lands, it must reconcile (mirror + fetch A) rather
    // than be misclassified as a lagging echo of the abandoned B commit.
    const modeSwitchEcho = makeQuery({ assetRid: ASSET_A.rid, assetInputMethod: 'direct' });
    rerender(args({ query: modeSwitchEcho, onChange, markInteracted, hasUserInteracted: true }));

    await waitFor(() => {
      expect(result.current.selectedAsset?.rid).toBe(ASSET_A.rid);
    });
    expect(result.current.directRID).toBe(ASSET_A.rid);
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
