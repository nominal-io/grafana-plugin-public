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
  mockGetAssetSelectValue.mockClear();

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
    mockGetAssetSelectValue.mockClear();
    consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    const consoleErrorCalls = consoleErrorSpy.mock.calls;
    consoleErrorSpy.mockRestore();
    expect(consoleErrorCalls).toEqual([]);
  });

  it('loads assets on mount', async () => {
    mockSearchAssets.mockResolvedValue([ASSET]);
    const hookArgs = args();
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitFor(() => {
      expect(result.current.assetSearchResultCount).toBe(1);
    });
    await waitForAssetSearchToSettle(result);
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
      result.current.selectAsset({ value: ASSET.rid });
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
    const { result } = renderHook(() => useAssetSelection(hookArgs));
    await waitFor(() => {
      expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET.rid);
      expect(result.current.selectedAsset?.rid).toBe(ASSET.rid);
    });
    await waitForAssetSearchToSettle(result);
    expect(result.current.directRID).toBe(ASSET.rid);
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
      result.current.selectAsset({ value: ASSET_A.rid });
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectAsset({ value: ASSET_B.rid });
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

  // A saved direct-mode query whose RID is a template variable is eligible for both
  // the mount-restore effect and the resolved-asset effect. The pendingAssetFetchRef guard must
  // ensure only ONE fetch is issued for that RID.
  it('fetches a saved direct-mode template RID only once', async () => {
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
    await waitForAssetSearchToSettle(result);

    expect(mockFetchAssetByRid).toHaveBeenCalledTimes(1);
    expect(mockFetchAssetByRid).toHaveBeenCalledWith('/api/x', ASSET.rid);
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
    expect(mockGetAssetSelectValue).not.toHaveBeenCalled();
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
      expect(mockGetAssetSelectValue).toHaveBeenCalledTimes(1);
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
      expect(mockGetAssetSelectValue).not.toHaveBeenCalled();
      expect(mockBuildDataScopeOptions).toHaveBeenCalledTimes(1);
    }
  );

  // A saved SEARCH-mode RID absent from the search results can only be restored via the
  // resolved-asset by-RID fetch. Under React 18 StrictMode the mount runs setup -> cleanup
  // -> setup; the in-flight guard must not permanently suppress the re-run, or selectedAsset
  // never gets set and the channel picker (gated on selectedAsset !== null) stays hidden.
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
