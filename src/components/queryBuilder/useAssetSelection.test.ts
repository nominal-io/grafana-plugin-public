import { act, renderHook, waitFor } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { fetchAssetByRid, searchAssets, type Asset } from '../../utils/api';
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

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return { refId: 'A', ...overrides } as NominalQuery;
}

function args(overrides: Record<string, unknown> = {}) {
  return {
    query: makeQuery(),
    onChange: jest.fn(),
    datasourceUrl: '/api/x',
    resolvedAssetRid: '',
    resolvedDataScopeName: '',
    hasUserInteracted: false,
    markInteracted: jest.fn(),
    ...overrides,
  };
}

async function waitForAssetSearchToSettle(result: { current: ReturnType<typeof useAssetSelection> }) {
  await waitFor(() => {
    expect(mockSearchAssets).toHaveBeenCalled();
    expect(result.current.isLoadingAssets).toBe(false);
  });
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
      resolvedAssetRid: ASSET.rid,
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

    act(() => {
      result.current.changeAssetInputMethod('direct');
    });

    expect(markInteracted).toHaveBeenCalled();
    expect(result.current.assetInputMethod).toBe('direct');
    expect(result.current.selectedAsset).toBeNull();
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetInputMethod: 'direct' }));
  });
});
