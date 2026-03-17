import { getBackendSrv } from '@grafana/runtime';
import { fetchAssetByRid, clearAssetCache } from './QueryEditor';

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getBackendSrv: jest.fn(),
  getTemplateSrv: jest.fn(() => ({ replace: (v: string) => v })),
}));

const mockBackendSrv = { post: jest.fn() };

beforeEach(() => {
  jest.clearAllMocks();
  clearAssetCache();
  (getBackendSrv as jest.Mock).mockReturnValue(mockBackendSrv);
});

const DATASOURCE_URL = '/api/datasources/uid/test-uid/resources';
const VALID_RID = 'ri.scout.main.asset.abc-123';

describe('fetchAssetByRid', () => {
  it('returns null for empty RID', async () => {
    const result = await fetchAssetByRid(DATASOURCE_URL, '');
    expect(result).toBeNull();
    expect(mockBackendSrv.post).not.toHaveBeenCalled();
  });

  it('returns null for RID not starting with "ri."', async () => {
    const result = await fetchAssetByRid(DATASOURCE_URL, 'not-a-valid-rid');
    expect(result).toBeNull();
    expect(mockBackendSrv.post).not.toHaveBeenCalled();
  });

  it('calls batch lookup endpoint with correct URL and payload', async () => {
    mockBackendSrv.post.mockResolvedValue({});
    await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      `${DATASOURCE_URL}/scout/v1/asset/multiple`,
      [VALID_RID]
    );
  });

  it('returns the asset when found with dataScopes', async () => {
    const asset = {
      rid: VALID_RID,
      title: 'Test Asset',
      labels: [],
      dataScopes: [{ dataScopeName: 'ds1', dataSource: { type: 'dataset' } }],
    };
    mockBackendSrv.post.mockResolvedValue({ [VALID_RID]: asset });

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toEqual(asset);
  });

  it('returns null when asset has empty dataScopes', async () => {
    mockBackendSrv.post.mockResolvedValue({
      [VALID_RID]: { rid: VALID_RID, title: 'Empty', dataScopes: [] },
    });

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toBeNull();
  });

  it('returns null when RID is not in response map', async () => {
    mockBackendSrv.post.mockResolvedValue({});

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toBeNull();
  });

  it('propagates API errors to caller', async () => {
    mockBackendSrv.post.mockRejectedValue(new Error('Network error'));

    await expect(fetchAssetByRid(DATASOURCE_URL, VALID_RID)).rejects.toThrow('Network error');
  });

  it('deduplicates concurrent requests for the same RID', async () => {
    const asset = {
      rid: VALID_RID,
      title: 'Test Asset',
      labels: [],
      dataScopes: [{ dataScopeName: 'ds1', dataSource: { type: 'dataset' } }],
    };
    mockBackendSrv.post.mockResolvedValue({ [VALID_RID]: asset });

    // Simulate 70 panels all requesting the same asset concurrently
    const results = await Promise.all(
      Array.from({ length: 70 }, () => fetchAssetByRid(DATASOURCE_URL, VALID_RID))
    );

    // All 70 should get the same result
    for (const result of results) {
      expect(result).toEqual(asset);
    }
    // But only 1 HTTP call should have been made
    expect(mockBackendSrv.post).toHaveBeenCalledTimes(1);
  });

  it('retries after a failed request', async () => {
    mockBackendSrv.post.mockRejectedValueOnce(new Error('Network error'));

    await expect(fetchAssetByRid(DATASOURCE_URL, VALID_RID)).rejects.toThrow('Network error');

    // Cache entry should be evicted on failure — next call should retry
    const asset = {
      rid: VALID_RID,
      title: 'Test Asset',
      labels: [],
      dataScopes: [{ dataScopeName: 'ds1', dataSource: { type: 'dataset' } }],
    };
    mockBackendSrv.post.mockResolvedValue({ [VALID_RID]: asset });

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toEqual(asset);
    expect(mockBackendSrv.post).toHaveBeenCalledTimes(2);
  });
});
