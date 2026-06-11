import {
  Asset,
  DataScope,
  createBasicAsset,
  fetchAssetByRid,
  getDataSourceRid,
  searchChannels,
  getSupportedScopeNames,
  getSupportedScopes,
  resolveDataSourceRids,
} from './api';

const DATASOURCE_URL = '/api/datasources/uid/test/resources';
const VALID_RID = 'ri.scout.main.asset.abc-123';

const post = jest.fn();

jest.mock('@grafana/runtime', () => ({
  getBackendSrv: jest.fn(() => ({ post })),
}));

beforeEach(() => {
  post.mockReset();
});

const scope = (name: string, dataSource: DataScope['dataSource']): DataScope => ({
  dataScopeName: name,
  dataSource,
  timestampType: 'ABSOLUTE',
  seriesTags: {},
});

const buildAsset = (overrides: Partial<Asset> = {}): Asset => ({
  ...createBasicAsset('ri.scout.main.asset.abc', 'Test Asset'),
  ...overrides,
});

describe('getDataSourceRid', () => {
  it('returns the dataset rid for type=dataset', () => {
    expect(getDataSourceRid({ type: 'dataset', dataset: 'ri.dataset.x' })).toBe('ri.dataset.x');
  });

  it('returns the connection rid for type=connection', () => {
    expect(getDataSourceRid({ type: 'connection', connection: 'ri.conn.x' })).toBe('ri.conn.x');
  });

  it('returns the logSet rid for type=logSet', () => {
    expect(getDataSourceRid({ type: 'logSet', logSet: 'ri.logset.x' })).toBe('ri.logset.x');
  });

  it('returns undefined for type=video (unsupported)', () => {
    expect(getDataSourceRid({ type: 'video', video: 'ri.video.x' })).toBeUndefined();
  });
});

describe('getSupportedScopes / getSupportedScopeNames', () => {
  const supported = scope('ds1', { type: 'dataset', dataset: 'ri.dataset.a' });
  const connection = scope('ds2', { type: 'connection', connection: 'ri.conn.b' });
  const unsupported = scope('vid', { type: 'video', video: 'ri.video.c' });

  it('keeps dataset/connection/logSet scopes and drops video', () => {
    const asset = buildAsset({ dataScopes: [supported, connection, unsupported] });
    expect(getSupportedScopes(asset)).toEqual([supported, connection]);
  });

  it('returns an empty array when no scopes are supported', () => {
    const asset = buildAsset({ dataScopes: [unsupported] });
    expect(getSupportedScopes(asset)).toEqual([]);
  });

  it('returns the names of supported scopes', () => {
    const asset = buildAsset({ dataScopes: [supported, connection, unsupported] });
    expect(getSupportedScopeNames(asset)).toEqual(['ds1', 'ds2']);
  });
});

describe('resolveDataSourceRids', () => {
  const dataset = scope('ds1', { type: 'dataset', dataset: 'ri.dataset.a' });
  const connection = scope('ds2', { type: 'connection', connection: 'ri.conn.b' });
  const video = scope('ds3', { type: 'video', video: 'ri.video.c' });

  it('returns rids for all supported scopes when no filter is given', () => {
    const asset = buildAsset({ dataScopes: [dataset, connection] });
    expect(resolveDataSourceRids(asset)).toEqual(['ri.dataset.a', 'ri.conn.b']);
  });

  it('skips unsupported scope types (video) silently', () => {
    const asset = buildAsset({ dataScopes: [dataset, video, connection] });
    expect(resolveDataSourceRids(asset)).toEqual(['ri.dataset.a', 'ri.conn.b']);
  });

  it('filters by dataScopeName when provided', () => {
    const asset = buildAsset({ dataScopes: [dataset, connection] });
    expect(resolveDataSourceRids(asset, 'ds2')).toEqual(['ri.conn.b']);
  });

  it('returns an empty array when the filter matches nothing', () => {
    const asset = buildAsset({ dataScopes: [dataset, connection] });
    expect(resolveDataSourceRids(asset, 'nonexistent')).toEqual([]);
  });

  it('returns an empty array for an asset with no scopes', () => {
    expect(resolveDataSourceRids(buildAsset())).toEqual([]);
  });
});

describe('createBasicAsset', () => {
  it('creates an asset placeholder with empty collections', () => {
    const a = createBasicAsset('ri.scout.main.asset.x', 'Placeholder');
    expect(a).toEqual({
      rid: 'ri.scout.main.asset.x',
      title: 'Placeholder',
      labels: [],
      dataScopes: [],
      properties: {},
    });
  });
});

describe('fetchAssetByRid', () => {
  it('returns null for empty RID', async () => {
    const result = await fetchAssetByRid(DATASOURCE_URL, '');
    expect(result).toBeNull();
    expect(post).not.toHaveBeenCalled();
  });

  it('returns null for RID not starting with "ri."', async () => {
    const result = await fetchAssetByRid(DATASOURCE_URL, 'not-a-valid-rid');
    expect(result).toBeNull();
    expect(post).not.toHaveBeenCalled();
  });

  it('calls batch lookup endpoint with correct URL and payload', async () => {
    post.mockResolvedValue({});
    await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(post).toHaveBeenCalledWith(
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
    post.mockResolvedValue({ [VALID_RID]: asset });

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toEqual(asset);
  });

  it('returns null when asset has empty dataScopes', async () => {
    post.mockResolvedValue({
      [VALID_RID]: { rid: VALID_RID, title: 'Empty', dataScopes: [] },
    });

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toBeNull();
  });

  it('returns null when RID is not in response map', async () => {
    post.mockResolvedValue({});

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toBeNull();
  });

  it('propagates API errors to caller', async () => {
    post.mockRejectedValue(new Error('Network error'));

    await expect(fetchAssetByRid(DATASOURCE_URL, VALID_RID)).rejects.toThrow('Network error');
  });
});

describe('searchChannels', () => {
  it('passes requestId to BackendSrv so superseded channel searches can be cancelled', async () => {
    post.mockResolvedValue({ channels: [] });

    await searchChannels(DATASOURCE_URL, ['ri.dataset.a'], 'temp', {
      requestId: 'nominal-channel-options-1',
    });

    expect(post).toHaveBeenCalledWith(
      `${DATASOURCE_URL}/channels`,
      {
        dataSourceRids: ['ri.dataset.a'],
        searchText: 'temp',
      },
      { requestId: 'nominal-channel-options-1' }
    );
  });
});
