import {
  Asset,
  DataScope,
  assetToOption,
  createBasicAsset,
  getDataSourceRid,
  getSupportedScopeNames,
  getSupportedScopes,
  resolveDataSourceRids,
} from './api';

const scope = (name: string, dataSource: DataScope['dataSource']): DataScope => ({
  dataScopeName: name,
  dataSource,
  timestampType: 'ABSOLUTE',
  seriesTags: {},
});

const buildAsset = (overrides: Partial<Asset> = {}): Asset => ({
  rid: 'ri.scout.main.asset.abc',
  title: 'Test Asset',
  labels: [],
  dataScopes: [],
  properties: {},
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

describe('assetToOption', () => {
  it('summarizes labels and supported scope count', () => {
    const asset = buildAsset({
      title: 'Engine 12',
      labels: ['prod', 'eu'],
      dataScopes: [
        scope('ds1', { type: 'dataset', dataset: 'ri.dataset.a' }),
        scope('vid', { type: 'video', video: 'ri.video.c' }),
      ],
    });
    expect(assetToOption(asset)).toEqual({
      label: 'Engine 12',
      value: asset.rid,
      description: 'prod, eu - 1 data scope(s)',
    });
  });

  it('falls back to "No labels" when labels are empty', () => {
    const asset = buildAsset({ title: 'X', labels: [] });
    expect(assetToOption(asset).description).toBe('No labels - 0 data scope(s)');
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
