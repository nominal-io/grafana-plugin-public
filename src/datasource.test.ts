import { DataSource } from './datasource';
import { NominalDataSourceOptions } from './types';
import { DataSourceInstanceSettings } from '@grafana/data';
import { getTemplateSrv, getBackendSrv } from '@grafana/runtime';

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getTemplateSrv: jest.fn(),
  getBackendSrv: jest.fn(),
}));

const mockTemplateSrv = { replace: jest.fn((v: string) => v) };
const mockBackendSrv = { post: jest.fn() };

beforeEach(() => {
  jest.clearAllMocks();
  (getTemplateSrv as jest.Mock).mockReturnValue(mockTemplateSrv);
  (getBackendSrv as jest.Mock).mockReturnValue(mockBackendSrv);
  mockTemplateSrv.replace.mockImplementation((v: string) => v);
});

function createDataSource(): DataSource {
  const settings = {
    uid: 'test-uid',
    jsonData: {},
  } as DataSourceInstanceSettings<NominalDataSourceOptions>;
  return new DataSource(settings);
}

describe('metricFindQuery routing', () => {
  let ds: DataSource;

  beforeEach(() => {
    ds = createDataSource();
    mockBackendSrv.post.mockResolvedValue([]);
  });

  it.each([
    ['', '/assets'],
    ['assets', '/assets'],
    ['Assets', '/assets'],
    ['assets()', '/assets'],
    ['ASSETS()', '/assets'],
  ])('query %j calls assets endpoint', async (query, expectedPath) => {
    await ds.metricFindQuery(query);
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining(expectedPath),
      expect.objectContaining({ searchText: '' })
    );
  });

  it.each([
    ['assets:rocket', 'rocket'],
    ['assets(rocket)', 'rocket'],
    ['assets( rocket )', 'rocket'],
  ])('query %j passes search text %j', async (query, expectedSearch) => {
    await ds.metricFindQuery(query);
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining('/assets'),
      expect.objectContaining({ searchText: expectedSearch })
    );
  });

  it('query "datascopes(ri.scout.main.asset.1)" calls datascopes endpoint', async () => {
    await ds.metricFindQuery('datascopes(ri.scout.main.asset.1)');
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining('/datascopes'),
      expect.objectContaining({ assetRid: 'ri.scout.main.asset.1' })
    );
  });

  it('query "channels(ri.scout.main.asset.1)" calls channelvariables endpoint', async () => {
    await ds.metricFindQuery('channels(ri.scout.main.asset.1)');
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining('/channelvariables'),
      expect.objectContaining({ assetRid: 'ri.scout.main.asset.1', dataScopeName: '' })
    );
  });

  it('query "channels(ri.scout.main.asset.1, myScope)" passes dataScopeName', async () => {
    await ds.metricFindQuery('channels(ri.scout.main.asset.1, myScope)');
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining('/channelvariables'),
      expect.objectContaining({ assetRid: 'ri.scout.main.asset.1', dataScopeName: 'myScope' })
    );
  });

  it('unknown query returns empty array without calling backend', async () => {
    const result = await ds.metricFindQuery('somethingElse');
    expect(result).toEqual([]);
    expect(mockBackendSrv.post).not.toHaveBeenCalled();
  });
});

describe('metricFindQuery template variable resolution', () => {
  let ds: DataSource;

  beforeEach(() => {
    ds = createDataSource();
    mockBackendSrv.post.mockResolvedValue([]);
  });

  it('resolves template variable in datascopes query', async () => {
    mockTemplateSrv.replace.mockImplementation((v: string) =>
      v === '${asset}' ? 'ri.scout.main.asset.resolved' : v
    );
    await ds.metricFindQuery('datascopes(${asset})');
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining('/datascopes'),
      expect.objectContaining({ assetRid: 'ri.scout.main.asset.resolved' })
    );
  });

  it('resolves template variables in channels query', async () => {
    mockTemplateSrv.replace.mockImplementation((v: string) => {
      if (v === '${asset}') {
        return 'ri.scout.main.asset.resolved';
      }
      if (v === '${scope}') {
        return 'myScope';
      }
      return v;
    });
    await ds.metricFindQuery('channels(${asset}, ${scope})');
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining('/channelvariables'),
      expect.objectContaining({ assetRid: 'ri.scout.main.asset.resolved', dataScopeName: 'myScope' })
    );
  });
});

describe('metricFindQuery unresolved variable short-circuits', () => {
  let ds: DataSource;

  beforeEach(() => {
    ds = createDataSource();
  });

  it('datascopes with unresolved $var returns empty without backend call', async () => {
    mockTemplateSrv.replace.mockImplementation((v: string) => v);
    const result = await ds.metricFindQuery('datascopes($asset)');
    expect(result).toEqual([]);
    expect(mockBackendSrv.post).not.toHaveBeenCalled();
  });

  it('channels with unresolved assetRid returns empty without backend call', async () => {
    mockTemplateSrv.replace.mockImplementation((v: string) => v);
    const result = await ds.metricFindQuery('channels($asset)');
    expect(result).toEqual([]);
    expect(mockBackendSrv.post).not.toHaveBeenCalled();
  });

  it('channels with unresolved dataScopeName returns empty without backend call', async () => {
    mockTemplateSrv.replace.mockImplementation((v: string) => {
      if (v === 'ri.scout.main.asset.1') {
        return v;
      }
      return v; // $scope stays unresolved
    });
    const result = await ds.metricFindQuery('channels(ri.scout.main.asset.1, $scope)');
    expect(result).toEqual([]);
    expect(mockBackendSrv.post).not.toHaveBeenCalled();
  });

  it('assets with unresolved search text returns empty without backend call', async () => {
    const result = await ds.metricFindQuery('assets:$search');
    expect(result).toEqual([]);
    expect(mockBackendSrv.post).not.toHaveBeenCalled();
  });
});

describe('validateMetricFindResponse', () => {
  let ds: DataSource;

  beforeEach(() => {
    ds = createDataSource();
  });

  it('transforms valid response', async () => {
    mockBackendSrv.post.mockResolvedValue([
      { text: 'Asset 1', value: 'rid-1' },
      { text: 'Asset 2', value: 'rid-2' },
    ]);
    const result = await ds.metricFindQuery('assets');
    expect(result).toEqual([
      { text: 'Asset 1', value: 'rid-1' },
      { text: 'Asset 2', value: 'rid-2' },
    ]);
  });

  it('throws on non-array response', async () => {
    mockBackendSrv.post.mockResolvedValue({ not: 'an array' });
    await expect(ds.metricFindQuery('assets')).rejects.toThrow('expected array');
  });

  it('throws on item missing text field', async () => {
    mockBackendSrv.post.mockResolvedValue([{ value: 'rid-1' }]);
    await expect(ds.metricFindQuery('assets')).rejects.toThrow('missing text or value');
  });
});
