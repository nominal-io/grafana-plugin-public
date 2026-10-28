import { DataSource } from './datasource';
import { NominalDataSourceOptions } from './types';
import { DataFrameType, DataQueryRequest, DataQueryResponse, DataSourceInstanceSettings, dateTime, FieldType, toDataFrame } from '@grafana/data';
import { getTemplateSrv, getBackendSrv } from '@grafana/runtime';
import { lastValueFrom, of } from 'rxjs';

const mockBackendQuery = jest.fn();

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    query(request: unknown) {
      return mockBackendQuery(request);
    }
  },
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

describe('backend health check routing', () => {
  it('does not override the backend health check with a frontend testDatasource method', () => {
    const ds = createDataSource();

    expect(Object.prototype.hasOwnProperty.call(Object.getPrototypeOf(ds), 'testDatasource')).toBe(false);
  });
});

describe('filterQuery', () => {
  let ds: DataSource;

  beforeEach(() => {
    ds = createDataSource();
  });

  it('skips hidden queries', () => {
    expect(ds.filterQuery({
      refId: 'A',
      hide: true,
      assetRid: 'ri.scout.main.asset.1',
      channel: 'temperature',
      dataScopeName: 'default',
    })).toBe(false);
  });

  it('skips whitespace-only queries', () => {
    expect(ds.filterQuery({
      refId: 'A',
      assetRid: '   ',
      channel: '  ',
      dataScopeName: '\t',
    })).toBe(false);
  });

  it('keeps complete Nominal and legacy queries', () => {
    expect(ds.filterQuery({
      refId: 'A',
      assetRid: 'ri.scout.main.asset.1',
      channel: 'temperature',
      dataScopeName: 'default',
    })).toBe(true);

    expect(ds.filterQuery({
      refId: 'B',
      queryText: 'legacy query',
    })).toBe(true);
  });

  it('accepts SQL queries with text and rejects empty ones', () => {
    expect(ds.filterQuery({ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1' })).toBe(true);
    expect(ds.filterQuery({ refId: 'A', queryType: 'sql', rawSql: '  ' })).toBe(false);
  });
});

describe('applyTemplateVariables', () => {
  it('interpolates raw SQL with SQL quoting', () => {
    const ds = createDataSource();
    mockTemplateSrv.replace.mockImplementation((value: string, _scopedVars?: unknown, format?: any) =>
      value === 'channel IN ($ch)' ? value.replace('$ch', format(['a', 'b'], { multi: true })) : value
    );

    const result = ds.applyTemplateVariables({ refId: 'A', queryType: 'sql', rawSql: 'channel IN ($ch)' }, {});

    expect(result.rawSql).toBe("channel IN ('a','b')");
    expect(mockTemplateSrv.replace).toHaveBeenCalledWith('channel IN ($ch)', {}, expect.any(Function));
  });

  it('keeps builder interpolation unchanged', () => {
    const ds = createDataSource();
    ds.applyTemplateVariables({ refId: 'A', assetRid: '$asset', channel: '$channel', dataScopeName: '$scope' }, {});

    expect(mockTemplateSrv.replace).toHaveBeenCalledWith('$asset', {});
    expect(mockTemplateSrv.replace).toHaveBeenCalledWith('$channel', {});
    expect(mockTemplateSrv.replace).toHaveBeenCalledWith('$scope', {});
  });
});

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

  it('uses scoped variables when resolving chained variable queries', async () => {
    mockTemplateSrv.replace.mockImplementation((v: string, scopedVars?: any) => {
      if (v === '${asset}') {
        return scopedVars?.asset?.value || v;
      }
      if (v === '${scope}') {
        return scopedVars?.scope?.value || v;
      }
      return v;
    });

    await ds.metricFindQuery('channels(${asset}, ${scope})', {
      scopedVars: {
        asset: { text: 'Asset 1', value: 'ri.scout.main.asset.scoped' },
        scope: { text: 'Primary', value: 'primary' },
      },
    });

    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining('/channelvariables'),
      expect.objectContaining({ assetRid: 'ri.scout.main.asset.scoped', dataScopeName: 'primary' })
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

  it('throws a user-safe error when the variable request fails', async () => {
    mockBackendSrv.post.mockRejectedValue(new Error('secret backend detail'));

    await expect(ds.metricFindQuery('assets')).rejects.toThrow('Unable to load Nominal assets');
  });
});

describe('annotations', () => {
  it('runs annotation targets as SQL tables', () => {
    const ds = createDataSource();
    const prepare = ds.annotations!.prepareQuery!;

    expect(prepare({ enable: true, name: 'a', iconColor: 'red', target: { refId: 'Anno', rawSql: 'SELECT 1' } })).toEqual({
      refId: 'Anno',
      rawSql: 'SELECT 1',
      queryType: 'sql',
      format: 'table',
    });
    expect(
      prepare({ enable: true, name: 'a', iconColor: 'red', target: { refId: 'Anno', rawSql: 'SELECT 1', format: 'timeseries' } })
    ).toMatchObject({ format: 'table' });
    // A v2 dashboard loaded by Grafana 12.1 carries the query only under query.spec.
    expect(
      prepare({ enable: true, name: 'a', iconColor: 'red', query: { kind: 'DataQuery', spec: { refId: 'Anno', rawSql: 'SELECT 1' } } })
    ).toEqual({ refId: 'Anno', rawSql: 'SELECT 1', queryType: 'sql', format: 'table' });
    expect(prepare({ enable: true, name: 'a', iconColor: 'red' })).toBeUndefined();
  });
});

describe('query', () => {
  const range = (from: number, to: number) => ({ from: dateTime(from), to: dateTime(to), raw: { from: '', to: '' } });
  const frame = (type: DataFrameType, nanos?: number[]) =>
    toDataFrame({
      meta: { type },
      fields: [
        { name: 'timestamp', type: FieldType.time, values: [1, 2], nanos },
        { name: 'body', type: FieldType.string, values: ['a', 'b'] },
      ],
    });

  it.each([
    ['pads a log frame without nanos', DataFrameType.LogLines, undefined, [0, 0]],
    ['keeps nanos a log frame already has', DataFrameType.LogLines, [5, 7], [5, 7]],
    ['leaves other frames alone', DataFrameType.TimeSeriesMulti, undefined, undefined],
  ])('%s', async (_, type, nanos, want) => {
    const response: DataQueryResponse = { data: [frame(type, nanos)], key: 'k' };
    mockBackendQuery.mockReturnValue(of(response));
    const result = await lastValueFrom(createDataSource().query({ targets: [], range: range(0, 10), requestId: 'SQR1' } as unknown as DataQueryRequest<any>));
    expect(result.data[0].fields[0].nanos).toEqual(want);
    expect(result.key).toBe('k');
  });

  describe('continues a log page from its edge line', () => {
    // The edges live in module state, so each test loads a fresh module.
    let createDataSource: () => DataSource;
    beforeEach(() => {
      jest.isolateModules(() => {
        const { DataSource: Fresh } = require('./datasource');
        createDataSource = () => new Fresh({ uid: 'test-uid', jsonData: {} });
      });
    });
    const sql = { refId: 'A', queryType: 'sql', format: 'logs', rawSql: 'SELECT 1' };
    // A page whose newest line is at 3000 ms plus 7 ns and oldest at 2000 ms plus 345 ns.
    const logPage = (values: number[], nanos: number[], ids: string[]) =>
      toDataFrame({
        refId: 'A',
        meta: { type: DataFrameType.LogLines },
        fields: [
          { name: 'timestamp', type: FieldType.time, values, nanos },
          { name: 'id', type: FieldType.string, values: ids },
        ],
      });
    const page = logPage([3000, 2000], [7, 345], ['newest', 'oldest']);
    const scroll = async (ds: DataSource, request: object) => {
      mockBackendQuery.mockReturnValue(of({ data: [page] }));
      await lastValueFrom(
        ds.query({ app: 'dashboard', requestId: 'SQR1', ...request } as unknown as DataQueryRequest<any>)
      );
      const { scrollStartNanos, scrollStartId, scrollEndNanos, scrollEndId } = mockBackendQuery.mock.calls.at(-1)[0].targets[0];
      return { scrollStartNanos, scrollStartId, scrollEndNanos, scrollEndId };
    };
    const none = { scrollStartNanos: undefined, scrollStartId: undefined, scrollEndNanos: undefined, scrollEndId: undefined };
    const older = { ...none, scrollEndNanos: 345, scrollEndId: 'oldest' };
    const newer = { ...none, scrollStartNanos: 7, scrollStartId: 'newest' };
    const explore = { ...sql, supportingQueryType: 'infiniteScroll' };
    // A page older than page: its oldest line is at 1000 ms plus 9 ns.
    const olderPage = logPage([1500, 1000], [0, 9], ['older newest', 'older oldest']);

    it.each([
      ['the next older page', { targets: [sql], range: range(0, 2000) }, older],
      ['the next newer page', { targets: [sql], range: range(3000, 9000) }, newer],
      ['a range that ends elsewhere', { targets: [sql], range: range(0, 2001) }, none],
      ['a range that starts elsewhere', { targets: [sql], range: range(1, 9000) }, none],
      ['changed SQL', { targets: [{ ...sql, rawSql: 'SELECT 2' }], range: range(0, 2000) }, none],
      ['a refreshed dashboard', { targets: [sql], range: range(0, 2000), requestId: 'SQR2' }, none],
      ['a rerun in Explore', { targets: [sql], range: range(0, 2000), app: 'explore', requestId: 'explore_a' }, none],
      [
        'another Explore pane',
        { targets: [explore], range: range(0, 2000), app: 'explore', requestId: 'explore_b' },
        none,
      ],
      [
        'the next page in Explore',
        { targets: [explore], range: range(0, 2000), app: 'explore', requestId: 'explore_a' },
        older,
      ],
    ])('%s', async (_, next, want) => {
      const ds = createDataSource();
      const first = next as { app?: string; requestId?: string };
      const start = { app: first.app ?? 'dashboard', requestId: first.app ? 'explore_a' : 'SQR1' };
      expect(await scroll(ds, { targets: [sql], range: range(0, 5000), ...start })).toEqual(none);
      expect(await scroll(ds, next)).toEqual(want);
    });

    // A page newer than page: its newest line is at 4000 ms plus 11 ns.
    const newerPage = logPage([4000, 3500], [11, 0], ['newer newest', 'newer oldest']);
    it.each([
      ['an older page', olderPage, range(0, 2000), range(3000, 9000), newer, range(0, 1000), { ...none, scrollEndNanos: 9, scrollEndId: 'older oldest' }],
      ['a newer page', newerPage, range(3000, 9000), range(0, 2000), older, range(4000, 9000), { ...none, scrollStartNanos: 11, scrollStartId: 'newer newest' }],
    ])('continues from the edges of every page in the run after %s', async (_, next, nextRange, keptRange, kept, movedRange, moved) => {
      const ds = createDataSource();
      await scroll(ds, { targets: [sql], range: range(0, 5000) });
      mockBackendQuery.mockReturnValue(of({ data: [next] }));
      await lastValueFrom(ds.query({ app: 'dashboard', requestId: 'SQR1', targets: [sql], range: nextRange } as any));
      // The edge on the other side is still the first page's.
      expect(await scroll(ds, { targets: [sql], range: keptRange })).toEqual(kept);
      expect(await scroll(ds, { targets: [sql], range: movedRange })).toEqual(moved);
    });

    it('continues a run through another instance of the data source', async () => {
      await scroll(createDataSource(), { targets: [sql], range: range(0, 5000) });
      expect(await scroll(createDataSource(), { targets: [sql], range: range(0, 2000) })).toEqual(older);
    });

    it('keeps a run for each query of a request', async () => {
      const ds = createDataSource();
      const other = { ...sql, refId: 'B', rawSql: 'SELECT 2' };
      const otherPage = { ...logPage([3000, 2000], [8, 111], ['b newest', 'b oldest']), refId: 'B' };
      const request = (to: number) => ({ app: 'dashboard', requestId: 'SQR1', targets: [sql, other], range: range(0, to) });
      mockBackendQuery.mockReturnValue(of({ data: [page, otherPage] }));
      await lastValueFrom(ds.query(request(5000) as any));
      await lastValueFrom(ds.query(request(2000) as any));
      const [a, b] = mockBackendQuery.mock.calls.at(-1)[0].targets;
      expect([a.scrollEndId, b.scrollEndId]).toEqual(['oldest', 'b oldest']);
    });

    it('starts over when Explore reruns the query', async () => {
      const ds = createDataSource();
      const explorePage = (request: object) => ({ app: 'explore', requestId: 'explore_a', ...request });
      mockBackendQuery.mockReturnValue(of({ data: [olderPage] }));
      await lastValueFrom(ds.query(explorePage({ targets: [sql], range: range(0, 5000) }) as any));
      await scroll(ds, explorePage({ targets: [sql], range: range(0, 5000) }));
      expect(await scroll(ds, explorePage({ targets: [explore], range: range(0, 2000) }))).toEqual(older);
    });

    it('keeps the run when Explore drops a scroll page with an error', async () => {
      const ds = createDataSource();
      const explorePage = (request: object) => ({ app: 'explore', requestId: 'explore_a', ...request });
      await scroll(ds, explorePage({ targets: [sql], range: range(0, 5000) }));
      mockBackendQuery.mockReturnValue(of({ data: [olderPage], errors: [{ refId: 'B', message: 'timeout' }] }));
      await lastValueFrom(ds.query(explorePage({ targets: [explore], range: range(0, 2000) }) as any));
      expect(await scroll(ds, explorePage({ targets: [explore], range: range(0, 2000) }))).toEqual(older);
    });

    it('forgets the last run when Explore reruns the query without lines', async () => {
      const ds = createDataSource();
      const explorePage = (request: object) => ({ app: 'explore', requestId: 'explore_a', ...request });
      await scroll(ds, explorePage({ targets: [sql], range: range(0, 5000) }));
      mockBackendQuery.mockReturnValue(of({ data: [logPage([], [], [])] }));
      await lastValueFrom(ds.query(explorePage({ targets: [sql], range: range(0, 5000) }) as any));
      expect(await scroll(ds, explorePage({ targets: [explore], range: range(0, 2000) }))).toEqual(none);
    });

    it('moves only the edge a page continued from when lines share a time', async () => {
      const ds = createDataSource();
      const request = (to: number, from = 0) => ({ app: 'dashboard', requestId: 'SQR1', targets: [sql], range: range(from, to) });
      // Every line is at 3000 ms plus 7 ns; the service orders them by id. An oldest-first panel loads
      // newer lines, then asks for older ones.
      mockBackendQuery.mockReturnValue(of({ data: [logPage([3000, 3000], [7, 7], ['n4', 'n3'])] }));
      await lastValueFrom(ds.query(request(5000) as any));
      mockBackendQuery.mockReturnValue(of({ data: [logPage([3000, 3000], [7, 7], ['n6', 'n5'])] }));
      await lastValueFrom(ds.query(request(9000, 3000) as any));
      expect(await scroll(ds, request(3000))).toEqual({ ...none, scrollEndNanos: 7, scrollEndId: 'n3' });
    });

    it('continues a Compute log query', async () => {
      const ds = createDataSource();
      const compute = { refId: 'A', queryType: 'compute', assetRid: 'ri.asset', channel: 'app.logs', channelDataType: 'log' };
      await scroll(ds, { targets: [compute], range: range(0, 5000) });
      expect(await scroll(ds, { targets: [compute], range: range(0, 2000) })).toEqual(older);
      expect(await scroll(ds, { targets: [{ ...compute, channel: 'other' }], range: range(0, 2000) })).toEqual(none);
    });

    it('continues a mixed-datasource panel, whose first request has a prefixed requestId', async () => {
      const ds = createDataSource();
      await scroll(ds, { targets: [sql], range: range(0, 5000), requestId: 'mixed-1-SQR1' });
      expect(await scroll(ds, { targets: [sql], range: range(0, 2000) })).toEqual(older);
    });
  });
});
