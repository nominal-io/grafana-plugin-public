import { DataSource } from './datasource';
import { NominalDataSourceOptions } from './types';
import {
  DataFrame,
  DataFrameType,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceInstanceSettings,
  dateTime,
  FieldType,
  LoadingState,
  toDataFrame,
} from '@grafana/data';
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
    // The runs live in module state, so each test loads a fresh module.
    let createDataSource: () => DataSource;
    beforeEach(() => {
      jest.isolateModules(() => {
        const { DataSource: Fresh } = require('./datasource');
        createDataSource = () => new Fresh({ uid: 'test-uid', jsonData: {} });
      });
    });
    const sql = { refId: 'A', queryType: 'sql', format: 'logs', rawSql: 'SELECT 1' };
    const logPage = (values: number[], nanos: number[], ids: string[], refId = 'A') =>
      toDataFrame({
        refId,
        meta: { type: DataFrameType.LogLines },
        fields: [
          { name: 'timestamp', type: FieldType.time, values, nanos },
          { name: 'id', type: FieldType.string, values: ids },
        ],
      });
    // A page whose newest line is at 3000 ms plus 7 ns and oldest at 2000 ms plus 345 ns.
    const page = logPage([3000, 2000], [7, 345], ['newest', 'oldest']);
    // Pages older and newer than page.
    const olderPage = logPage([1500, 1000], [0, 9], ['older newest', 'older oldest']);
    const newerPage = logPage([4000, 3500], [11, 0], ['newer newest', 'newer oldest']);
    // scroll sends a request whose response is the given frames and returns where the first query's
    // page continues from.
    const scroll = async (ds: DataSource, request: object, data: DataFrame[] = [page], failure?: object) => {
      mockBackendQuery.mockReturnValue(of({ data, ...failure }));
      const response = await lastValueFrom(
        ds.query({
          app: 'dashboard',
          requestId: 'SQR1',
          targets: [sql],
          ...request,
        } as unknown as DataQueryRequest<any>)
      );
      const { scrollId, scrollNewer } = mockBackendQuery.mock.calls.at(-1)[0].targets[0];
      return Object.assign({ scrollId, scrollNewer }, { response });
    };
    const continues = async (...args: Parameters<typeof scroll>) => {
      const { scrollId, scrollNewer } = await scroll(...args);
      return { scrollId, scrollNewer };
    };
    const none = { scrollId: undefined, scrollNewer: undefined };
    const older = (scrollId: string) => ({ scrollId, scrollNewer: undefined });
    const newer = (scrollId?: string) => ({ scrollId, scrollNewer: true });
    const explore = { ...sql, supportingQueryType: 'infiniteScroll' };
    const inExplore = (request: object) => ({ app: 'explore', requestId: 'explore_a', ...request });

    it.each([
      ['the next older page', { range: range(0, 2000) }, older('oldest')],
      ['an older page whose bound is past the line', { range: range(0, 1500) }, older('oldest')],
      // The first page set the bound, so the lines between are hidden ones the run has read.
      ['an older page whose bound is short of the line', { range: range(0, 2500) }, older('oldest')],
      ['the next newer page', { range: range(3000, 9000) }, newer('newest')],
      ['a newer page whose bound is past the line', { range: range(3500, 9000) }, newer('newest')],
      ['changed SQL', { targets: [{ ...sql, rawSql: 'SELECT 2' }], range: range(0, 2000) }, none],
      ['a refreshed dashboard', { range: range(0, 2000), requestId: 'SQR2' }, none],
      ['a rerun in Explore', inExplore({ range: range(0, 2000) }), none],
      ['another Explore pane', inExplore({ targets: [explore], range: range(0, 2000), requestId: 'explore_b' }), none],
      ['the next page in Explore', inExplore({ targets: [explore], range: range(0, 2000) }), older('oldest')],
    ])('%s', async (_, next, want) => {
      const ds = createDataSource();
      const first = next as { app?: string };
      const start = { range: range(0, 5000) };
      expect(await continues(ds, first.app ? inExplore(start) : start)).toEqual(none);
      expect(await continues(ds, next)).toEqual(want);
    });

    it.each([
      // The bound is between the lines the last page started and ended at, so it came from that page.
      ['a bound from the last page', 'SQR1', range(0, 1500), older('older oldest')],
      // The bound is at the line the last page started from, as when Grafana loses that page.
      ['a bound from an earlier page', 'SQR1', range(0, 2000), none],
      ['a mixed panel', 'mixed-1-SQR1', range(0, 1500), none],
    ])('after two pages, a bound short of the line continues it for %s', async (_, requestId, nextRange, want) => {
      const ds = createDataSource();
      await scroll(ds, { range: range(0, 5000), requestId });
      await scroll(ds, { range: range(0, 2000) }, [olderPage]);
      expect(await continues(ds, { range: nextRange })).toEqual(want);
    });

    it('reads a newer page from the start of its range when it cannot continue from a line', async () => {
      const ds = createDataSource();
      await scroll(ds, { range: range(0, 5000) });
      await scroll(ds, { range: range(3000, 9000) }, [newerPage]);
      expect(await continues(ds, { range: range(3500, 9000) }, [newerPage])).toEqual(newer('newer newest'));
      expect(await continues(ds, { range: range(3000, 9000) })).toEqual(newer());
    });

    it.each([
      [
        'an older page',
        olderPage,
        range(0, 2000),
        range(3000, 9000),
        newer('newest'),
        range(0, 1000),
        older('older oldest'),
      ],
      [
        'a newer page',
        newerPage,
        range(3000, 9000),
        range(0, 2000),
        older('oldest'),
        range(4000, 9000),
        newer('newer newest'),
      ],
    ])('continues from the edges of every page in the run after %s', async (_, next, nextRange, keptRange, kept, movedRange, moved) => {
      const ds = createDataSource();
      await scroll(ds, { range: range(0, 5000) });
      await scroll(ds, { range: nextRange }, [next]);
      // The edge on the other side is still the first page's.
      expect(await continues(ds, { range: keptRange })).toEqual(kept);
      expect(await continues(ds, { range: movedRange })).toEqual(moved);
    });

    it('continues a run through another instance of the data source', async () => {
      await scroll(createDataSource(), { range: range(0, 5000) });
      expect(await continues(createDataSource(), { range: range(0, 2000) })).toEqual(older('oldest'));
    });

    it('continues each query of a request from its own line', async () => {
      const ds = createDataSource();
      const other = { ...sql, refId: 'B', rawSql: 'SELECT 2' };
      const otherPage = logPage([3000, 2500], [8, 111], ['b newest', 'b oldest'], 'B');
      // Query A's oldest line sets the bound, which is before query B's oldest line.
      await scroll(ds, { targets: [sql, other], range: range(0, 5000) }, [page, otherPage]);
      await scroll(ds, { targets: [sql, other], range: range(0, 2000) }, [page, otherPage]);
      const [a, b] = mockBackendQuery.mock.calls.at(-1)[0].targets;
      expect([a.scrollId, b.scrollId]).toEqual(['oldest', 'b oldest']);
    });

    it('moves only the edge a page continued from when lines share a time', async () => {
      const ds = createDataSource();
      // Every line is at 3000 ms plus 7 ns; the service orders them by id. An oldest-first panel loads
      // newer lines, then asks for older ones.
      await scroll(ds, { range: range(0, 5000) }, [logPage([3000, 3000], [7, 7], ['n4', 'n3'])]);
      await scroll(ds, { range: range(3000, 9000) }, [logPage([3000, 3000], [7, 7], ['n6', 'n5'])]);
      expect(await continues(ds, { range: range(0, 3000) })).toEqual(older('n3'));
    });

    it('starts over when Explore reruns the query', async () => {
      const ds = createDataSource();
      await scroll(ds, inExplore({ range: range(0, 5000) }), [olderPage]);
      await scroll(ds, inExplore({ range: range(0, 5000) }));
      expect(await continues(ds, inExplore({ targets: [explore], range: range(0, 2000) }))).toEqual(older('oldest'));
    });

    it.each([
      ['a query error', { state: LoadingState.Error, errors: [{ refId: 'B', message: 'timeout' }] }],
      ['a request error', { state: LoadingState.Error, error: { message: 'Bad Gateway', status: 502 } }],
    ])('keeps the run when Explore drops a scroll page with %s', async (_, failure) => {
      const ds = createDataSource();
      await scroll(ds, inExplore({ range: range(0, 5000) }));
      await scroll(ds, inExplore({ targets: [explore], range: range(0, 2000) }), [olderPage], failure);
      expect(await continues(ds, inExplore({ targets: [explore], range: range(0, 2000) }))).toEqual(older('oldest'));
    });

    it('starts a run from an Explore query whose other query failed', async () => {
      const ds = createDataSource();
      const failed = { state: LoadingState.Error, errors: [{ refId: 'B', message: 'timeout' }] };
      await scroll(ds, inExplore({ range: range(0, 5000) }), [page], failed);
      expect(await continues(ds, inExplore({ targets: [explore], range: range(0, 2000) }))).toEqual(older('oldest'));
    });

    it('sends a scroll page again when Explore repeats its request', async () => {
      const ds = createDataSource();
      await scroll(ds, inExplore({ range: range(0, 5000) }));
      // Explore drops this page when a query of another data source fails, which this one cannot see.
      await scroll(ds, inExplore({ targets: [explore], range: range(0, 2000) }), [olderPage]);
      const retry = logPage([2000, 1500], [345, 0], ['oldest', 'older newest']);
      const { response } = await scroll(ds, inExplore({ targets: [explore], range: range(0, 2000) }), [retry]);
      const [times, ids] = response.data[0].fields;
      expect(ids.values).toEqual(['oldest', 'older newest', 'older oldest']);
      expect(times.nanos).toEqual([345, 0, 9]);
    });

    it.each([
      ['five newer runs ran', 5, ['oldest', 'older newest', 'older oldest']],
      ['six newer runs ran', 6, ['oldest', 'older newest']],
    ])('sends a repeated scroll page again only while %s', async (_, runCount, want) => {
      const ds = createDataSource();
      await scroll(ds, inExplore({ range: range(0, 5000) }));
      await scroll(ds, inExplore({ targets: [explore], range: range(0, 2000) }), [olderPage]);
      for (const requestId of Array.from({ length: runCount }, (_, i) => `explore_${i}`)) {
        await scroll(ds, inExplore({ range: range(0, 5000), requestId }));
      }
      const retry = logPage([2000, 1500], [345, 0], ['oldest', 'older newest']);
      const { response } = await scroll(ds, inExplore({ targets: [explore], range: range(0, 2000) }), [retry]);
      expect(response.data[0].fields[1].values).toEqual(want);
    });

    it('forgets the last run when Explore reruns the query without lines', async () => {
      const ds = createDataSource();
      await scroll(ds, inExplore({ range: range(0, 5000) }));
      await scroll(ds, inExplore({ range: range(0, 5000) }), [logPage([], [], [])]);
      expect(await continues(ds, inExplore({ targets: [explore], range: range(0, 2000) }))).toEqual(none);
    });

    it('continues a Compute log query', async () => {
      const ds = createDataSource();
      const compute = { refId: 'A', queryType: 'compute', assetRid: 'ri.asset', channel: 'app.logs', channelDataType: 'log' };
      await scroll(ds, { targets: [compute], range: range(0, 5000) });
      expect(await continues(ds, { targets: [compute], range: range(0, 2000) })).toEqual(older('oldest'));
      expect(await continues(ds, { targets: [{ ...compute, channel: 'other' }], range: range(0, 2000) })).toEqual(none);
    });

    it('continues a mixed-datasource panel, whose first request has a prefixed requestId', async () => {
      const ds = createDataSource();
      await scroll(ds, { range: range(0, 5000), requestId: 'mixed-1-SQR1' });
      expect(await continues(ds, { range: range(0, 2000) })).toEqual(older('oldest'));
    });

    it('keeps a mid-scroll run while other requests without logs come and go', async () => {
      const ds = createDataSource();
      await scroll(ds, { range: range(0, 5000) });
      for (let i = 0; i < 250; i++) {
        await scroll(ds, { range: range(0, 5000), requestId: `other${i}` }, []);
      }
      expect(await continues(ds, { range: range(0, 2000) })).toEqual(older('oldest'));
    });

    // Whole runs: a fake backend serves each query's pages, and a model of Explore's infinite scroll asks
    // for them until every line of every query shows.
    describe('scrolling a whole range in Explore', () => {
      interface Line {
        ms: number;
        nanos: number;
        sid: number;
        id: string;
      }
      interface Query {
        target: { refId: string; [key: string]: unknown };
        compute: boolean;
        lines: Line[];
        size: number;
      }
      const ns = (l: Line) => l.ms * 1e6 + l.nanos;
      // Newest first; lines that share a time are ordered by service id, highest first.
      const newestFirst = (a: Line, b: Line) => ns(b) - ns(a) || b.sid - a.sid;
      const every = (from: number, to: number, step: number) =>
        Array.from({ length: Math.floor((to - from) / step) + 1 }, (_, i) => [to - i * step]);
      // A line is [ms, nanos, service id].
      const query = (refId: string, kind: 'sql' | 'compute', lines: number[][], size: number): Query => ({
        target:
          kind === 'sql'
            ? { ...sql, refId, rawSql: `SELECT ${refId}` }
            : { refId, queryType: 'compute', assetRid: 'ri.asset', channel: refId, channelDataType: 'log' },
        compute: kind === 'compute',
        lines: lines.map(([ms, nanos = 0, sid = 0]) => ({ ms, nanos, sid, id: `${refId}${ms}.${nanos}.${sid}` })),
        size,
      });

      // serve models the backend's page. SQL ends an older page at the line it continues from, inclusive and
      // by time only. Compute pages strictly after the line by time and service id. Grafana's bound covers
      // its whole millisecond.
      const serve = (q: Query, from: number, to: number, scrollId?: string) => {
        const c = q.lines.find((l) => l.id === scrollId);
        const start = from * 1e6;
        const cursor = c && ns(c) >= start ? c : undefined;
        const end = cursor ? ns(cursor) : to * 1e6 + 999999;
        const rows = q.lines
          .filter((l) => ns(l) >= start && ns(l) <= end && !(cursor && q.compute && newestFirst(cursor, l) >= 0))
          .sort(newestFirst)
          .slice(0, q.size);
        return logPage(
          rows.map((l) => l.ms),
          rows.map((l) => l.nanos),
          rows.map((l) => l.id),
          q.target.refId
        );
      };

      // scrollAll loads a first page, then scroll pages bounded by the oldest line shown, until that line is
      // within a second of the range's start, nothing new shows for three requests, or 40 requests. Requests
      // numbered in drops are dropped by Explore because another data source failed. With latch, as in
      // Grafana 12.3 and later, Explore stops asking once a page adds no line.
      const scrollAll = async (queries: Query[], { drops = [] as number[], latch = false } = {}) => {
        const ds = createDataSource();
        const shown = new Map<string, number>();
        mockBackendQuery.mockImplementation((request) => {
          const { from, to } = request.range;
          const data = request.targets.map((t: any) =>
            serve(queries.find((q) => q.target.refId === t.refId)!, from.valueOf(), to.valueOf(), t.scrollId)
          );
          return of({ data });
        });
        const load = async (n: number, to: number) => {
          const targets = queries.map((q) => (n > 0 ? { ...q.target, supportingQueryType: 'infiniteScroll' } : q.target));
          const response = await lastValueFrom(ds.query(inExplore({ targets, range: range(0, to) }) as any));
          const before = shown.size;
          if (!drops.includes(n)) {
            response.data.forEach(({ fields: [times, ids] }: DataFrame) =>
              ids.values.forEach((id: string, i: number) => shown.set(id, times.values[i]))
            );
          }
          return shown.size - before;
        };
        await load(0, 60000);
        for (let n = 1, idle = 0; n <= 40 && idle < 3; n++) {
          const bound = Math.min(...shown.values());
          if (bound <= 1000) {
            break;
          }
          const added = await load(n, bound);
          if (added === 0 && latch && !drops.includes(n)) {
            break;
          }
          idle = added === 0 ? idle + 1 : 0;
        }
        return [...shown.keys()].sort();
      };
      const ids = (queries: Query[]) => queries.flatMap((q) => q.lines.map((l) => l.id)).sort();

      const one = () => [query('A', 'sql', every(2000, 59000, 1000), 5)];
      const two = () => [query('A', 'sql', every(2000, 59000, 1500), 4), query('B', 'sql', every(2500, 59500, 2500), 3)];
      it.each([
        ['one query', one, []],
        ['one query, a page dropped', one, [2]],
        ['one query, a page and its repeat dropped', one, [2, 3]],
        ['two queries', two, []],
        ['two queries, two pages dropped', two, [2, 5]],
        ['two queries, a page dropped twice', two, [2, 3]],
      ])('shows every line of %s', async (_, queries, drops) => {
        const q = queries();
        expect(await scrollAll(q, { drops })).toEqual(ids(q));
      });

      // The sparse query's lines end long before the range does, so the dense query's pages stay newer
      // than its oldest line and Grafana asks with the same bound for many pages.
      it.each([false, true])('shows every line of a sparse and a dense query (latch %s)', async (latch) => {
        const q = [query('S', 'sql', [[59000], [30000]], 5), query('T', 'sql', every(2000, 59500, 1000), 5)];
        expect(await scrollAll(q, { latch })).toEqual(ids(q));
      });

      // Each page stays inside the millisecond Grafana bounds the next with, so Grafana asks with the same
      // bound although it showed the page.
      it.each([false, true])('shows every line of a page inside one millisecond (latch %s)', async (latch) => {
        const dense = [1, 2, 3, 4, 5, 6, 7, 8].map((nanos) => [30000, nanos]);
        const q = [query('D', 'sql', [...dense, [20000], [10000]], 3)];
        expect(await scrollAll(q, { latch })).toEqual(ids(q));
      });

      it.each([
        ['', []],
        [', a page dropped', [1]],
      ])('shows every Compute line at one time across page edges%s', async (_, drops) => {
        const tied = [9, 8, 7, 6, 5, 4, 3, 2, 1].map((sid) => [30000, 0, sid]);
        const q = [query('C', 'compute', [...tied, [20000], [10000]], 3)];
        expect(await scrollAll(q, { drops })).toEqual(ids(q));
      });
    });
  });
});
