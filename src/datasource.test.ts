import { DataSource } from './datasource';
import { NominalDataSourceOptions } from './types';
import { DataSourceInstanceSettings } from '@grafana/data';
import { getTemplateSrv, getBackendSrv } from '@grafana/runtime';

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getTemplateSrv: jest.fn(),
  getBackendSrv: jest.fn(),
}));

const mockTemplateSrv = { replace: jest.fn((v: string) => v), getVariables: jest.fn(() => [] as Array<Record<string, unknown>>) };
const mockBackendSrv = { post: jest.fn() };

beforeEach(() => {
  jest.clearAllMocks();
  (getTemplateSrv as jest.Mock).mockReturnValue(mockTemplateSrv);
  (getBackendSrv as jest.Mock).mockReturnValue(mockBackendSrv);
  mockTemplateSrv.replace.mockImplementation((v: string) => v);
  mockTemplateSrv.getVariables.mockReturnValue([]);
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

  it.each([
    ['complete run query', { computeBy: 'run', runRid: 'ri.scout.main.run.1', channel: 'temp', dataScopeName: 'default' }, true],
    ['run query without a run', { computeBy: 'run', assetRid: 'ri.scout.main.asset.1', channel: 'temp', dataScopeName: 'default' }, false],
    ['asset query ignores a kept runRid', { computeBy: 'asset', runRid: 'ri.scout.main.run.1', channel: 'temp', dataScopeName: 'default' }, false],
  ] as const)('%s', (_name, fields, want) => {
    expect(ds.filterQuery({ refId: 'A', ...fields })).toBe(want);
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
    ds.applyTemplateVariables({ refId: 'A', assetRid: '$asset', runRid: '$run', channel: '$channel', dataScopeName: '$scope' }, {});

    expect(mockTemplateSrv.replace).toHaveBeenCalledWith('$asset', {});
    expect(mockTemplateSrv.replace).toHaveBeenCalledWith('$run', {});
    expect(mockTemplateSrv.replace).toHaveBeenCalledWith('$channel', {});
    expect(mockTemplateSrv.replace).toHaveBeenCalledWith('$scope', {});
  });

  it.each([
    ['$run', { raw: '$run', name: 'run' }],
    ['${myvar}', { raw: '${myvar}', name: 'myvar' }],
  ])('sends the variable behind %s as a template source', (runRid, want) => {
    const result = createDataSource().applyTemplateVariables({ refId: 'A', computeBy: 'run', runRid }, {});
    expect(result.templateSources).toEqual({ runRid: want });
  });

  it('omits template sources for literal RIDs', () => {
    const result = createDataSource().applyTemplateVariables({ refId: 'A', computeBy: 'run', runRid: 'ri.scout.main.run.1' }, {});
    expect(result.templateSources).toBeUndefined();
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

describe('run variable queries', () => {
  let ds: DataSource;
  const run = { rid: 'ri.scout.main.run.r1', title: 'Hot fire', runNumber: 7, startMs: 1700000000000, assetRids: [] };

  beforeEach(() => {
    ds = createDataSource();
    mockBackendSrv.post.mockResolvedValue([run]);
  });

  it.each([
    ['runs()', '', []],
    ['runs(*)', '*', []],
    ['runs($asset)', 'ri.scout.main.asset.a', ['ri.scout.main.asset.a']],
    ['runs($asset)', ['a', 'b'], ['a', 'b']],
    ['runs($asset)', '$asset', []],
  ])('%s with replacement %j sends assetRids %j', async (query, replaced, want) => {
    mockTemplateSrv.replace.mockImplementation((_v: string, _s?: unknown, format?: any) =>
      Array.isArray(replaced) ? format(replaced) : replaced
    );
    await ds.metricFindQuery(query);
    expect(mockBackendSrv.post).toHaveBeenCalledWith(
      expect.stringContaining('/runs'),
      { assetRids: want }
    );
  });

  it('labels run options with title and UTC start, valued by RID, and marks multi-asset runs', async () => {
    const multi = { ...run, rid: 'ri.scout.main.run.r2', title: 'Two stage', assetRids: ['a', 'b'] };
    mockBackendSrv.post.mockResolvedValue([run, multi]);
    expect(await ds.metricFindQuery('runs()')).toEqual([
      { text: 'Hot fire · 2023-11-14 22:13 UTC', value: run.rid },
      { text: 'Two stage · 2023-11-14 22:13 UTC · spans 2 assets', value: multi.rid },
    ]);
  });

  it('runstart returns the start in ms', async () => {
    mockBackendSrv.post.mockResolvedValue(run);
    expect(await ds.metricFindQuery(`runstart(${run.rid})`)).toEqual([{ text: '1700000000000', value: '1700000000000' }]);
    expect(mockBackendSrv.post).toHaveBeenCalledWith(expect.stringMatching(/\/run$/), { runRid: run.rid }, expect.anything());
  });

  it('runend returns the end in ms, or now for a run that has not ended', async () => {
    mockBackendSrv.post.mockResolvedValue({ ...run, endMs: 1700000100000 });
    expect(await ds.metricFindQuery(`runend(${run.rid})`)).toEqual([{ text: '1700000100000', value: '1700000100000' }]);
    mockBackendSrv.post.mockResolvedValue(run);
    expect(await ds.metricFindQuery(`runend(${run.rid})`)).toEqual([{ text: 'now', value: 'now' }]);
  });

  describe('several runs', () => {
    const early = { ...run, rid: 'ri.scout.main.run.early', startMs: 1000, endMs: 2000 };
    const late = { ...run, rid: 'ri.scout.main.run.late', startMs: 3000, endMs: 4000 };
    const open = { ...run, rid: 'ri.scout.main.run.open', startMs: 5000 };
    const byRid: Record<string, unknown> = { [early.rid]: early, [late.rid]: late, [open.rid]: open };

    beforeEach(() => {
      mockBackendSrv.post.mockImplementation(
        async (_url: string, body: { runRid: string }) => byRid[body.runRid] ?? null
      );
    });

    it.each([
      ['runstart', [late.rid, early.rid], '1000'],
      ['runend', [late.rid, early.rid], '4000'],
      ['runend', [early.rid, open.rid], 'now'],
      ['runend', [early.rid, 'ri.scout.main.run.deleted'], '2000'],
    ])('%s($run) over %j is %s', async (fn, rids, want) => {
      mockTemplateSrv.replace.mockImplementation((_v: string, _s?: unknown, format?: any) => format(rids));
      expect(await ds.metricFindQuery(`${fn}($run)`)).toEqual([{ text: want, value: want }]);
    });

    it('spans the runs that loaded and logs the ones that failed', async () => {
      const warn = jest.spyOn(console, 'warn').mockImplementation(() => {});
      mockBackendSrv.post.mockImplementation(async (_url: string, body: { runRid: string }) => {
        if (body.runRid === 'ri.scout.main.run.broken') {
          throw { status: 500 };
        }
        return byRid[body.runRid] ?? null;
      });
      mockTemplateSrv.replace.mockImplementation((_v: string, _s?: unknown, format?: any) =>
        format([early.rid, 'ri.scout.main.run.broken', late.rid])
      );
      expect(await ds.metricFindQuery('runend($run)')).toEqual([{ text: '4000', value: '4000' }]);
      expect(warn).toHaveBeenCalledWith(expect.any(String), ['ri.scout.main.run.broken']);
    });

    it('throws a user-safe error when every run fails to load', async () => {
      mockBackendSrv.post.mockRejectedValue({ status: 500 });
      mockTemplateSrv.replace.mockImplementation((_v: string, _s?: unknown, format?: any) => format([early.rid]));
      await expect(ds.metricFindQuery('runstart($run)')).rejects.toThrow(
        'Unable to load the Nominal run for the variable query.'
      );
    });

    it.each(['$__all', ['$__all']])('returns empty without a request when the run variable is All (%j)', async (v) => {
      mockTemplateSrv.getVariables.mockReturnValue([{ name: 'run', current: { value: v } }]);
      mockTemplateSrv.replace.mockImplementation((_v: string, _s?: unknown, format?: any) =>
        format([early.rid, late.rid])
      );
      expect(await ds.metricFindQuery('runstart(${run})')).toEqual([]);
      expect(mockBackendSrv.post).not.toHaveBeenCalled();
    });
  });

  it('runend with an unresolved variable returns empty without a request', async () => {
    expect(await ds.metricFindQuery('runend($run)')).toEqual([]);
    expect(mockBackendSrv.post).not.toHaveBeenCalled();
  });

  it('throws a user-safe error when the runs request fails', async () => {
    mockBackendSrv.post.mockRejectedValue(new Error('secret backend detail'));
    await expect(ds.metricFindQuery('runs()')).rejects.toThrow('Unable to load Nominal runs for the variable query.');
  });
});
