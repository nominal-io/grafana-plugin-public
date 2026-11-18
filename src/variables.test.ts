import { of, lastValueFrom } from 'rxjs';
import {
  AppEvents,
  DataQueryRequest,
  DataQueryResponse,
  FieldType,
  ScopedVars,
  createDataFrame,
  dateTime,
} from '@grafana/data';
import { getAppEvents, getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { DataSource } from './datasource';
import { NominalVariableQuery, toVariableQuery } from './types';
import { framesToVariableOptions, NominalVariableSupport } from './variables';

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    uid: string;
    constructor(settings: { uid: string }) {
      this.uid = settings.uid;
    }
  },
  getTemplateSrv: jest.fn(),
  getBackendSrv: jest.fn(),
  getAppEvents: jest.fn(),
}));

const mockBackendSrv = { post: jest.fn() };
const mockAppEvents = { publish: jest.fn() };

beforeEach(() => {
  jest.clearAllMocks();
  (getTemplateSrv as jest.Mock).mockReturnValue({ replace: (v: string) => v });
  (getBackendSrv as jest.Mock).mockReturnValue(mockBackendSrv);
  (getAppEvents as jest.Mock).mockReturnValue(mockAppEvents);
});

function makeDataSource(response?: DataQueryResponse) {
  const ds = new DataSource({ uid: 'test-uid', jsonData: {} } as any);
  const query = jest.fn(() => of(response ?? { data: [] }));
  (ds as any).query = query;
  return { ds, query };
}

const range = { from: dateTime(0), to: dateTime(1000), raw: { from: 'now-1h', to: 'now' } };
const scopedVars = { asset: { text: 'a', value: 'ri.asset.1' } };

const sql = (query: string): NominalVariableQuery => ({ refId: 'V', mode: 'sql', query });

const run = (ds: DataSource, target: NominalVariableQuery | string) =>
  lastValueFrom(
    (ds.variables as NominalVariableSupport).query({ targets: [target], range, scopedVars } as unknown as DataQueryRequest<
      NominalVariableQuery
    >)
  );

const column = (name: string, values: unknown[], type = FieldType.string) => ({ name, type, values });

describe('toVariableQuery', () => {
  it.each([
    ['undefined', undefined, { mode: 'catalog', query: '' }],
    ['object without mode', { query: 'assets' }, { refId: 'variable', mode: 'catalog', query: 'assets' }],
    ['object without query', { refId: 'A', mode: 'sql' }, { refId: 'A', mode: 'sql', query: '' }],
  ])('normalizes a %s', (_name, input, expected) => {
    expect(toVariableQuery(input as Partial<NominalVariableQuery> | string | null | undefined)).toMatchObject(
      expected
    );
  });
});

describe('framesToVariableOptions', () => {
  const option = (text: string, value = text) => ({ text, value });

  it.each([
    ['one string column', [column('channel', ['a', 'b'])], [option('a'), option('b')]],
    ['one numeric column', [column('n', [42, 0], FieldType.number)], [option('42'), option('0')]],
    ['__text and __value, ignoring other columns', [
      column('extra', ['x', 'y']),
      column('__value', ['ri.1', 'ri.2']),
      column('__text', ['Rover', 'Lander']),
    ], [option('Rover', 'ri.1'), option('Lander', 'ri.2')]],
    ['__value alone', [column('__value', ['ri.1']), column('b', ['y'])], [option('ri.1')]],
    ['__text alone', [column('b', ['y']), column('__text', ['Rover'])], [option('Rover')]],
    ['null values skipped', [column('c', ['a', null, 'b'])], [option('a'), option('b')]],
    ['null text falls back to the value', [column('__text', [null]), column('__value', ['ri.1'])], [option('ri.1')]],
    ['a time column with sub-millisecond nanos', [
      { ...column('ts', [Date.UTC(2001, 1, 3, 4, 5, 6, 7), Date.UTC(2001, 1, 3, 4, 5, 6)], FieldType.time), nanos: [8, 0] },
    ], [option('2001-02-03 04:05:06.007000008'), option('2001-02-03 04:05:06')]],
  ])('maps %s', (_name, fields, expected) => {
    expect(framesToVariableOptions([createDataFrame({ fields })])).toEqual(expected);
  });

  it('returns no options for no frames', () => {
    expect(framesToVariableOptions([])).toEqual([]);
  });

  it('rejects two unnamed columns', () => {
    const fields = [column('a', ['x']), column('b', ['y'])];
    expect(() => framesToVariableOptions([createDataFrame({ fields })])).toThrow(
      /one column, or a column named __value or __text/
    );
  });
});

describe('NominalVariableSupport.query', () => {
  it('sends SQL to the backend as a table query with the variable scope and range', async () => {
    const { ds, query } = makeDataSource({ data: [createDataFrame({ fields: [column('channel', ['temp'])] })] });

    const response = await run(ds, sql("SELECT channel FROM channels WHERE dataset_rid = '$asset'"));

    expect(query).toHaveBeenCalledWith(
      expect.objectContaining({
        targets: [{ refId: 'V', queryType: 'sql', rawSql: "SELECT channel FROM channels WHERE dataset_rid = '$asset'", format: 'table' }],
        scopedVars: { __searchFilter: { value: '', text: '' }, ...scopedVars },
        range,
      })
    );
    expect(response.data).toEqual([{ text: 'temp', value: 'temp' }]);
    expect(mockAppEvents.publish).not.toHaveBeenCalled();
  });

  it('keeps the partial options and warns when the SQL row limit cut the result', async () => {
    const notice = { severity: 'warning' as const, text: 'Results have been limited to 1 rows because the SQL row limit was reached' };
    const { ds } = makeDataSource({ data: [createDataFrame({ fields: [column('channel', ['a'])], meta: { notices: [notice] } })] });

    const response = await run(ds, sql('SELECT channel FROM channels'));

    expect(response.data).toEqual([{ text: 'a', value: 'a' }]);
    expect(mockAppEvents.publish).toHaveBeenCalledWith(
      expect.objectContaining({ type: AppEvents.alertWarning.name, payload: expect.arrayContaining([expect.stringContaining(notice.text)]) })
    );
  });

  it('passes a backend error through for Grafana to show', async () => {
    const errors = [{ message: 'table not found' }];
    const { ds } = makeDataSource({ data: [createDataFrame({ fields: [] })], errors });
    expect(await run(ds, sql('SELECT x FROM nope'))).toMatchObject({ data: [], errors });
  });

  it('returns no options for blank SQL without calling the backend', async () => {
    const { ds, query } = makeDataSource();
    const response = await run(ds, sql('   '));
    expect(response.data).toEqual([]);
    expect(query).not.toHaveBeenCalled();
  });

  it('runs a legacy catalog string through the catalog endpoints with the request scopedVars', async () => {
    mockBackendSrv.post.mockResolvedValue([{ text: 'default', value: 'default' }]);
    (getTemplateSrv as jest.Mock).mockReturnValue({
      replace: (v: string, vars?: { asset?: { value?: string } }) => v.replace('$asset', vars?.asset?.value ?? '$asset'),
    });
    const { ds } = makeDataSource();

    const response = await run(ds, 'datascopes($asset)');

    expect(mockBackendSrv.post).toHaveBeenCalledWith(expect.stringContaining('datascopes'), {
      assetRid: 'ri.asset.1',
    });
    expect(response.data).toEqual([{ text: 'default', value: 'default' }]);
  });

  describe('dynamic dropdown search', () => {
    const term = (value: string) => ({ value, text: value });
    const cases: Array<[string, ScopedVars, string]> = [
      ['an unopened dropdown', {}, ''],
      ['legacy typed text', { searchFilter: term('$team') }, '$team'],
      ['Scenes typed text', { __searchFilter: term('motor') }, 'motor'],
      ['a cleared Scenes filter with a stale legacy filter', { __searchFilter: term(''), searchFilter: term('motor') }, ''],
    ];
    const runWithSearch = (ds: DataSource, target: NominalVariableQuery | string, searchVars: ScopedVars) =>
      lastValueFrom(
        (ds.variables as NominalVariableSupport).query({
          targets: [target],
          range,
          scopedVars: searchVars,
        } as unknown as DataQueryRequest<NominalVariableQuery>)
      );

    beforeEach(() => {
      (getTemplateSrv as jest.Mock).mockReturnValue({
        replace: (v: string, vars?: Record<string, { value?: unknown }>) =>
          v.replace('$__searchFilter', () => String(vars?.__searchFilter?.value ?? '$__searchFilter')),
      });
    });

    it.each(cases)('sends %s as the catalog search text', async (_, searchVars, expected) => {
      mockBackendSrv.post.mockResolvedValue([]);
      const { ds } = makeDataSource();

      await runWithSearch(ds, 'assets($__searchFilter)', searchVars);

      expect(mockBackendSrv.post).toHaveBeenCalledWith(
        expect.stringContaining('/assets'),
        expect.objectContaining({ searchText: expected }),
        expect.anything()
      );
    });

    describe('overlapping searches', () => {
      const sceneVars = (key: string, search: string) =>
        ({ __sceneObject: { value: { valueOf: () => ({ state: { key } }) } }, __searchFilter: term(search) }) as ScopedVars;

      // Grafana's backend service cancels an in-flight request when another starts with the same request ID.
      beforeEach(() => {
        const inFlight = new Map<string, (reason: unknown) => void>();
        mockBackendSrv.post.mockImplementation(
          (_url: string, _data: unknown, options?: { requestId?: string }) =>
            new Promise((resolve, reject) => {
              if (options?.requestId) {
                inFlight.get(options.requestId)?.({ cancelled: true });
                inFlight.set(options.requestId, reject);
              }
              setTimeout(() => resolve([]), 10);
            })
        );
      });

      it.each(['assets($__searchFilter)', 'channels(ri.asset.1, , $__searchFilter)'])(
        'cancels the outdated %s search of a Scenes variable and marks it cancelled for Scenes',
        async (query) => {
          const { ds } = makeDataSource();
          const outdated = runWithSearch(ds, query, sceneVars('v1', 'a_b'));
          const current = runWithSearch(ds, query, sceneVars('v1', 'a_bus'));

          await expect(outdated).rejects.toMatchObject({ cancelled: true });
          await expect(current).resolves.toEqual({ data: [] });
        }
      );

      it.each([
        ['the same variable on another selected data source', 'v1'],
        ['another variable on the same dashboard', 'v2'],
      ])('does not cancel %s', async (_, otherKey) => {
        const { ds } = makeDataSource();
        const other = otherKey === 'v1' ? new DataSource({ uid: 'other-uid', jsonData: {} } as any) : ds;
        const first = runWithSearch(ds, 'channels(ri.asset.1, , $__searchFilter)', sceneVars('v1', 'rpm'));
        const second = runWithSearch(other, 'channels(ri.asset.1, , $__searchFilter)', sceneVars(otherKey, 'rpm'));

        await expect(first).resolves.toEqual({ data: [] });
        await expect(second).resolves.toEqual({ data: [] });
      });
    });

    it.each(cases)('passes %s to SQL as __searchFilter', async (_, searchVars, expected) => {
      const { ds, query } = makeDataSource();

      await runWithSearch(ds, sql("SELECT title FROM assets WHERE title LIKE '%$__searchFilter%'"), searchVars);

      expect(query).toHaveBeenCalledWith(
        expect.objectContaining({
          scopedVars: expect.objectContaining({ __searchFilter: expect.objectContaining({ value: expected }) }),
        })
      );
    });
  });
});
