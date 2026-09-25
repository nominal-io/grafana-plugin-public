import { of, lastValueFrom } from 'rxjs';
import {
  AppEvents,
  DataQueryRequest,
  DataQueryResponse,
  FieldType,
  QueryResultMetaNotice,
  createDataFrame,
  dateTime,
} from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { DataSource } from './datasource';
import { NominalVariableQuery, toVariableQuery } from './types';
import { framesToVariableOptions, NominalVariableSupport } from './variables';

const publish = jest.fn();

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getTemplateSrv: jest.fn(),
  getBackendSrv: jest.fn(),
  getAppEvents: jest.fn(() => ({ publish })),
}));

const mockBackendSrv = { post: jest.fn() };

beforeEach(() => {
  jest.clearAllMocks();
  (getTemplateSrv as jest.Mock).mockReturnValue({ replace: (v: string) => v });
  (getBackendSrv as jest.Mock).mockReturnValue(mockBackendSrv);
});

function makeDataSource(response?: DataQueryResponse) {
  const ds = new DataSource({ uid: 'test-uid', jsonData: {} } as any);
  const query = jest.fn(() => of(response ?? { data: [] }));
  (ds as any).query = query;
  return { ds, query };
}

const range = { from: dateTime(0), to: dateTime(1000), raw: { from: 'now-1h', to: 'now' } };
const scopedVars = { asset: { text: 'a', value: 'ri.asset.1' } };

function request(target: NominalVariableQuery | string): DataQueryRequest<NominalVariableQuery> {
  return { targets: [target as NominalVariableQuery], range, scopedVars } as unknown as DataQueryRequest<
    NominalVariableQuery
  >;
}

const sql = (query: string): NominalVariableQuery => ({ refId: 'V', mode: 'sql', query });

const run = (ds: DataSource, target: NominalVariableQuery | string) =>
  lastValueFrom((ds.variables as NominalVariableSupport).query(request(target)));

describe('toVariableQuery', () => {
  it.each([
    ['legacy string', 'datascopes(${asset})', { mode: 'catalog', query: 'datascopes(${asset})' }],
    ['empty string', '', { mode: 'catalog', query: '' }],
    ['undefined', undefined, { mode: 'catalog', query: '' }],
    ['null', null, { mode: 'catalog', query: '' }],
    ['object without mode', { query: 'assets' }, { refId: 'variable', mode: 'catalog', query: 'assets' }],
    ['object without query', { refId: 'A', mode: 'sql' }, { refId: 'A', mode: 'sql', query: '' }],
  ])('normalizes a %s', (_name, input, expected) => {
    expect(toVariableQuery(input as Partial<NominalVariableQuery> | string | null | undefined)).toMatchObject(
      expected
    );
  });

  it('keeps an object query as saved', () => {
    const saved: NominalVariableQuery = { refId: 'V', mode: 'sql', query: 'SELECT 1' };
    expect(toVariableQuery(saved)).toEqual(saved);
  });
});

describe('framesToVariableOptions', () => {
  it.each([
    ['one string column', [{ name: 'channel', type: FieldType.string, values: ['a', 'b'] }], [
      { text: 'a', value: 'a' },
      { text: 'b', value: 'b' },
    ]],
    ['one numeric column', [{ name: 'n', type: FieldType.number, values: [42, 0] }], [
      { text: '42', value: '42' },
      { text: '0', value: '0' },
    ]],
    ['__text and __value, ignoring other columns', [
      { name: 'extra', type: FieldType.string, values: ['x', 'y'] },
      { name: '__value', type: FieldType.string, values: ['ri.1', 'ri.2'] },
      { name: '__text', type: FieldType.string, values: ['Rover', 'Lander'] },
    ], [
      { text: 'Rover', value: 'ri.1' },
      { text: 'Lander', value: 'ri.2' },
    ]],
    ['null values skipped', [{ name: 'c', type: FieldType.string, values: ['a', null, 'b'] }], [
      { text: 'a', value: 'a' },
      { text: 'b', value: 'b' },
    ]],
    ['null text falls back to the value', [
      { name: '__text', type: FieldType.string, values: [null] },
      { name: '__value', type: FieldType.string, values: ['ri.1'] },
    ], [{ text: 'ri.1', value: 'ri.1' }]],
    ['zero rows', [{ name: 'c', type: FieldType.string, values: [] }], []],
  ])('maps %s', (_name, fields, expected) => {
    expect(framesToVariableOptions([createDataFrame({ fields })])).toEqual(expected);
  });

  it('returns no options for no frames', () => {
    expect(framesToVariableOptions([])).toEqual([]);
  });

  it.each([
    ['two unnamed columns', [
      { name: 'a', type: FieldType.string, values: ['x'] },
      { name: 'b', type: FieldType.string, values: ['y'] },
    ], /one column, or columns named __text and __value/],
    ['only __value alongside another column', [
      { name: '__value', type: FieldType.string, values: ['x'] },
      { name: 'b', type: FieldType.string, values: ['y'] },
    ], /one column, or columns named __text and __value/],
    ['a timestamp column', [{ name: 'ts', type: FieldType.time, values: [1] }], /"ts" must be text or numeric/],
  ])('rejects %s', (_name, fields, message) => {
    expect(() => framesToVariableOptions([createDataFrame({ fields })])).toThrow(message);
  });
});

describe('NominalVariableSupport.query', () => {
  it('sends SQL to the backend as a table query with the variable scope and range', async () => {
    const frame = createDataFrame({ fields: [{ name: 'channel', type: FieldType.string, values: ['temp'] }] });
    const { ds, query } = makeDataSource({ data: [frame] });

    const response = await run(ds, sql("SELECT channel FROM channels WHERE dataset_rid = '$asset'"));

    const sent = (query.mock.calls[0] as any[])[0] as DataQueryRequest;
    expect(sent.targets).toEqual([
      { refId: 'V', queryType: 'sql', rawSql: "SELECT channel FROM channels WHERE dataset_rid = '$asset'", format: 'table' },
    ]);
    expect(sent.scopedVars).toEqual(scopedVars);
    expect(sent.range).toBe(range);
    expect(response.data).toEqual([{ text: 'temp', value: 'temp' }]);
  });

  it.each([
    ['errors', { data: [], errors: [{ message: 'table not found' }] }],
    ['error', { data: [], error: { message: 'table not found' } }],
  ])('fails when the backend reports %s', async (_name, backendResponse) => {
    const { ds } = makeDataSource(backendResponse as DataQueryResponse);
    await expect(run(ds, sql('SELECT x FROM nope'))).rejects.toThrow('table not found');
  });

  it.each(['', '   '])('returns no options for blank SQL %j without calling the backend', async (blank) => {
    const { ds, query } = makeDataSource();
    const response = await run(ds, sql(blank));
    expect(response.data).toEqual([]);
    expect(query).not.toHaveBeenCalled();
  });

  const runWithNotices = (ds: DataSource) => run(ds, sql('SELECT channel FROM channels'));
  const frameWithNotices = (notices?: QueryResultMetaNotice[]) =>
    createDataFrame({
      fields: [{ name: 'channel', type: FieldType.string, values: ['temp'] }],
      meta: notices ? { notices } : undefined,
    });

  it('publishes a warning toast when the row limit was reached', async () => {
    const text = 'Results have been limited to 1000 rows because the SQL row limit was reached';
    const { ds } = makeDataSource({ data: [frameWithNotices([{ severity: 'warning', text }])] });

    await runWithNotices(ds);

    expect(publish).toHaveBeenCalledTimes(1);
    const [{ type, payload }] = publish.mock.calls[0];
    expect(type).toBe(AppEvents.alertWarning.name);
    expect(payload[0]).toContain(text);
  });

  it.each([
    ['an info notice', [{ severity: 'info', text: 'unrelated' }] as QueryResultMetaNotice[]],
    ['no notices', undefined],
  ])('does not publish a toast for %s', async (_name, notices) => {
    const { ds } = makeDataSource({ data: [frameWithNotices(notices)] });
    await runWithNotices(ds);
    expect(publish).not.toHaveBeenCalled();
  });

  it('runs a legacy catalog string through the catalog endpoints with the request scopedVars', async () => {
    mockBackendSrv.post.mockResolvedValue([{ text: 'default', value: 'default' }]);
    (getTemplateSrv as jest.Mock).mockReturnValue({
      replace: (v: string, vars?: { asset?: { value?: string } }) => v.replace('$asset', vars?.asset?.value ?? '$asset'),
    });
    const { ds, query } = makeDataSource();

    const response = await run(ds, 'datascopes($asset)');

    expect(mockBackendSrv.post).toHaveBeenCalledWith(expect.stringContaining('datascopes'), {
      assetRid: 'ri.asset.1',
    });
    expect(response.data).toEqual([{ text: 'default', value: 'default' }]);
    expect(query).not.toHaveBeenCalled();
  });
});
