import { FieldType, createDataFrame } from '@grafana/data';
import { NominalVariableQuery, toVariableQuery } from './types';
import { framesToVariableOptions } from './variables';

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
