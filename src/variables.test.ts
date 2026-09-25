import { FieldType, createDataFrame } from '@grafana/data';
import { NominalVariableQuery, toVariableQuery } from './types';
import { framesToVariableOptions } from './variables';

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
