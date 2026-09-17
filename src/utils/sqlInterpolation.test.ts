import { isSqlVariableQuery, sqlInterpolateVariable } from './sqlInterpolation';

describe('sqlInterpolateVariable', () => {
  it('leaves a single value raw', () => {
    expect(sqlInterpolateVariable('engine_rpm', {})).toBe('engine_rpm');
  });

  it('quotes a single multi-value variable', () => {
    expect(sqlInterpolateVariable('a', { multi: true })).toBe("'a'");
  });

  it('quotes arrays and escapes single quotes', () => {
    expect(sqlInterpolateVariable(["a'b", 'c'], { multi: true })).toBe("'a''b','c'");
  });

  it('passes numbers through', () => {
    expect(sqlInterpolateVariable(5, {})).toBe('5');
  });
});

describe('isSqlVariableQuery', () => {
  it('detects SELECT and WITH queries', () => {
    expect(isSqlVariableQuery('  select dataset_rid from datasets')).toBe(true);
    expect(isSqlVariableQuery('WITH x AS (SELECT 1) SELECT * FROM x')).toBe(true);
    expect(isSqlVariableQuery('assets(foo)')).toBe(false);
  });
});
