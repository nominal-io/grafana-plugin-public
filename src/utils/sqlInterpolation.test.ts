import { sqlInterpolateVariable } from './sqlInterpolation';

describe('sqlInterpolateVariable', () => {
  it('leaves a single value raw', () => {
    expect(sqlInterpolateVariable('engine_rpm', {})).toBe('engine_rpm');
  });

  it('escapes quotes in single values used inside SQL string literals', () => {
    expect(sqlInterpolateVariable("pilot's sensor", {})).toBe("pilot''s sensor");
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
