import { buildDefaultLinks, buildDefaultVariables, findRunVariable } from './defaultControls';

const PLUGIN = 'nominal-nominalds-datasource';
const runsVar = (name: string, query: unknown = 'runs()', datasource: object | null = { type: PLUGIN, uid: 'u1' }) => ({
  name,
  type: 'query',
  query,
  datasource,
});

describe('findRunVariable', () => {
  it.each([
    ['a runs( query under any name', [runsVar('flight')], 'flight'],
    ['the first of several', [runsVar('a'), runsVar('b')], 'a'],
    ['an object query', [runsVar('flight', { query: 'runs(${asset})' })], 'flight'],
    ['a case-insensitive, padded query', [runsVar('flight', '  RUNS(x)')], 'flight'],
    ['a datasource matched by uid only', [runsVar('flight', 'runs()', { uid: 'test-uid' })], 'flight'],
    ['the runs( variable over one named run', [{ name: 'run' }, runsVar('flight')], 'flight'],
    ['a variable named run when no runs( query exists', [{ name: 'asset' }, { name: 'run' }], 'run'],
    ['a variable named run on a templated datasource', [{ name: 'run', datasource: { type: '${ds}' } }], 'run'],
  ])('picks %s', (_name, variables, want) => {
    expect(findRunVariable(variables, 'test-uid')?.name).toBe(want);
  });

  it.each([
    ['no variables', []],
    ['another plugin', [runsVar('flight', 'runs()', { type: 'prometheus', uid: 'p' })]],
    ['a variable named run on another plugin', [{ name: 'run', datasource: { type: 'prometheus' } }]],
    ['a non-runs query', [runsVar('flight', 'assets()')]],
    ['a non-query variable', [{ name: 'flight', type: 'custom', query: 'runs()', datasource: { type: PLUGIN } }]],
  ])('finds nothing for %s', (_name, variables) => {
    expect(findRunVariable(variables, 'test-uid')).toBeUndefined();
  });
});

describe('default controls', () => {
  it.each([
    ['the detected datasource uid', { name: 'flight', datasource: { uid: 'u1' } }, 'u1'],
    ['this datasource when the uid is a variable', { name: 'flight', datasource: { uid: '${ds}' } }, 'test-uid'],
    ['this datasource when there is no datasource', { name: 'flight', datasource: null }, 'test-uid'],
  ])('queries through %s', (_name, runVariable, want) => {
    expect(buildDefaultVariables(runVariable, 'test-uid').every((v) => v.spec.query.datasource.name === want)).toBe(true);
  });

  it('hides two bound variables that follow the detected run variable', () => {
    const vars = buildDefaultVariables({ name: 'flight' }, 'test-uid');
    expect(vars.map((v) => [v.spec.name, v.spec.hide, v.spec.query.spec.__legacyStringValue])).toEqual([
      ['run_start', 'hideVariable', 'runstart(${flight})'],
      ['run_end', 'hideVariable', 'runend(${flight})'],
    ]);
  });

  it('links to the run bounds on the current dashboard', () => {
    expect(buildDefaultLinks()).toEqual([
      expect.objectContaining({
        title: 'Snap to run',
        url: '${__url.path}?from=${nominal_nominalds_datasource_run_start}&to=${nominal_nominalds_datasource_run_end}',
        includeVars: true,
        keepTime: false,
      }),
    ]);
    expect(buildDefaultLinks()[0]).not.toHaveProperty('placement');
  });
});
