import { dashboardVariableOptions } from './variableOptions';

jest.mock('@grafana/runtime', () => ({
  getTemplateSrv: () => ({
    getVariables: () => [{ name: 'run', label: 'Test run' }, { name: 'asset' }, { name: 'x', label: 'Hot Fire' }],
  }),
}));

describe('dashboardVariableOptions', () => {
  it.each([
    ['$', ['$run', '$asset', '$x']],
    ['$RU', ['$run']],
    ['${as', ['$asset']],
    ['${asset}', ['$asset']],
    ['$fire', ['$x']],
    ['$nothing', []],
    ['', ['$run', '$asset', '$x']],
    ['fire', ['$x']],
  ])('%s', (text, values) => {
    expect(dashboardVariableOptions(text).map((o) => o.value)).toEqual(values);
  });

  it('shows the variable name and puts its label in the description', () => {
    expect(dashboardVariableOptions('$')).toEqual([
      { value: '$run', description: 'Dashboard variable: Test run' },
      { value: '$asset', description: 'Dashboard variable' },
      { value: '$x', description: 'Dashboard variable: Hot Fire' },
    ]);
  });
});
