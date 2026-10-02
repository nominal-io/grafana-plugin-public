import { dashboardVariableOptions } from './variableOptions';

jest.mock('@grafana/runtime', () => ({
  getTemplateSrv: () => ({
    getVariables: () => [
      { name: 'run', label: 'Test run' },
      { name: 'asset' },
      { name: 'x', label: 'Hot Fire' },
    ],
  }),
}));

describe('dashboardVariableOptions', () => {
  it.each([
    ['$', ['$run', '$asset', '$x']],
    ['$RU', ['$run']],
    ['$fire', ['$x']],
    ['$nothing', []],
    ['run', []],
  ])('%s', (text, values) => {
    expect(dashboardVariableOptions(text).map((o) => o.value)).toEqual(values);
  });
});
