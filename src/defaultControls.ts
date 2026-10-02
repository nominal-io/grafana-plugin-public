const PLUGIN_ID = 'nominal-nominalds-datasource';
// Grafana prefixes default variable names with the plugin id, non-word characters replaced.
const SHIPPED_VARIABLE_PREFIX = `${PLUGIN_ID.replace(/\W/g, '_')}_`;
const ref = (short: 'run_start' | 'run_end') => `\${${SHIPPED_VARIABLE_PREFIX}${short}}`;

interface DashboardVariable {
  name: string;
  type?: string;
  query?: unknown;
  datasource?: { type?: string; uid?: string } | null;
}

// The dashboard's run variable: the first Nominal `runs(...)` query variable under any name,
// else a variable named `run` unless it belongs to another plugin.
export function findRunVariable(variables: DashboardVariable[], uid: string): DashboardVariable | undefined {
  const isRunsQuery = (v: DashboardVariable) => {
    const q = typeof v.query === 'object' && v.query !== null ? (v.query as { query?: unknown }).query : v.query;
    return (
      v.type === 'query' &&
      (v.datasource?.type === PLUGIN_ID || v.datasource?.uid === uid) &&
      typeof q === 'string' &&
      /^\s*runs\(/i.test(q)
    );
  };
  const isOtherPlugin = (v: DashboardVariable) => {
    const type = v.datasource?.type;
    return !!type && type !== PLUGIN_ID && !type.startsWith('$');
  };
  return variables.find(isRunsQuery) ?? variables.find((v) => v.name === 'run' && !isOtherPlugin(v));
}

// The `as const` literals match the Grafana 13 schema unions that
// getDefaultVariables and getDefaultLinks return; @grafana/data 12.1 lacks those types.
export function buildDefaultVariables(runVariable: DashboardVariable, uid: string) {
  const dsUid = runVariable.datasource?.uid;
  const datasourceName = dsUid && !dsUid.startsWith('$') ? dsUid : uid;
  const variable = (name: string, definition: string) => ({
    kind: 'QueryVariable' as const,
    spec: {
      name,
      hide: 'hideVariable' as const,
      skipUrlSync: false,
      refresh: 'onDashboardLoad' as const,
      sort: 'disabled' as const,
      regex: '',
      multi: false,
      includeAll: false,
      allowCustomValue: false,
      definition,
      current: { text: '', value: '' },
      options: [],
      query: {
        kind: 'DataQuery' as const,
        group: PLUGIN_ID,
        version: 'v0' as const,
        datasource: { name: datasourceName },
        spec: { __legacyStringValue: definition },
      },
    },
  });

  return [
    variable('run_start', `runstart(\${${runVariable.name}})`),
    variable('run_end', `runend(\${${runVariable.name}})`),
  ];
}

export function buildDefaultLinks() {
  return [
    {
      title: 'Snap to run',
      type: 'link' as const,
      url: `\${__url.path}?from=${ref('run_start')}&to=${ref('run_end')}`,
      tooltip: "Set the time range to the selected run's start and end",
      icon: 'external link' as const,
      tags: [],
      asDropdown: false,
      targetBlank: false,
      includeVars: true,
      keepTime: false,
    },
  ];
}
