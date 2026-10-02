import { getTemplateSrv } from '@grafana/runtime';
import type { PickerOption } from './queryBuilderTypes';

/** Lists dashboard variables whose name or label matches the text after a leading `$`. */
export function dashboardVariableOptions(searchText: string): PickerOption[] {
  if (!searchText.startsWith('$')) {
    return [];
  }
  const needle = searchText.slice(1).toLowerCase();
  return getTemplateSrv()
    .getVariables()
    .filter((v) => v.name.toLowerCase().includes(needle) || (v.label ?? '').toLowerCase().includes(needle))
    .map((v) => ({ label: v.label || v.name, value: `$${v.name}`, description: 'Dashboard variable' }));
}
