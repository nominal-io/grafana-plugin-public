import { getTemplateSrv } from '@grafana/runtime';
import type { PickerOption } from './queryBuilderTypes';

/** Lists dashboard variables whose name or label matches the text, ignoring a leading `$` or `${`. */
export function dashboardVariableOptions(searchText: string): PickerOption[] {
  const needle = searchText.replace(/^\$\{?|\}$/g, '').toLowerCase();
  return getTemplateSrv()
    .getVariables()
    .filter((v) => v.name.toLowerCase().includes(needle) || (v.label ?? '').toLowerCase().includes(needle))
    .map((v) => ({
      value: `$${v.name}`,
      description: v.label ? `Dashboard variable: ${v.label}` : 'Dashboard variable',
    }));
}
