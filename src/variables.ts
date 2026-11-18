import { from, map, Observable, of } from 'rxjs';
import {
  AppEvents,
  CustomVariableSupport,
  DataFrame,
  DataQueryRequest,
  DataQueryResponse,
  Field,
  FieldType,
  MetricFindValue,
  ScopedVars,
} from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
import type { DataSource } from './datasource';
import { VariableQueryEditor } from './components/VariableQueryEditor';
import { NominalVariableQuery, QUERY_TYPE_SQL, toVariableQuery } from './types';

// UTC with full nanosecond precision, so the value matches its row in a filter such as ts = TIMESTAMP '$t'.
function sqlTimestamp(ms: number, nanos = 0): string {
  const iso = new Date(ms).toISOString();
  const fraction = (iso.slice(20, 23) + String(nanos).padStart(6, '0')).replace(/0+$/, '');
  return iso.slice(0, 19).replace('T', ' ') + (fraction ? '.' + fraction : '');
}

function cellText(field: Field, row: number): string | undefined {
  const value = field.values[row];
  if (value == null) {
    return undefined;
  }
  return field.type === FieldType.time ? sqlTimestamp(value, field.nanos?.[row]) : String(value);
}

export function framesToVariableOptions(frames: DataFrame[]): MetricFindValue[] {
  const frame = frames[0];
  if (!frame) {
    return [];
  }
  const byName = (name: string) => frame.fields.find((field) => field.name === name);
  // Either named column alone serves as both text and value.
  const valueField =
    byName('__value') ?? byName('__text') ?? (frame.fields.length === 1 ? frame.fields[0] : undefined);
  if (!valueField) {
    throw new Error('Variable SQL must return one column, or a column named __value or __text.');
  }
  const textField = byName('__text') ?? valueField;

  const options: MetricFindValue[] = [];
  for (let row = 0; row < frame.length; row++) {
    const value = cellText(valueField, row);
    if (value === undefined) {
      continue;
    }
    options.push({ text: cellText(textField, row) ?? value, value });
  }
  return options;
}

function runSql(ds: DataSource, request: DataQueryRequest<NominalVariableQuery>, target: NominalVariableQuery) {
  // Skip the backend until the variable has SQL; blank SQL fails its validation.
  const rawSql = target.query;
  if (!rawSql.trim()) {
    return of({ data: [] });
  }
  return ds
    .query({
      ...request,
      targets: [{ refId: target.refId, queryType: QUERY_TYPE_SQL, rawSql, format: 'table' }],
    })
    .pipe(
      map((response) => {
        // A failed query still returns an empty frame, which must not hide the backend error behind a column error.
        if (response.errors?.length) {
          return { ...response, data: [] };
        }
        // Grafana drops warnings from variable queries, so a row-limit cut keeps the partial list and shows a toast.
        const frame: DataFrame | undefined = response.data[0];
        const warning = frame?.meta?.notices?.find((notice) => notice.severity === 'warning');
        if (warning) {
          getAppEvents().publish({
            type: AppEvents.alertWarning.name,
            payload: ['SQL variable options are incomplete', `${warning.text}. Narrow the query, for example with DISTINCT.`],
          });
        }
        return { ...response, data: framesToVariableOptions(response.data) };
      })
    );
}

// Scenes lets every dropdown search run to completion, so a slow outdated search can overwrite newer options or
// raise an error toast after them. Grafana cancels an in-flight request when another starts with the same request
// ID, so one ID per variable drops the outdated search. The legacy picker sends nothing that identifies the variable.
function catalogRequestId(scopedVars?: ScopedVars): string | undefined {
  const key = scopedVars?.__sceneObject?.value?.valueOf()?.state?.key;
  return key ? `nominal-variable-${key}` : undefined;
}

export class NominalVariableSupport extends CustomVariableSupport<DataSource, NominalVariableQuery> {
  editor = VariableQueryEditor;

  constructor(private readonly datasource: DataSource) {
    super();
  }

  query(originalRequest: DataQueryRequest<NominalVariableQuery>): Observable<DataQueryResponse> {
    const { scopedVars } = originalRequest;
    const request = {
      ...originalRequest,
      scopedVars: {
        ...scopedVars,
        // Grafana's legacy dashboard picker sends searchFilter. Scenes sends __searchFilter.
        // Remove the fallback and the editor's target marker when the minimum Grafana version is 13.
        __searchFilter: scopedVars?.__searchFilter ?? scopedVars?.searchFilter ?? { value: '', text: '' },
      },
    };
    const target = toVariableQuery(request.targets[0]);
    if (target.mode === 'sql') {
      return runSql(this.datasource, request, target);
    }
    const requestId = catalogRequestId(scopedVars);
    return from(this.datasource.metricFindQuery(target.query, { scopedVars: request.scopedVars, requestId })).pipe(
      map((data) => ({ data }))
    );
  }
}
