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
  QueryResultMetaNotice,
} from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
import type { DataSource } from './datasource';
import { VariableQueryEditor } from './components/VariableQueryEditor';
import { NominalVariableQuery, QUERY_TYPE_SQL, toVariableQuery } from './types';

const COLUMN_CONTRACT = 'Variable SQL must return one column, or columns named __text and __value.';

function optionField(field: Field): Field {
  if (field.type !== FieldType.string && field.type !== FieldType.number) {
    throw new Error(`Variable column "${field.name}" must be text or numeric.`);
  }
  return field;
}

export function framesToVariableOptions(frames: DataFrame[]): MetricFindValue[] {
  const frame = frames[0];
  if (!frame) {
    return [];
  }
  const byName = (name: string) => frame.fields.find((field) => field.name === name);
  const textField = byName('__text');
  const valueField = byName('__value');

  let text: Field;
  let value: Field;
  if (textField && valueField) {
    [text, value] = [optionField(textField), optionField(valueField)];
  } else if (frame.fields.length === 1) {
    text = value = optionField(frame.fields[0]);
  } else {
    throw new Error(COLUMN_CONTRACT);
  }

  const options: MetricFindValue[] = [];
  for (let row = 0; row < frame.length; row++) {
    const rowValue = value.values[row];
    if (rowValue == null) {
      continue;
    }
    options.push({ text: String(text.values[row] ?? rowValue), value: String(rowValue) });
  }
  return options;
}

function runSql(ds: DataSource, request: DataQueryRequest<NominalVariableQuery>, target: NominalVariableQuery) {
  // The backend rejects blank SQL with "SQL query is empty", and switching mode to SQL
  // clears the text, so a fresh SQL variable should show an empty list, not an error.
  if (!target.query.trim()) {
    return of({ data: [] });
  }
  return ds
    .query({
      ...request,
      targets: [{ refId: target.refId, queryType: QUERY_TYPE_SQL, rawSql: target.query, format: 'table' }],
    })
    .pipe(
      map((response) => {
        // eslint-disable-next-line @typescript-eslint/no-deprecated -- a non-200 HTTP failure sets only response.error
        const error = response.errors?.[0] ?? response.error;
        if (error) {
          throw new Error(error.message ?? 'The variable SQL query failed.');
        }
        const limited = response.data[0]?.meta?.notices?.find(
          (notice: QueryResultMetaNotice) => notice.severity === 'warning'
        );
        if (limited) {
          getAppEvents().publish({
            type: AppEvents.alertWarning.name,
            payload: [`Variable options: ${limited.text}. Add LIMIT to the variable query.`],
          });
        }
        return { data: framesToVariableOptions(response.data) };
      })
    );
}

export class NominalVariableSupport extends CustomVariableSupport<DataSource, NominalVariableQuery> {
  editor = VariableQueryEditor;

  constructor(private readonly datasource: DataSource) {
    super();
  }

  query(request: DataQueryRequest<NominalVariableQuery>): Observable<DataQueryResponse> {
    const target = toVariableQuery(request.targets[0]);
    if (target.mode === 'sql') {
      return runSql(this.datasource, request, target);
    }
    return from(this.datasource.metricFindQuery(target.query, { scopedVars: request.scopedVars })).pipe(
      map((data) => ({ data }))
    );
  }
}
