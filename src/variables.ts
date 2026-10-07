import { DataFrame, Field, FieldType, MetricFindValue } from '@grafana/data';

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
