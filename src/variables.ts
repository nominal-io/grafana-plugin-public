import { DataFrame, Field, FieldType, MetricFindValue } from '@grafana/data';

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
