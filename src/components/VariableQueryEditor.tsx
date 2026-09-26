import React, { useEffect, useState } from 'react';
import { QueryEditorProps } from '@grafana/data';
import { Button, InlineField, Input, RadioButtonGroup, Stack, Text } from '@grafana/ui';
import type { DataSource } from '../datasource';
import {
  NominalDataSourceOptions,
  NominalQuery,
  NominalVariableQuery,
  VARIABLE_SQL_EXAMPLE,
  VariableQueryMode,
  toVariableQuery,
} from '../types';
import { SqlCodeEditor } from './SqlCodeEditor';

type Props = QueryEditorProps<DataSource, NominalQuery, NominalDataSourceOptions, NominalVariableQuery>;

const modes = [
  { label: 'Catalog', value: 'catalog' as const },
  { label: 'SQL', value: 'sql' as const },
];

export function VariableQueryEditor({ query, onChange }: Props) {
  const current = toVariableQuery(query as NominalVariableQuery | string);
  const [draft, setDraft] = useState(current.query);

  useEffect(() => {
    setDraft(current.query);
  }, [current.query]);

  const commit = (text: string) => {
    if (text !== current.query) {
      onChange({ ...current, query: text });
    }
  };

  const setMode = (mode: VariableQueryMode) => {
    if (mode !== current.mode) {
      onChange({ ...current, mode, query: '' });
    }
  };

  return (
    <Stack direction="column" gap={1}>
      <InlineField label="Mode" labelWidth={10}>
        <RadioButtonGroup<VariableQueryMode> value={current.mode} options={modes} onChange={setMode} />
      </InlineField>
      {current.mode === 'sql' ? (
        <>
          <Text variant="bodySmall" color="secondary">
            Return one column, or columns named __text and __value. Rows with a null value are skipped.
          </Text>
          {!current.query && !draft && (
            <Button variant="secondary" onClick={() => onChange({ ...current, query: VARIABLE_SQL_EXAMPLE })}>
              Use variable example
            </Button>
          )}
          <SqlCodeEditor value={draft} onChange={setDraft} onCommit={commit} />
        </>
      ) : (
        <InlineField label="Query" labelWidth={10} grow tooltip="assets, assets(<search>), datascopes(${asset}), channels(${asset}, ${datascope})">
          <Input
            aria-label="Catalog query"
            placeholder="assets"
            value={draft}
            onChange={(e) => setDraft(e.currentTarget.value)}
            onBlur={(e) => commit(e.currentTarget.value)}
          />
        </InlineField>
      )}
    </Stack>
  );
}
