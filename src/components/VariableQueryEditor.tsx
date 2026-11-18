import React, { useEffect, useState } from 'react';
import { containsSearchFilter, QueryEditorProps } from '@grafana/data';
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
  const isSql = current.mode === 'sql';
  const saved = current.query;
  const [draft, setDraft] = useState(saved);
  // Grafana scans the whole saved query for variable references, so the other mode's text stays out of it.
  const [otherModeText, setOtherModeText] = useState('');

  useEffect(() => {
    setDraft(saved);
  }, [saved]);

  const saveQuery = (mode: VariableQueryMode, text: string, runUnchanged = false) => {
    const target = containsSearchFilter(text) ? '$__searchFilter' : undefined;
    if (mode !== current.mode || text !== saved || target !== current.target || (runUnchanged && text.trim())) {
      onChange({ ...current, mode, query: text, target });
    }
  };

  // Grafana's variable editor passes a no-op onRunQuery; onChange is what reruns the variable.
  const commit = (text: string, runUnchanged = false) => saveQuery(current.mode, text, runUnchanged);

  const onModeChange = (mode: VariableQueryMode) => {
    setOtherModeText(draft);
    saveQuery(mode, otherModeText);
  };

  return (
    <Stack direction="column" gap={1}>
      <InlineField label="Mode" labelWidth={10}>
        <RadioButtonGroup<VariableQueryMode> value={current.mode} options={modes} onChange={onModeChange} />
      </InlineField>
      {isSql ? (
        <>
          <Text variant="bodySmall" color="secondary">
            Return one column, or columns named __text and __value. Null values are skipped.
          </Text>
          {!saved && !draft && (
            <Button variant="secondary" onClick={() => commit(VARIABLE_SQL_EXAMPLE)}>
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
