import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  CodeEditor,
  Alert,
  Button,
  InlineField,
  RadioButtonGroup,
  Stack,
  type CodeEditorSuggestionItem,
  type Monaco,
  type MonacoEditor,
} from '@grafana/ui';
import { buildSql } from '../utils/sqlBuilder';
import { SqlQueryBuilder } from './SqlQueryBuilder';
import { DEFAULT_SQL, DEFAULT_SQL_BUILDER, SqlBuilderState, NominalQuery, QUERY_TYPE_SQL, SqlFormat } from '../types';

interface Props {
  query: NominalQuery;
  datasourceUrl?: string;
  onChange: (query: NominalQuery) => void;
  onRunQuery: () => void;
}

const suggestions: CodeEditorSuggestionItem[] = [
  { label: '$__timeFilter(column)', detail: 'Filters a timestamp column to the panel time range.' },
  { label: '$__timeFrom()', detail: 'Expands to the panel start timestamp.' },
  { label: '$__timeTo()', detail: 'Expands to the panel end timestamp.' },
  { label: '$__timeGroup(column, interval)', detail: 'Groups timestamps using the panel or supplied interval.' },
  { label: '$__interval', detail: 'Expands to Grafana’s calculated panel interval.' },
];

export function SqlQueryEditor({ query, onChange, onRunQuery, datasourceUrl = '' }: Props) {
  const [value, setValue] = useState(query.rawSql ?? '');

  useEffect(() => {
    setValue(query.rawSql ?? '');
  }, [query.rawSql]);

  const commit = useCallback(
    (text: string, runUnchanged = false) => {
      if (text !== query.rawSql) {
        onChange({ ...query, queryType: QUERY_TYPE_SQL, rawSql: text });
        onRunQuery();
      } else if (runUnchanged && text.trim()) {
        onRunQuery();
      }
    },
    [onChange, onRunQuery, query]
  );

  // Monaco registers the command once at mount, so it must read the latest commit through a ref.
  const commitRef = useRef(commit);
  commitRef.current = commit;

  const onEditorDidMount = useCallback((editor: MonacoEditor, monaco: Monaco) => {
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => commitRef.current(editor.getValue(), true));
  }, []);

  const mode = query.sqlEditorMode ?? 'code'; // Saved raw SQL always opens without conversion.
  const builder = query.sqlBuilder ?? DEFAULT_SQL_BUILDER;
  const [confirmReplace, setConfirmReplace] = useState(false);
  const changeBuilder = (next: SqlBuilderState) => {
    const rawSql = buildSql(next);
    onChange({ ...query, queryType: QUERY_TYPE_SQL, sqlEditorMode: 'builder', sqlBuilder: next, rawSql });
    setValue(rawSql);
  };
  const switchMode = (next: 'builder' | 'code') => {
    if (next === 'builder' && value.trim() && value !== buildSql(builder)) {
      setConfirmReplace(true);
      return;
    }
    onChange({ ...query, sqlEditorMode: next, rawSql: next === 'builder' ? buildSql(builder) : value });
  };

  const onFormatChange = (format: SqlFormat) => {
    onChange({ ...query, queryType: QUERY_TYPE_SQL, rawSql: mode === 'code' ? value : query.rawSql, format });
    onRunQuery();
  };

  return (
    <Stack direction="column" gap={1}>
      <InlineField label="Editor" labelWidth={8}>
        <RadioButtonGroup
          value={mode}
          options={[
            { label: 'Builder', value: 'builder' },
            { label: 'Code', value: 'code' },
          ]}
          onChange={switchMode}
        />
      </InlineField>
      {confirmReplace && (
        <Alert severity="warning" title="Replace edited SQL with the builder query?">
          Custom SQL cannot be converted automatically. Returning to Builder will replace your code with the last
          builder selections.
          <Stack>
            <Button
              variant="destructive"
              onClick={() => {
                changeBuilder(builder);
                setConfirmReplace(false);
              }}
            >
              Replace SQL
            </Button>
            <Button variant="secondary" onClick={() => setConfirmReplace(false)}>
              Keep code
            </Button>
          </Stack>
        </Alert>
      )}
      {mode === 'builder' ? (
        <>
          <SqlQueryBuilder builder={builder} datasourceUrl={datasourceUrl} onChange={changeBuilder} />
          <Button disabled={!query.rawSql?.trim()} onClick={onRunQuery}>
            Run query
          </Button>
        </>
      ) : (
        <>
          {!value && (
            <Button
              variant="secondary"
              onClick={() => {
                setValue(DEFAULT_SQL);
                onChange({ ...query, rawSql: DEFAULT_SQL });
              }}
            >
              Use time-series example
            </Button>
          )}
          <div data-testid="sql-code-editor">
            <CodeEditor
              value={value}
              language="sql"
              height={200}
              showLineNumbers
              showMiniMap={false}
              onChange={setValue}
              onBlur={commit}
              onSave={(text) => commit(text, true)}
              onEditorDidMount={onEditorDidMount}
              getSuggestions={() => suggestions}
            />
          </div>
        </>
      )}
      <InlineField label="Format" labelWidth={8}>
        <RadioButtonGroup<SqlFormat>
          value={query.format ?? 'timeseries'}
          options={[
            { label: 'Time series', value: 'timeseries' },
            { label: 'Table', value: 'table' },
          ]}
          onChange={onFormatChange}
        />
      </InlineField>
    </Stack>
  );
}
