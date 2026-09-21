import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  CodeEditor,
  Button,
  InlineField,
  RadioButtonGroup,
  Stack,
  type CodeEditorSuggestionItem,
  type Monaco,
  type MonacoEditor,
} from '@grafana/ui';
import { DEFAULT_SQL, NominalQuery, QUERY_TYPE_SQL, SqlFormat } from '../types';

interface Props {
  query: NominalQuery;
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

export function SqlQueryEditor({ query, onChange, onRunQuery }: Props) {
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

  const onFormatChange = (format: SqlFormat) => {
    onChange({ ...query, queryType: QUERY_TYPE_SQL, rawSql: value, format });
    onRunQuery();
  };

  return (
    <Stack direction="column" gap={1}>
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
