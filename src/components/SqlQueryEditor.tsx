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
  { label: '$__timeFilter(column)', insertText: '$__timeFilter(ts)', detail: 'Filters a timestamp column to the panel time range.' },
  { label: '$__timeFrom()', detail: 'Expands to the panel start timestamp.' },
  { label: '$__timeTo()', detail: 'Expands to the panel end timestamp.' },
  {
    label: '$__timeGroup(column[, interval])',
    insertText: '$__timeGroup(ts)',
    detail: 'Buckets timestamps by the panel interval, or by a duration such as 1m.',
  },
];

const getSuggestions = () => suggestions;

export function SqlQueryEditor({ query, onChange, onRunQuery }: Props) {
  const savedSql = query.rawSql ?? '';
  const [value, setValue] = useState(savedSql);

  useEffect(() => {
    setValue(savedSql);
  }, [savedSql]);

  const commit = useCallback(
    (text: string, runUnchanged = false) => {
      if (text !== (query.rawSql ?? '')) {
        onChange({ ...query, queryType: QUERY_TYPE_SQL, rawSql: text });
        onRunQuery();
      } else if (runUnchanged && text.trim()) {
        onRunQuery();
      }
    },
    [onChange, onRunQuery, query]
  );

  // The Monaco action outlives renders, so it reads the latest commit through a ref.
  const commitRef = useRef(commit);
  commitRef.current = commit;
  const runAction = useRef<{ dispose: () => void } | undefined>(undefined);

  const onEditorDidMount = useCallback((editor: MonacoEditor, monaco: Monaco) => {
    // An action is bound to this editor; editor.addCommand would bind the key for every editor on the page.
    runAction.current = editor.addAction({
      id: 'nominal.sql.runQuery',
      label: 'Run query',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => commitRef.current(editor.getValue(), true),
    });
  }, []);

  const onEditorWillUnmount = useCallback(() => runAction.current?.dispose(), []);

  const onFormatChange = (format: SqlFormat) => {
    onChange({ ...query, queryType: QUERY_TYPE_SQL, rawSql: value, format });
    onRunQuery();
  };

  const insertExample = () => {
    setValue(DEFAULT_SQL);
    onChange({ ...query, queryType: QUERY_TYPE_SQL, rawSql: DEFAULT_SQL, format: 'timeseries' });
  };

  return (
    <Stack direction="column" gap={1}>
      {!savedSql && !value && (
        <Button variant="secondary" onClick={insertExample}>
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
          onEditorWillUnmount={onEditorWillUnmount}
          getSuggestions={getSuggestions}
        />
      </div>
      <InlineField label="Format" labelWidth={8}>
        <RadioButtonGroup<SqlFormat>
          aria-label="Result format"
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
