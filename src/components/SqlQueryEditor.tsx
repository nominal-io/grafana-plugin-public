import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  CodeEditor,
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
  const [value, setValue] = useState(query.rawSql || DEFAULT_SQL);

  useEffect(() => {
    setValue(query.rawSql || DEFAULT_SQL);
  }, [query.rawSql]);

  const commit = useCallback((text: string) => {
    if ((!query.rawSql && text === DEFAULT_SQL) || text === query.rawSql) {
      return;
    }

    onChange({ ...query, queryType: QUERY_TYPE_SQL, rawSql: text });
    onRunQuery();
  }, [onChange, onRunQuery, query]);

  // Monaco registers the command once at mount, so it must read the latest commit through a ref.
  const commitRef = useRef(commit);
  commitRef.current = commit;

  const onEditorDidMount = useCallback((editor: MonacoEditor, monaco: Monaco) => {
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => commitRef.current(editor.getValue()));
  }, []);

  const onFormatChange = (format: SqlFormat) => {
    onChange({ ...query, queryType: QUERY_TYPE_SQL, format });
    onRunQuery();
  };

  return (
    <Stack direction="column" gap={1}>
      <div data-testid="sql-code-editor">
        <CodeEditor
          value={value}
          language="sql"
          height={200}
          showLineNumbers
          showMiniMap={false}
          onChange={setValue}
          onBlur={commit}
          onSave={commit}
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
