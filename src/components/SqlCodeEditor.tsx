import React, { useCallback, useRef } from 'react';
import { CodeEditor, type CodeEditorSuggestionItem, type Monaco, type MonacoEditor } from '@grafana/ui';

interface Props {
  value: string;
  onChange: (text: string) => void;
  onCommit: (text: string, runUnchanged: boolean) => void;
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

export function SqlCodeEditor({ value, onChange, onCommit }: Props) {
  // The Monaco action outlives renders, so it reads the latest commit through a ref.
  const commitRef = useRef(onCommit);
  commitRef.current = onCommit;
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

  return (
    <div data-testid="sql-code-editor">
      <CodeEditor
        value={value}
        language="sql"
        height={200}
        showLineNumbers
        showMiniMap={false}
        onChange={onChange}
        onBlur={(text) => onCommit(text, false)}
        onSave={(text) => onCommit(text, true)}
        onEditorDidMount={onEditorDidMount}
        onEditorWillUnmount={onEditorWillUnmount}
        getSuggestions={getSuggestions}
      />
    </div>
  );
}
