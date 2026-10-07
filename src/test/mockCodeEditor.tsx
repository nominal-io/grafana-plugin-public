import React from 'react';

interface MockCodeEditorProps {
  value: string;
  onBlur?: (value: string) => void;
  onChange?: (value: string) => void;
  onSave?: (value: string) => void;
}

// Monaco does not render in jsdom, so tests swap Grafana's CodeEditor for this textarea.
export function MockCodeEditor({ value, onBlur, onChange, onSave }: MockCodeEditorProps) {
  return (
    <textarea
      value={value}
      onChange={(event) => onChange?.(event.target.value)}
      onBlur={(event) => onBlur?.(event.target.value)}
      onKeyDown={(event) => {
        if ((event.metaKey || event.ctrlKey) && event.key === 's') {
          event.preventDefault();
          onSave?.(event.currentTarget.value);
        }
      }}
    />
  );
}
