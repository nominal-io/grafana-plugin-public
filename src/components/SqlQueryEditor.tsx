import React from 'react';
import { Button, InlineField, RadioButtonGroup, Stack } from '@grafana/ui';
import { DEFAULT_SQL, SqlFormat } from '../types';
import { SqlCodeEditor } from './SqlCodeEditor';
import { useSqlDraft, type SqlEditorProps } from './useSqlDraft';

export function SqlQueryEditor({ query, onChange, onRunQuery }: SqlEditorProps) {
  const { value, setValue, isBlank, commit, save } = useSqlDraft(query, onChange, onRunQuery);

  const onFormatChange = (format: SqlFormat) => {
    save(value, { format });
    onRunQuery();
  };

  return (
    <Stack direction="column" gap={1}>
      {isBlank && (
        <Button variant="secondary" onClick={() => save(DEFAULT_SQL, { format: 'timeseries' })}>
          Use time-series example
        </Button>
      )}
      <SqlCodeEditor value={value} onChange={setValue} onCommit={commit} />
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
