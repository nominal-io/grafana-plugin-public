import React from 'react';
import { Button, InlineField, RadioButtonGroup, Stack } from '@grafana/ui';
import { DEFAULT_SQL, LOGS_SQL_EXAMPLE, SqlFormat } from '../types';
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
        <Stack gap={1}>
          <Button variant="secondary" onClick={() => save(DEFAULT_SQL, { format: 'timeseries' })}>
            Use time-series example
          </Button>
          <Button variant="secondary" onClick={() => save(LOGS_SQL_EXAMPLE, { format: 'logs' })}>
            Use logs example
          </Button>
        </Stack>
      )}
      <SqlCodeEditor value={value} onChange={setValue} onCommit={commit} />
      <InlineField label="Format" labelWidth={8}>
        <RadioButtonGroup<SqlFormat>
          aria-label="Result format"
          value={query.format ?? 'timeseries'}
          options={[
            { label: 'Time series', value: 'timeseries' },
            { label: 'Table', value: 'table' },
            { label: 'Logs', value: 'logs' },
          ]}
          onChange={onFormatChange}
        />
      </InlineField>
    </Stack>
  );
}
