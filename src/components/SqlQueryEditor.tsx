import React, { useEffect, useState } from 'react';
import { Button, InlineField, RadioButtonGroup, Stack } from '@grafana/ui';
import { DEFAULT_SQL, NominalQuery, QUERY_TYPE_SQL, SqlFormat } from '../types';
import { SqlCodeEditor } from './SqlCodeEditor';

interface Props {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  onRunQuery: () => void;
}

export function SqlQueryEditor({ query, onChange, onRunQuery }: Props) {
  const savedSql = query.rawSql ?? '';
  const [value, setValue] = useState(savedSql);

  useEffect(() => {
    setValue(savedSql);
  }, [savedSql]);

  const commit = (text: string, runUnchanged: boolean) => {
    if (text !== savedSql) {
      onChange({ ...query, queryType: QUERY_TYPE_SQL, rawSql: text });
      onRunQuery();
    } else if (runUnchanged && text.trim()) {
      onRunQuery();
    }
  };

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
