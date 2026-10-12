import React from 'react';
import { Button, Stack, Text } from '@grafana/ui';
import { ANNOTATION_SQL_EXAMPLE } from '../types';
import { SqlCodeEditor } from './SqlCodeEditor';
import { useSqlDraft, type SqlEditorProps } from './useSqlDraft';

export function SqlAnnotationEditor({ query, onChange, onRunQuery }: SqlEditorProps) {
  // Grafana reruns annotation queries when the saved query changes, and its onRunQuery
  // still reads the previous query at that point, so a changed query must not run here.
  const { value, setValue, isBlank, commit, save } = useSqlDraft(query, onChange, onRunQuery, { runOnChange: false });

  return (
    <Stack direction="column" gap={1}>
      <Text variant="bodySmall" color="secondary">
        Return a &quot;time&quot; column and a text column (named text, or the first string column). Optionally
        return &quot;timeEnd&quot;, title and tags. Map other column names below.
      </Text>
      {isBlank && (
        <Button variant="secondary" onClick={() => save(ANNOTATION_SQL_EXAMPLE)}>
          Use events example
        </Button>
      )}
      <SqlCodeEditor value={value} onChange={setValue} onCommit={commit} />
    </Stack>
  );
}
