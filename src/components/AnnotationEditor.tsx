import React, { useState } from 'react';
import { QueryEditorProps } from '@grafana/data';
import { InlineField, RadioButtonGroup, Stack } from '@grafana/ui';
import type { DataSource } from '../datasource';
import {
  annotationMode,
  NominalDataSourceOptions,
  NominalQuery,
  QUERY_TYPE_EVENTS,
  QUERY_TYPE_SQL,
} from '../types';
import { EventsAnnotationEditor } from './EventsAnnotationEditor';
import { SqlAnnotationEditor } from './SqlAnnotationEditor';

type Props = QueryEditorProps<DataSource, NominalQuery, NominalDataSourceOptions>;
type Mode = ReturnType<typeof annotationMode>;

const modes = [
  { label: 'Events', value: QUERY_TYPE_EVENTS },
  { label: 'SQL', value: QUERY_TYPE_SQL },
];

export function AnnotationEditor(props: Props) {
  const { query, onChange } = props;
  const mode = annotationMode(query);

  // The saved query holds only the active mode's fields; the other mode's are kept here so switching back restores them.
  const [other, setOther] = useState<Partial<NominalQuery>>({});
  const onModeChange = (next: Mode) => {
    setOther(query);
    onChange({ ...other, refId: query.refId, queryType: next });
  };

  return (
    <Stack direction="column" gap={1}>
      <InlineField label="Mode" labelWidth={10}>
        <RadioButtonGroup<Mode> value={mode} options={modes} onChange={onModeChange} />
      </InlineField>
      {mode === QUERY_TYPE_SQL ? <SqlAnnotationEditor {...props} /> : <EventsAnnotationEditor {...props} />}
    </Stack>
  );
}
