import React, { useState } from 'react';
import { AnnotationQuery, QueryEditorProps } from '@grafana/data';
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

type Props = QueryEditorProps<DataSource, NominalQuery, NominalDataSourceOptions> & {
  annotation?: AnnotationQuery<NominalQuery>;
  onAnnotationChange?: (annotation: AnnotationQuery<NominalQuery>) => void;
};
type Stash = { target: Partial<NominalQuery>; mappings?: AnnotationQuery['mappings'] };
type Mode = ReturnType<typeof annotationMode>;

const modes = [
  { label: 'Events', value: QUERY_TYPE_EVENTS },
  { label: 'SQL', value: QUERY_TYPE_SQL },
];

export function AnnotationEditor(props: Props) {
  const { query, onChange, annotation, onAnnotationChange } = props;
  const mode = annotationMode(query);

  // The saved query holds only the active mode's fields; the other mode's target and mappings are
  // kept here so switching back restores them.
  const [other, setOther] = useState<Stash>({ target: {} });
  const onModeChange = (next: Mode) => {
    setOther({ target: query, mappings: annotation?.mappings });
    const target = { ...other.target, refId: query.refId, queryType: next };
    if (annotation && onAnnotationChange) {
      // Column mappings belong to the outgoing mode's result shape, and one call keeps them in step with the target.
      onAnnotationChange({ ...annotation, target, mappings: other.mappings });
    } else {
      onChange(target);
    }
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
