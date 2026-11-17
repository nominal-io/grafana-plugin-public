import React from 'react';
import { QueryEditorProps } from '@grafana/data';
import { getTemplateSrv } from '@grafana/runtime';
import { Combobox, ComboboxOption, InlineField, MultiCombobox, Stack, Text } from '@grafana/ui';
import type { DataSource } from '../datasource';
import { NominalDataSourceOptions, NominalQuery, QUERY_TYPE_EVENTS } from '../types';
import { searchAllAssets, searchEventLabels } from '../utils/api';

type Props = QueryEditorProps<DataSource, NominalQuery, NominalDataSourceOptions>;

const eventTypeOptions = ['INFO', 'FLAG', 'ERROR', 'SUCCESS'].map((value) => ({ value }));

export function EventsAnnotationEditor({ query, onChange, datasource }: Props) {
  // Changes only save: Grafana reruns annotations when the saved query changes, and
  // onRunQuery still holds the previous query at that point.
  const update = (patch: Partial<NominalQuery>) => onChange({ ...query, queryType: QUERY_TYPE_EVENTS, ...patch });

  const loadAssets = async (text: string): Promise<Array<ComboboxOption<string>>> => {
    const variables = getTemplateSrv()
      .getVariables()
      .map((v) => ({ value: `$${v.name}`, description: 'Dashboard variable' }))
      .filter((o) => o.value.toLowerCase().includes(text.toLowerCase()));
    const assets = await searchAllAssets(datasource.url, text);
    return [...variables, ...assets.map((a) => ({ value: a.rid, label: a.title }))];
  };

  const loadLabels = async (text: string) =>
    (await searchEventLabels(datasource.url, text)).map((value) => ({ value }));

  return (
    <Stack direction="column" gap={1}>
      <Text variant="bodySmall" color="secondary">
        An asset is required. Leave event types and labels empty to include all events. Shows events that overlap the dashboard
        time range, up to 1,000, newest first.
      </Text>
      <InlineField label="Asset" labelWidth={14}>
        <Combobox
          id="nominal-annotation-asset"
          value={query.assetRid || null}
          options={loadAssets}
          onChange={(o) => update({ assetRid: o?.value })}
          placeholder="Search assets or choose a variable"
          createCustomValue
          isClearable
          width={40}
        />
      </InlineField>
      <InlineField label="Event types" labelWidth={14}>
        <MultiCombobox
          options={eventTypeOptions}
          value={query.eventTypes ?? []}
          onChange={(selected) => update({ eventTypes: selected.map((o) => o.value) })}
          placeholder="All types"
          width={40}
        />
      </InlineField>
      <InlineField label="Labels" labelWidth={14}>
        <MultiCombobox
          options={loadLabels}
          value={query.eventLabels ?? []}
          onChange={(selected) => update({ eventLabels: selected.map((o) => o.value) })}
          placeholder="All labels"
          createCustomValue
          width={40}
        />
      </InlineField>
    </Stack>
  );
}
