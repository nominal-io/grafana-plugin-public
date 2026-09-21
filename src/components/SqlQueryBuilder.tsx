import React, { useCallback, useState } from 'react';
import { Alert, Combobox, InlineField, Input, MultiCombobox, Stack, type ComboboxOption } from '@grafana/ui';
import { getBackendSrv } from '@grafana/runtime';
import { SqlBuilderState } from '../types';
import { buildSql, sqlBuilderError } from '../utils/sqlBuilder';

interface Props {
  builder: SqlBuilderState;
  datasourceUrl: string;
  onChange: (builder: SqlBuilderState) => void;
}

export function SqlQueryBuilder({ builder, datasourceUrl, onChange }: Props) {
  const [metadataError, setMetadataError] = useState<string>();
  const loadOptions = useCallback(
    async (kind: 'datasets' | 'channels', searchText: string): Promise<Array<ComboboxOption<string>>> => {
      try {
        const options = await getBackendSrv().post(`${datasourceUrl}/sql/${kind}`, {
          searchText,
          ...(kind === 'channels' ? { datasetRid: builder.datasetRid } : {}),
        });
        setMetadataError(undefined);
        return options;
      } catch {
        setMetadataError(
          'Could not load SQL metadata. Check the data source API key, workspace and SQL access. You can also enter a dataset RID or channel name directly.'
        );
        return [];
      }
    },
    [builder.datasetRid, datasourceUrl]
  );
  const datasets = useCallback((search: string) => loadOptions('datasets', search), [loadOptions]);
  const channels = useCallback((search: string) => loadOptions('channels', search), [loadOptions]);
  const update = (patch: Partial<SqlBuilderState>) => onChange({ ...builder, ...patch });
  const error = sqlBuilderError(builder);
  const sql = buildSql(builder);

  return (
    <Stack direction="column" gap={1}>
      {metadataError && (
        <Alert severity="warning" title="SQL metadata unavailable">
          {metadataError}
        </Alert>
      )}
      <Stack wrap>
        <InlineField label="Dataset">
          <Combobox
            value={
              builder.datasetRid
                ? { value: builder.datasetRid, label: builder.datasetName || builder.datasetRid }
                : null
            }
            options={datasets}
            createCustomValue
            isClearable={false}
            width="auto"
            minWidth={35}
            placeholder="Search datasets or paste a RID..."
            onChange={(option) => update({ datasetRid: option.value, datasetName: option.label, channels: [] })}
            data-testid="sql-dataset-picker"
          />
        </InlineField>
        <InlineField
          label="Value type"
          tooltip="Choose the storage type of your channels. Use Code for strings, logs and other tables."
        >
          <Combobox
            value={builder.table}
            options={[
              { label: 'Floating point', value: 'points_double' },
              { label: 'Integer', value: 'points_int' },
            ]}
            onChange={(option) => update({ table: option.value })}
            width={20}
          />
        </InlineField>
      </Stack>
      {builder.datasetRid && (
        <InlineField label="Channels">
          <MultiCombobox
            key={builder.datasetRid}
            value={builder.channels}
            options={channels}
            createCustomValue
            placeholder="Search channels..."
            width="auto"
            minWidth={35}
            onChange={(options) => update({ channels: options.map((option) => option.value) })}
            data-testid="sql-channel-picker"
          />
        </InlineField>
      )}
      <Stack wrap>
        <InlineField label="Aggregation">
          <Combobox
            value={builder.aggregation}
            options={(['AVG', 'MIN', 'MAX', 'SUM', 'COUNT'] as const).map((value) => ({
              label: value === 'AVG' ? 'Mean' : value,
              value,
            }))}
            onChange={(option) => update({ aggregation: option.value })}
            width={16}
          />
        </InlineField>
        <InlineField
          label="Time bucket"
          tooltip="Auto uses Grafana’s interval for the selected time range and panel resolution."
        >
          <Input
            aria-label="Time bucket"
            value={builder.interval}
            placeholder="Auto"
            width={18}
            onChange={(event) => update({ interval: event.currentTarget.value })}
          />
        </InlineField>
      </Stack>
      <Stack wrap>
        <InlineField label="Tag key (optional)">
          <Input
            aria-label="Tag key"
            value={builder.tagKey ?? ''}
            onChange={(event) => update({ tagKey: event.currentTarget.value })}
          />
        </InlineField>
        <InlineField label="Tag value">
          <Input
            aria-label="Tag value"
            value={builder.tagValue ?? ''}
            onChange={(event) => update({ tagValue: event.currentTarget.value })}
          />
        </InlineField>
      </Stack>
      {error ? (
        <div>{error}</div>
      ) : (
        <details>
          <summary>Generated SQL</summary>
          <pre data-testid="generated-sql">{sql}</pre>
        </details>
      )}
    </Stack>
  );
}
