import React, { useSyncExternalStore } from 'react';
import { LogSortOrderChangeEvent } from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
import { LogsSortOrder } from '@grafana/schema';
import { Alert, Button, InlineField, RadioButtonGroup, Stack } from '@grafana/ui';
import { DEFAULT_SQL, LOGS_SQL_EXAMPLE, SqlFormat } from '../types';
import { SqlCodeEditor } from './SqlCodeEditor';
import { useSqlDraft, type SqlEditorProps } from './useSqlDraft';

// sqlLogOrder returns the direction of the first sort key of a query's last top-level ORDER BY.
export function sqlLogOrder(sql: string): LogsSortOrder | undefined {
  // Blank out comments, quoted text and parenthesized expressions so only top-level words remain.
  let top = sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\/|'(?:[^']|'')*'|"(?:[^"]|"")*"/g, ' ');
  for (let prev = ''; prev !== top; ) {
    prev = top;
    top = top.replace(/\([^()]*\)/g, ' ');
  }
  const key = Array.from(top.matchAll(/\border\s+by\s+([^,;]*)/gi)).at(-1)?.[1];
  if (key === undefined) {
    return undefined;
  }
  return /\bdesc\b/i.test(key) ? LogsSortOrder.Descending : LogsSortOrder.Ascending;
}

// A dashboard Logs panel announces its Order option only when it mounts or the option changes, and
// never when it unmounts. The last announcement is kept from when the plugin loads, so an editor that
// mounts after the panel, such as a query added in panel edit, still sees it.
let lastPanelOrder: LogsSortOrder | undefined;
const panelOrderListeners = new Set<() => void>();
getAppEvents().subscribe(LogSortOrderChangeEvent, (event) => {
  lastPanelOrder = event.payload.order;
  panelOrderListeners.forEach((listener) => listener());
});
const subscribePanelOrder = (listener: () => void) => {
  panelOrderListeners.add(listener);
  return () => panelOrderListeners.delete(listener);
};

export function SqlQueryEditor({ query, onChange, onRunQuery }: SqlEditorProps) {
  const { value, setValue, isBlank, commit, save } = useSqlDraft(query, onChange, onRunQuery);
  // Infinite scroll loads older lines for newest first and newer lines for oldest first, so the
  // query's ORDER BY has to match the panel's order.
  const panelOrder = useSyncExternalStore(subscribePanelOrder, () => lastPanelOrder);
  const sqlOrder = query.format === 'logs' ? sqlLogOrder(value) : undefined;

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
      {panelOrder && sqlOrder && sqlOrder !== panelOrder && (
        <Alert severity="warning" title="Query order may not match the panel's order">
          The Logs panel was last set to show {panelOrder === LogsSortOrder.Descending ? 'newest' : 'oldest'} lines
          first, but the first key of the query&apos;s last ORDER BY sorts{' '}
          {sqlOrder === LogsSortOrder.Descending ? 'descending' : 'ascending'}. Scrolling loads the right lines only
          when the query sorts by its log time column in the panel&apos;s order, as in ORDER BY ts{' '}
          {panelOrder === LogsSortOrder.Descending ? 'DESC' : 'ASC'}. Change the query or the panel&apos;s Order option.
        </Alert>
      )}
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
