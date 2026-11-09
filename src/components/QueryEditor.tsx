import React from 'react';
import { css, keyframes } from '@emotion/css';
import {
  Combobox,
  RadioButtonGroup,
  InlineField,
  Input,
  Stack,
  MultiCombobox,
  useStyles2,
} from '@grafana/ui';
import { CoreApp, type GrafanaTheme2, type QueryEditorProps } from '@grafana/data';
import type { DataQuery } from '@grafana/schema';
import type { DataSource } from '../datasource';
import { QUERY_TYPE_COMPUTE, QUERY_TYPE_SQL, type NominalDataSourceOptions, type NominalQuery } from '../types';
import { getSupportedScopeNames } from '../utils/api';
import { useNominalQueryBuilder } from './queryBuilder/useNominalQueryBuilder';
import { toAggregationComboboxOptions, toChannelOption } from './queryBuilder/queryBuilderOptions';
import { SqlQueryEditor } from './SqlQueryEditor';

type Props = QueryEditorProps<DataSource, NominalQuery, NominalDataSourceOptions>;

const fadeInOut = keyframes({
  '0%': { opacity: 0, transform: 'translateY(5px)' },
  '20%': { opacity: 1, transform: 'translateY(0)' },
  '80%': { opacity: 1, transform: 'translateY(0)' },
  '100%': { opacity: 0, transform: 'translateY(-5px)' },
});

const getStyles = (theme: GrafanaTheme2) => ({
  root: css({
    width: '100%',
    padding: theme.spacing(0.5),
  }),
  editorBox: (configComplete: boolean) =>
    css({
      padding: theme.spacing(1, 1.5),
      backgroundColor: theme.colors.background.primary,
      borderRadius: theme.shape.radius.default,
      border: `1px solid ${configComplete ? theme.colors.success.main : theme.colors.border.weak}`,
      marginBottom: theme.spacing(0.5),
      width: '100%',
    }),
  assetSummary: css({
    marginTop: theme.spacing(0.75),
    padding: theme.spacing(0.75, 1.25),
    backgroundColor: theme.colors.background.secondary,
    borderRadius: theme.shape.radius.default,
    fontSize: theme.typography.bodySmall.fontSize,
    border: `1px solid ${theme.colors.border.medium}`,
    color: theme.colors.text.maxContrast,
    lineHeight: theme.typography.bodySmall.lineHeight,
  }),
  summaryLabel: css({
    color: theme.colors.text.secondary,
  }),
  summaryPill: css({
    fontFamily: theme.typography.fontFamilyMonospace,
    color: theme.colors.text.primary,
    backgroundColor: theme.colors.background.canvas,
    padding: theme.spacing(0.25, 0.625),
    borderRadius: theme.shape.radius.default,
    marginLeft: theme.spacing(0.75),
    marginRight: theme.spacing(1),
  }),
  ridWrapper: css({
    position: 'relative',
    display: 'inline-block',
  }),
  ridClickTarget: css({
    fontFamily: theme.typography.fontFamilyMonospace,
    color: theme.colors.primary.text,
    cursor: 'pointer',
    textDecoration: 'underline',
    textDecorationStyle: 'dotted',
    textDecorationColor: theme.colors.primary.shade,
    marginLeft: theme.spacing(0.75),
    marginRight: theme.spacing(1),
    fontSize: theme.typography.bodySmall.fontSize,
    transition: 'background-color 0.15s ease, padding 0.15s ease',
    '&:hover': {
      backgroundColor: theme.colors.action.hover,
      borderRadius: theme.shape.radius.default,
      padding: theme.spacing(0.125, 0.375),
    },
  }),
  copiedMessage: css({
    position: 'absolute',
    top: theme.spacing(-3),
    left: theme.spacing(0.75),
    backgroundColor: theme.colors.success.shade,
    color: theme.colors.success.text,
    padding: theme.spacing(0.25, 0.75),
    borderRadius: theme.shape.radius.default,
    fontSize: theme.typography.bodySmall.fontSize,
    whiteSpace: 'nowrap',
    border: `1px solid ${theme.colors.success.border}`,
    zIndex: theme.zIndex.tooltip,
    animation: `${fadeInOut} 2s ease-in-out`,
  }),
  scopeCount: css({
    color: theme.colors.success.text,
    fontWeight: theme.typography.fontWeightMedium,
    marginLeft: theme.spacing(0.5),
  }),
});

function BuilderQueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const styles = useStyles2(getStyles);
  // Grafana's Combobox can repeat a selection the query already holds. In Explore
  // each onChange re-renders synchronously, so the repeats loop until React aborts
  // with error #185. Skip writes that would not change the query.
  const latestQuery = React.useRef(query);
  latestQuery.current = query;
  const onChangeIfChanged = React.useCallback(
    (next: NominalQuery) => {
      if (JSON.stringify(next) !== JSON.stringify(latestQuery.current)) {
        onChange(next);
      }
    },
    [onChange]
  );
  const { state, commands } = useNominalQueryBuilder({
    query,
    onChange: onChangeIfChanged,
    onRunQuery,
    datasourceUrl: datasource.url,
  });

  const aggregationOptions = React.useMemo(
    () => toAggregationComboboxOptions(state.aggregationState.options),
    [state.aggregationState.options]
  );

  return (
    <div className={styles.root}>
      <div className={styles.editorBox(state.configComplete)}>
        <Stack gap={1} direction="column">
          <Stack gap={1} direction="row" wrap alignItems="center" data-testid="query-editor-asset-scope-row">
            {/* Asset Selection */}
            <InlineField label="Asset" labelWidth={8}>
              <Combobox
                id="nominal-query-asset-picker"
                value={state.assetSelectValue}
                options={state.assetOptions}
                onChange={(selection) => commands.selectAsset(selection.value)}
                placeholder="Search assets or paste a RID..."
                createCustomValue
                isClearable={false}
                width="auto"
                minWidth={30}
                maxWidth={100}
                data-testid="asset-combobox"
              />
            </InlineField>

            {state.assetComplete && (
              <InlineField label="Data scope" labelWidth={12} loading={!state.selectedAsset && state.assetComplete}>
                <Combobox
                  id="nominal-query-data-scope-picker"
                  value={query?.dataScopeName || ''}
                  onChange={(selection) => commands.selectDataScope(selection.value)}
                  options={state.dataScopeOptions}
                  placeholder="Choose scope or use $variable..."
                  width="auto"
                  minWidth={30}
                  maxWidth={100}
                  isClearable={false}
                  createCustomValue
                  data-testid="data-scope-combobox"
                />
              </InlineField>
            )}
          </Stack>

          {/* Channel Selection - only show if asset is selected */}
          {state.assetComplete && (
            <Stack gap={1} direction="row" wrap alignItems="center" data-testid="query-editor-channel-aggregation-row">
              {state.hasChannelSearch && (
                <InlineField label="Channel" labelWidth={8}>
                  {/*
                    TODO: add Combobox prefixIcon after bumping @grafana/ui to >=12.3.0;
                    the current 12.1.0 pin does not include it.
                  */}
                  <Combobox
                    id="nominal-query-channel-picker"
                    key={`${state.resolvedAssetRid || 'no-asset'}-${state.resolvedDataScopeName}`}
                    value={state.channelSelectValue}
                    onChange={(selection) => commands.selectChannel(toChannelOption(selection))}
                    options={state.channelOptions}
                    placeholder="Search for channel..."
                    width="auto"
                    minWidth={30}
                    maxWidth={100}
                    createCustomValue
                    isClearable={false}
                    data-testid="channel-combobox"
                  />
                </InlineField>
              )}

              {query?.channel && (
                <InlineField label="Aggregation(s)" tooltip={state.aggregationState.tooltip}>
                  {state.aggregationState.kind === 'string' ? (
                    <Input value={state.aggregationState.value[0]} disabled readOnly width={10} />
                  ) : state.aggregationState.kind === 'log' ? (
                    <Input value={state.aggregationState.value[0]} disabled readOnly width={12} />
                  ) : (
                    <MultiCombobox
                      options={aggregationOptions}
                      value={state.aggregationState.value}
                      onChange={commands.changeAggregations}
                      placeholder="Select aggregations..."
                      width="auto"
                      minWidth={40}
                      maxWidth={100}
                      data-testid="aggregation-multi-combobox"
                    />
                  )}
                </InlineField>
              )}
            </Stack>
          )}
        </Stack>

        {/* Asset info display - compact single line */}
        {state.selectedAsset && (
          <div className={styles.assetSummary}>
            <span className={styles.summaryLabel}>Asset:</span>
            <span className={styles.summaryPill}>
              {state.selectedAsset.title}
            </span>
            <span className={styles.summaryLabel}>RID:</span>
            <span className={styles.ridWrapper}>
              <span
                onClick={commands.copySelectedAssetRid}
                title="Click to copy RID"
                className={styles.ridClickTarget}
              >
                {state.selectedAsset.rid}
              </span>
              {state.showCopiedMessage && (
                <span className={styles.copiedMessage}>
                  ✓ Copied to clipboard
                </span>
              )}
            </span>
            <span className={styles.summaryLabel}>Data Scopes:</span>
            <span className={styles.scopeCount}>
              {getSupportedScopeNames(state.selectedAsset).length}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}

type QueryAPI = NonNullable<NominalQuery['queryType']>;

// Which API owns each query field. A field added to NominalQuery fails typecheck until it is listed here.
const FIELD_API = {
  assetRid: QUERY_TYPE_COMPUTE,
  channel: QUERY_TYPE_COMPUTE,
  dataScopeName: QUERY_TYPE_COMPUTE,
  channelDataType: QUERY_TYPE_COMPUTE,
  aggregations: QUERY_TYPE_COMPUTE,
  buckets: QUERY_TYPE_COMPUTE,
  templateVariables: QUERY_TYPE_COMPUTE,
  queryText: QUERY_TYPE_COMPUTE,
  constant: QUERY_TYPE_COMPUTE,
  rawSql: QUERY_TYPE_SQL,
  format: QUERY_TYPE_SQL,
} satisfies Record<Exclude<keyof NominalQuery, keyof DataQuery>, QueryAPI>;

// A field whose value is undefined was already removed, so it is not moved or restored.
function splitFields(query: NominalQuery, api: QueryAPI): [rest: NominalQuery, owned: Partial<NominalQuery>] {
  const owned = ([key]: [string, unknown]) => FIELD_API[key as keyof typeof FIELD_API] === api;
  const entries = Object.entries(query).filter(([, value]) => value !== undefined);
  return [Object.fromEntries(entries.filter((entry) => !owned(entry))) as NominalQuery, Object.fromEntries(entries.filter(owned))];
}

// The query's API and that API's field values, to tell whether a query is the one the editor last sent. Grafana
// adds its own fields (hide, datasource) and may reorder keys, so the whole query cannot be compared.
const ownedValues = (query: NominalQuery, api: QueryAPI) =>
  JSON.stringify([api, ...Object.entries(FIELD_API).map(([key, owner]) => (owner === api ? query[key as keyof typeof FIELD_API] : null))]);

// The alert rule editor merges onChange into its stored model, so a removed field must be sent as undefined there.
// Elsewhere it is left out: Explore would see the undefined field as a change and run the query again.
const removedFields = (fields: Partial<NominalQuery>, app?: CoreApp) =>
  app === CoreApp.UnifiedAlerting ? Object.fromEntries(Object.keys(fields).map((key) => [key, undefined])) : {};

export function QueryEditor({ query, onChange, onRunQuery, datasource, app }: Props) {
  const activeAPI: QueryAPI = query.queryType === QUERY_TYPE_SQL ? QUERY_TYPE_SQL : QUERY_TYPE_COMPUTE;
  const inactiveAPI: QueryAPI = activeAPI === QUERY_TYPE_SQL ? QUERY_TYPE_COMPUTE : QUERY_TYPE_SQL;

  // Fields of the API not in use, kept out of the saved query so a leftover such as assetRid '$asset' cannot
  // re-run a SQL panel, and restored on switching back. Explore can replace the query while the editor stays
  // mounted, so they are dropped once a query arrives that the editor did not send.
  const inactive = React.useRef<{ fields: Partial<NominalQuery>; sent: string }>(undefined);
  const latest = React.useRef(query);
  latest.current = query;
  const remembered = React.useCallback(
    () => (inactive.current?.sent === ownedValues(latest.current, activeAPI) ? inactive.current.fields : undefined),
    [activeAPI]
  );

  // Older saved queries can still hold the other API's fields. Move them out on the next edit rather than on load,
  // so opening the editor alone does not modify the dashboard.
  const onEdit = React.useCallback(
    (next: NominalQuery) => {
      const [clean, stale] = splitFields(next, inactiveAPI);
      const fields = Object.keys(stale).length > 0 ? stale : remembered();
      inactive.current = fields && { fields, sent: ownedValues(clean, activeAPI) };
      onChange({ ...removedFields(stale, app), ...clean });
    },
    [activeAPI, inactiveAPI, app, onChange, remembered]
  );

  const onQueryAPIChange = (api: QueryAPI) => {
    if (api === activeAPI) {
      return;
    }
    const [shared, leaving] = splitFields(query, activeAPI);
    const next = { ...remembered(), ...shared, queryType: api };
    inactive.current = { fields: leaving, sent: ownedValues(next, api) };
    onChange({ ...removedFields(leaving, app), ...next });
  };

  return (
    <Stack direction="column" gap={1}>
      <InlineField label="Query API" labelWidth="auto" tooltip="Choose how this query reads Nominal data. Other queries can use either API on the same data source.">
        <RadioButtonGroup
          aria-label="Query API"
          value={activeAPI}
          options={[{ label: 'Compute', value: QUERY_TYPE_COMPUTE }, { label: 'SQL', value: QUERY_TYPE_SQL }]}
          onChange={onQueryAPIChange}
        />
      </InlineField>
      {activeAPI === QUERY_TYPE_SQL
        ? <SqlQueryEditor query={query} onChange={onEdit} onRunQuery={onRunQuery} />
        : <BuilderQueryEditor query={query} onChange={onEdit} onRunQuery={onRunQuery} datasource={datasource} />}
    </Stack>
  );
}
