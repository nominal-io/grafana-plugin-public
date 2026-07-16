import React from 'react';
import { css, keyframes } from '@emotion/css';
import {
  Combobox,
  InlineField,
  Input,
  Stack,
  MultiCombobox,
  RadioButtonGroup,
  useStyles2,
} from '@grafana/ui';
import type { GrafanaTheme2, QueryEditorProps } from '@grafana/data';
import type { DataSource } from '../datasource';
import type { NominalDataSourceOptions, NominalQuery } from '../types';
import { getSupportedScopeNames } from '../utils/api';
import { useNominalQueryBuilder } from './queryBuilder/useNominalQueryBuilder';
import { toAggregationComboboxOptions, toChannelOption } from './queryBuilder/queryBuilderOptions';

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
  methodToggle: css({
    marginRight: theme.spacing(1),
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

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const styles = useStyles2(getStyles);
  const { state, commands } = useNominalQueryBuilder({
    query,
    onChange,
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
            {/* Asset Input Method */}
            <div className={styles.methodToggle}>
              <RadioButtonGroup
                options={[
                  { label: 'Asset Search', value: 'search' },
                  { label: 'Asset RID', value: 'direct' },
                ]}
                value={state.assetInputMethod}
                onChange={commands.changeAssetInputMethod}
                size="sm"
              />
            </div>

            {/* Asset Selection */}
            {state.assetInputMethod === 'search' ? (
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
            ) : (
              <InlineField label="Asset RID" labelWidth={12}>
                <Input
                  placeholder="ri.scout.cerulean-staging.asset..."
                  value={state.directRID}
                  onChange={(event) => commands.changeDirectRID(event.currentTarget.value)}
                  width={40}
                />
              </InlineField>
            )}

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

              {/* Aggregation selector - shown when a channel is selected */}
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
