import React from 'react';
import { css, keyframes } from '@emotion/css';
import {
  InlineField,
  Input,
  Stack,
  Select,
  MultiCombobox,
  RadioButtonGroup,
  useStyles2,
  type ComboboxOption,
} from '@grafana/ui';
import type { GrafanaTheme2, QueryEditorProps } from '@grafana/data';
import type { DataSource } from '../datasource';
import type { NominalDataSourceOptions, NominalQuery } from '../types';
import { useNominalQueryBuilder } from './queryBuilder/useNominalQueryBuilder';

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
    () =>
      state.aggregationState.options
        .map((option): ComboboxOption<string> | null =>
          option.value
            ? {
                label: option.label,
                value: option.value,
                // Only attach description when present. Grafana's Combobox sizes rows by
                // `'description' in option`, so an always-present `description: undefined`
                // forces every row to the taller description height and makes the 7
                // aggregation options scroll (regression vs. passing bare options).
                ...(option.description ? { description: option.description } : {}),
              }
            : null
        )
        .filter((option): option is ComboboxOption<string> => option !== null),
    [state.aggregationState.options]
  );

  return (
    <div className={styles.root}>
      <div className={styles.editorBox(state.configComplete)}>
        <Stack gap={1} direction="row" wrap alignItems="center">
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
            <>
              <InlineField label="Search" labelWidth={8}>
                <Input
                  placeholder="Search assets"
                  value={state.searchQuery}
                  onChange={(event) => commands.changeAssetSearchQuery(event.currentTarget.value)}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter') {
                      commands.runAssetSearch();
                    }
                  }}
                  width={20}
                />
              </InlineField>

              {state.assetSearchResultCount > 0 && (
                <InlineField label="Asset" labelWidth={8}>
                  {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                  <Select
                    key={`asset-select-${state.assetSearchResultCount}-${state.selectedAsset?.rid || ''}`}
                    options={state.assetOptions}
                    value={state.assetSelectValue}
                    onChange={commands.selectAsset}
                    width={30}
                    placeholder="Choose asset..."
                    isLoading={state.isLoadingAssets}
                    isClearable={false}
                    allowCustomValue={true}
                  />
                </InlineField>
              )}
            </>
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

          {/* Channel Selection - only show if asset is selected */}
          {state.assetComplete && (
            <>
              <InlineField label="Data scope" labelWidth={12}>
                {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                <Select
                  value={query?.dataScopeName || ''}
                  onChange={(value) => commands.selectDataScope(value?.value || '')}
                  options={state.dataScopeOptions}
                  placeholder="Choose scope or use $variable..."
                  width={30}
                  isClearable={false}
                  allowCustomValue={true}
                  isLoading={!state.selectedAsset && state.assetComplete}
                />
              </InlineField>

              {state.hasChannelSearch && (
                <InlineField label="Channel" labelWidth={8}>
                  {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                  <Select
                    key={`${state.resolvedAssetRid || 'no-asset'}-${state.resolvedDataScopeName}`}
                    value={state.channelSelectValue}
                    onChange={commands.selectChannel}
                    options={state.channelOptions}
                    onInputChange={(text, action) => {
                      if (action.action === 'input-change') {
                        commands.searchChannels(text);
                      }
                    }}
                    onOpenMenu={commands.openChannelMenu}
                    isLoading={state.isLoadingChannels}
                    placeholder="Search for channel..."
                    width={50}
                    allowCustomValue
                    isClearable={false}
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
                      width={35}
                    />
                  )}
                </InlineField>
              )}
            </>
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
              {state.selectedAssetSupportedScopeCount}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
