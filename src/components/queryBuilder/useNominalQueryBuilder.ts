import { useCallback, useEffect, useRef, useState } from 'react';
import { getTemplateSrv } from '@grafana/runtime';
import type { NominalQuery } from '../../types';
import type { QueryBuilderModel } from './queryBuilderTypes';
import { useAssetSelection } from './useAssetSelection';
import { useChannelOptions } from './useChannelOptions';
import { useAggregationRun } from './useAggregationRun';
import { useRunSelection } from './useRunSelection';
import { resolveQueryTemplateValues, resolveTemplateValue } from './templateResolution';

export { AGGREGATION_RUN_DELAY_MS } from './useAggregationRun';

interface UseNominalQueryBuilderArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  onRunQuery: () => void;
  datasourceUrl: string;
}

export function useNominalQueryBuilder({
  query,
  onChange,
  onRunQuery,
  datasourceUrl,
}: UseNominalQueryBuilderArgs): QueryBuilderModel {
  // Track whether the user has interacted with query fields - prevents auto-clearing on
  // initial load. Cross-cutting: written by asset + channel commands, read by the asset
  // dependent-fields effect. Owned here so it is single-sourced.
  const [hasUserInteracted, setHasUserInteracted] = useState(false);
  const markInteracted = useCallback(() => setHasUserInteracted(true), []);
  const [showCopiedMessage, setShowCopiedMessage] = useState(false);
  const copiedTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Compute resolved values on every render - these change when template variables change.
  // Child hooks memoize work from the primitive fields, not these object identities.
  const replaceTemplateValue = useCallback((value: string) => getTemplateSrv().replace(value), []);
  const queryResolution = resolveQueryTemplateValues({ query, replace: replaceTemplateValue });
  const resolveTemplateText = useCallback(
    (value: string) => resolveTemplateValue(value, replaceTemplateValue),
    [replaceTemplateValue]
  );

  const runSelection = useRunSelection({
    query,
    onChange,
    datasourceUrl,
    queryResolution,
    replace: replaceTemplateValue,
    markInteracted,
  });
  const { isRunMode, cascade } = runSelection;
  const { query: builderQuery, onChange: builderOnChange, resolution: builderResolution } = cascade;

  const asset = useAssetSelection({
    query: builderQuery,
    onChange: builderOnChange,
    datasourceUrl,
    assetRidResolution: builderResolution.assetRid,
    dataScopeResolution: builderResolution.dataScopeName,
    resolveTemplateText,
    hasUserInteracted,
    markInteracted,
  });

  const channel = useChannelOptions({
    query: builderQuery,
    onChange: builderOnChange,
    selectedAsset: asset.selectedAsset,
    channelResolution: builderResolution.channel,
    dataScopeResolution: builderResolution.dataScopeName,
    datasourceUrl,
    markInteracted,
  });

  const aggregation = useAggregationRun({ query: builderQuery, onChange: builderOnChange, onRunQuery });

  const showCopiedForDuration = useCallback(() => {
    clearTimeout(copiedTimerRef.current);
    setShowCopiedMessage(true);
    copiedTimerRef.current = setTimeout(() => {
      setShowCopiedMessage(false);
    }, 2000);
  }, []);

  const copyToClipboard = useCallback(
    async (text: string) => {
      try {
        await navigator.clipboard.writeText(text);
      } catch {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        document.body.appendChild(textArea);
        textArea.select();
        // eslint-disable-next-line @typescript-eslint/no-deprecated
        document.execCommand('copy');
        document.body.removeChild(textArea);
      }

      showCopiedForDuration();
    },
    [showCopiedForDuration]
  );

  const copySelectedAssetRid = useCallback(() => {
    if (asset.selectedAsset) {
      copyToClipboard(asset.selectedAsset.rid);
    }
  }, [copyToClipboard, asset.selectedAsset]);

  useEffect(() => {
    return () => clearTimeout(copiedTimerRef.current);
  }, []);

  // Step completion status. Asset is complete only when the saved RID actually
  // resolves - a template variable without a value does not open the
  // scope/channel fields.
  const assetComplete = builderResolution.assetRid.resolved !== '' && builderResolution.assetRid.isResolved;
  const configComplete = assetComplete && Boolean(query?.dataScopeName) && Boolean(query?.channel);
  // Show the channel selector whenever an asset is selected (even if dataScopes is empty).
  const hasChannelSearch = asset.selectedAsset !== null;

  return {
    state: {
      isRunMode,
      run: runSelection.run,
      runOptions: runSelection.runOptions,
      runSelectValue: runSelection.runSelectValue,
      selectedAsset: asset.selectedAsset,
      assetOptions: asset.assetOptions,
      assetSelectValue: asset.assetSelectValue,
      dataScopeOptions: asset.dataScopeOptions,
      channelOptions: channel.channelOptions,
      channelSelectValue: channel.channelSelectValue,
      resolvedAssetRid: builderResolution.assetRid.resolved,
      resolvedDataScopeName: builderResolution.dataScopeName.resolved,
      resolvedChannel: builderResolution.channel.resolved,
      assetComplete,
      configComplete,
      hasChannelSearch,
      showCopiedMessage,
      aggregationState: aggregation.aggregationState,
    },
    commands: {
      selectRun: runSelection.selectRun,
      selectAsset: asset.selectAsset,
      selectDataScope: asset.selectDataScope,
      selectChannel: channel.selectChannel,
      changeAggregations: aggregation.changeAggregations,
      copySelectedAssetRid,
    },
  };
}
