import { useCallback, useEffect, useRef, useState } from 'react';
import { getTemplateSrv } from '@grafana/runtime';
import type { NominalQuery } from '../../types';
import type { QueryBuilderModel } from './queryBuilderTypes';
import { useAssetSelection } from './useAssetSelection';
import { useChannelOptions } from './useChannelOptions';
import { useAggregationRun } from './useAggregationRun';
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
  // Single source of Grafana template resolution, passed down to the hooks that need it.
  const replaceTemplateValue = useCallback((value: string) => getTemplateSrv().replace(value), []);
  const queryResolution = resolveQueryTemplateValues({ query, replace: replaceTemplateValue });
  const resolveTemplateText = useCallback(
    (value: string) => resolveTemplateValue(value, replaceTemplateValue),
    [replaceTemplateValue]
  );

  const asset = useAssetSelection({
    query,
    onChange,
    datasourceUrl,
    assetRidResolution: queryResolution.assetRid,
    dataScopeResolution: queryResolution.dataScopeName,
    resolveTemplateText,
    hasUserInteracted,
    markInteracted,
  });

  const channel = useChannelOptions({
    query,
    onChange,
    selectedAsset: asset.selectedAsset,
    assetInputMethod: asset.assetInputMethod,
    channelResolution: queryResolution.channel,
    dataScopeResolution: queryResolution.dataScopeName,
    datasourceUrl,
    markInteracted,
  });

  const aggregation = useAggregationRun({ query, onChange, onRunQuery });

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

  // Step completion status
  const assetComplete =
    asset.assetInputMethod === 'search'
      ? queryResolution.assetRid.resolved !== '' && queryResolution.assetRid.isResolved
      : asset.directRID.trim() !== '';
  const configComplete = assetComplete && Boolean(query?.dataScopeName) && Boolean(query?.channel);
  // Show the channel selector whenever an asset is selected (even if dataScopes is empty).
  const hasChannelSearch = asset.selectedAsset !== null;

  return {
    state: {
      assetInputMethod: asset.assetInputMethod,
      directRID: asset.directRID,
      searchQuery: asset.searchQuery,
      selectedAsset: asset.selectedAsset,
      assetSearchResultCount: asset.assetSearchResultCount,
      selectedAssetSupportedScopeCount: asset.selectedAssetSupportedScopeCount,
      assetOptions: asset.assetOptions,
      assetSelectValue: asset.assetSelectValue,
      dataScopeOptions: asset.dataScopeOptions,
      channelOptions: channel.channelOptions,
      channelSelectValue: channel.channelSelectValue,
      isLoadingAssets: asset.isLoadingAssets,
      isLoadingChannels: channel.isLoadingChannels,
      resolvedAssetRid: queryResolution.assetRid.resolved,
      resolvedDataScopeName: queryResolution.dataScopeName.resolved,
      resolvedChannel: queryResolution.channel.resolved,
      assetComplete,
      configComplete,
      hasChannelSearch,
      showCopiedMessage,
      aggregationState: aggregation.aggregationState,
    },
    commands: {
      changeAssetInputMethod: asset.changeAssetInputMethod,
      changeAssetSearchQuery: asset.changeAssetSearchQuery,
      runAssetSearch: asset.runAssetSearch,
      selectAsset: asset.selectAsset,
      changeDirectRID: asset.changeDirectRID,
      selectDataScope: asset.selectDataScope,
      searchChannels: channel.searchChannels,
      openChannelMenu: channel.openChannelMenu,
      selectChannel: channel.selectChannel,
      changeAggregations: aggregation.changeAggregations,
      copySelectedAssetRid,
    },
  };
}
