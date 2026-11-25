import { useCallback, useEffect, useRef, useState } from 'react';
import type { ScopedVars } from '@grafana/data';
import { getTemplateSrv } from '@grafana/runtime';
import { isCompleteComputeQuery, type NominalQuery } from '../../types';
import type { QueryBuilderModel, SourceMode } from './queryBuilderTypes';
import { useAssetSelection } from './useAssetSelection';
import { useDatasetSelection } from './useDatasetSelection';
import { useChannelOptions } from './useChannelOptions';
import { useAggregationRun } from './useAggregationRun';
import { resolveQueryTemplateValues, resolveTemplateValue } from './templateResolution';

export { AGGREGATION_RUN_DELAY_MS } from './useAggregationRun';

interface UseNominalQueryBuilderArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  onRunQuery: () => void;
  datasourceUrl: string;
  mode: SourceMode;
  timeZone?: string;
  scopedVars?: ScopedVars;
}

export function useNominalQueryBuilder({
  query,
  onChange,
  onRunQuery,
  datasourceUrl,
  mode,
  timeZone,
  scopedVars,
}: UseNominalQueryBuilderArgs): QueryBuilderModel {
  // Track whether the user has interacted with query fields - prevents auto-clearing on
  // initial load. Cross-cutting: written by the asset, dataset, and channel commands, read
  // by the asset dependent-fields effect. Owned here so it is single-sourced.
  const [hasUserInteracted, setHasUserInteracted] = useState(false);
  const markInteracted = useCallback(() => setHasUserInteracted(true), []);
  const [showCopiedMessage, setShowCopiedMessage] = useState(false);
  const copiedTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Compute resolved values on every render - these change when template variables change.
  // Child hooks memoize work from the primitive fields, not these object identities.
  // The last run's scopedVars carry a repeated panel's own value of the repeat variable.
  const replaceTemplateValue = useCallback(
    (value: string) => getTemplateSrv().replace(value, scopedVars),
    [scopedVars]
  );
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
    // The asset hook fills in data scopes only while Asset is the chosen source, so a lookup that
    // finishes after a switch to Dataset does not write to the query.
    hasUserInteracted: hasUserInteracted && mode === 'asset',
    markInteracted,
    active: mode === 'asset',
  });

  const dataset = useDatasetSelection({
    query,
    onChange,
    datasourceUrl,
    datasetRidResolution: queryResolution.datasetRid,
    markInteracted,
    active: mode === 'dataset',
    timeZone,
  });

  const source = mode === 'dataset' ? dataset.channelSource : asset.channelSource;

  const channel = useChannelOptions({
    query,
    onChange,
    dataSourceRids: source.dataSourceRids,
    channelResolution: queryResolution.channel,
    datasourceUrl,
    hasUserInteracted,
    markInteracted,
  });

  const aggregation = useAggregationRun({
    query,
    onChange,
    onRunQuery,
    channelDataType: channel.channelDataType,
  });

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

  useEffect(() => {
    return () => clearTimeout(copiedTimerRef.current);
  }, []);

  const configComplete = source.complete && isCompleteComputeQuery(query);

  return {
    state: {
      selectedAsset: asset.selectedAsset,
      assetOptions: asset.assetOptions,
      assetSelectValue: asset.assetSelectValue,
      assetError: asset.assetError,
      selectedDataset: dataset.selectedDataset,
      datasetOptions: dataset.datasetOptions,
      datasetSelectValue: dataset.datasetSelectValue,
      datasetError: dataset.datasetError,
      dataScopeOptions: asset.dataScopeOptions,
      channelOptions: channel.channelOptions,
      channelSelectValue: channel.channelSelectValue,
      resolvedChannel: queryResolution.channel.resolved,
      assetComplete: asset.channelSource.complete,
      configComplete,
      sourceComplete: source.complete,
      channelPickerKey: source.channelPickerKey,
      hasChannelSearch: source.canSearchChannels,
      showCopiedMessage,
      aggregationState: aggregation.aggregationState,
    },
    commands: {
      selectAsset: asset.selectAsset,
      selectDataset: dataset.selectDataset,
      selectDataScope: asset.selectDataScope,
      selectChannel: channel.selectChannel,
      changeAggregations: aggregation.changeAggregations,
      copyRid: copyToClipboard,
    },
  };
}
