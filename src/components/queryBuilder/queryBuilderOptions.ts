import type { SelectableValue } from '@grafana/data';
import { AggregationType, DEFAULT_AGGREGATIONS } from '../../types';
import { assetToOption, type Asset, type Channel } from '../../utils/api';

export type ChannelOption = SelectableValue<string> & { dataType?: string };

export const DATA_TYPE_ICONS: Record<string, string> = {
  string: 'font',
  numeric: 'calculator-alt',
  log: 'gf-logs',
};

export const NUMERIC_AGG_OPTIONS = [
  { label: 'Mean', value: AggregationType.Mean },
  { label: 'Min', value: AggregationType.Min },
  { label: 'Max', value: AggregationType.Max },
  { label: 'Count', value: AggregationType.Count },
  { label: 'Variance', value: AggregationType.Variance },
  { label: 'First', value: AggregationType.FirstPoint },
  { label: 'Last', value: AggregationType.LastPoint },
];

export function buildAssetOptions({
  assets,
  selectedAsset,
  currentAssetRid,
}: {
  assets: Asset[];
  selectedAsset: Asset | null;
  currentAssetRid: string;
}): Array<SelectableValue<string>> {
  const options = assets.map(assetToOption);

  if (selectedAsset && !assets.some((asset) => asset.rid === selectedAsset.rid)) {
    options.unshift(assetToOption(selectedAsset));
  }

  if (currentAssetRid.includes('$') && !options.some((option) => option.value === currentAssetRid)) {
    const resolvedTitle = selectedAsset?.title;
    const label = resolvedTitle && !resolvedTitle.includes('$') ? `${currentAssetRid} → ${resolvedTitle}` : currentAssetRid;
    options.unshift({
      label,
      value: currentAssetRid,
      description: 'Template variable',
    });
  }

  return options;
}

export function getAssetSelectValue({
  currentAssetRid,
  resolvedAssetRid,
  assetOptions,
}: {
  currentAssetRid: string;
  resolvedAssetRid: string;
  assetOptions: Array<SelectableValue<string>>;
}): string {
  if (currentAssetRid.includes('$')) {
    return currentAssetRid;
  }
  return assetOptions.some((option) => option.value === resolvedAssetRid) ? resolvedAssetRid : '';
}

export function buildDataScopeOptions({
  dataScopes,
  currentDataScopeName,
  resolvedDataScopeName,
}: {
  dataScopes: string[];
  currentDataScopeName: string;
  resolvedDataScopeName: string;
}): Array<SelectableValue<string>> {
  const options = dataScopes.map((scope) => ({
    label: scope,
    value: scope,
  }));

  if (currentDataScopeName.includes('$') && !dataScopes.includes(currentDataScopeName)) {
    const resolvedIsValid =
      resolvedDataScopeName &&
      resolvedDataScopeName !== currentDataScopeName &&
      !resolvedDataScopeName.includes('$') &&
      (!dataScopes.length || dataScopes.includes(resolvedDataScopeName));
    options.unshift({
      label: resolvedIsValid ? `${currentDataScopeName} → ${resolvedDataScopeName}` : currentDataScopeName,
      value: currentDataScopeName,
    });
  }

  return options;
}

export function channelsToOptions(channels: Channel[]): ChannelOption[] {
  return channels.map((channel) => ({
    label: channel.name,
    value: channel.name,
    description: channel.description || `Channel: ${channel.name}`,
    dataType: channel.dataType || '',
    icon: channel.dataType ? DATA_TYPE_ICONS[channel.dataType] : undefined,
  }));
}

export function buildChannelOptions({
  channelResults,
  currentChannel,
  resolvedChannel,
}: {
  channelResults: ChannelOption[];
  currentChannel: string;
  resolvedChannel: string;
}): ChannelOption[] {
  const options = [...channelResults];
  if (currentChannel.includes('$') && !options.some((option) => option.value === currentChannel)) {
    const resolvedIsValid = resolvedChannel && resolvedChannel !== currentChannel && !resolvedChannel.includes('$');
    options.unshift({
      label: resolvedIsValid ? `${currentChannel} → ${resolvedChannel}` : currentChannel,
      value: currentChannel,
    });
  }
  return options;
}

export function getChannelSelectValue({
  currentChannel,
  resolvedChannel,
}: {
  currentChannel: string;
  resolvedChannel: string;
}): SelectableValue<string> | null {
  if (!currentChannel) {
    return null;
  }
  return {
    label:
      currentChannel.includes('$') && resolvedChannel && resolvedChannel !== currentChannel && !resolvedChannel.includes('$')
        ? `${currentChannel} → ${resolvedChannel}`
        : currentChannel,
    value: currentChannel,
  };
}

export function getAggregationValue(aggregations: string[] | undefined): string[] {
  return aggregations?.length ? aggregations : [...DEFAULT_AGGREGATIONS];
}
