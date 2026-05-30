import type { SelectableValue } from '@grafana/data';
import { AggregationType, DEFAULT_AGGREGATIONS } from '../../types';
import { assetToOption, type Asset, type Channel } from '../../utils/api';
import { templateDisplayLabel, type TemplateValueResolution } from './templateResolution';
import type { ChannelOption } from './queryBuilderTypes';

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
  assetRid,
}: {
  assets: Asset[];
  selectedAsset: Asset | null;
  assetRid: TemplateValueResolution;
}): Array<SelectableValue<string>> {
  const options = assets.map(assetToOption);

  if (selectedAsset && !assets.some((asset) => asset.rid === selectedAsset.rid)) {
    options.unshift(assetToOption(selectedAsset));
  }

  if (assetRid.hasTemplate && !options.some((option) => option.value === assetRid.raw)) {
    const resolvedTitle = selectedAsset?.title;
    const label = resolvedTitle && !resolvedTitle.includes('$') ? `${assetRid.raw} \u2192 ${resolvedTitle}` : assetRid.raw;
    options.unshift({
      label,
      value: assetRid.raw,
      description: 'Template variable',
    });
  }

  return options;
}

export function getAssetSelectValue({
  assetRid,
  assetOptions,
}: {
  assetRid: TemplateValueResolution;
  assetOptions: Array<SelectableValue<string>>;
}): string {
  if (assetRid.hasTemplate) {
    return assetRid.raw;
  }
  return assetOptions.some((option) => option.value === assetRid.resolved) ? assetRid.resolved : '';
}

export function buildDataScopeOptions({
  dataScopes,
  dataScopeName,
}: {
  dataScopes: string[];
  dataScopeName: TemplateValueResolution;
}): Array<SelectableValue<string>> {
  const options = dataScopes.map((scope) => ({
    label: scope,
    value: scope,
  }));

  if (dataScopeName.hasTemplate && !dataScopes.includes(dataScopeName.raw)) {
    const resolvedIsValid =
      dataScopeName.resolved &&
      dataScopeName.resolved !== dataScopeName.raw &&
      dataScopeName.isResolved &&
      (!dataScopes.length || dataScopes.includes(dataScopeName.resolved));
    options.unshift({
      label: resolvedIsValid ? templateDisplayLabel(dataScopeName) : dataScopeName.raw,
      value: dataScopeName.raw,
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
  channel,
}: {
  channelResults: ChannelOption[];
  channel: TemplateValueResolution;
}): ChannelOption[] {
  const options = [...channelResults];
  if (channel.hasTemplate && !options.some((option) => option.value === channel.raw)) {
    options.unshift({
      label: channel.isResolved && channel.resolved ? templateDisplayLabel(channel) : channel.raw,
      value: channel.raw,
    });
  }
  return options;
}

export function getChannelSelectValue({
  channel,
}: {
  channel: TemplateValueResolution;
}): SelectableValue<string> | null {
  if (!channel.raw) {
    return null;
  }
  return {
    label: templateDisplayLabel(channel),
    value: channel.raw,
  };
}

export function getAggregationValue(aggregations: string[] | undefined): string[] {
  return aggregations?.length ? aggregations : [...DEFAULT_AGGREGATIONS];
}
