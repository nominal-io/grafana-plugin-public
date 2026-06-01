import type { SelectableValue } from '@grafana/data';
import type { Asset } from '../../utils/api';

export type AssetInputMethod = 'search' | 'direct';

export type ChannelOption = SelectableValue<string> & { dataType?: string };

export type AggregationDisplayKind = 'string' | 'log' | 'numeric';
export type AggregationOption = SelectableValue<string> & { value: string };

export interface AggregationState {
  kind: AggregationDisplayKind;
  tooltip: string;
  value: string[];
  options: AggregationOption[];
}

export interface QueryBuilderState {
  assetInputMethod: AssetInputMethod;
  directRID: string;
  searchQuery: string;
  selectedAsset: Asset | null;
  assetSearchResultCount: number;
  selectedAssetSupportedScopeCount: number;
  assetOptions: Array<SelectableValue<string>>;
  assetSelectValue: string;
  dataScopeOptions: Array<SelectableValue<string>>;
  channelOptions: ChannelOption[];
  channelSelectValue: SelectableValue<string> | null;
  isLoadingAssets: boolean;
  isLoadingChannels: boolean;
  resolvedAssetRid: string;
  resolvedDataScopeName: string;
  resolvedChannel: string;
  assetComplete: boolean;
  configComplete: boolean;
  hasChannelSearch: boolean;
  showCopiedMessage: boolean;
  aggregationState: AggregationState;
}

export interface QueryBuilderCommands {
  changeAssetInputMethod: (method: AssetInputMethod) => void;
  changeAssetSearchQuery: (searchQuery: string) => void;
  runAssetSearch: () => void;
  selectAsset: (selection: SelectableValue<string>) => void;
  changeDirectRID: (rid: string) => void;
  selectDataScope: (dataScopeName: string) => void;
  searchChannels: (searchText: string) => void;
  openChannelMenu: () => void;
  selectChannel: (selection: ChannelOption) => void;
  changeAggregations: (selected: Array<SelectableValue<string>>) => void;
  copySelectedAssetRid: () => void;
}

export interface QueryBuilderModel {
  state: QueryBuilderState;
  commands: QueryBuilderCommands;
}
