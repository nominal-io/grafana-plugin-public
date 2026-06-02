import type { SelectableValue } from '@grafana/data';
import type { ComboboxOption } from '@grafana/ui';
import type { Asset } from '../../utils/api';

export type AssetInputMethod = 'search' | 'direct';

export type PickerOption = ComboboxOption<string>;
export type AssetOption = PickerOption;
export type DataScopeOption = PickerOption;
export type ChannelOption = PickerOption & { dataType?: string };
export type ChannelOptionsLoader = (searchText: string) => Promise<ChannelOption[]>;

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
  assetOptions: AssetOption[];
  assetSelectValue: string;
  dataScopeOptions: DataScopeOption[];
  channelOptions: ChannelOptionsLoader;
  channelSelectValue: ChannelOption | null;
  isLoadingAssets: boolean;
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
  selectAsset: (assetRid: string) => void;
  changeDirectRID: (rid: string) => void;
  selectDataScope: (dataScopeName: string) => void;
  selectChannel: (selection: ChannelOption) => void;
  changeAggregations: (selected: Array<SelectableValue<string>>) => void;
  copySelectedAssetRid: () => void;
}

export interface QueryBuilderModel {
  state: QueryBuilderState;
  commands: QueryBuilderCommands;
}
