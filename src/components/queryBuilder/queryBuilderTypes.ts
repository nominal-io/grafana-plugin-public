import type { SelectableValue } from '@grafana/data';
import type { ComboboxOption } from '@grafana/ui';
import type { Asset, Dataset } from '../../utils/api';

export type PickerOption = ComboboxOption<string>;
export type AssetOption = PickerOption;
export type DataScopeOption = PickerOption;
export type ChannelOption = PickerOption & { dataType?: string };
export type ChannelOptionsLoader = (searchText: string) => Promise<ChannelOption[]>;
export type DatasetOptionsLoader = (searchText: string) => Promise<PickerOption[]>;
export type AssetOptionsLoader = (searchText: string) => Promise<AssetOption[]>;

export type AggregationDisplayKind = 'string' | 'log' | 'numeric';
export type AggregationOption = SelectableValue<string> & { value: string; group?: string };

export interface AggregationState {
  kind: AggregationDisplayKind;
  tooltip: string;
  value: string[];
  options: AggregationOption[];
}

export interface ChannelSource {
  complete: boolean;
  dataSourceRids: string[];
  canSearchChannels: boolean;
  channelPickerKey: string;
}

export type SourceMode = 'asset' | 'dataset';

export interface QueryBuilderState {
  selectedAsset: Asset | null;
  assetOptions: AssetOptionsLoader;
  assetSelectValue: AssetOption | null;
  assetError: string | undefined;
  selectedDataset: Dataset | null;
  datasetOptions: DatasetOptionsLoader;
  datasetSelectValue: PickerOption | null;
  datasetError: string | undefined;
  dataScopeOptions: DataScopeOption[];
  channelOptions: ChannelOptionsLoader;
  channelSelectValue: ChannelOption | null;
  resolvedChannel: string;
  assetComplete: boolean;
  configComplete: boolean;
  sourceComplete: boolean;
  channelPickerKey: string;
  hasChannelSearch: boolean;
  showCopiedMessage: boolean;
  aggregationState: AggregationState;
}

export interface QueryBuilderCommands {
  selectAsset: (assetRid: string) => void;
  selectDataset: (datasetRid: string) => void;
  selectDataScope: (dataScopeName: string) => void;
  selectChannel: (selection: ChannelOption) => void;
  changeAggregations: (selected: Array<SelectableValue<string>>) => void;
  copyRid: (rid: string) => void;
}

export interface QueryBuilderModel {
  state: QueryBuilderState;
  commands: QueryBuilderCommands;
}
