import { DEFAULT_AGGREGATIONS, AggregationType } from '../../types';
import type { Asset } from '../../utils/api';
import { resolveTemplateValue } from './templateResolution';
import {
  buildAssetOptions,
  buildChannelOptions,
  buildDataScopeOptions,
  channelsToOptions,
  getAggregationValue,
  getAssetPickerValue,
  getChannelSelectValue,
  NUMERIC_AGG_OPTIONS,
  toAggregationComboboxOptions,
  toChannelOption,
} from './queryBuilderOptions';

const assetA: Asset = {
  rid: 'ri.scout.main.asset.a',
  title: 'Asset A',
  labels: ['flight'],
  dataScopes: [
    {
      dataScopeName: 'default',
      dataSource: { type: 'dataset', dataset: 'ri.scout.main.dataset.a' },
      timestampType: 'ABSOLUTE',
      seriesTags: {},
    },
  ],
  properties: {},
};

const assetB: Asset = {
  ...assetA,
  rid: 'ri.scout.main.asset.b',
  title: 'Asset B',
};

describe('queryBuilderOptions', () => {
  it('inserts asset combobox options with selected assets and descriptions', () => {
    const options = buildAssetOptions({
      assets: [assetA],
      selectedAsset: assetB,
      assetRid: resolveTemplateValue(assetB.rid, (value) => value),
    });

    expect(options).toEqual([
      {
        label: 'Asset B',
        value: assetB.rid,
        description: 'flight - 1 data scope(s)',
      },
      {
        label: 'Asset A',
        value: assetA.rid,
        description: 'flight - 1 data scope(s)',
      },
    ]);
  });

  it('falls back to no labels and zero supported data scopes in asset combobox descriptions', () => {
    const assetWithoutLabelsOrScopes: Asset = {
      ...assetA,
      rid: 'ri.scout.main.asset.empty',
      title: 'Empty Asset',
      labels: [],
      dataScopes: [],
    };

    const options = buildAssetOptions({
      assets: [assetWithoutLabelsOrScopes],
      selectedAsset: null,
      assetRid: resolveTemplateValue(assetWithoutLabelsOrScopes.rid, (value) => value),
    });

    expect(options[0]).toEqual({
      label: 'Empty Asset',
      value: assetWithoutLabelsOrScopes.rid,
      description: 'No labels - 0 data scope(s)',
    });
  });

  it('adds a template-variable asset combobox option with resolved title when available', () => {
    const options = buildAssetOptions({
      assets: [],
      selectedAsset: assetA,
      assetRid: resolveTemplateValue('$asset', () => assetA.rid),
    });

    expect(options[0]).toEqual({
      label: '$asset \u2192 Asset A',
      value: '$asset',
      description: 'Template variable',
    });
  });

  it('returns variable picker value unchanged and direct picker value only when present', () => {
    const options = [{ label: 'Asset A', value: assetA.rid }];

    expect(getAssetPickerValue({ assetRid: resolveTemplateValue('$asset', () => assetA.rid), assetOptions: options })).toBe('$asset');
    expect(getAssetPickerValue({ assetRid: resolveTemplateValue(assetA.rid, (value) => value), assetOptions: options })).toBe(assetA.rid);
    expect(getAssetPickerValue({ assetRid: resolveTemplateValue(assetB.rid, (value) => value), assetOptions: options })).toBe('');
  });

  it('adds data scope template-variable labels only when the resolved scope is valid', () => {
    expect(
      buildDataScopeOptions({
        dataScopes: ['primary', 'backup'],
        dataScopeName: resolveTemplateValue('$scope', () => 'primary'),
      })[0]
    ).toEqual({ label: '$scope → primary', value: '$scope' });

    expect(
      buildDataScopeOptions({
        dataScopes: ['primary'],
        dataScopeName: resolveTemplateValue('$scope', () => 'missing'),
      })[0]
    ).toEqual({ label: '$scope', value: '$scope' });

    // Scopes not loaded yet (empty list): a resolved variable is still treated as valid.
    expect(
      buildDataScopeOptions({
        dataScopes: [],
        dataScopeName: resolveTemplateValue('$scope', () => 'primary'),
      })[0]
    ).toEqual({ label: '$scope → primary', value: '$scope' });
  });

  it('maps channels to dense combobox options while preserving real descriptions and data types', () => {
    const options = channelsToOptions([
      { name: 'temperature', dataSource: 'ds', description: 'Ambient temperature', dataType: 'numeric' },
      { name: 'status', dataSource: 'ds', description: '', dataType: 'string' },
      { name: 'app.logs', dataSource: 'logs', description: '', dataType: 'log' },
    ]);

    expect(options[0]).toEqual({
      label: 'temperature',
      value: 'temperature',
      description: 'Ambient temperature',
      dataType: 'numeric',
    });
    expect(options[1]).toEqual({
      label: 'status',
      value: 'status',
      dataType: 'string',
    });
    expect(options[2]).toEqual({
      label: 'app.logs',
      value: 'app.logs',
      dataType: 'log',
    });
    expect(options[1]).not.toHaveProperty('description');
    expect(options[1]).not.toHaveProperty('icon');
    expect(options[2]).not.toHaveProperty('description');
    expect(options[2]).not.toHaveProperty('icon');
  });

  it('adds template-variable channel labels', () => {
    const options = channelsToOptions([
      { name: 'temperature', dataSource: 'ds', description: 'Ambient temperature', dataType: 'numeric' },
    ]);

    expect(buildChannelOptions({ channelResults: options, channel: resolveTemplateValue('$chan', () => 'temperature') })[0]).toEqual({
      label: '$chan → temperature',
      value: '$chan',
    });
  });

  it('builds channel combobox values for empty, plain, resolved, and unresolved channels', () => {
    expect(getChannelSelectValue({ channel: resolveTemplateValue('', (value) => value) })).toBeNull();
    expect(
      getChannelSelectValue({
        channel: resolveTemplateValue('temperature', (value) => value),
        channelDataType: 'numeric',
      })
    ).toEqual({
      label: 'temperature',
      value: 'temperature',
      dataType: 'numeric',
    });
    expect(getChannelSelectValue({ channel: resolveTemplateValue('$chan', () => 'temperature') })).toEqual({
      label: '$chan → temperature',
      value: '$chan',
    });
    expect(getChannelSelectValue({ channel: resolveTemplateValue('$chan', (value) => value) })).toEqual({
      label: '$chan',
      value: '$chan',
    });
    expect(getChannelSelectValue({ channel: resolveTemplateValue('$chan', () => '$other') })).toEqual({
      label: '$chan',
      value: '$chan',
    });
  });

  it('normalizes channel picker selections while preserving selected data type', () => {
    expect(toChannelOption({ label: 'temperature', value: 'temperature', dataType: 'numeric' })).toEqual({
      label: 'temperature',
      value: 'temperature',
      dataType: 'numeric',
    });

    expect(toChannelOption({ label: 'manual.channel', value: 'manual.channel' })).toEqual({
      label: 'manual.channel',
      value: 'manual.channel',
    });
  });

  it('maps aggregation options to dense combobox options without undefined descriptions', () => {
    const options = toAggregationComboboxOptions([
      { label: 'Mean', value: AggregationType.Mean },
      { label: 'Explained', value: 'explained', description: 'Shown in the menu' },
    ]);

    expect(options[0]).toEqual({
      label: 'Mean',
      value: AggregationType.Mean,
    });
    expect(options[0]).not.toHaveProperty('description');
    expect(options[1]).toEqual({
      label: 'Explained',
      value: 'explained',
      description: 'Shown in the menu',
    });
  });

  it('falls back to default numeric aggregations when saved aggregations are empty', () => {
    expect(getAggregationValue(undefined)).toEqual(DEFAULT_AGGREGATIONS);
    expect(getAggregationValue([])).toEqual(DEFAULT_AGGREGATIONS);
    expect(getAggregationValue([AggregationType.Min, AggregationType.Max])).toEqual([AggregationType.Min, AggregationType.Max]);
  });

  it('keeps numeric aggregation options in QueryEditor order', () => {
    expect(NUMERIC_AGG_OPTIONS).toEqual([
      { label: 'Mean', value: AggregationType.Mean },
      { label: 'Min', value: AggregationType.Min },
      { label: 'Max', value: AggregationType.Max },
      { label: 'Count', value: AggregationType.Count },
      { label: 'Variance', value: AggregationType.Variance },
      { label: 'First', value: AggregationType.FirstPoint },
      { label: 'Last', value: AggregationType.LastPoint },
    ]);
  });
});
