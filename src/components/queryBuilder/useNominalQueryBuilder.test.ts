// eslint-disable-next-line @typescript-eslint/no-deprecated
import { renderHook } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { useAggregationRun } from './useAggregationRun';
import { useAssetSelection } from './useAssetSelection';
import { useChannelOptions } from './useChannelOptions';
import { useNominalQueryBuilder } from './useNominalQueryBuilder';

const mockReplace = jest.fn((value: string) => value);

jest.mock('@grafana/runtime', () => ({
  getTemplateSrv: jest.fn(() => ({ replace: mockReplace })),
}));

jest.mock('./useAssetSelection', () => ({
  useAssetSelection: jest.fn(),
}));

jest.mock('./useChannelOptions', () => ({
  useChannelOptions: jest.fn(),
}));

jest.mock('./useAggregationRun', () => ({
  AGGREGATION_RUN_DELAY_MS: 400,
  useAggregationRun: jest.fn(),
}));

const mockUseAssetSelection = useAssetSelection as jest.MockedFunction<typeof useAssetSelection>;
const mockUseChannelOptions = useChannelOptions as jest.MockedFunction<typeof useChannelOptions>;
const mockUseAggregationRun = useAggregationRun as jest.MockedFunction<typeof useAggregationRun>;

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return {
    refId: 'A',
    assetRid: '$asset',
    dataScopeName: '$scope',
    channel: '$channel',
    ...overrides,
  } as NominalQuery;
}

describe('useNominalQueryBuilder', () => {
  beforeEach(() => {
    mockReplace.mockReset();
    mockUseAssetSelection.mockReset();
    mockUseChannelOptions.mockReset();
    mockUseAggregationRun.mockReset();

    mockReplace.mockImplementation(
      (value: string) =>
        ({
          '$asset': 'ri.scout.main.asset.a',
          '$scope': 'default',
          '$channel': 'temperature',
        })[value] ?? value
    );

    mockUseAssetSelection.mockReturnValue({
      assetInputMethod: 'search',
      directRID: '',
      searchQuery: '',
      selectedAsset: null,
      assetSearchResultCount: 0,
      selectedAssetSupportedScopeCount: 0,
      assetOptions: [],
      assetSelectValue: '',
      dataScopeOptions: [],
      isLoadingAssets: false,
      changeAssetInputMethod: jest.fn(),
      changeAssetSearchQuery: jest.fn(),
      runAssetSearch: jest.fn(),
      selectAsset: jest.fn(),
      changeDirectRID: jest.fn(),
      selectDataScope: jest.fn(),
    });
    mockUseChannelOptions.mockReturnValue({
      channelOptions: [],
      channelSelectValue: null,
      isLoadingChannels: false,
      searchChannels: jest.fn(),
      openChannelMenu: jest.fn(),
      selectChannel: jest.fn(),
    });
    mockUseAggregationRun.mockReturnValue({
      aggregationState: {
        kind: 'numeric',
        tooltip: '',
        value: [],
        options: [],
      },
      changeAggregations: jest.fn(),
    });
  });

  it('keeps template resolution prop identities stable when resolved values are unchanged', () => {
    const query = makeQuery();
    const onChange = jest.fn();
    const onRunQuery = jest.fn();
    const { rerender } = renderHook(
      ({ target }) =>
        useNominalQueryBuilder({
          query: target,
          onChange,
          onRunQuery,
          datasourceUrl: '/api/x',
        }),
      { initialProps: { target: query } }
    );

    const firstAssetArgs = mockUseAssetSelection.mock.calls[0][0];
    const firstChannelArgs = mockUseChannelOptions.mock.calls[0][0];

    rerender({ target: query });

    const secondAssetArgs = mockUseAssetSelection.mock.calls[1][0];
    const secondChannelArgs = mockUseChannelOptions.mock.calls[1][0];

    expect(secondAssetArgs.assetRidResolution).toBe(firstAssetArgs.assetRidResolution);
    expect(secondAssetArgs.dataScopeResolution).toBe(firstAssetArgs.dataScopeResolution);
    expect(secondChannelArgs.channelResolution).toBe(firstChannelArgs.channelResolution);
    expect(secondChannelArgs.dataScopeResolution).toBe(firstChannelArgs.dataScopeResolution);
  });

  it('refreshes resolution prop identities when template values resolve differently', () => {
    let resolvedAssetRid = 'ri.scout.main.asset.a';
    mockReplace.mockImplementation(
      (value: string) =>
        ({
          '$asset': resolvedAssetRid,
          '$scope': 'default',
          '$channel': 'temperature',
        })[value] ?? value
    );
    const query = makeQuery();
    const onChange = jest.fn();
    const onRunQuery = jest.fn();
    const { rerender } = renderHook(
      ({ target }) =>
        useNominalQueryBuilder({
          query: target,
          onChange,
          onRunQuery,
          datasourceUrl: '/api/x',
        }),
      { initialProps: { target: query } }
    );

    const firstAssetArgs = mockUseAssetSelection.mock.calls[0][0];

    resolvedAssetRid = 'ri.scout.main.asset.b';
    rerender({ target: query });

    const secondAssetArgs = mockUseAssetSelection.mock.calls[1][0];

    expect(secondAssetArgs.assetRidResolution).not.toBe(firstAssetArgs.assetRidResolution);
    expect(secondAssetArgs.assetRidResolution.resolved).toBe('ri.scout.main.asset.b');
    expect(secondAssetArgs.dataScopeResolution).toBe(firstAssetArgs.dataScopeResolution);
  });
});
