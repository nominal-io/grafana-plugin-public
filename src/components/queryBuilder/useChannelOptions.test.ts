// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, renderHook, waitFor } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import { searchChannels, type Asset } from '../../utils/api';
import type { AssetInputMethod } from './queryBuilderTypes';
import { useChannelOptions } from './useChannelOptions';

const publish = jest.fn();
jest.mock('@grafana/runtime', () => ({
  getTemplateSrv: jest.fn(() => ({ replace: (v: string) => v })),
  getAppEvents: jest.fn(() => ({ publish })),
}));

jest.mock('../../utils/api', () => ({
  ...jest.requireActual('../../utils/api'),
  searchChannels: jest.fn(),
}));

const mockSearchChannels = searchChannels as jest.MockedFunction<typeof searchChannels>;

const ASSET: Asset = {
  rid: 'ri.scout.main.asset.a',
  title: 'Asset A',
  labels: [],
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

function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
  return { refId: 'A', dataScopeName: 'default', ...overrides } as NominalQuery;
}

function args(overrides: Record<string, unknown> = {}) {
  return {
    query: makeQuery(),
    onChange: jest.fn(),
    selectedAsset: ASSET as Asset | null,
    assetInputMethod: 'search' as AssetInputMethod,
    resolvedChannel: '',
    resolvedDataScopeName: 'default',
    datasourceUrl: '/api/x',
    markInteracted: jest.fn(),
    ...overrides,
  };
}

describe('useChannelOptions', () => {
  beforeEach(() => {
    publish.mockReset();
    mockSearchChannels.mockReset();
    jest.useFakeTimers();
  });
  afterEach(() => jest.useRealTimers());

  it('openChannelMenu loads channel options into state', async () => {
    mockSearchChannels.mockResolvedValue([{ name: 'temp', dataSource: 'ds', description: '', dataType: 'numeric' }]);
    const { result } = renderHook(() => useChannelOptions(args()));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.openChannelMenu();
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.channelOptions.some((o) => o.value === 'temp')).toBe(true);
    });
  });

  it('selectChannel marks interaction and emits channel + dataType', () => {
    const onChange = jest.fn();
    const markInteracted = jest.fn();
    const { result } = renderHook(() => useChannelOptions(args({ onChange, markInteracted })));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.selectChannel({ label: 'temp', value: 'temp', dataType: 'numeric' });
    });

    expect(markInteracted).toHaveBeenCalled();
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ channel: 'temp', channelDataType: 'numeric' }));
  });

  it('returns no channel options and does not call the API without a selected asset', async () => {
    const { result } = renderHook(() => useChannelOptions(args({ selectedAsset: null })));

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      result.current.openChannelMenu();
    });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      jest.advanceTimersByTime(300);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.channelOptions).toEqual([]);
    });
    expect(mockSearchChannels).not.toHaveBeenCalled();
  });
});
