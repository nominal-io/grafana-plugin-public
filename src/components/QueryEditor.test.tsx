import React from 'react';
// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, render, screen, waitFor } from '@testing-library/react';
import { QueryEditor } from './QueryEditor';
import { NominalQuery } from '../types';
import { DataSource } from '../datasource';

const DATASOURCE_URL = '/api/datasources/uid/test/resources';
const ASSET_RID = 'ri.scout.main.asset.abc123';
const LOG_DS_RID = 'ri.logset.main.log-set.xyz';

const ASSET = {
  rid: ASSET_RID,
  title: 'Test Asset',
  labels: [],
  dataScopes: [
    {
      dataScopeName: 'default',
      dataSource: { type: 'logSet', logSet: LOG_DS_RID },
    },
  ],
};

// Component tests install a URL-routing implementation in their describe block.
const post = jest.fn();
const publish = jest.fn();

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getBackendSrv: jest.fn(() => ({ post })),
  getAppEvents: jest.fn(() => ({ publish })),
  getTemplateSrv: jest.fn(() => ({
    replace: (v: string) => {
      if (v === '$asset') {
        return ASSET_RID;
      }
      if (v === '$scope') {
        return 'default';
      }
      if (v === '$logChan') {
        return 'app.logs';
      }
      return v;
    },
  })),
}));

beforeEach(() => {
  post.mockReset();
  publish.mockReset();
});

afterEach(() => {
  jest.useRealTimers();
  jest.restoreAllMocks();
});

describe('channel data type inference effect', () => {
  // Per-test overrides for the /channels response routed below.
  let channelsResponse: unknown = { channels: [{ name: 'app.logs', dataType: 'log' }] };
  let channelsShouldReject = false;

  const mockDatasource = { url: DATASOURCE_URL } as unknown as DataSource;

  function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
    return {
      refId: 'A',
      assetRid: ASSET_RID,
      assetInputMethod: 'direct',
      dataScopeName: 'default',
      queryType: 'decimation',
      buckets: 1000,
      ...overrides,
    } as NominalQuery;
  }

  beforeEach(() => {
    channelsResponse = { channels: [{ name: 'app.logs', dataType: 'log' }] };
    channelsShouldReject = false;
    post.mockImplementation(async (url: string, _body?: unknown) => {
      if (url.endsWith('/scout/v1/asset/multiple')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/channels')) {
        if (channelsShouldReject) {
          throw new Error('simulated /channels failure');
        }
        return channelsResponse;
      }
      return {};
    });
  });

  it('restores a saved direct RID query into the direct RID input', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/scout/v1/asset/multiple')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/scout/v1/search-assets')) {
        return { results: [ASSET] };
      }
      return {};
    });

    render(
      <QueryEditor
        query={makeQuery({ assetRid: '$asset', assetInputMethod: 'direct' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    expect(screen.getByDisplayValue('$asset')).toBeInTheDocument();
    await waitFor(() => {
      expect(post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/scout/v1/asset/multiple`)).toBe(true);
    });
  });

  it('restores a saved search-mode asset after search assets load', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/scout/v1/search-assets')) {
        return { results: [ASSET] };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });

    render(
      <QueryEditor
        query={makeQuery({ assetRid: ASSET_RID, assetInputMethod: 'search' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await waitFor(() => {
      expect(screen.getByText('Test Asset')).toBeInTheDocument();
    });
  });

  it('publishes a Grafana alert when channel option loading fails', async () => {
    jest.useFakeTimers();
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/scout/v1/asset/multiple')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/scout/v1/search-assets')) {
        return { results: [ASSET] };
      }
      if (url.endsWith('/channels')) {
        throw new Error('simulated channel failure');
      }
      return {};
    });

    render(
      <QueryEditor
        query={makeQuery({ channel: 'temperature', channelDataType: 'numeric' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await waitFor(() => {
      expect(post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/scout/v1/asset/multiple`)).toBe(true);
    });

    // Flush the debounced channel preload.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => {
      jest.advanceTimersByTime(300);
    });

    await waitFor(() => {
      expect(publish).toHaveBeenCalledWith(
        expect.objectContaining({
          payload: ['Unable to load Nominal channels', 'Check the selected asset, data scope, and data source configuration.'],
        })
      );
    });
  });

  it('calls /channels and emits channelDataType when channel is a template variable', async () => {
    const onChange = jest.fn();

    render(
      <QueryEditor
        query={makeQuery({ channel: '$logChan' })}
        onChange={onChange}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await waitFor(() => {
      expect(
        post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/channels`)
      ).toBe(true);
    });

    const channelsCall = post.mock.calls.find(
      (call) => call[0] === `${DATASOURCE_URL}/channels`
    )!;
    expect(channelsCall[1]).toEqual({
      dataSourceRids: [LOG_DS_RID],
      searchText: 'app.logs',
    });

    await waitFor(() => {
      expect(onChange).toHaveBeenCalledWith(
        expect.objectContaining({ channelDataType: 'log' })
      );
    });
  });

  it('skips /channels when channelDataType is already set for a direct channel', async () => {
    const onChange = jest.fn();

    render(
      <QueryEditor
        query={makeQuery({ channel: 'temperature', channelDataType: 'numeric' })}
        onChange={onChange}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await waitFor(() => {
      expect(
        post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/scout/v1/asset/multiple`)
      ).toBe(true);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    const channelsCalls = post.mock.calls.filter(
      (call) => call[0] === `${DATASOURCE_URL}/channels`
    );
    expect(channelsCalls).toHaveLength(0);
  });

  it('does not crash or emit channelDataType when /channels fetch fails', async () => {
    channelsShouldReject = true;
    const onChange = jest.fn();
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    render(
      <QueryEditor
        query={makeQuery({ channel: '$logChan' })}
        onChange={onChange}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await waitFor(() => {
      expect(
        post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/channels`)
      ).toBe(true);
    });

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    const channelTypeUpdates = onChange.mock.calls.filter(
      (call) => call[0]?.channelDataType !== undefined
    );
    expect(channelTypeUpdates).toHaveLength(0);
    expect(warnSpy).not.toHaveBeenCalled();
  });
});
