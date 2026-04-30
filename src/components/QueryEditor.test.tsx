import React from 'react';
import { act, render, waitFor } from '@testing-library/react';
import { fetchAssetByRid, QueryEditor } from './QueryEditor';
import { NominalQuery } from '../types';
import { DataSource } from '../datasource';

const DATASOURCE_URL = '/api/datasources/uid/test/resources';
const VALID_RID = 'ri.scout.main.asset.abc-123';
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

// Shared `post` mock. Unit tests (fetchAssetByRid) set per-test responses
// via mockResolvedValue/mockRejectedValue. Component tests install a
// URL-routing implementation below in their describe block.
const post = jest.fn();

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getBackendSrv: jest.fn(() => ({ post })),
  getTemplateSrv: jest.fn(() => ({
    replace: (v: string) => {
      if (v === '$logChan') {
        return 'app.logs';
      }
      return v;
    },
  })),
}));

beforeEach(() => {
  post.mockReset();
});

describe('fetchAssetByRid', () => {
  it('returns null for empty RID', async () => {
    const result = await fetchAssetByRid(DATASOURCE_URL, '');
    expect(result).toBeNull();
    expect(post).not.toHaveBeenCalled();
  });

  it('returns null for RID not starting with "ri."', async () => {
    const result = await fetchAssetByRid(DATASOURCE_URL, 'not-a-valid-rid');
    expect(result).toBeNull();
    expect(post).not.toHaveBeenCalled();
  });

  it('calls batch lookup endpoint with correct URL and payload', async () => {
    post.mockResolvedValue({});
    await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(post).toHaveBeenCalledWith(
      `${DATASOURCE_URL}/scout/v1/asset/multiple`,
      [VALID_RID]
    );
  });

  it('returns the asset when found with dataScopes', async () => {
    const asset = {
      rid: VALID_RID,
      title: 'Test Asset',
      labels: [],
      dataScopes: [{ dataScopeName: 'ds1', dataSource: { type: 'dataset' } }],
    };
    post.mockResolvedValue({ [VALID_RID]: asset });

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toEqual(asset);
  });

  it('returns null when asset has empty dataScopes', async () => {
    post.mockResolvedValue({
      [VALID_RID]: { rid: VALID_RID, title: 'Empty', dataScopes: [] },
    });

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toBeNull();
  });

  it('returns null when RID is not in response map', async () => {
    post.mockResolvedValue({});

    const result = await fetchAssetByRid(DATASOURCE_URL, VALID_RID);
    expect(result).toBeNull();
  });

  it('propagates API errors to caller', async () => {
    post.mockRejectedValue(new Error('Network error'));

    await expect(fetchAssetByRid(DATASOURCE_URL, VALID_RID)).rejects.toThrow('Network error');
  });
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

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    const channelTypeUpdates = onChange.mock.calls.filter(
      (call) => call[0]?.channelDataType !== undefined
    );
    expect(channelTypeUpdates).toHaveLength(0);
    expect(warnSpy).toHaveBeenCalled();

    warnSpy.mockRestore();
  });
});
