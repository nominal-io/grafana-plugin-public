import React from 'react';
// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, render, waitFor } from '@testing-library/react';
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

    warnSpy.mockRestore();
  });
});
