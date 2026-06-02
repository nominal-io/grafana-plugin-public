import React from 'react';
// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
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
const mockComboboxProps = jest.fn();
// Per-test overrides for template variable resolution. Lets a test simulate a
// dashboard variable changing value mid-session (e.g. $asset resolving to a new RID).
let mockReplaceOverrides: Record<string, string> = {};

jest.mock('@grafana/ui', () => {
  const React = jest.requireActual('react');
  const actual = jest.requireActual('@grafana/ui');
  return {
    ...actual,
    Combobox: (props: Record<string, any>) => {
      mockComboboxProps(props);
      const value =
        typeof props.value === 'object'
          ? props.value?.label ?? props.value?.value ?? ''
          : props.value ?? '';
      return React.createElement('input', {
        'data-testid': props['data-testid'] ?? 'mock-combobox',
        'data-id': props.id ?? '',
        'data-prefix-icon': props.prefixIcon ?? '',
        'data-width': props.width ?? '',
        'data-min-width': String(props.minWidth ?? ''),
        'data-max-width': String(props.maxWidth ?? ''),
        'data-loading': String(props.loading ?? false),
        'data-create-custom-value': String(props.createCustomValue ?? false),
        'data-clearable': String(props.isClearable ?? ''),
        placeholder: props.placeholder,
        value,
        onChange: async (event: React.ChangeEvent<HTMLInputElement>) => {
          if (typeof props.options === 'function') {
            await props.options(event.currentTarget.value);
          }
        },
      });
    },
  };
});

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getBackendSrv: jest.fn(() => ({ post })),
  getAppEvents: jest.fn(() => ({ publish })),
  getTemplateSrv: jest.fn(() => ({
    replace: (v: string) => {
      if (v in mockReplaceOverrides) {
        return mockReplaceOverrides[v];
      }
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
  mockComboboxProps.mockReset();
  mockReplaceOverrides = {};
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

  it('fetches a saved direct template RID only once on mount (no restore/resolved double fetch)', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/scout/v1/asset/multiple')) {
        return { [ASSET_RID]: ASSET };
      }
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
        query={makeQuery({ assetRid: '$asset', assetInputMethod: 'direct' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await waitFor(() => {
      expect(post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/scout/v1/asset/multiple`)).toBe(true);
    });

    // Let any racing effect settle before counting.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    const assetFetches = post.mock.calls.filter((call) => call[0] === `${DATASOURCE_URL}/scout/v1/asset/multiple`);
    expect(assetFetches).toHaveLength(1);
  });

  it('fetches a direct-mode RID once even when search-assets resolves mid-flight', async () => {
    // Gate the by-RID asset fetch so it stays in-flight while search-assets resolves.
    // On the pre-fix code, setAssets could re-run a query-driven by-RID path, aborting
    // and re-issuing the same asset/multiple POST (2 calls). The reconciler's direct-mode
    // inputs stay independent of `assets`, so this fires exactly once.
    let releaseAssetFetch!: () => void;
    const assetFetchGate = new Promise<void>((resolve) => {
      releaseAssetFetch = resolve;
    });

    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/scout/v1/asset/multiple')) {
        await assetFetchGate;
        return { [ASSET_RID]: ASSET };
      }
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
        query={makeQuery({ assetRid: ASSET_RID, assetInputMethod: 'direct' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    // Wait for search-assets to resolve (the trigger for the buggy re-run).
    await waitFor(() => {
      expect(post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/scout/v1/search-assets`)).toBe(true);
    });
    // Let setAssets flush and any (buggy) effect re-run happen while the asset fetch is still gated.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await Promise.resolve();
    });

    // Release the gated asset fetch and settle.
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      releaseAssetFetch();
      await Promise.resolve();
    });

    const assetFetches = post.mock.calls.filter((call) => call[0] === `${DATASOURCE_URL}/scout/v1/asset/multiple`);
    expect(assetFetches).toHaveLength(1);
  });

  it('refetches the asset when a direct-mode template variable resolves to a new RID', async () => {
    const ASSET_RID_B = 'ri.scout.main.asset.def456';
    const ASSET_B = { ...ASSET, rid: ASSET_RID_B, title: 'Asset B' };

    post.mockImplementation(async (url: string, body?: unknown) => {
      if (url.endsWith('/scout/v1/asset/multiple')) {
        const requestedRid = Array.isArray(body) ? body[0] : undefined;
        // Respond with whichever asset was requested so both resolutions succeed.
        if (requestedRid === ASSET_RID_B) {
          return { [ASSET_RID_B]: ASSET_B };
        }
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/scout/v1/search-assets')) {
        return { results: [ASSET] };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });

    const { rerender } = render(
      <QueryEditor
        query={makeQuery({ assetRid: '$asset', assetInputMethod: 'direct' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    // First resolution: $asset -> ASSET_RID.
    await waitFor(() => {
      expect(
        post.mock.calls.some(
          (call) => call[0] === `${DATASOURCE_URL}/scout/v1/asset/multiple` && Array.isArray(call[1]) && call[1][0] === ASSET_RID
        )
      ).toBe(true);
    });

    // Simulate the dashboard variable changing: $asset now resolves to a different RID.
    mockReplaceOverrides['$asset'] = ASSET_RID_B;
    rerender(
      <QueryEditor
        query={makeQuery({ assetRid: '$asset', assetInputMethod: 'direct' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    // The query-driven reconcile effect must fire again when the template resolves
    // to a different RID.
    await waitFor(() => {
      expect(
        post.mock.calls.some(
          (call) => call[0] === `${DATASOURCE_URL}/scout/v1/asset/multiple` && Array.isArray(call[1]) && call[1][0] === ASSET_RID_B
        )
      ).toBe(true);
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
      expect(screen.getByText('RID:')).toBeInTheDocument();
      expect(screen.getByText(ASSET_RID)).toBeInTheDocument();
    });
  });

  it('renders the asset picker as a combobox with custom RID support', async () => {
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

    const input = await screen.findByTestId('asset-combobox');

    expect(input).toHaveAttribute('data-id', 'nominal-query-asset-picker');
    expect(input).toHaveAttribute('data-width', 'auto');
    expect(input).toHaveAttribute('data-min-width', '30');
    expect(input).toHaveAttribute('data-max-width', '80');
    expect(input).toHaveAttribute('data-loading', 'false');
    expect(input).toHaveAttribute('data-create-custom-value', 'true');
    expect(input).toHaveAttribute('data-clearable', 'false');
  });

  it('renders the data scope picker as a combobox with variable support', async () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'temperature', channelDataType: 'numeric' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const input = await screen.findByTestId('data-scope-combobox');

    expect(input).toHaveAttribute('data-id', 'nominal-query-data-scope-picker');
    expect(input).toHaveAttribute('data-width', 'auto');
    expect(input).toHaveAttribute('data-min-width', '30');
    expect(input).toHaveAttribute('data-max-width', '60');
    await waitFor(() => {
      expect(screen.getByTestId('data-scope-combobox')).toHaveAttribute('data-loading', 'false');
    });
    expect(input).toHaveAttribute('data-create-custom-value', 'true');
    expect(input).toHaveAttribute('data-clearable', 'false');
  });

  it('publishes a Grafana alert when channel option loading fails', async () => {
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

    await screen.findByTestId('channel-combobox');
    const lastProps = mockComboboxProps.mock.calls.at(-1)?.[0];
    await act(async () => {
      await lastProps.options('temperature');
    });

    await waitFor(() => {
      expect(publish).toHaveBeenCalledWith(
        expect.objectContaining({
          payload: ['Unable to load Nominal channels', 'Check the selected asset, data scope, and data source configuration.'],
        })
      );
    });
  });

  it('renders the channel combobox with autosizing bounds and selected numeric prefix icon', async () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'temperature', channelDataType: 'numeric' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const input = await screen.findByTestId('channel-combobox');

    expect(input).toHaveAttribute('data-prefix-icon', 'calculator-alt');
    expect(input).toHaveAttribute('data-width', 'auto');
    expect(input).toHaveAttribute('data-min-width', '30');
    expect(input).toHaveAttribute('data-max-width', '80');

    const lastProps = mockComboboxProps.mock.calls.at(-1)?.[0];
    expect(typeof lastProps.options).toBe('function');
    expect(lastProps.createCustomValue).toBe(true);
    expect(lastProps.isClearable).toBe(false);
  });

  it('omits the channel prefix icon when the selected channel type is unknown', async () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'manual.channel', channelDataType: '' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const input = await screen.findByTestId('channel-combobox');

    expect(input).toHaveAttribute('data-prefix-icon', '');
  });

  it('uses the combobox async options loader to search channels with typed text', async () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'temperature', channelDataType: 'numeric' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const input = await screen.findByTestId('channel-combobox');
    fireEvent.change(input, { target: { value: 'long.temperature.channel' } });

    await waitFor(() => {
      expect(post).toHaveBeenCalledWith(
        `${DATASOURCE_URL}/channels`,
        expect.objectContaining({
          dataSourceRids: [LOG_DS_RID],
          searchText: 'long.temperature.channel',
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
