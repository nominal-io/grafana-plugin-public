import React from 'react';
// eslint-disable-next-line @typescript-eslint/no-deprecated
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { CoreApp, dateTime, makeTimeRange, type TimeRange } from '@grafana/data';
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
const mockMultiComboboxProps = jest.fn();
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
    MultiCombobox: (props: Record<string, any>) => {
      mockMultiComboboxProps(props);
      return React.createElement('input', {
        'data-testid': props['data-testid'] ?? 'mock-multi-combobox',
        'data-width': props.width ?? '',
        'data-min-width': String(props.minWidth ?? ''),
        'data-max-width': String(props.maxWidth ?? ''),
        placeholder: props.placeholder,
        value: Array.isArray(props.value) ? props.value.join(',') : '',
        readOnly: true,
      });
    },
    CodeEditor: jest.requireActual('../test/mockCodeEditor').MockCodeEditor,
  };
});

jest.mock('./NominalSignIn', () => ({
  NominalSignIn: ({ datasource, onSignedIn }: { datasource: any; onSignedIn: () => void }) =>
    datasource.usesOAuth ? (
      <button
        onClick={() => {
          datasource.completeSignIn();
          onSignedIn();
        }}
      >
        Finish Nominal sign-in
      </button>
    ) : null,
}));

const signedOutState = { signedIn: undefined, signIns: 0 };
const oauthStub = { subscribeOAuthState: () => () => {}, getOAuthState: () => signedOutState };

// A data source whose sign-in state is shared by every editor rendered with it.
function makeOAuthDatasource() {
  let state = { signedIn: false as boolean | undefined, signIns: 0 };
  const listeners = new Set<() => void>();
  return {
    url: DATASOURCE_URL,
    usesOAuth: true,
    subscribeOAuthState: (listener: () => void) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    getOAuthState: () => state,
    completeSignIn: () => {
      state = { signedIn: true, signIns: state.signIns + 1 };
      listeners.forEach((listener) => listener());
    },
  } as unknown as DataSource;
}

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {},
  getBackendSrv: jest.fn(() => ({ post })),
  getAppEvents: jest.fn(() => ({ publish })),
  getTemplateSrv: jest.fn(() => ({
    getVariables: () => [{ name: 'dsvar', label: 'Dataset var' }],
    replace: (v: string, scopedVars?: Record<string, { value: string }>) => {
      const scoped = scopedVars?.[v.replace(/^\$/, '')];
      if (scoped) {
        return scoped.value;
      }
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
  mockMultiComboboxProps.mockReset();
  mockReplaceOverrides = {};
});

afterEach(() => {
  jest.useRealTimers();
  jest.restoreAllMocks();
});

// Returns the most recent props captured for the Combobox rendered with the
// given data-testid. The bare-<input> mock hides the real onChange/value wiring,
// so reaching the captured props is the only way to exercise the QueryEditor-level
// onChange wrappers and value props directly (the Select->Combobox migration boundary).
function latestComboboxProps(testId: string): Record<string, any> | undefined {
  const calls = mockComboboxProps.mock.calls.filter((call) => call[0]?.['data-testid'] === testId);
  return calls.at(-1)?.[0];
}

async function settleEffects(): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-deprecated
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}

const mockDatasource = { url: DATASOURCE_URL, ...oauthStub } as unknown as DataSource;

function EditableQuery({
  initialQuery,
  onChange = jest.fn(),
  range,
}: {
  initialQuery: NominalQuery;
  onChange?: jest.Mock;
  range?: TimeRange;
}) {
  const [query, setQuery] = React.useState(initialQuery);
  return (
    <QueryEditor
      query={query}
      range={range}
      onChange={(next) => {
        setQuery(next);
        onChange(next);
      }}
      onRunQuery={jest.fn()}
      datasource={mockDatasource}
    />
  );
}

function renderEditor(initialQuery: NominalQuery, range?: TimeRange) {
  const onChange = jest.fn();
  render(<EditableQuery initialQuery={initialQuery} onChange={onChange} range={range} />);
  return onChange;
}

describe('query API selection', () => {
  beforeEach(() => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/assets-by-rid')) { return { [ASSET_RID]: ASSET }; }
      if (url.endsWith('/channels')) { return { channels: [{ name: 'app.logs', dataType: 'log' }] }; }
      return { results: [] };
    });
  });

  it('keeps only the active API fields in the query and restores the other API when switching back', async () => {
    const onChange = jest.fn();
    const compute = { refId: 'A', queryType: 'compute' as const, assetRid: '$asset', channel: 'app.logs', channelDataType: 'log', dataScopeName: 'default', aggregations: ['MAX'], hide: true };
    render(<EditableQuery initialQuery={compute} onChange={onChange} />);
    await settleEffects();
    fireEvent.click(screen.getByRole('radio', { name: 'SQL' }));
    expect(onChange).toHaveBeenLastCalledWith({ refId: 'A', queryType: 'sql', hide: true });
    expect(screen.queryByTestId('asset-combobox')).not.toBeInTheDocument();
    const editor = screen.getByTestId('sql-code-editor').querySelector('textarea')!;
    fireEvent.change(editor, { target: { value: 'SELECT 42' } });
    fireEvent.blur(editor);
    fireEvent.click(screen.getByRole('radio', { name: 'Compute' }));
    await settleEffects();
    expect(onChange).toHaveBeenLastCalledWith(compute);
    fireEvent.click(screen.getByRole('radio', { name: 'SQL' }));
    expect(onChange).toHaveBeenLastCalledWith({ refId: 'A', queryType: 'sql', rawSql: 'SELECT 42', hide: true });
    expect(screen.getByTestId('sql-code-editor').querySelector('textarea')).toHaveValue('SELECT 42');
  });

  it('leaves a saved SQL query with Compute fields alone until it is edited, then moves them out', () => {
    const onChange = jest.fn();
    render(<EditableQuery initialQuery={{ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1', format: 'table', assetRid: '$asset', channel: 'app.logs' }} onChange={onChange} />);
    expect(screen.getByRole('radio', { name: 'SQL' })).toBeChecked();
    expect(onChange).not.toHaveBeenCalled();
    const editor = screen.getByTestId('sql-code-editor').querySelector('textarea')!;
    fireEvent.change(editor, { target: { value: 'SELECT 2' } });
    fireEvent.blur(editor);
    expect(onChange).toHaveBeenLastCalledWith({ refId: 'A', queryType: 'sql', rawSql: 'SELECT 2', format: 'table' });
    fireEvent.click(screen.getByRole('radio', { name: 'Compute' }));
    expect(onChange).toHaveBeenLastCalledWith({ refId: 'A', queryType: 'compute', assetRid: '$asset', channel: 'app.logs' });
  });

  it('restores Compute fields left in a saved SQL query when switching to Compute without editing', () => {
    const onChange = jest.fn();
    render(<EditableQuery initialQuery={{ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1', assetRid: '$asset' }} onChange={onChange} />);
    fireEvent.click(screen.getByRole('radio', { name: 'Compute' }));
    expect(onChange).toHaveBeenLastCalledWith({ refId: 'A', queryType: 'compute', assetRid: '$asset' });
  });

  describe('restoring the other API after the query changes', () => {
    const numeric: NominalQuery = { refId: 'A', queryType: 'compute', assetRid: ASSET_RID, dataScopeName: 'default', channel: 'temp', channelDataType: 'numeric' };
    let current: NominalQuery;
    let replaceQuery: (query: NominalQuery) => void;

    // Holds the query as Grafana would. merge applies onChange the way the alert rule editor does.
    function Host({ initial, app, merge = false }: { initial: NominalQuery; app?: CoreApp; merge?: boolean }) {
      const [query, setQuery] = React.useState(initial);
      current = query;
      replaceQuery = setQuery;
      return <QueryEditor query={query} app={app} onChange={(next) => setQuery((prev) => (merge ? { ...prev, ...next } : next))} onRunQuery={jest.fn()} datasource={mockDatasource} />;
    }
    const pick = async (name: 'Compute' | 'SQL') => {
      fireEvent.click(screen.getByRole('radio', { name }));
      await settleEffects();
    };
    const typeSql = (text: string) => {
      const editor = screen.getByTestId('sql-code-editor').querySelector('textarea')!;
      fireEvent.change(editor, { target: { value: text } });
      fireEvent.blur(editor);
    };
    const replace = async (query: NominalQuery) => {
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      act(() => replaceQuery(query));
      await settleEffects();
    };
    const setAggregations = async (values: string[]) => {
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      act(() => mockMultiComboboxProps.mock.calls.at(-1)![0].onChange(values.map((value) => ({ value }))));
      await settleEffects();
    };
    const withoutUndefined = (query: NominalQuery) => JSON.parse(JSON.stringify(query));

    beforeEach(() => {
      post.mockImplementation(async (url: string) => {
        if (url.endsWith('/assets-by-rid')) { return { [ASSET_RID]: ASSET }; }
        if (url.endsWith('/channels')) { return { channels: [{ name: 'app.logs', dataType: 'log' }, { name: 'temp', dataType: 'numeric' }] }; }
        return { results: [] };
      });
    });

    it.each([
      ['nothing happens in between', true, async () => {}],
      ['the Compute query is edited', true, () => setAggregations(['MIN'])],
      ['the query is hidden', true, () => replace({ ...current, hide: true })],
      ['a different query replaces it', false, () => replace({ refId: 'A', queryType: 'compute', assetRid: 'ri.scout.main.asset.other' })],
      ['a different query replaces it and is then edited', false, async () => {
        await replace({ ...numeric, aggregations: ['MAX'] });
        await setAggregations(['MIN']);
      }],
      ['a different query replaces it and its channel type is inferred', false, () => replace({ ...numeric, channel: '$logChan', channelDataType: undefined })],
    ])('switching back to SQL after %s restores the SQL: %s', async (_, restores, between) => {
      render(<Host initial={numeric} />);
      await settleEffects();
      await pick('SQL');
      typeSql('SELECT x');
      await pick('Compute');
      await between();
      await pick('SQL');
      expect(current.rawSql).toBe(restores ? 'SELECT x' : undefined);
    });

    it.each([
      ['a SQL query', { refId: 'A', queryType: 'sql', rawSql: 'SELECT y' }, 'Compute'],
      ['an empty Compute query', { refId: 'A', queryType: 'compute' }, 'SQL'],
    ] as const)('does not restore fields into %s picked while in SQL', async (_, picked, target) => {
      render(<Host initial={numeric} />);
      await settleEffects();
      await pick('SQL');
      await replace(picked);
      await pick(target);
      expect(withoutUndefined(current)).toEqual({ refId: 'A', queryType: target === 'SQL' ? 'sql' : 'compute' });
    });

    it('moves SQL fields out of a saved Compute query on its first edit and restores them in SQL', async () => {
      render(<Host initial={{ ...numeric, rawSql: 'SELECT 1', format: 'table' }} />);
      await settleEffects();
      await setAggregations(['MIN']);
      expect(current).toEqual({ ...numeric, aggregations: ['MIN'] });
      await pick('SQL');
      expect(current).toMatchObject({ rawSql: 'SELECT 1', format: 'table' });
    });

    it('leaves removed fields out of the query outside the alert rule editor', async () => {
      render(<Host initial={numeric} />);
      await settleEffects();
      await pick('SQL');
      expect(Object.keys(current).sort()).toEqual(['queryType', 'refId']);
    });

    it.each([
      ['a Compute edit', undefined, () => setAggregations(['MIN'])],
      ['a Compute edit in the alert rule editor', CoreApp.UnifiedAlerting, () => setAggregations(['MIN'])],
      ['switching to SQL in the alert rule editor', CoreApp.UnifiedAlerting, () => pick('SQL')],
      ['a SQL edit', undefined, async () => {
        await pick('SQL');
        typeSql('SELECT 1');
      }],
    ])('drops fields that older saved queries hold on %s', async (_, app, edit) => {
      render(<Host initial={{ ...numeric, buckets: 1000, assetInputMethod: 'direct' } as NominalQuery} app={app} merge={app !== undefined} />);
      await settleEffects();
      await edit();
      expect(['buckets', 'assetInputMethod'].filter((key) => key in withoutUndefined(current))).toEqual([]);
    });

    it('removes the other API fields in the alert rule editor, which merges each change into the stored model', async () => {
      render(<Host initial={numeric} app={CoreApp.UnifiedAlerting} merge />);
      await settleEffects();
      await pick('SQL');
      expect(withoutUndefined(current)).toEqual({ refId: 'A', queryType: 'sql' });
      typeSql('SELECT x');
      await pick('Compute');
      expect(withoutUndefined(current)).toEqual(numeric);
      await pick('SQL');
      expect(withoutUndefined(current)).toEqual({ refId: 'A', queryType: 'sql', rawSql: 'SELECT x' });
    });
  });
});

describe('channel data type inference effect', () => {
  // Per-test overrides for the /channels response routed below.
  let channelsResponse: unknown = { channels: [{ name: 'app.logs', dataType: 'log' }] };
  let channelsShouldReject = false;


  function makeQuery(overrides: Partial<NominalQuery> = {}): NominalQuery {
    return {
      refId: 'A',
      assetRid: ASSET_RID,
      dataScopeName: 'default',
      queryType: 'compute',
      ...overrides,
    } as NominalQuery;
  }

  beforeEach(() => {
    channelsResponse = { channels: [{ name: 'app.logs', dataType: 'log' }] };
    channelsShouldReject = false;
    post.mockImplementation(async (url: string, _body?: unknown) => {
      if (url.endsWith('/assets-by-rid')) {
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

  it('restores a saved legacy direct-mode RID query into the asset combobox', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/search-assets')) {
        return { results: [ASSET] };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });

    // Old dashboards persisted assetInputMethod; the stale JSON key must be
    // ignored and the query must restore through the single Combobox path.
    const legacyQuery = { ...makeQuery({ assetRid: '$asset' }), assetInputMethod: 'direct' } as NominalQuery;

    render(
      <QueryEditor query={legacyQuery} onChange={jest.fn()} onRunQuery={jest.fn()} datasource={mockDatasource} />
    );

    const assetInput = await screen.findByTestId('asset-combobox');
    await waitFor(() => expect(assetInput).toHaveValue('$asset → Test Asset'));
    expect(screen.queryByText('Asset Search')).not.toBeInTheDocument();
    expect(screen.queryByText('Asset RID')).not.toBeInTheDocument();

    await waitFor(() => {
      expect(post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/assets-by-rid`)).toBe(true);
    });
  });

  it('keeps data scope and channel fields hidden while a saved asset template is unresolved', async () => {
    // $asset has no value yet, so it resolves to itself: assetComplete stays false.
    mockReplaceOverrides['$asset'] = '$asset';

    render(
      <QueryEditor
        query={makeQuery({ assetRid: '$asset', dataScopeName: '' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const assetInput = await screen.findByTestId('asset-combobox');
    expect(assetInput).toHaveValue('$asset');

    // With assetComplete false the dependent fields never open...
    expect(screen.queryByTestId('data-scope-combobox')).not.toBeInTheDocument();
    expect(screen.queryByTestId('query-editor-channel-aggregation-row')).not.toBeInTheDocument();

    // ...and no by-RID asset fetch is attempted for the unresolved template.
    await settleEffects();
    expect(post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/assets-by-rid`)).toBe(false);
  });

  it('reloads a saved query that failed while signed out once the user signs in', async () => {
    let signedIn = false;
    post.mockImplementation(async (url: string) => {
      if (!signedIn) {
        throw { status: 401, data: { message: 'sign in to Nominal' } };
      }
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });
    const onRunQuery = jest.fn();
    const otherOnRunQuery = jest.fn();
    const oauthDatasource = makeOAuthDatasource();

    render(
      <>
        <QueryEditor query={makeQuery()} onChange={jest.fn()} onRunQuery={onRunQuery} datasource={oauthDatasource} />
        <QueryEditor query={makeQuery()} onChange={jest.fn()} onRunQuery={otherOnRunQuery} datasource={oauthDatasource} />
      </>
    );
    await settleEffects();
    screen.getAllByTestId('asset-combobox').forEach((combobox) => expect(combobox).not.toHaveValue('Test Asset'));

    signedIn = true;
    fireEvent.click(screen.getAllByText('Finish Nominal sign-in')[0]);

    await waitFor(() =>
      screen.getAllByTestId('asset-combobox').forEach((combobox) => expect(combobox).toHaveValue('Test Asset'))
    );
    expect(onRunQuery).toHaveBeenCalledTimes(1);
    expect(otherOnRunQuery).not.toHaveBeenCalled();
  });

  it('fetches a saved template RID only once on mount (no restore/resolved double fetch)', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/search-assets')) {
        return { results: [ASSET] };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });

    render(
      <QueryEditor
        query={makeQuery({ assetRid: '$asset' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await waitFor(() => {
      expect(post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/assets-by-rid`)).toBe(true);
    });

    await settleEffects();

    const assetFetches = post.mock.calls.filter((call) => call[0] === `${DATASOURCE_URL}/assets-by-rid`);
    expect(assetFetches).toHaveLength(1);
  });

  it('refetches the asset when a template variable resolves to a new RID', async () => {
    const ASSET_RID_B = 'ri.scout.main.asset.def456';
    const ASSET_B = { ...ASSET, rid: ASSET_RID_B, title: 'Asset B' };

    post.mockImplementation(async (url: string, body?: unknown) => {
      if (url.endsWith('/assets-by-rid')) {
        const requestedRid = Array.isArray(body) ? body[0] : undefined;
        // Respond with whichever asset was requested so both resolutions succeed.
        if (requestedRid === ASSET_RID_B) {
          return { [ASSET_RID_B]: ASSET_B };
        }
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/search-assets')) {
        return { results: [ASSET] };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });

    const { rerender } = render(
      <QueryEditor
        query={makeQuery({ assetRid: '$asset' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    // First resolution: $asset -> ASSET_RID.
    await waitFor(() => {
      expect(
        post.mock.calls.some(
          (call) => call[0] === `${DATASOURCE_URL}/assets-by-rid` && Array.isArray(call[1]) && call[1][0] === ASSET_RID
        )
      ).toBe(true);
    });

    // Simulate the dashboard variable changing: $asset now resolves to a different RID.
    mockReplaceOverrides['$asset'] = ASSET_RID_B;
    rerender(
      <QueryEditor
        query={makeQuery({ assetRid: '$asset' })}
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
          (call) => call[0] === `${DATASOURCE_URL}/assets-by-rid` && Array.isArray(call[1]) && call[1][0] === ASSET_RID_B
        )
      ).toBe(true);
    });
  });

  it('renders asset and data scope pickers as comboboxes with custom value support', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/search-assets')) {
        return { results: [ASSET] };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });

    render(
      <QueryEditor
        query={makeQuery({ assetRid: ASSET_RID })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const assetInput = await screen.findByTestId('asset-combobox');
    const dataScopeInput = await screen.findByTestId('data-scope-combobox');

    expect(assetInput).toHaveAttribute('data-id', 'nominal-query-asset-picker');
    expect(dataScopeInput).toHaveAttribute('data-id', 'nominal-query-data-scope-picker');
    expect(assetInput).toHaveAttribute('data-create-custom-value', 'true');
    expect(dataScopeInput).toHaveAttribute('data-create-custom-value', 'true');
    expect(assetInput).toHaveAttribute('data-clearable', 'false');
    expect(dataScopeInput).toHaveAttribute('data-clearable', 'false');
  });

  it('renders asset and data scope controls in a separate row from channel controls', async () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'temperature', channelDataType: 'numeric' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const dataScopePicker = await screen.findByTestId('data-scope-combobox');
    const channelPicker = await screen.findByTestId('channel-combobox');

    const assetScopeRow = screen.getByTestId('query-editor-asset-scope-row');
    const channelAggregationRow = screen.getByTestId('query-editor-channel-aggregation-row');

    expect(assetScopeRow).toContainElement(dataScopePicker);
    expect(assetScopeRow).not.toContainElement(channelPicker);
    expect(channelAggregationRow).toContainElement(channelPicker);
    expect(channelAggregationRow).not.toContainElement(dataScopePicker);
  });

  it('publishes a Grafana alert when channel option loading fails', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/search-assets')) {
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
      expect(post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/assets-by-rid`)).toBe(true);
    });

    await screen.findByTestId('channel-combobox');
    const lastProps = mockComboboxProps.mock.calls.at(-1)?.[0];
    await act(async () => {
      await lastProps.options('temperature');
    });

    await waitFor(() => {
      expect(publish).toHaveBeenCalledWith(
        expect.objectContaining({
          payload: ['Unable to load Nominal channels', 'Check the selected source and data source configuration.'],
        })
      );
    });
  });

  it('renders channel and aggregation pickers with autosizing bounds', async () => {
    render(
      <QueryEditor
        query={makeQuery({ channel: 'temperature', channelDataType: 'numeric' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const channelInput = await screen.findByTestId('channel-combobox');
    const aggregationInput = await screen.findByTestId('aggregation-multi-combobox');

    expect(channelInput).toHaveAttribute('data-id', 'nominal-query-channel-picker');
    expect(channelInput).toHaveAttribute('data-width', 'auto');
    expect(channelInput).toHaveAttribute('data-min-width', '30');
    expect(channelInput).toHaveAttribute('data-max-width', '100');
    expect(aggregationInput).toHaveAttribute('data-width', 'auto');
    expect(aggregationInput).toHaveAttribute('data-min-width', '40');
    expect(aggregationInput).toHaveAttribute('data-max-width', '100');

    const lastProps = mockComboboxProps.mock.calls.at(-1)?.[0];
    expect(typeof lastProps.options).toBe('function');
    expect(lastProps.createCustomValue).toBe(true);
    expect(lastProps.isClearable).toBe(false);

    const lastMultiProps = mockMultiComboboxProps.mock.calls.at(-1)?.[0];
    expect(lastMultiProps.value).toEqual(['MEAN']);
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
        }),
        { requestId: expect.stringMatching(/^nominal-channel-options-\d+$/) }
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
        post.mock.calls.some((call) => call[0] === `${DATASOURCE_URL}/assets-by-rid`)
      ).toBe(true);
    });

    await settleEffects();

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

    await settleEffects();

    const channelTypeUpdates = onChange.mock.calls.filter(
      (call) => call[0]?.channelDataType !== undefined
    );
    expect(channelTypeUpdates).toHaveLength(0);
    expect(warnSpy).not.toHaveBeenCalled();
  });

  // The bare-<input> mock's onChange only invokes the async options loader, never
  // props.onChange. These tests reach the captured props to exercise the selection
  // wrappers in QueryEditor.tsx (the destructuring that changed in the Combobox
  // migration: `selection.value` for asset/scope, `toChannelOption(selection)` for
  // channel). A mis-wired or mis-destructured handler is invisible to every other
  // test layer, so cover it here.
  it('wires the asset picker onChange to selectAsset with the option value', async () => {
    const ASSET_RID_B = 'ri.scout.main.asset.def456';
    const ASSET_B = { ...ASSET, rid: ASSET_RID_B, title: 'Asset B' };
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/search-assets')) {
        return { results: [ASSET, ASSET_B] };
      }
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID_B]: ASSET_B };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });
    const onChange = jest.fn();

    render(
      <QueryEditor
        query={makeQuery({ assetRid: ASSET_RID })}
        onChange={onChange}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await screen.findByTestId('asset-combobox');
    const props = latestComboboxProps('asset-combobox');
    expect(props).toBeDefined();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      props!.onChange({ value: ASSET_RID_B, label: 'Asset B' });
    });

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ assetRid: ASSET_RID_B }));
  });

  it('wires the data scope picker onChange to selectDataScope with the option value', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });
    const onChange = jest.fn();

    render(
      <QueryEditor
        query={makeQuery({ assetRid: ASSET_RID, dataScopeName: 'default' })}
        onChange={onChange}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    const props = (await waitFor(() => {
      const captured = latestComboboxProps('data-scope-combobox');
      expect(captured).toBeDefined();
      return captured;
    }))!;

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      props.onChange({ value: 'secondary', label: 'Secondary scope' });
    });

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ dataScopeName: 'secondary' }));
  });

  it('wires the channel picker onChange to selectChannel via toChannelOption', async () => {
    const onChange = jest.fn();

    render(
      <QueryEditor
        query={makeQuery({ channel: 'temperature', channelDataType: 'numeric' })}
        onChange={onChange}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await screen.findByTestId('channel-combobox');
    const props = latestComboboxProps('channel-combobox');
    expect(props).toBeDefined();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      props!.onChange({ value: 'pressure', label: 'Pressure', dataType: 'numeric' });
    });

    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ channel: 'pressure', channelDataType: 'numeric' })
    );
  });

  // The data scope picker passes the raw query value (`query?.dataScopeName || ''`)
  // straight into the Combobox `value` prop, bypassing the option adapters that the other
  // pickers' value props go through. Nothing else asserts this passthrough, including the
  // template-variable case where the raw `$scope` syntax must reach the picker verbatim.
  it('passes the raw query dataScopeName through to the data scope picker value', async () => {
    post.mockImplementation(async (url: string) => {
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/channels')) {
        return { channels: [] };
      }
      return {};
    });

    render(
      <QueryEditor
        query={makeQuery({ assetRid: ASSET_RID, dataScopeName: '$scope' })}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
      />
    );

    await waitFor(() => {
      const props = latestComboboxProps('data-scope-combobox');
      expect(props).toBeDefined();
      expect(props!.value).toBe('$scope');
    });
  });
});

describe('dataset source', () => {
  const DATASET_A = 'ri.scout.main.dataset.aaa';
  const DATASET_B = 'ri.scout.main.dataset.bbb';
  const DATASETS: Record<string, { rid: string; name: string; startMs?: number; endMs?: number }> = {
    [DATASET_A]: {
      rid: DATASET_A,
      name: 'Dataset A',
      startMs: Date.UTC(2026, 0, 2, 3, 4, 5),
      endMs: Date.UTC(2026, 0, 9),
    },
    [DATASET_B]: { rid: DATASET_B, name: 'Dataset B' },
  };
  const assetQuery = {
    refId: 'A',
    queryType: 'compute',
    assetRid: ASSET_RID,
    dataScopeName: 'default',
    channel: 'temp',
    channelDataType: 'numeric',
    buckets: 1000,
  } as NominalQuery;
  const datasetQuery = {
    refId: 'A',
    queryType: 'compute',
    datasetRid: DATASET_A,
    channel: 'temp',
    channelDataType: 'numeric',
    buckets: 1000,
  } as NominalQuery;
  let channelTypeByRid: Record<string, string>;

  const requestsTo = (route: string) => post.mock.calls.filter((call) => call[0].endsWith(`/${route}`));

  beforeEach(() => {
    channelTypeByRid = { [DATASET_A]: 'numeric', [DATASET_B]: 'string' };
    post.mockImplementation(async (url: string, body: any) => {
      if (url.endsWith('/assets-by-rid')) {
        return { [ASSET_RID]: ASSET };
      }
      if (url.endsWith('/datasets-by-rid')) {
        return body.rids.flatMap((rid: string) => (DATASETS[rid] ? [DATASETS[rid]] : []));
      }
      if (url.endsWith('/datasets')) {
        return Object.values(DATASETS);
      }
      if (url.endsWith('/channels')) {
        return { channels: [{ name: 'temp', dataType: channelTypeByRid[body.dataSourceRids[0]] ?? 'numeric' }] };
      }
      return {};
    });
  });

  it('follows the datasetRid key and shows the saved dataset name', async () => {
    renderEditor(datasetQuery);
    await settleEffects();
    expect(screen.getByRole('radio', { name: 'Dataset' })).toBeChecked();
    expect(screen.getByTestId('dataset-combobox')).toHaveValue('Dataset A');
    expect(screen.queryByTestId('asset-combobox')).not.toBeInTheDocument();
    expect(screen.queryByTestId('data-scope-combobox')).not.toBeInTheDocument();
    expect(screen.getByTestId('channel-combobox')).toBeInTheDocument();
    expect(screen.getByText(DATASET_A)).toBeInTheDocument();
  });

  it.each([
    ['inside', Date.UTC(2026, 0, 5), Date.UTC(2026, 0, 6), false],
    ['overlapping the start', Date.UTC(2026, 0, 1), Date.UTC(2026, 0, 3), false],
    ['before', Date.UTC(2025, 11, 1), Date.UTC(2026, 0, 1), true],
    ['after', Date.UTC(2026, 0, 10), Date.UTC(2026, 0, 11), true],
  ])('shows the dataset data range and warns only when the time range misses it: %s', async (_, from, to, warns) => {
    renderEditor(datasetQuery, makeTimeRange(dateTime(from), dateTime(to)));
    await settleEffects();
    expect(screen.getByText('Data:')).toBeInTheDocument();
    expect(!!screen.queryByText('No data in the selected time range')).toBe(warns);
  });

  it('writes nothing when the source is switched without a pick', async () => {
    const onChange = renderEditor(assetQuery);
    await settleEffects();
    fireEvent.click(screen.getByRole('radio', { name: 'Dataset' }));
    expect(screen.getByTestId('dataset-combobox')).toBeInTheDocument();
    expect(screen.queryByTestId('channel-combobox')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('radio', { name: 'Asset' }));
    await settleEffects();
    expect(screen.getByTestId('asset-combobox')).toHaveValue('Test Asset');
    expect(onChange).not.toHaveBeenCalled();
  });

  it('stays on Dataset when an asset lookup started before the switch finishes after it', async () => {
    let finishLookup!: () => void;
    const lookup = new Promise<void>((resolve) => (finishLookup = resolve));
    const route = post.getMockImplementation()!;
    post.mockImplementation(async (url: string, body: any) => {
      if (url.endsWith('/assets-by-rid')) {
        await lookup;
      }
      return route(url, body);
    });
    const onChange = renderEditor({ refId: 'A', queryType: 'compute' } as NominalQuery);
    await settleEffects();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => latestComboboxProps('asset-combobox')!.onChange({ value: ASSET_RID }));
    fireEvent.click(screen.getByRole('radio', { name: 'Dataset' }));
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => finishLookup());
    await settleEffects();
    expect(screen.getByRole('radio', { name: 'Dataset' })).toBeChecked();
    expect(onChange.mock.calls.at(-1)![0]).not.toHaveProperty('dataScopeName');

    // Back on Asset, the asset's only data scope is filled in.
    fireEvent.click(screen.getByRole('radio', { name: 'Asset' }));
    await settleEffects();
    expect(onChange.mock.calls.at(-1)![0]).toMatchObject({ assetRid: ASSET_RID, dataScopeName: 'default' });
  });

  it('looks a dataset up again when it is picked again after a lookup found nothing', async () => {
    const route = post.getMockImplementation()!;
    let lookups = 0;
    post.mockImplementation(async (url: string, body: any) =>
      url.endsWith('/datasets-by-rid') && lookups++ === 0 ? [] : route(url, body)
    );
    renderEditor(datasetQuery);
    await settleEffects();
    expect(screen.getByText('Dataset not found')).toBeInTheDocument();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => latestComboboxProps('dataset-combobox')!.onChange({ value: DATASET_A }));
    await settleEffects();
    expect(screen.queryByText('Dataset not found')).not.toBeInTheDocument();
    expect(screen.getByTestId('dataset-combobox')).toHaveValue('Dataset A');
    expect(screen.getByTestId('channel-combobox')).toBeInTheDocument();
  });

  it('drops the other source keys on each pick and writes nothing on a switch between picks', async () => {
    const onChange = renderEditor(assetQuery);
    await settleEffects();
    fireEvent.click(screen.getByRole('radio', { name: 'Dataset' }));
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => latestComboboxProps('dataset-combobox')!.onChange({ value: DATASET_A }));
    await settleEffects();
    const pickedDataset = onChange.mock.calls.at(-1)![0];
    expect(pickedDataset).toMatchObject({ datasetRid: DATASET_A, channel: 'temp', channelDataType: 'numeric' });
    expect(pickedDataset).not.toHaveProperty('assetRid');
    expect(pickedDataset).not.toHaveProperty('dataScopeName');

    const writes = onChange.mock.calls.length;
    const channelRequests = requestsTo('channels').length;
    fireEvent.click(screen.getByRole('radio', { name: 'Asset' }));
    await settleEffects();
    expect(onChange).toHaveBeenCalledTimes(writes);
    expect(requestsTo('channels')).toHaveLength(channelRequests);
    expect(screen.queryByText('Test Asset')).not.toBeInTheDocument();

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => latestComboboxProps('asset-combobox')!.onChange({ value: ASSET_RID }));
    await settleEffects();
    const pickedAsset = onChange.mock.calls.at(-1)![0];
    expect(pickedAsset).toMatchObject({ assetRid: ASSET_RID, channel: 'temp' });
    expect(pickedAsset).not.toHaveProperty('datasetRid');
  });

  it('releases the held source choice once a pick commits it', async () => {
    const { rerender } = render(
      <QueryEditor query={assetQuery} onChange={jest.fn()} onRunQuery={jest.fn()} datasource={mockDatasource} />
    );
    await settleEffects();
    fireEvent.click(screen.getByRole('radio', { name: 'Dataset' }));
    rerender(
      <QueryEditor query={datasetQuery} onChange={jest.fn()} onRunQuery={jest.fn()} datasource={mockDatasource} />
    );
    await settleEffects();
    expect(screen.getByRole('radio', { name: 'Dataset' })).toBeChecked();

    rerender(
      <QueryEditor query={assetQuery} onChange={jest.fn()} onRunQuery={jest.fn()} datasource={mockDatasource} />
    );
    await settleEffects();
    expect(screen.getByRole('radio', { name: 'Asset' })).toBeChecked();
    expect(screen.getByTestId('asset-combobox')).toBeInTheDocument();
  });

  it('holds the source choice across a copy of the query and drops it for a query with another source', async () => {
    const ui = (query: NominalQuery) => (
      <QueryEditor query={query} onChange={jest.fn()} onRunQuery={jest.fn()} datasource={mockDatasource} />
    );
    const { rerender } = render(ui(assetQuery));
    await settleEffects();
    fireEvent.click(screen.getByRole('radio', { name: 'Dataset' }));
    rerender(ui({ ...assetQuery }));
    await settleEffects();
    expect(screen.getByRole('radio', { name: 'Dataset' })).toBeChecked();

    // Explore loading another query from history while the editor stays mounted.
    rerender(ui({ ...assetQuery, assetRid: 'ri.scout.main.asset.other' }));
    await settleEffects();
    expect(screen.getByRole('radio', { name: 'Asset' })).toBeChecked();
    expect(screen.getByTestId('asset-combobox')).toBeInTheDocument();
  });

  it.each([
    ['a dataset on an asset query', assetQuery, 'Dataset', 'dataset', DATASET_A, ['assetRid', 'dataScopeName']],
    ['an asset on a dataset query', datasetQuery, 'Asset', 'asset', ASSET_RID, ['datasetRid']],
  ] as const)(
    'clears the other source in the alert rule editor when picking %s',
    async (_, initial, radio, field, rid, cleared) => {
      let current: NominalQuery = initial;
      // The alert rule editor merges onChange into its stored model.
      function AlertRuleEditor() {
        const [query, setQuery] = React.useState(current);
        current = query;
        return (
          <QueryEditor
            query={query}
            app={CoreApp.UnifiedAlerting}
            onChange={(next) => setQuery((prev) => ({ ...prev, ...next }))}
            onRunQuery={jest.fn()}
            datasource={mockDatasource}
          />
        );
      }
      render(<AlertRuleEditor />);
      await settleEffects();
      fireEvent.click(screen.getByRole('radio', { name: radio }));
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      act(() => latestComboboxProps(`${field}-combobox`)!.onChange({ value: rid }));
      await settleEffects();
      expect(current).toMatchObject({ [`${field}Rid`]: rid, channel: 'temp' });
      for (const key of cleared) {
        expect(current[key]).toBeUndefined();
      }
    }
  );

  it('opens an asset query in Asset mode, even with an empty datasetRid', async () => {
    renderEditor({ ...assetQuery, datasetRid: '' });
    await settleEffects();
    expect(screen.getByRole('radio', { name: 'Asset' })).toBeChecked();
    expect(screen.getByTestId('asset-combobox')).toHaveValue('Test Asset');
    expect(screen.getByTestId('data-scope-combobox')).toBeInTheDocument();
    expect(screen.queryByTestId('dataset-combobox')).not.toBeInTheDocument();
  });

  it.each([
    ['dataset', '$ds', DATASET_A, 'Dataset A'],
    ['asset', '$asset', ASSET_RID, 'Test Asset'],
  ])(
    'shows an inline error and fetches nothing when the %s field resolves to a non-RID value',
    async (field, variable, loadedRid, loadedTitle) => {
      const query = (
        field === 'dataset' ? { ...datasetQuery, datasetRid: variable } : { ...assetQuery, assetRid: variable }
      ) as NominalQuery;
      const ui = <QueryEditor query={query} onChange={jest.fn()} onRunQuery={jest.fn()} datasource={mockDatasource} />;
      mockReplaceOverrides[variable] = loadedRid;
      const { rerender } = render(ui);
      await settleEffects();
      expect((screen.getByTestId(`${field}-combobox`) as HTMLInputElement).value).toContain(loadedTitle);

      const wrongTypeMessage = field === 'dataset' ? 'Not a dataset RID' : 'Not an asset RID';
      for (const [resolved, message] of [
        [`{${DATASET_A},${DATASET_B}}`, 'Has 2 values. Pick one.'],
        [`${DATASET_A},${DATASET_B}`, 'Has 2 values. Pick one.'],
        [field === 'dataset' ? ASSET_RID : DATASET_A, wrongTypeMessage],
      ]) {
        const requestsBefore = post.mock.calls.length;
        mockReplaceOverrides[variable] = resolved;
        rerender(
          <QueryEditor query={{ ...query }} onChange={jest.fn()} onRunQuery={jest.fn()} datasource={mockDatasource} />
        );
        await settleEffects();
        expect(screen.getByText(message)).toBeInTheDocument();
        expect(post.mock.calls.slice(requestsBefore)).toHaveLength(0);
        expect(screen.queryByText(loadedTitle)).not.toBeInTheDocument();
        expect((screen.getByTestId(`${field}-combobox`) as HTMLInputElement).value).not.toContain(loadedTitle);
        expect(screen.queryByTestId('channel-combobox')).not.toBeInTheDocument();
      }
    }
  );

  it.each([
    ['dataset', datasetQuery, ASSET_RID, 'Not a dataset RID', DATASET_B, 'Asset'],
    ['asset', assetQuery, DATASET_A, 'Not an asset RID', ASSET_RID, 'Dataset'],
  ] as const)(
    'keeps the %s for a typed RID of the other type until a valid pick or a source switch',
    async (field, query, wrongRid, message, validRid, otherSource) => {
      const onChange = renderEditor(query);
      await settleEffects();
      const submit = (value: string) =>
        // eslint-disable-next-line @typescript-eslint/no-deprecated
        act(() => latestComboboxProps(`${field}-combobox`)!.onChange({ value }));

      submit(wrongRid);
      expect(screen.getByText(message)).toBeInTheDocument();
      expect(onChange).not.toHaveBeenCalled();

      submit(validRid);
      expect(screen.queryByText(message)).not.toBeInTheDocument();

      submit(wrongRid);
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      act(() => screen.getByRole('radio', { name: otherSource }).click());
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      act(() => screen.getByRole('radio', { name: field === 'dataset' ? 'Dataset' : 'Asset' }).click());
      expect(screen.queryByText(message)).not.toBeInTheDocument();
    }
  );

  it("resolves a repeated panel's own value of a multi-value asset variable", async () => {
    mockReplaceOverrides['$asset'] = `{${ASSET_RID},ri.scout.main.asset.other}`;
    render(
      <QueryEditor
        query={{ ...assetQuery, assetRid: '$asset' }}
        onChange={jest.fn()}
        onRunQuery={jest.fn()}
        datasource={mockDatasource}
        data={{ request: { scopedVars: { asset: { value: ASSET_RID } } } } as any}
      />
    );
    await settleEffects();
    expect((screen.getByTestId('asset-combobox') as HTMLInputElement).value).toContain('Test Asset');
    expect(screen.getByTestId('channel-combobox')).toBeInTheDocument();
  });

  it('keeps no empty data scope when a channel is picked on a dataset query', async () => {
    const onChange = renderEditor(datasetQuery);
    await settleEffects();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() =>
      latestComboboxProps('channel-combobox')!.onChange({ value: 'pressure', label: 'pressure', dataType: 'numeric' })
    );
    expect(onChange.mock.calls.at(-1)![0]).not.toHaveProperty('dataScopeName');
  });

  it('re-infers a kept channel type after the source changes and not on mount', async () => {
    const onChange = renderEditor(datasetQuery);
    await settleEffects();
    expect(requestsTo('channels')).toHaveLength(0);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => latestComboboxProps('dataset-combobox')!.onChange({ value: DATASET_B }));
    await waitFor(() =>
      expect(onChange.mock.calls.at(-1)![0]).toMatchObject({ datasetRid: DATASET_B, channelDataType: 'string' })
    );
  });

  it('shows the type a kept channel has on a dataset a variable switches to, without writing it', async () => {
    channelTypeByRid[DATASET_B] = 'log';
    const routes = post.getMockImplementation()!;
    let releaseA: () => void = () => undefined;
    post.mockImplementation((url: string, body: any) =>
      url.endsWith('/channels') && body.dataSourceRids[0] === DATASET_A
        ? new Promise((resolve) => (releaseA = () => resolve(routes(url, body))))
        : routes(url, body)
    );
    const onChange = jest.fn();
    const query = { ...datasetQuery, datasetRid: '$ds' };
    const showDataset = async (rid: string) => {
      mockReplaceOverrides.$ds = rid;
      rerender(<QueryEditor query={query} onChange={onChange} onRunQuery={jest.fn()} datasource={mockDatasource} />);
      await settleEffects();
    };
    mockReplaceOverrides.$ds = DATASET_A;
    const { rerender } = render(
      <QueryEditor query={query} onChange={onChange} onRunQuery={jest.fn()} datasource={mockDatasource} />
    );
    await settleEffects();
    expect(screen.getByTestId('aggregation-multi-combobox')).toBeInTheDocument();

    await showDataset(DATASET_B);
    expect(screen.getByDisplayValue('Logs (raw)')).toBeInTheDocument();

    // The saved type stands in while the lookup for A is pending, and its late answer is dropped.
    await showDataset(DATASET_A);
    expect(screen.getByTestId('aggregation-multi-combobox')).toBeInTheDocument();
    await showDataset(DATASET_B);
    expect(screen.getByDisplayValue('Logs (raw)')).toBeInTheDocument();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => releaseA());
    expect(screen.getByDisplayValue('Logs (raw)')).toBeInTheDocument();
    expect(screen.queryByTestId('aggregation-multi-combobox')).not.toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  });

  it('re-infers a kept channel type after the data scope changes', async () => {
    const twoScopeAsset = {
      ...ASSET,
      dataScopes: [
        { dataScopeName: 'default', dataSource: { type: 'dataset', dataset: DATASET_A } },
        { dataScopeName: 'other', dataSource: { type: 'dataset', dataset: DATASET_B } },
      ],
    };
    const routes = post.getMockImplementation()!;
    post.mockImplementation(async (url: string, body: any) =>
      url.endsWith('/assets-by-rid') ? { [ASSET_RID]: twoScopeAsset } : routes(url, body)
    );
    const onChange = renderEditor(assetQuery);
    await settleEffects();
    expect(requestsTo('channels')).toHaveLength(0);

    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => latestComboboxProps('data-scope-combobox')!.onChange({ value: 'other' }));
    await waitFor(() =>
      expect(onChange.mock.calls.at(-1)![0]).toMatchObject({ dataScopeName: 'other', channelDataType: 'string' })
    );
  });

  it('lists datasets with their start time and lists variables for $ text', async () => {
    renderEditor(datasetQuery);
    await settleEffects();
    const loader = latestComboboxProps('dataset-combobox')!.options;
    let options: any[] = [];
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      options = await loader('a');
    });
    expect(options).toEqual([
      { label: 'Dataset A', value: DATASET_A, description: expect.stringContaining('2026') },
      { label: 'Dataset B', value: DATASET_B, description: undefined },
    ]);
    expect(requestsTo('datasets').at(-1)![1]).toEqual({ searchText: 'a' });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      options = await loader('$dsv');
    });
    expect(options).toEqual([{ value: '$dsvar', description: 'Dashboard variable: Dataset var' }]);
  });

  it('does not alert for a dataset search superseded by a $ search', async () => {
    renderEditor(datasetQuery);
    await settleEffects();
    let rejectSearch!: (error: Error) => void;
    post.mockImplementationOnce(() => new Promise((_, reject) => (rejectSearch = reject)));
    const loader = latestComboboxProps('dataset-combobox')!.options;
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      const superseded = loader('a');
      await loader('$dsv');
      rejectSearch(new Error('superseded'));
      await superseded;
    });
    expect(publish).not.toHaveBeenCalled();
  });

  it.each([
    ['dataset', 'text that is not a RID', 'hello'],
    ['dataset', 'an asset RID', ASSET_RID],
    ['dataset', 'a RID with no locator', 'ri.scout.main.dataset.'],
    ['asset', 'text that is not a RID', 'hello'],
    ['asset', 'a dataset RID', DATASET_A],
  ])('ignores %s picker input that is %s', async (picker, _case, value) => {
    // The asset has no 'old-scope', which a real edit would replace with one it has.
    const onChange = renderEditor(picker === 'dataset' ? datasetQuery : { ...assetQuery, dataScopeName: 'old-scope' });
    await settleEffects();
    onChange.mockClear();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    act(() => latestComboboxProps(`${picker}-combobox`)!.onChange({ value }));
    await settleEffects();
    expect(onChange).not.toHaveBeenCalled();
  });

  it.each([
    ['dataset', 'Asset'],
    ['asset', 'Dataset'],
  ])('does not alert for a %s search that fails after a switch to %s', async (picker, other) => {
    renderEditor(picker === 'dataset' ? datasetQuery : assetQuery);
    await settleEffects();
    let rejectSearch!: (error: Error) => void;
    post.mockImplementationOnce(() => new Promise((_, reject) => (rejectSearch = reject)));
    const search = latestComboboxProps(`${picker}-combobox`)!.options('a');
    fireEvent.click(screen.getByRole('radio', { name: other }));
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      rejectSearch(new Error('late'));
      await search;
    });
    expect(publish).not.toHaveBeenCalled();
  });

  it('shows the backend message for a dataset search it rejected', async () => {
    renderEditor(datasetQuery);
    await settleEffects();
    post.mockRejectedValueOnce({ status: 400, data: { error: 'Datasets need an https Nominal API base URL' } });
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await latestComboboxProps('dataset-combobox')!.options('a');
    });
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: ['Unable to load Nominal datasets', 'Datasets need an https Nominal API base URL'],
      })
    );
  });

  it('keeps a dataset usable when its name lookup fails', async () => {
    const respond = post.getMockImplementation()!;
    post.mockImplementation(async (url: string, body: any) => {
      if (url.endsWith('/datasets-by-rid')) {
        throw new Error('lookup failed');
      }
      return respond(url, body);
    });
    renderEditor(datasetQuery);
    await settleEffects();
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({ payload: ['Unable to load Nominal dataset', expect.any(String)] })
    );
    expect(screen.queryByText('Dataset not found')).not.toBeInTheDocument();
    expect(screen.getByTestId('channel-combobox')).toBeInTheDocument();
  });

  it('shows the invalid state for a dataset that is not found', async () => {
    renderEditor({ ...datasetQuery, datasetRid: 'ri.scout.main.dataset.missing' });
    await settleEffects();
    expect(screen.getByText('Dataset not found')).toBeInTheDocument();
    expect(requestsTo('channels')).toHaveLength(0);
    expect(screen.queryByTestId('channel-combobox')).not.toBeInTheDocument();
  });

  it('searches channels in the data scope a templated scope resolves to', async () => {
    const multiScopeAsset = {
      ...ASSET,
      dataScopes: [
        { dataScopeName: 'scope-a', dataSource: { type: 'dataset', dataset: DATASET_A } },
        { dataScopeName: 'scope-b', dataSource: { type: 'dataset', dataset: DATASET_B } },
      ],
    };
    post.mockImplementation(async (url: string) =>
      url.endsWith('/assets-by-rid') ? { [ASSET_RID]: multiScopeAsset } : { channels: [] }
    );
    mockReplaceOverrides['$scope'] = 'scope-b';
    renderEditor({ ...assetQuery, dataScopeName: '$scope' });
    await settleEffects();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    await act(async () => {
      await latestComboboxProps('channel-combobox')!.options('');
    });
    expect(requestsTo('channels').at(-1)![1]).toMatchObject({ dataSourceRids: [DATASET_B] });
  });
});
