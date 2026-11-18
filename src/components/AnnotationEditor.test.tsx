import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { AnnotationEventFieldSource, AnnotationQuery } from '@grafana/data';
import type { DataSource } from '../datasource';
import { NominalQuery } from '../types';
import { AnnotationEditor } from './AnnotationEditor';

const post = jest.fn();
const comboboxProps: Record<string, any> = {};

jest.mock('@grafana/runtime', () => ({
  getBackendSrv: jest.fn(() => ({ post })),
  getTemplateSrv: jest.fn(() => ({ getVariables: () => [{ name: 'asset' }] })),
}));

// The real comboboxes virtualize their lists, so tests drive them through their props.
jest.mock('@grafana/ui', () => {
  const React = jest.requireActual('react');
  const capture = (name: string) => {
    const Mock = (props: Record<string, any>) => {
      comboboxProps[props.placeholder ?? name] = props;
      return React.createElement('div', { 'data-testid': name });
    };
    return Mock;
  };
  return {
    ...jest.requireActual('@grafana/ui'),
    Combobox: capture('combobox'),
    MultiCombobox: capture('multi'),
    CodeEditor: jest.requireActual('../test/mockCodeEditor').MockCodeEditor,
  };
});

const datasource = { url: '/ds' } as DataSource;
const asset = () => comboboxProps['Search assets or choose a variable'];
const types = () => comboboxProps['All types'];
const labels = () => comboboxProps['All labels'];

function renderEditor(query: NominalQuery) {
  const onChange = jest.fn();
  const onRunQuery = jest.fn();
  render(
    <AnnotationEditor
      query={query}
      onChange={onChange}
      onRunQuery={onRunQuery}
      datasource={datasource}
      range={undefined}
    />
  );
  return { onChange, onRunQuery };
}

beforeEach(() => post.mockReset());

describe('AnnotationEditor', () => {
  it('lists Events first and selects it for an empty query', () => {
    renderEditor({ refId: 'Anno' });
    const radios = screen.getAllByRole('radio');

    expect(radios.map((r) => (r as HTMLInputElement).labels?.[0]?.textContent?.trim())).toEqual(['Events', 'SQL']);
    expect(screen.getByRole('radio', { name: 'Events' })).toBeChecked();
  });

  it('saves event changes without running the query', () => {
    const { onChange, onRunQuery } = renderEditor({ refId: 'Anno', queryType: 'events', assetRid: 'ri.a.1' });
    types().onChange([{ value: 'FLAG' }, { value: 'ERROR' }]);
    labels().onChange([{ value: 'valve' }]);
    asset().onChange({ value: '$asset' });

    expect(onChange.mock.calls.map((c) => c[0])).toEqual([
      { refId: 'Anno', queryType: 'events', assetRid: 'ri.a.1', eventTypes: ['FLAG', 'ERROR'] },
      { refId: 'Anno', queryType: 'events', assetRid: 'ri.a.1', eventLabels: ['valve'] },
      { refId: 'Anno', queryType: 'events', assetRid: '$asset' },
    ]);
    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('offers dashboard variables and matching assets as asset options', async () => {
    renderEditor({ refId: 'Anno' });
    const scope = { dataScopeName: 'main', dataSource: { type: 'dataset', dataset: 'ri.d.1' } };
    post.mockResolvedValue({ results: [
        { rid: 'ri.a.1', title: 'Pump', labels: [], dataScopes: [scope] },
        { rid: 'ri.a.2', title: 'Valve', labels: [], dataScopes: [] },
      ],
    });

    expect(await asset().options('')).toEqual([
      { value: '$asset', description: 'Dashboard variable' },
      { value: 'ri.a.1', label: 'Pump' },
      { value: 'ri.a.2', label: 'Valve' },
    ]);
    expect((await asset().options('zzz')).map((o: { value: string }) => o.value)).toEqual(['ri.a.1', 'ri.a.2']);
  });

  it('still offers dashboard variables when the asset search fails', async () => {
    renderEditor({ refId: 'Anno' });
    post.mockRejectedValue(new Error('boom'));

    expect(await asset().options('')).toEqual([{ value: '$asset', description: 'Dashboard variable' }]);
  });

  it('sends typed label text to the label route and offers the labels', async () => {
    renderEditor({ refId: 'Anno' });
    post.mockResolvedValue({ labels: ['valve', 'pump'] });

    const options = await labels().options('va');

    expect(post).toHaveBeenCalledWith('/ds/event-labels', { q: 'va' });
    expect(options).toEqual([{ value: 'valve' }, { value: 'pump' }]);
  });

  it('drops the other mode from the saved query and restores it when switching back', () => {
    const query: NominalQuery = { refId: 'Anno', queryType: 'sql', rawSql: 'SELECT 1' };
    const onChange = jest.fn();
    const props = { onChange, onRunQuery: jest.fn(), datasource, range: undefined };
    const view = render(<AnnotationEditor {...props} query={query} />);

    fireEvent.click(screen.getByRole('radio', { name: 'Events' }));
    expect(onChange).toHaveBeenLastCalledWith({ refId: 'Anno', queryType: 'events' });

    view.rerender(<AnnotationEditor {...props} query={{ refId: 'Anno', queryType: 'events', assetRid: 'ri.a.1' }} />);
    fireEvent.click(screen.getByRole('radio', { name: 'SQL' }));
    expect(onChange).toHaveBeenLastCalledWith({ refId: 'Anno', queryType: 'sql', rawSql: 'SELECT 1' });

    view.rerender(<AnnotationEditor {...props} query={{ refId: 'Anno', queryType: 'sql', rawSql: 'SELECT 1' }} />);
    fireEvent.click(screen.getByRole('radio', { name: 'Events' }));
    expect(onChange).toHaveBeenLastCalledWith({ refId: 'Anno', queryType: 'events', assetRid: 'ri.a.1' });
  });

  it('swaps mappings with the target in one annotation update when available', () => {
    const sqlMappings: AnnotationQuery['mappings'] = { text: { source: AnnotationEventFieldSource.Field, value: 'description' } };
    const sqlTarget: NominalQuery = { refId: 'Anno', queryType: 'sql', rawSql: 'SELECT 1' };
    const onChange = jest.fn();
    const onAnnotationChange = jest.fn();
    const props = { onChange, onAnnotationChange, onRunQuery: jest.fn(), datasource, range: undefined };
    const base = { name: 'a', enable: true, iconColor: 'red' };
    const view = render(
      <AnnotationEditor {...props} query={sqlTarget} annotation={{ ...base, target: sqlTarget, mappings: sqlMappings }} />
    );

    fireEvent.click(screen.getByRole('radio', { name: 'Events' }));
    expect(onAnnotationChange).toHaveBeenCalledTimes(1);
    expect(onAnnotationChange).toHaveBeenLastCalledWith({
      ...base,
      target: { refId: 'Anno', queryType: 'events' },
      mappings: undefined,
    });

    const eventsTarget: NominalQuery = { refId: 'Anno', queryType: 'events', assetRid: 'ri.a.1' };
    view.rerender(<AnnotationEditor {...props} query={eventsTarget} annotation={{ ...base, target: eventsTarget }} />);
    fireEvent.click(screen.getByRole('radio', { name: 'SQL' }));
    expect(onAnnotationChange).toHaveBeenLastCalledWith({ ...base, target: sqlTarget, mappings: sqlMappings });
    expect(onChange).not.toHaveBeenCalled();
  });
});
