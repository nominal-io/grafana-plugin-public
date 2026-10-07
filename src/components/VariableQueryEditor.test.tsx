import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { VariableQueryEditor } from './VariableQueryEditor';
import { NominalVariableQuery } from '../types';

jest.mock('@grafana/ui', () => ({
  ...jest.requireActual('@grafana/ui'),
  CodeEditor: jest.requireActual('../test/mockCodeEditor').MockCodeEditor,
}));

function renderEditor(query: NominalVariableQuery | string) {
  const onChange = jest.fn();
  const editor = (q: NominalVariableQuery | string) => (
    <VariableQueryEditor {...({ query: q, onChange, onRunQuery: jest.fn(), datasource: {} } as any)} />
  );
  const { rerender } = render(editor(query));
  return { onChange, rerender: (q: NominalVariableQuery) => rerender(editor(q)) };
}

const sqlQuery = (query: string): NominalVariableQuery => ({ refId: 'V', mode: 'sql', query });
const sqlEditor = () => within(screen.getByTestId('sql-code-editor')).getByRole('textbox');

describe('VariableQueryEditor', () => {
  it('shows a legacy string as a catalog query without saving it', () => {
    const { onChange } = renderEditor('datascopes(${asset})');
    expect(screen.getByRole('radio', { name: 'Catalog' })).toBeChecked();
    expect(screen.getByLabelText('Catalog query')).toHaveValue('datascopes(${asset})');
    expect(onChange).not.toHaveBeenCalled();
  });

  it('converts a legacy string to the object model when edited', () => {
    const { onChange } = renderEditor('assets');
    const input = screen.getByLabelText('Catalog query');
    fireEvent.change(input, { target: { value: 'assets(engine)' } });
    fireEvent.blur(input);
    expect(onChange).toHaveBeenCalledWith({ refId: 'variable', mode: 'catalog', query: 'assets(engine)' });
  });

  it("restores the other mode's text on switching back, without saving it", () => {
    const { onChange, rerender } = renderEditor(sqlQuery('SELECT 1'));
    fireEvent.click(screen.getByRole('radio', { name: 'Catalog' }));
    const catalog = { refId: 'V', mode: 'catalog' as const, query: '' };
    expect(onChange).toHaveBeenLastCalledWith(catalog);
    rerender(catalog);
    fireEvent.click(screen.getByRole('radio', { name: 'SQL' }));
    expect(onChange).toHaveBeenLastCalledWith(sqlQuery('SELECT 1'));
  });

  it.each([
    ['blur without a change, skips the commit', 'SELECT 1', (el: HTMLElement) => fireEvent.blur(el), false],
    ['blur after a change, commits', 'SELECT 2', (el: HTMLElement) => fireEvent.blur(el), true],
    ['save without a change, commits', 'SELECT 1', (el: HTMLElement) => fireEvent.keyDown(el, { key: 's', ctrlKey: true }), true],
  ])('on %s', (_name, text, act, commits) => {
    const { onChange } = renderEditor(sqlQuery('SELECT 1'));
    fireEvent.change(sqlEditor(), { target: { value: text } });
    act(sqlEditor());
    expect(onChange.mock.calls).toEqual(commits ? [[sqlQuery(text)]] : []);
  });
});
