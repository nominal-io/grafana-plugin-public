import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { VariableQueryEditor } from './VariableQueryEditor';
import { NominalVariableQuery } from '../types';

jest.mock('@grafana/ui', () => {
  const React = jest.requireActual('react');
  return {
    ...jest.requireActual('@grafana/ui'),
    CodeEditor: ({ value, onBlur, onChange }: any) =>
      React.createElement('textarea', {
        value,
        onChange: (e: any) => onChange?.(e.target.value),
        onBlur: (e: any) => onBlur?.(e.target.value),
      }),
  };
});

function renderEditor(query: NominalVariableQuery | string) {
  const onChange = jest.fn();
  render(<VariableQueryEditor {...({ query, onChange, onRunQuery: jest.fn(), datasource: {} } as any)} />);
  return { onChange };
}

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

  it('switches a saved catalog query to SQL, keeping its refId and clearing the text', () => {
    const { onChange } = renderEditor({ refId: 'V', mode: 'catalog', query: 'assets' });
    fireEvent.click(screen.getByRole('radio', { name: 'SQL' }));
    expect(onChange).toHaveBeenCalledWith({ refId: 'V', mode: 'sql', query: '' });
  });

  it('commits SQL on blur only when it changed', () => {
    const { onChange } = renderEditor({ refId: 'V', mode: 'sql', query: 'SELECT 1' });
    const editor = within(screen.getByTestId('sql-code-editor')).getByRole('textbox');
    fireEvent.blur(editor);
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.change(editor, { target: { value: 'SELECT 2' } });
    fireEvent.blur(editor);
    expect(onChange).toHaveBeenCalledWith({ refId: 'V', mode: 'sql', query: 'SELECT 2' });
  });

  it('inserts the variable example into an empty SQL query', () => {
    const { onChange } = renderEditor({ refId: 'V', mode: 'sql', query: '' });
    fireEvent.click(screen.getByRole('button', { name: 'Use variable example' }));
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ mode: 'sql', query: expect.stringContaining('FROM channels') }));
  });
});
