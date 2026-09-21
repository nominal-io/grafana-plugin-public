import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { SqlQueryEditor } from './SqlQueryEditor';

jest.mock('@grafana/ui', () => {
  const React = jest.requireActual('react');
  const actual = jest.requireActual('@grafana/ui');
  return {
    ...actual,
    CodeEditor: ({ value, onBlur, onChange, onSave }: any) => React.createElement('textarea', {
      value,
      onChange: (event: React.ChangeEvent<HTMLTextAreaElement>) => onChange?.(event.target.value),
      onBlur: (event: React.FocusEvent<HTMLTextAreaElement>) => onBlur?.(event.target.value),
      onKeyDown: (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
        if ((event.metaKey || event.ctrlKey) && event.key === 's') {
          event.preventDefault();
          onSave?.(event.currentTarget.value);
        }
      },
    }),
  };
});

describe('SqlQueryEditor', () => {
  it('reruns unchanged SQL when explicitly saved', () => {
    const onChange = jest.fn();
    const onRunQuery = jest.fn();
    render(<SqlQueryEditor query={{ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1' }} onChange={onChange} onRunQuery={onRunQuery} />);
    fireEvent.keyDown(within(screen.getByTestId('sql-code-editor')).getByRole('textbox'), { key: 's', ctrlKey: true });
    expect(onChange).not.toHaveBeenCalled();
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it('commits SQL on blur and runs the query', () => {
    const onChange = jest.fn();
    const onRunQuery = jest.fn();
    render(<SqlQueryEditor query={{ refId: 'A', queryType: 'sql', rawSql: '' }} onChange={onChange} onRunQuery={onRunQuery} />);

    const editor = within(screen.getByTestId('sql-code-editor')).getByRole('textbox');
    fireEvent.change(editor, { target: { value: 'SELECT 1' } });
    fireEvent.blur(editor);

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ queryType: 'sql', rawSql: 'SELECT 1' }));
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it('does not invent SQL for an empty code query', () => {
    const onChange = jest.fn();
    const onRunQuery = jest.fn();
    render(<SqlQueryEditor query={{ refId: 'A', queryType: 'sql', rawSql: '' }} onChange={onChange} onRunQuery={onRunQuery} />);

    fireEvent.blur(within(screen.getByTestId('sql-code-editor')).getByRole('textbox'));

    expect(onChange).not.toHaveBeenCalled();
    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('switches format to table and runs the query', () => {
    const onChange = jest.fn();
    const onRunQuery = jest.fn();
    render(<SqlQueryEditor query={{ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1' }} onChange={onChange} onRunQuery={onRunQuery} />);

    fireEvent.click(screen.getByRole('radio', { name: /table/i }));

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ format: 'table' }));
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });
  it('retains uncommitted code when changing the result format', () => {
    const onChange = jest.fn();
    render(<SqlQueryEditor query={{ refId: 'A', rawSql: 'SELECT 1' }} onChange={onChange} onRunQuery={jest.fn()} />);
    fireEvent.change(within(screen.getByTestId('sql-code-editor')).getByRole('textbox'), { target: { value: 'SELECT 2' } });
    fireEvent.click(screen.getByRole('radio', { name: 'Table' }));
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ rawSql: 'SELECT 2', format: 'table' }));
  });

  it('requires confirmation before replacing custom SQL with builder selections', () => {
    const onChange = jest.fn();
    render(<SqlQueryEditor query={{ refId: 'A', rawSql: 'SELECT 1' }} onChange={onChange} onRunQuery={jest.fn()} />);
    fireEvent.click(screen.getByRole('radio', { name: 'Builder' }));
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Keep code' }));
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('radio', { name: 'Builder' }));
    fireEvent.click(screen.getByRole('button', { name: 'Replace SQL' }));
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ sqlEditorMode: 'builder', rawSql: '' }));
  });

});
