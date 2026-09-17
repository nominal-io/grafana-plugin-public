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

  it('does not persist the placeholder when unchanged', () => {
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
});
