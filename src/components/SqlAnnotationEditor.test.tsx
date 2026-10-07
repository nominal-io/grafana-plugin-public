import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { SqlAnnotationEditor } from './SqlAnnotationEditor';
import { ANNOTATION_SQL_EXAMPLE, NominalQuery } from '../types';

jest.mock('@grafana/ui', () => ({
  ...jest.requireActual('@grafana/ui'),
  CodeEditor: jest.requireActual('../test/mockCodeEditor').MockCodeEditor,
}));

function renderEditor(query: NominalQuery) {
  const onChange = jest.fn();
  const onRunQuery = jest.fn();
  const view = render(<SqlAnnotationEditor query={query} onChange={onChange} onRunQuery={onRunQuery} />);
  return { ...view, editor: () => within(view.container).queryByRole('textbox'), onChange, onRunQuery };
}

describe('SqlAnnotationEditor', () => {
  it('offers the events example for an empty query and inserts it without running', () => {
    const { editor, onChange, onRunQuery } = renderEditor({ refId: 'Anno' });
    fireEvent.click(screen.getByRole('button', { name: 'Use events example' }));

    expect(onChange).toHaveBeenCalledWith({ refId: 'Anno', queryType: 'sql', rawSql: ANNOTATION_SQL_EXAMPLE });
    expect((editor() as HTMLTextAreaElement).value).toBe(ANNOTATION_SQL_EXAMPLE);
    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('hides the example button when SQL exists', () => {
    renderEditor({ refId: 'Anno', rawSql: 'SELECT 1' });
    expect(screen.queryByRole('button', { name: 'Use events example' })).not.toBeInTheDocument();
  });

  it('commits changed SQL on blur without running it', () => {
    const { editor, onChange, onRunQuery } = renderEditor({ refId: 'Anno', rawSql: 'SELECT 1' });
    fireEvent.change(editor()!, { target: { value: 'SELECT 2' } });
    fireEvent.blur(editor()!);

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ queryType: 'sql', rawSql: 'SELECT 2' }));
    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('runs unchanged SQL once when explicitly saved', () => {
    const { editor, onChange, onRunQuery } = renderEditor({ refId: 'Anno', rawSql: 'SELECT 1' });
    fireEvent.keyDown(editor()!, { key: 's', metaKey: true });

    expect(onChange).not.toHaveBeenCalled();
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });
});
