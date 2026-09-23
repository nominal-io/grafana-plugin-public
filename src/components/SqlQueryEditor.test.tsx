import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { SqlQueryEditor } from './SqlQueryEditor';
import { NominalQuery } from '../types';

// Mirrors Monaco's keybinding resolution: the most recent matching registration wins, where
// addCommand matches in every editor and addAction only in the editor that registered it.
const mockKeybindings: Array<{ editorId?: string; run: () => void }> = [];

function pressRunShortcut(textarea: HTMLElement) {
  const editorId = textarea.dataset.editorId;
  const binding = [...mockKeybindings].reverse().find((entry) => entry.editorId === undefined || entry.editorId === editorId);
  binding?.run();
}

jest.mock('@grafana/ui', () => {
  const React = jest.requireActual('react');
  const actual = jest.requireActual('@grafana/ui');
  let nextEditorId = 0;
  return {
    ...actual,
    CodeEditor: ({ value, onBlur, onChange, onSave, onEditorDidMount, onEditorWillUnmount }: any) => {
      const ref = React.useRef(null);
      const [editorId] = React.useState(() => String(nextEditorId++));
      React.useEffect(() => {
        const editor = {
          getValue: () => ref.current.value,
          addCommand: (_keybinding: number, run: () => void) => {
            mockKeybindings.push({ run });
          },
          addAction: ({ run }: { run: () => void }) => {
            const entry = { editorId, run };
            mockKeybindings.push(entry);
            return { dispose: () => mockKeybindings.splice(mockKeybindings.indexOf(entry), 1) };
          },
        };
        onEditorDidMount?.(editor, { KeyMod: { CtrlCmd: 2048 }, KeyCode: { Enter: 3 } });
        return () => onEditorWillUnmount?.();
        // eslint-disable-next-line react-hooks/exhaustive-deps
      }, []);
      return React.createElement('textarea', {
        ref,
        value,
        'data-editor-id': editorId,
        onChange: (event: React.ChangeEvent<HTMLTextAreaElement>) => onChange?.(event.target.value),
        onBlur: (event: React.FocusEvent<HTMLTextAreaElement>) => onBlur?.(event.target.value),
        onKeyDown: (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
          if ((event.metaKey || event.ctrlKey) && event.key === 's') {
            event.preventDefault();
            onSave?.(event.currentTarget.value);
          }
        },
      });
    },
  };
});

function renderEditor(query: NominalQuery, onChange = jest.fn(), onRunQuery = jest.fn()) {
  const view = render(<SqlQueryEditor query={query} onChange={onChange} onRunQuery={onRunQuery} />);
  const editor = within(view.container).getByRole('textbox');
  return { ...view, editor, onChange, onRunQuery };
}

describe('SqlQueryEditor', () => {
  afterEach(() => {
    mockKeybindings.length = 0;
  });

  it('reruns unchanged SQL when explicitly saved', () => {
    const { editor, onChange, onRunQuery } = renderEditor({ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1' });
    fireEvent.keyDown(editor, { key: 's', ctrlKey: true });
    expect(onChange).not.toHaveBeenCalled();
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it('commits SQL on blur and runs the query', () => {
    const { editor, onChange, onRunQuery } = renderEditor({ refId: 'A', queryType: 'sql', rawSql: '' });
    fireEvent.change(editor, { target: { value: 'SELECT 1' } });
    fireEvent.blur(editor);

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ queryType: 'sql', rawSql: 'SELECT 1' }));
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['empty', ''],
    ['missing', undefined],
  ])('does not run when an editor with %s SQL loses focus', (_name, rawSql) => {
    const { editor, onChange, onRunQuery } = renderEditor({ refId: 'A', queryType: 'sql', rawSql });
    fireEvent.blur(editor);

    expect(onChange).not.toHaveBeenCalled();
    expect(onRunQuery).not.toHaveBeenCalled();
  });

  it('runs its own query when several SQL editors are open', () => {
    const first = renderEditor({ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1' });
    const second = renderEditor({ refId: 'B', queryType: 'sql', rawSql: 'SELECT 2' });

    fireEvent.change(first.editor, { target: { value: 'SELECT 10' } });
    pressRunShortcut(first.editor);

    expect(first.onChange).toHaveBeenCalledWith(expect.objectContaining({ refId: 'A', rawSql: 'SELECT 10' }));
    expect(second.onChange).not.toHaveBeenCalled();
    expect(second.onRunQuery).not.toHaveBeenCalled();

    second.unmount();
    pressRunShortcut(first.editor);
    expect(first.onRunQuery).toHaveBeenCalledTimes(2);
    expect(second.onChange).not.toHaveBeenCalled();
  });

  it('switches format to table and runs the query', () => {
    const { onChange, onRunQuery } = renderEditor({ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1' });
    fireEvent.click(screen.getByRole('radio', { name: /table/i }));

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ format: 'table' }));
    expect(onRunQuery).toHaveBeenCalledTimes(1);
  });

  it('retains uncommitted code when changing the result format', () => {
    const { editor, onChange } = renderEditor({ refId: 'A', rawSql: 'SELECT 1' });
    fireEvent.change(editor, { target: { value: 'SELECT 2' } });
    fireEvent.click(screen.getByRole('radio', { name: 'Table' }));
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ rawSql: 'SELECT 2', format: 'table' }));
  });

  it('inserts the time-series example into an empty query without running it', () => {
    const { editor, onChange, onRunQuery } = renderEditor({ refId: 'A', queryType: 'sql', format: 'table' });
    fireEvent.click(screen.getByRole('button', { name: 'Use time-series example' }));

    expect((editor as HTMLTextAreaElement).value).toContain('$__timeGroup(ts)');
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ queryType: 'sql', format: 'timeseries' }));
    expect(onRunQuery).not.toHaveBeenCalled();
    expect(screen.queryByRole('button', { name: 'Use time-series example' })).not.toBeInTheDocument();
  });

  it('keeps the example button hidden while an existing query is cleared', () => {
    const { editor } = renderEditor({ refId: 'A', queryType: 'sql', rawSql: 'SELECT 1' });
    fireEvent.change(editor, { target: { value: '' } });
    expect(screen.queryByRole('button', { name: 'Use time-series example' })).not.toBeInTheDocument();
  });
});
