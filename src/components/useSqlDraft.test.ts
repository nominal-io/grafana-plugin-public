import { renderHook } from '@testing-library/react';
import type { NominalQuery } from '../types';
import { useSqlDraft } from './useSqlDraft';

function setup(query: NominalQuery) {
  const onChange = jest.fn();
  const onRunQuery = jest.fn();
  const hook = renderHook(({ q }) => useSqlDraft(q, onChange, onRunQuery), { initialProps: { q: query } });
  return { ...hook, onChange, onRunQuery };
}

describe('useSqlDraft', () => {
  it.each([
    ['unchanged text, not forced', 'SELECT 1', 'SELECT 1', false, 0],
    ['missing saved SQL, empty text, not forced', undefined, '', false, 0],
    ['unchanged text, forced', 'SELECT 1', 'SELECT 1', true, 1],
    ['unchanged empty text, forced', '', '', true, 0],
    ['unchanged whitespace text, forced', '  \n', '  \n', true, 0],
  ])('%s: onChange never fires', (_name, saved, text, runUnchanged, runs) => {
    const { result, onChange, onRunQuery } = setup({ refId: 'A', rawSql: saved });
    result.current.commit(text, runUnchanged);

    expect(onChange).not.toHaveBeenCalled();
    expect(onRunQuery).toHaveBeenCalledTimes(runs);
  });

  it('resets the draft when the saved SQL changes', () => {
    const { result, rerender } = setup({ refId: 'A', rawSql: 'SELECT 1' });
    rerender({ q: { refId: 'A', rawSql: 'SELECT 3' } });
    expect(result.current.value).toBe('SELECT 3');
  });
});
