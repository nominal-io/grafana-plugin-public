import { useEffect, useState } from 'react';
import { QUERY_TYPE_SQL, type NominalQuery } from '../types';

export interface SqlEditorProps {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  onRunQuery: () => void;
}

export function useSqlDraft(query: NominalQuery, onChange: (query: NominalQuery) => void, onRunQuery: () => void) {
  const savedSql = query.rawSql ?? '';
  const [value, setValue] = useState(savedSql);

  useEffect(() => {
    setValue(savedSql);
  }, [savedSql]);

  const save = (text: string, changes?: Pick<NominalQuery, 'format'>) => {
    setValue(text);
    onChange({ ...query, ...changes, queryType: QUERY_TYPE_SQL, rawSql: text });
  };

  const commit = (text: string, runUnchanged: boolean) => {
    if (text !== savedSql) {
      save(text);
      onRunQuery();
    } else if (runUnchanged && text.trim()) {
      onRunQuery();
    }
  };

  return { value, setValue, isBlank: !savedSql && !value, commit, save };
}
