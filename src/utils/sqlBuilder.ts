import { SqlBuilderState } from '../types';

export const quoteSqlString = (value: string) => `'${value.replace(/'/g, "''")}'`;

export function sqlBuilderError(builder: SqlBuilderState): string | undefined {
  if (!builder.datasetRid) {
    return 'Choose a dataset to start building a query.';
  }
  if (!builder.channels.length) {
    return 'Choose at least one channel.';
  }
  if (builder.interval && !/^[1-9]\d*(ms|s|m|h|d|w)$/.test(builder.interval)) {
    return 'Use a positive time bucket such as 500ms, 10s or 1m, or leave it empty for Auto.';
  }
  if (Boolean(builder.tagKey) !== Boolean(builder.tagValue)) {
    return 'Enter both a tag key and value, or leave both empty.';
  }
  return undefined;
}

// Persist generated SQL with the query so Grafana alerts can run it without the editor.
export function buildSql(builder: SqlBuilderState): string {
  if (sqlBuilderError(builder)) {
    return '';
  }
  const bucket = builder.interval ? `$__timeGroup(ts, ${builder.interval})` : '$__timeGroup(ts)';
  const tag = builder.tagKey
    ? `\n  AND tags[${quoteSqlString(builder.tagKey)}] = ${quoteSqlString(builder.tagValue ?? '')}`
    : '';
  return `SELECT ${bucket} AS "time", channel, ${builder.aggregation}(value) AS "value"
FROM ${builder.table}
WHERE dataset_rid = ${quoteSqlString(builder.datasetRid)}
  AND channel IN (${builder.channels.map(quoteSqlString).join(', ')})
  AND $__timeFilter(ts)${tag}
GROUP BY 1, 2
ORDER BY 1`;
}
