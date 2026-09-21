import { DEFAULT_SQL_BUILDER } from '../types';
import { buildSql, sqlBuilderError } from './sqlBuilder';

const builder = { ...DEFAULT_SQL_BUILDER, datasetRid: 'dataset-1', channels: ["engine's rpm", 'temperature'] };

test('builds a bounded numeric query with escaped selectors and automatic time buckets', () => {
  expect(buildSql(builder)).toBe(`SELECT $__timeGroup(ts) AS "time", channel, AVG(value) AS "value"
FROM points_double
WHERE dataset_rid = 'dataset-1'
  AND channel IN ('engine''s rpm', 'temperature')
  AND $__timeFilter(ts)
GROUP BY 1, 2
ORDER BY 1`);
});

test('supports integer points, explicit buckets and tag filters', () => {
  const sql = buildSql({
    ...builder,
    table: 'points_int',
    aggregation: 'MAX',
    interval: '500ms',
    tagKey: 'sensor',
    tagValue: "a'b",
  });
  expect(sql).toContain('$__timeGroup(ts, 500ms)');
  expect(sql).toContain('MAX(value)');
  expect(sql).toContain('FROM points_int');
  expect(sql).toContain("tags['sensor'] = 'a''b'");
});

test.each([
  DEFAULT_SQL_BUILDER,
  { ...builder, channels: [] },
  { ...builder, interval: '0s' },
  { ...builder, interval: '1s); DROP TABLE x' },
  { ...builder, tagKey: 'sensor' },
])('does not run an incomplete or invalid builder query', (state) => {
  expect(sqlBuilderError(state)).toBeDefined();
  expect(buildSql(state)).toBe('');
});
