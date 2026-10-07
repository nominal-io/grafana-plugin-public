import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export type SqlFormat = 'timeseries' | 'table';
export const QUERY_TYPE_COMPUTE = 'compute' as const;
export const QUERY_TYPE_SQL = 'sql' as const;

export interface NominalQuery extends DataQuery {
  // Asset information
  assetRid?: string;
  channel?: string;
  dataScopeName?: string;
  channelDataType?: string;

  // Bucket aggregations, or LTTB alone. Empty means MEAN. Numeric channels only.
  aggregations?: string[];

  // Query parameters
  buckets?: number;
  // Saved dashboards may still hold the retired 'timeShift', 'decimation' or 'raw'; treat anything but SQL as Compute.
  queryType?: typeof QUERY_TYPE_COMPUTE | typeof QUERY_TYPE_SQL;
  rawSql?: string;
  format?: SqlFormat;

  // Template variables support
  templateVariables?: Record<string, any>;

  // Legacy support
  queryText?: string;
  constant?: number;
}

// Aggregation enum values matching the API. Keep in sync with Go constants in pkg/plugin/aggregation.go.
export const AggregationType = {
  Mean: 'MEAN',
  Min: 'MIN',
  Max: 'MAX',
  Count: 'COUNT',
  Variance: 'VARIANCE',
  FirstPoint: 'FIRST_POINT',
  LastPoint: 'LAST_POINT',
  Lttb: 'LTTB',
} as const;

export type AggregationTypeValue = (typeof AggregationType)[keyof typeof AggregationType];

export const DEFAULT_AGGREGATIONS: AggregationTypeValue[] = [AggregationType.Mean];

export const DEFAULT_QUERY: Partial<NominalQuery> = {
  queryType: QUERY_TYPE_COMPUTE,
  buckets: 1000,
};

export const DEFAULT_SQL = `SELECT $__timeGroup(ts) AS "time", channel, AVG(value) AS "value"
FROM points_double
WHERE dataset_rid = '<dataset-rid>'
  AND $__timeFilter(ts)
GROUP BY 1, 2
ORDER BY 1`;

export interface DataPoint {
  Time: number;
  Value: number;
}

export interface DataSourceResponse {
  datapoints: DataPoint[];
}

/**
 * Nominal timestamp with nanosecond precision
 */
export interface NominalTimestamp {
  seconds: number;
  nanos: number;
  picos?: number | null;
}

/**
 * These are options configured for each DataSource instance
 */
export interface NominalDataSourceOptions extends DataSourceJsonData {
  baseUrl?: string;
  workspaceRid?: string;
  path?: string; // Legacy support
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface NominalSecureJsonData {
  apiKey?: string;
}

// Legacy type aliases for backward compatibility
export type MyQuery = NominalQuery;
export type MyDataSourceOptions = NominalDataSourceOptions;
export type MySecureJsonData = NominalSecureJsonData;

export type VariableQueryMode = 'catalog' | 'sql';

export interface NominalVariableQuery extends DataQuery {
  mode: VariableQueryMode;
  // A catalog pattern or SQL, depending on mode.
  query: string;
}

// Older dashboards store catalog queries as strings; provisioned queries may omit fields.
export function toVariableQuery(
  query: Partial<NominalVariableQuery> | string | null | undefined
): NominalVariableQuery {
  if (typeof query === 'string' || query == null) {
    return { refId: 'variable', mode: 'catalog', query: query ?? '' };
  }
  return {
    ...query,
    refId: query.refId ?? 'variable',
    mode: query.mode === 'sql' ? 'sql' : 'catalog',
    query: query.query ?? '',
  };
}

export const VARIABLE_SQL_EXAMPLE = `SELECT title AS __text, asset_rid AS __value
FROM assets
ORDER BY 1`;

export const ANNOTATION_SQL_EXAMPLE = `SELECT e.start_time AS "time",
       e.start_time + e.duration_seconds * INTERVAL '1' SECOND AS "timeEnd",
       e.name AS title,
       e.description AS text,
       e.type AS tags
FROM event_assets AS ea
JOIN events AS e ON ea.event_rid = e.event_rid
WHERE ea.asset_rid IN (\${asset:sqlstring})
  AND e.start_time <= $__timeTo()
  AND e.start_time + e.duration_seconds * INTERVAL '1' SECOND >= $__timeFrom()
ORDER BY e.start_time
LIMIT 1000`;
