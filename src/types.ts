import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export type ComputeQueryType = 'timeShift' | 'decimation' | 'raw';
export type SqlFormat = 'timeseries' | 'table';
export const QUERY_TYPE_SQL = 'sql' as const;

export interface NominalQuery extends DataQuery {
  // Asset information
  assetRid?: string;
  channel?: string;
  dataScopeName?: string;
  channelDataType?: string;

  // Aggregation functions to request for numeric channels.
  // Options: "MEAN", "MIN", "MAX", "COUNT", "VARIANCE", "FIRST_POINT", "LAST_POINT". Empty/missing defaults to ["MEAN"].
  // For enum/string channels, this field is ignored — the backend uses Mode.
  aggregations?: string[];

  // Query parameters
  buckets?: number;
  queryType?: ComputeQueryType | typeof QUERY_TYPE_SQL;
  // Retain the Compute mode when switching this query to SQL and back.
  computeQueryType?: ComputeQueryType;
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
} as const;

export type AggregationTypeValue = (typeof AggregationType)[keyof typeof AggregationType];

export const DEFAULT_AGGREGATIONS: AggregationTypeValue[] = [AggregationType.Mean];

export const DEFAULT_QUERY: Partial<NominalQuery> = {
  queryType: 'timeShift',
  buckets: 1000,
  constant: 6.5,
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
