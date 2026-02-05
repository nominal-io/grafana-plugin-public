import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export interface NominalQuery extends DataQuery {
  // Asset information
  assetRid?: string;
  channel?: string;
  dataScopeName?: string;

  // Query parameters
  buckets?: number;
  queryType?: 'timeShift' | 'decimation' | 'raw';

  // Template variables support
  templateVariables?: Record<string, any>;

  // Legacy support
  queryText?: string;
  constant?: number;
}

export const DEFAULT_QUERY: Partial<NominalQuery> = {
  queryType: 'timeShift',
  buckets: 1000,
  dataScopeName: 'car_driv',
  constant: 6.5,
};

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
