import {
  DataSourceInstanceSettings,
  CoreApp,
  ScopedVars,
  MetricFindValue
} from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv, getBackendSrv } from '@grafana/runtime';

import { NominalQuery, NominalDataSourceOptions, DEFAULT_QUERY } from './types';

export class DataSource extends DataSourceWithBackend<NominalQuery, NominalDataSourceOptions> {
  url?: string;


  constructor(instanceSettings: DataSourceInstanceSettings<NominalDataSourceOptions>) {
    super(instanceSettings);

    // For backend datasources using CallResource, we use the resource endpoint
    this.url = `/api/datasources/uid/${instanceSettings.uid}/resources`;
  }



  getDefaultQuery(_: CoreApp): Partial<NominalQuery> {
    return DEFAULT_QUERY;
  }

  applyTemplateVariables(query: NominalQuery, scopedVars: ScopedVars) {
    return {
      ...query,
      queryText: getTemplateSrv().replace(query.queryText || '', scopedVars),
      assetRid: getTemplateSrv().replace(query.assetRid || '', scopedVars),
      channel: getTemplateSrv().replace(query.channel || '', scopedVars),
      dataScopeName: getTemplateSrv().replace(query.dataScopeName || '', scopedVars),
    };
  }

  filterQuery(query: NominalQuery): boolean {
    // Allow queries with either legacy queryText or new Nominal parameters
    return !!(query.queryText || (query.assetRid && query.channel));
  }

  /**
   * Used by Grafana to populate template variables.
   * Supports query types:
   * - "assets" or empty: Returns all assets with text=title, value=rid
   * - "assets:<search>": Returns assets matching search text
   * - "channels(<assetRid>)": Returns all channels for a specific asset
   * - "channels(<assetRid>, <dataScopeName>)": Returns channels filtered to a specific datascope
   * - "datascopes(<assetRid>)": Returns datascopes for a specific asset
   */
  async metricFindQuery(query: string, options?: any): Promise<MetricFindValue[]> {
    const trimmedQuery = (query || '').trim();
    const lowerQuery = trimmedQuery.toLowerCase();

    // Handle channels query: channels(<assetRid>) or channels(<assetRid>, <dataScopeName>)
    const channelsMatch = trimmedQuery.match(/^channels\(([^,)]+)(?:,\s*([^)]+))?\)$/i);
    if (channelsMatch) {
      const assetRidRaw = channelsMatch[1].trim();
      const dataScopeNameRaw = channelsMatch[2]?.trim() || '';
      const assetRid = getTemplateSrv().replace(assetRidRaw);
      const dataScopeName = dataScopeNameRaw ? getTemplateSrv().replace(dataScopeNameRaw) : '';
      return this.fetchChannelVariables(assetRid, dataScopeName);
    }

    // Handle datascopes query: datascopes(<assetRid>) or datascopes(${asset})
    const datascopesMatch = trimmedQuery.match(/^datascopes\((.+)\)$/i);
    if (datascopesMatch) {
      const assetRidRaw = datascopesMatch[1].trim();
      // Resolve any template variables in the asset RID
      const assetRid = getTemplateSrv().replace(assetRidRaw);
      return this.fetchDatascopeVariables(assetRid);
    }

    // Default to assets if query is empty or starts with "assets"
    if (!lowerQuery || lowerQuery === 'assets' || lowerQuery.startsWith('assets:')) {
      const searchText = lowerQuery.startsWith('assets:')
        ? trimmedQuery.substring(7).trim()
        : '';

      return this.fetchAssetVariables(searchText);
    }

    // Return empty for unknown query types
    return [];
  }

  /** Post to a resource endpoint and parse the response as MetricFindValue[]. */
  private async fetchVariableValues(path: string, body: Record<string, unknown>): Promise<MetricFindValue[]> {
    const response = await getBackendSrv().post(`${this.url}/${path}`, body);

    if (!Array.isArray(response)) {
      throw new Error(`Invalid response from /${path}: expected array`);
    }

    return response.map((item: any) => ({
      text: String(item.text ?? ''),
      value: String(item.value ?? ''),
    }));
  }

  private async fetchAssetVariables(searchText: string): Promise<MetricFindValue[]> {
    return this.fetchVariableValues('assets', { searchText, maxResults: 500 });
  }

  private async fetchDatascopeVariables(assetRid: string): Promise<MetricFindValue[]> {
    if (!assetRid || assetRid.includes('$')) {
      return [];
    }
    return this.fetchVariableValues('datascopes', { assetRid });
  }

  private async fetchChannelVariables(assetRid: string, dataScopeName: string): Promise<MetricFindValue[]> {
    if (!assetRid || assetRid.includes('$')) {
      return [];
    }
    if (dataScopeName && dataScopeName.includes('$')) {
      return [];
    }
    return this.fetchVariableValues('channelvariables', { assetRid, dataScopeName });
  }

  // No custom query method - let DataSourceWithBackend handle routing to Go backend

  /**
   * Test the connection to Nominal API
   * For backend datasources, this method is often not needed as CheckHealth handles it
   * But some Grafana versions still call this method
   */
  async testDatasource() {
    // The Go backend CheckHealth method does the actual connection test
    return {
      status: 'success',
      message: 'Connection test delegated to backend'
    };
  }
}
