import {
  DataSourceInstanceSettings,
  CoreApp,
  ScopedVars,
  MetricFindValue
} from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv, getBackendSrv } from '@grafana/runtime';

import { NominalQuery, NominalDataSourceOptions, TemplateSource, DEFAULT_QUERY, QUERY_TYPE_SQL, COMPUTE_BY_RUN } from './types';
import { sqlInterpolateVariable } from './utils/sqlInterpolation';
import { RunItem, fetchRun, formatRunStart, searchRuns } from './utils/api';
import {
  buildDefaultLinks,
  buildDefaultVariables,
  findRunVariable,
} from './defaultControls';
import resourceRoutes from './resourceRoutes.json';

// Lets the backend name the variable behind a bad RID in its error message.
function source(raw: string | undefined): TemplateSource | undefined {
  const name = raw?.match(/^\$\{?(\w+)/)?.[1];
  return raw && name ? { raw, name } : undefined;
}

export class DataSource extends DataSourceWithBackend<NominalQuery, NominalDataSourceOptions> {
  url: string;

  constructor(instanceSettings: DataSourceInstanceSettings<NominalDataSourceOptions>) {
    super(instanceSettings);

    // For backend datasources using CallResource, we use the resource endpoint
    this.url = `/api/datasources/uid/${instanceSettings.uid}/resources`;
  }

  // Called by Grafana 13 and later at dashboard load. Grafana 12 never calls these.
  async getDefaultVariables() {
    const runVariable = findRunVariable(getTemplateSrv().getVariables(), this.uid);
    return runVariable ? buildDefaultVariables(runVariable, this.uid) : [];
  }

  async getDefaultLinks() {
    return findRunVariable(getTemplateSrv().getVariables(), this.uid) ? buildDefaultLinks() : [];
  }

  getDefaultQuery(_: CoreApp): Partial<NominalQuery> {
    return DEFAULT_QUERY;
  }

  applyTemplateVariables(query: NominalQuery, scopedVars: ScopedVars) {
    if (query.queryType === QUERY_TYPE_SQL) {
      return {
        ...query,
        rawSql: getTemplateSrv().replace(query.rawSql || '', scopedVars, sqlInterpolateVariable),
      };
    }

    const replace = (raw: string) => getTemplateSrv().replace(raw, scopedVars);
    const assetSource = source(query.assetRid);
    const runSource = source(query.runRid);

    return {
      ...query,
      queryText: getTemplateSrv().replace(query.queryText || '', scopedVars),
      assetRid: replace(query.assetRid || ''),
      runRid: replace(query.runRid || ''),
      channel: replace(query.channel || ''),
      dataScopeName: replace(query.dataScopeName || ''),
      templateSources: assetSource || runSource ? { assetRid: assetSource, runRid: runSource } : undefined,
    };
  }

  filterQuery(query: NominalQuery): boolean {
    if (query.hide) {
      return false;
    }

    if (query.queryType === QUERY_TYPE_SQL) {
      return !!query.rawSql?.trim();
    }

    const channel = query.channel?.trim();
    const dataScopeName = query.dataScopeName?.trim();

    if (query.computeBy === COMPUTE_BY_RUN) {
      return !!(query.runRid?.trim() && channel && dataScopeName);
    }

    // Allow queries with either legacy queryText or new Nominal parameters.
    // All three fields (assetRid, channel, dataScopeName) are required for a valid Nominal query.
    return !!(query.queryText?.trim() || (query.assetRid?.trim() && channel && dataScopeName));
  }

  /**
   * Used by Grafana to populate template variables.
   * Supports query types:
   * - "assets", "assets()", or empty: Returns all assets with text=title, value=rid
   * - "assets(<search>)" or "assets:<search>": Returns assets matching search text
   * - "channels(<assetRid>)": Returns all channels for a specific asset
   * - "channels(<assetRid>, <dataScopeName>)": Returns channels filtered to a specific datascope
   * - "datascopes(<assetRid | runRid>)": Returns datascopes for a specific asset or run
   * - "runs()", "runs(<assetRid>[,<assetRid>...])": Returns runs (text=title and start, value=rid); empty or "*" means all runs
   * - "runstart(<runRid>[,<runRid>...])", "runend(...)": Returns the earliest start or latest end in epoch ms
   *   ("now" if a run has not ended); empty when the run variable is All
   */
  async metricFindQuery(query: string, options?: { scopedVars?: ScopedVars }): Promise<MetricFindValue[]> {
    const trimmedQuery = (query || '').trim();
    const lowerQuery = trimmedQuery.toLowerCase();
    const scopedVars = options?.scopedVars;

    const runsMatch = trimmedQuery.match(/^runs\(([^)]*)\)$/i);
    if (runsMatch) {
      const assetArg = getTemplateSrv().replace(runsMatch[1].trim(), scopedVars, joinValues);
      return this.fetchRunVariables(assetFilter(assetArg));
    }

    const runBoundMatch = trimmedQuery.match(/^run(start|end)\(([^)]+)\)$/i);
    if (runBoundMatch) {
      const arg = runBoundMatch[2].trim();
      if (isAllSelected(arg)) {
        return [];
      }
      const runRids = splitRids(getTemplateSrv().replace(arg, scopedVars, joinValues));
      return this.fetchRunBoundVariable(runRids, runBoundMatch[1].toLowerCase() === 'start' ? 'start' : 'end');
    }

    // Handle channels query: channels(<assetRid>) or channels(<assetRid>, <dataScopeName>)
    const channelsMatch = trimmedQuery.match(/^channels\(([^,)]+)(?:,\s*([^)]+))?\)$/i);
    if (channelsMatch) {
      const assetRidRaw = channelsMatch[1].trim();
      const dataScopeNameRaw = channelsMatch[2]?.trim() || '';
      const assetRid = getTemplateSrv().replace(assetRidRaw, scopedVars);
      const dataScopeName = dataScopeNameRaw ? getTemplateSrv().replace(dataScopeNameRaw, scopedVars) : '';
      return this.fetchChannelVariables(assetRid, dataScopeName);
    }

    // Handle datascopes query: datascopes(<assetRid>) or datascopes(${asset})
    const datascopesMatch = trimmedQuery.match(/^datascopes\(([^,)]+)\)$/i);
    if (datascopesMatch) {
      const assetRidRaw = datascopesMatch[1].trim();
      // Resolve any template variables in the asset RID
      const assetRid = getTemplateSrv().replace(assetRidRaw, scopedVars);
      return this.fetchDatascopeVariables(assetRid);
    }

    // Handle assets query: assets, assets(), assets(<search>), assets:<search>, or empty
    const assetsMatch = trimmedQuery.match(/^assets\(([^)]*)\)$/i);
    if (assetsMatch) {
      const searchText = getTemplateSrv().replace(assetsMatch[1].trim(), scopedVars);
      return this.fetchAssetVariables(searchText);
    }

    if (!lowerQuery || lowerQuery === 'assets' || lowerQuery.startsWith('assets:')) {
      const rawSearchText = lowerQuery.startsWith('assets:')
        ? trimmedQuery.substring(7).trim()
        : '';
      const searchText = getTemplateSrv().replace(rawSearchText, scopedVars);

      return this.fetchAssetVariables(searchText);
    }

    // Return empty for unknown query types
    return [];
  }

  /**
   * Validates and transforms a backend response into MetricFindValue[].
   * All variable endpoints return the same {text, value} format.
   */
  private validateMetricFindResponse(response: unknown, entityName: string): MetricFindValue[] {
    if (!Array.isArray(response)) {
      throw new Error(`Invalid response: expected array of ${entityName}s`);
    }
    return response.map((item: unknown, index: number) => {
      if (typeof item !== 'object' || item === null) {
        throw new Error(`Invalid ${entityName} at index ${index}: expected object`);
      }
      const obj = item as Record<string, unknown>;
      if (typeof obj.text !== 'string' || typeof obj.value !== 'string') {
        throw new Error(`Invalid ${entityName} at index ${index}: missing text or value`);
      }
      return { text: obj.text, value: obj.value };
    });
  }

  private async fetchRunVariables(assetRids: string[]): Promise<MetricFindValue[]> {
    let runs: RunItem[];
    try {
      runs = await searchRuns(this.url, { assetRids });
    } catch {
      throw new Error('Unable to load Nominal runs for the variable query.');
    }
    return runs.map((run) => {
      const spans = run.assetRids.length > 1 ? ` · spans ${run.assetRids.length} assets` : '';
      return { text: `${run.title} · ${formatRunStart(run.startMs)}${spans}`, value: run.rid };
    });
  }

  // Several runs span from the earliest start to the latest end of the runs
  // that loaded. Runs that failed or were not found are left out and logged.
  private async fetchRunBoundVariable(runRids: string[], bound: 'start' | 'end'): Promise<MetricFindValue[]> {
    if (runRids.length === 0) {
      return [];
    }
    const results = await Promise.allSettled(runRids.map((rid) => fetchRun(this.url, rid)));
    const loaded = results.map((r) => (r.status === 'fulfilled' ? r.value : null));
    const runs = loaded.filter((run) => run !== null);
    const skipped = runRids.filter((_, i) => loaded[i] === null);
    if (runs.length === 0 && results.some((r) => r.status === 'rejected')) {
      throw new Error('Unable to load the Nominal run for the variable query.');
    }
    if (runs.length === 0) {
      return [];
    }
    if (skipped.length > 0) {
      console.warn(`Nominal run${bound}: left out runs that failed to load or were not found`, skipped);
    }
    const ends = runs.flatMap((run) => (run.endMs === undefined ? [] : [run.endMs]));
    const value =
      bound === 'start'
        ? String(Math.min(...runs.map((run) => run.startMs)))
        : ends.length < runs.length
          ? 'now'
          : String(Math.max(...ends));
    return [{ text: value, value }];
  }

  private async fetchAssetVariables(searchText: string): Promise<MetricFindValue[]> {
    // If search text contains unresolved variable, return empty
    if (searchText && searchText.includes('$')) {
      return [];
    }

    let response: unknown;
    try {
      response = await getBackendSrv().post(
        `${this.url}/${resourceRoutes.assets}`,
        {
          searchText: searchText,
          maxResults: 500,
        }
      );
    } catch {
      throw new Error('Unable to load Nominal assets for the variable query.');
    }
    return this.validateMetricFindResponse(response, 'asset');
  }

  private async fetchDatascopeVariables(assetRid: string): Promise<MetricFindValue[]> {
    // If asset RID contains unresolved variable, return empty
    if (!assetRid || assetRid.includes('$')) {
      return [];
    }

    let response: unknown;
    try {
      response = await getBackendSrv().post(
        `${this.url}/${resourceRoutes.datascopes}`,
        {
          assetRid: assetRid,
        }
      );
    } catch {
      throw new Error('Unable to load Nominal data scopes for the variable query.');
    }
    return this.validateMetricFindResponse(response, 'datascope');
  }

  private async fetchChannelVariables(assetRid: string, dataScopeName: string): Promise<MetricFindValue[]> {
    // If asset RID contains unresolved variable, return empty
    if (!assetRid || assetRid.includes('$')) {
      return [];
    }
    // If dataScopeName contains unresolved variable, return empty
    if (dataScopeName && dataScopeName.includes('$')) {
      return [];
    }

    let response: unknown;
    try {
      response = await getBackendSrv().post(
        `${this.url}/${resourceRoutes.channelVariables}`,
        {
          assetRid: assetRid,
          dataScopeName: dataScopeName,
        }
      );
    } catch {
      throw new Error('Unable to load Nominal channels for the variable query.');
    }
    return this.validateMetricFindResponse(response, 'channel');
  }

  // No custom query method - let DataSourceWithBackend handle routing to Go backend
}

const joinValues = (value: string | string[]) => (Array.isArray(value) ? value.join(',') : value);

// Empty text or an unresolved variable gives no RIDs.
const splitRids = (arg: string) =>
  arg.includes('$')
    ? []
    : arg
        .split(',')
        .map((rid) => rid.trim())
        .filter(Boolean);

// A run variable set to All expands to every listed run, which is no window to snap to.
function isAllSelected(arg: string): boolean {
  const name = arg.match(/^\$\{?(\w+)/)?.[1];
  const variable = getTemplateSrv().getVariables().find((v) => v.name === name);
  const value = variable && 'current' in variable ? variable.current.value : undefined;
  return value === '$__all' || (Array.isArray(value) && value.includes('$__all'));
}

// Empty, '*' (the custom All value) or an unresolved variable means no asset filter.
function assetFilter(arg: string): string[] {
  return arg === '*' ? [] : splitRids(arg);
}
