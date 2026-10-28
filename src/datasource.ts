import {
  DataSourceInstanceSettings,
  CoreApp,
  DataFrame,
  DataFrameType,
  DataQueryRequest,
  DataQueryResponse,
  FieldType,
  ScopedVars,
  MetricFindValue
} from '@grafana/data';
import { map, Observable } from 'rxjs';
import { DataSourceWithBackend, getTemplateSrv, getBackendSrv } from '@grafana/runtime';

import { NominalQuery, NominalDataSourceOptions, DEFAULT_QUERY, QUERY_TYPE_SQL } from './types';
import { sqlInterpolateVariable } from './utils/sqlInterpolation';
import { NominalVariableSupport } from './variables';
import { SqlAnnotationEditor } from './components/SqlAnnotationEditor';
import resourceRoutes from './resourceRoutes.json';

// Grafana asks for the next page of logs with a range that ends at the millisecond of the oldest line
// it has merged, or for oldest-first panels starts at the newest one's. pageEdges keeps those two lines
// of each log query run to the nanosecond, so the next page continues from the line instead of
// rereading its whole millisecond. Runs are keyed by requestId, which Grafana keeps across a run's
// scroll pages. The map is module-level because Grafana 13 dashboards send scroll requests through a
// different data source instance than the first query.
interface Edges {
  oldest: Edge;
  newest: Edge;
}
interface Edge {
  ms: number;
  nanos: number;
}
const pageEdges = new Map<string, Edges & { query: string }>();
// Dashboards start a new run, and so a new entry, on every refresh.
const maxPageEdges = 200;

const isOlder = (a: Edge, b: Edge) => a.ms < b.ms || (a.ms === b.ms && a.nanos < b.nanos);
// A run's scroll pages repeat its query.
const querySignature = (q: NominalQuery) =>
  JSON.stringify([q.queryType, q.rawSql, q.assetRid, q.dataScopeName, q.channel, q.channelDataType]);

function scrollBounds(runKey: string, query: string, from: number, to: number): Partial<NominalQuery> {
  const run = pageEdges.get(runKey);
  if (!run || run.query !== query) {
    return {};
  }
  if (to === run.oldest.ms) {
    return { scrollEndNanos: run.oldest.nanos };
  }
  if (from === run.newest.ms) {
    return { scrollStartNanos: run.newest.nanos };
  }
  return {};
}

// frameEdges returns a log frame's oldest and newest lines. Lines arrive newest first.
function frameEdges(frame: DataFrame): Edges | undefined {
  const times = frame.fields.find((field) => field.type === FieldType.time);
  if (!times || times.values.length === 0) {
    return undefined;
  }
  const edge = (i: number) => ({ ms: times.values[i], nanos: times.nanos?.[i] ?? 0 });
  return { oldest: edge(times.values.length - 1), newest: edge(0) };
}

// recordPage starts a run at a page's edges, or widens the run when the page continues it.
function recordPage(runKey: string, query: string, edges: Edges, continues: boolean) {
  const run = pageEdges.get(runKey);
  // Deleting first moves the run to the end of the map, so the cap evicts the least recently used.
  pageEdges.delete(runKey);
  pageEdges.set(
    runKey,
    continues && run && run.query === query
      ? {
          query,
          oldest: isOlder(run.oldest, edges.oldest) ? run.oldest : edges.oldest,
          newest: isOlder(edges.newest, run.newest) ? run.newest : edges.newest,
        }
      : { query, ...edges }
  );
  if (pageEdges.size > maxPageEdges) {
    pageEdges.delete(pageEdges.keys().next().value!);
  }
}

export class DataSource extends DataSourceWithBackend<NominalQuery, NominalDataSourceOptions> {
  url: string;


  constructor(instanceSettings: DataSourceInstanceSettings<NominalDataSourceOptions>) {
    super(instanceSettings);

    // For backend datasources using CallResource, we use the resource endpoint
    this.url = `/api/datasources/uid/${instanceSettings.uid}/resources`;
    this.variables = new NominalVariableSupport(this);
    this.annotations = {
      QueryEditor: SqlAnnotationEditor,
      prepareQuery: (anno) => {
        // Grafana 12.1 loads v2 dashboards without copying query.spec into target; 12.2 and later do.
        const target: NominalQuery | undefined = anno.target ?? anno.query?.spec;
        // The time series shape drops string columns, and annotation text lives in them.
        return target && { ...target, queryType: QUERY_TYPE_SQL, format: 'table' };
      },
    };
  }



  query(request: DataQueryRequest<NominalQuery>): Observable<DataQueryResponse> {
    // Mixed-datasource panels prefix each datasource's requestId; their scroll requests do not.
    const requestId = request.requestId.replace(/^mixed-\d+-/, '');
    const key = (target: NominalQuery) => `${target.datasource?.uid ?? this.uid}|${requestId}|${target.refId}`;
    // Explore keeps one requestId per pane for every run, so only its scroll targets, which Grafana
    // marks, continue a run.
    const scrolling = (target: NominalQuery) =>
      request.app !== CoreApp.Explore ||
      (target as { supportingQueryType?: string }).supportingQueryType === 'infiniteScroll';
    const from = request.range.from.valueOf();
    const to = request.range.to.valueOf();
    const targets = request.targets.map((target) =>
      scrolling(target) ? { ...target, ...scrollBounds(key(target), querySignature(target), from, to) } : target
    );
    return super.query({ ...request, targets }).pipe(
      map((response) => {
        // A non-scroll request starts a new run, so the last run's edges go even when this one has no lines.
        targets.forEach((target) => !scrolling(target) && pageEdges.delete(key(target)));
        // Explore drops a whole scroll page when any query in it errors, so that page must not move a run.
        const dropped = request.app === CoreApp.Explore && Boolean(response.errors?.length);
        response.data.forEach((frame: DataFrame) => {
          if (frame.meta?.type !== DataFrameType.LogLines) {
            return;
          }
          frame.fields.forEach((field) => {
            // Explore's page merge loses alignment when only some pages carry nanos.
            if (field.type === FieldType.time && !field.nanos) {
              field.nanos = new Array(field.values.length).fill(0);
            }
          });
          const target = targets.find((t) => t.refId === frame.refId);
          const edges = frameEdges(frame);
          if (target && edges && !(dropped && scrolling(target))) {
            recordPage(key(target), querySignature(target), edges, scrolling(target));
          }
        });
        return response;
      })
    );
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

    return {
      ...query,
      queryText: getTemplateSrv().replace(query.queryText || '', scopedVars),
      assetRid: getTemplateSrv().replace(query.assetRid || '', scopedVars),
      channel: getTemplateSrv().replace(query.channel || '', scopedVars),
      dataScopeName: getTemplateSrv().replace(query.dataScopeName || '', scopedVars),
    };
  }

  filterQuery(query: NominalQuery): boolean {
    if (query.hide) {
      return false;
    }

    if (query.queryType === QUERY_TYPE_SQL) {
      return !!query.rawSql?.trim();
    }

    const queryText = query.queryText?.trim();
    const assetRid = query.assetRid?.trim();
    const channel = query.channel?.trim();
    const dataScopeName = query.dataScopeName?.trim();

    // Allow queries with either legacy queryText or new Nominal parameters.
    // All three fields (assetRid, channel, dataScopeName) are required for a valid Nominal query.
    return !!(queryText || (assetRid && channel && dataScopeName));
  }

  /**
   * Resolves catalog variable queries.
   * Supports query types:
   * - "assets", "assets()", or empty: Returns all assets with text=title, value=rid
   * - "assets(<search>)" or "assets:<search>": Returns assets matching search text
   * - "channels(<assetRid>)": Returns all channels for a specific asset
   * - "channels(<assetRid>, <dataScopeName>)": Returns channels filtered to a specific datascope
   * - "datascopes(<assetRid>)": Returns datascopes for a specific asset
   */
  async metricFindQuery(query: string, options?: { scopedVars?: ScopedVars }): Promise<MetricFindValue[]> {
    const trimmedQuery = (query || '').trim();
    const lowerQuery = trimmedQuery.toLowerCase();
    const scopedVars = options?.scopedVars;

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
}
