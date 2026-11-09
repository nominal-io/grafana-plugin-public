import {
  DataSourceInstanceSettings,
  CoreApp,
  DataFrame,
  DataFrameType,
  DataQueryRequest,
  DataQueryResponse,
  FieldType,
  LoadingState,
  ScopedVars,
  MetricFindValue
} from '@grafana/data';
import { map, Observable } from 'rxjs';
import { DataSourceWithBackend, getDataSourceSrv, getTemplateSrv, getBackendSrv } from '@grafana/runtime';

import { NominalQuery, NominalDataSourceOptions, DEFAULT_QUERY, QUERY_TYPE_SQL } from './types';
import { sqlInterpolateVariable } from './utils/sqlInterpolation';
import { NominalVariableSupport } from './variables';
import { SqlAnnotationEditor } from './components/SqlAnnotationEditor';
import resourceRoutes from './resourceRoutes.json';

// Grafana asks for the next page of logs with a range that ends at the millisecond of the oldest line
// it shows, or for oldest-first panels starts at the newest one's, and every query of the panel gets
// that range. A run keeps each query's oldest and newest lines, so the query's next page can continue
// from its own line: from the line itself, not its whole millisecond, and from where this query
// stopped, not where another query did. Runs are keyed by requestId, which Grafana keeps across a
// run's scroll pages. The map is module-level because Grafana 13 dashboards send scroll requests
// through a different data source instance than the first query.
interface Edge {
  ms: number;
  nanos: number;
  id: string;
}
interface Edges {
  oldest: Edge;
  newest: Edge;
}
interface QueryRun {
  query: string;
  edges: Edges;
  // The edges before the last page.
  before?: Edges;
  // Explore's last page of the query, which replay sends again.
  page?: DataFrame;
}
interface Run {
  // The run's first range starts here; a scroll page that starts later reads newer lines.
  from: number;
  mixed: boolean;
  // The last scroll page's direction and bound, to tell when Explore asks for that page again.
  bound?: string;
  queries: Map<string, QueryRun>;
}
// A query as sent for a Logs scroll page: the id of the line the page continues from, and whether
// the page reads newer lines. Only request copies carry these; saved queries never do.
interface PageQuery extends NominalQuery {
  scrollId?: string;
  scrollNewer?: boolean;
}
const runs = new Map<string, Run>();
// Dashboard Logs panels start a new run, and so a new entry, on every refresh that records a log page.
const maxRuns = 200;

const isLog = (frame: DataFrame) => frame.meta?.type === DataFrameType.LogLines;
const isOlder = (a: Edge, b: Edge) => a.ms < b.ms || (a.ms === b.ms && a.nanos < b.nanos);
const widen = (a: Edges, b: Edges): Edges => ({
  oldest: isOlder(b.oldest, a.oldest) ? b.oldest : a.oldest,
  newest: isOlder(a.newest, b.newest) ? b.newest : a.newest,
});
// A run's scroll pages repeat its query.
const querySignature = (q: NominalQuery) =>
  JSON.stringify([q.queryType, q.rawSql, q.assetRid, q.dataScopeName, q.channel, q.channelDataType]);

// continuesFrom returns the line a query's scroll page continues from, or undefined for Grafana's range.
function continuesFrom(run: Run, q: QueryRun, newer: boolean, bound: number): Edge | undefined {
  const edge = newer ? q.edges.newest : q.edges.oldest;
  // A bound at or past the query's own line comes from this or another query's lines. A bound short of
  // it means the lines between are hidden, by a level filter or dedup, or that Grafana lost pages the
  // run read.
  if (newer ? bound >= edge.ms : bound <= edge.ms) {
    return edge;
  }
  // A bound past every query's line from before the last page came from that page, whose hidden lines
  // the run has. That does not hold in a mixed panel, whose other data sources set bounds too.
  const pastBefore = (other: QueryRun) =>
    !other.before || (newer ? bound > other.before.newest.ms : bound < other.before.oldest.ms);
  if (!run.mixed && [...run.queries.values()].every(pastBefore)) {
    return edge;
  }
  return undefined;
}

// lines returns a log frame's lines, which arrive newest first.
function lines(frame: DataFrame): Edge[] {
  const times = frame.fields.find((field) => field.type === FieldType.time);
  const ids = frame.fields.find((field) => field.name === 'id');
  return times?.values.map((ms, i) => ({ ms, nanos: times.nanos?.[i] ?? 0, id: ids?.values[i] })) ?? [];
}

// frameEdges returns a log frame's oldest and newest lines.
function frameEdges(frame: DataFrame): Edges | undefined {
  const all = lines(frame);
  return all.length ? { oldest: all[all.length - 1], newest: all[0] } : undefined;
}

// recordPage moves a query's run to take in a response's frame, and keeps page as the query's page to
// send again. A frame that continued from a line moves only that edge: lines that share a time cannot
// be ordered by time, so a time comparison cannot tell which of two tied lines is further along.
function recordPage(run: Run, target: PageQuery, frame: DataFrame | undefined, page?: DataFrame) {
  const query = querySignature(target);
  const last = run.queries.get(target.refId);
  const q = last?.query === query ? last : undefined;
  const edges = frame && frameEdges(frame);
  if (!edges) {
    q && run.queries.set(target.refId, { ...q, before: q.edges });
    return;
  }
  const side = target.scrollId ? (target.scrollNewer ? 'newest' : 'oldest') : undefined;
  run.queries.set(target.refId, {
    query,
    edges: !q ? edges : side ? { ...q.edges, [side]: edges[side] } : widen(q.edges, edges),
    before: q?.edges,
    page,
  });
}

// withLines adds the lines of an earlier page that a frame lacks, by id, keeping lines newest first.
// Grafana 12.1 and 12.2 ask again for a scroll page that Explore dropped because another data source
// failed, and the query has moved past that page, so a repeated request sends the page again.
function withLines(frame: DataFrame, page: DataFrame): DataFrame {
  const rows = (df: DataFrame) => lines(df).map((line, i) => ({ line, row: [df, i] as const }));
  const shown = new Set(lines(frame).map((line) => line.id));
  const missing = rows(page).filter(({ line }) => !shown.has(line.id));
  if (missing.length === 0) {
    return frame;
  }
  const all = [...rows(frame), ...missing].sort((a, b) => b.line.ms - a.line.ms || b.line.nanos - a.line.nanos);
  const sorted = all.map((r) => r.row);
  return withRows(frame, sorted);
}

// withRows returns a frame of the given rows, each a frame and a row index in it.
function withRows(frame: DataFrame, rows: ReadonlyArray<readonly [DataFrame, number]>): DataFrame {
  return {
    ...frame,
    length: rows.length,
    fields: frame.fields.map((field, f) => ({
      ...field,
      values: rows.map(([df, i]) => df.fields[f].values[i]),
      nanos: field.nanos && rows.map(([df, i]) => df.fields[f].nanos?.[i] ?? 0),
    })),
  };
}

// Grafana stops scrolling for good once the oldest line shown, or the newest in an oldest-first panel,
// is within this many milliseconds of the range's edge (SCROLLING_THRESHOLD in Grafana's infinite scroll
// component).
const scrollStop = 1000;

// lastSecond returns which lines a page of several queries holds back: those within scrollStop of the
// range's edge, while another query's page still has new lines short of it, or a query of the page
// failed and the page still shows a new line. A sparse query reaches that second long before a dense
// one, and showing its lines there would end scrolling for every query. The held lines are past the
// query's recorded edge, so its next page reads them again. A query in reread compares its page with
// where it was before its last page, which Explore did not show.
function lastSecond(run: Run, frames: DataFrame[], newer: boolean, edge: number, failed: boolean, reread: string[]) {
  const inSecond = (ms: number) => (newer ? ms >= edge - scrollStop : ms <= edge + scrollStop);
  const side = newer ? 'newest' : 'oldest';
  const fresh = (frame: DataFrame, e: Edge) => {
    const q = run.queries.get(frame.refId!);
    const last = (reread.includes(frame.refId!) ? q?.before : q?.edges)?.[side];
    return !last || (newer ? isOlder(last, e) : isOlder(e, last));
  };
  const waiting = frames.some((frame) => {
    const end = frameEdges(frame)?.[side];
    return end && fresh(frame, end) && !inSecond(end.ms);
  });
  // Holding on failure with no new line outside that second would add nothing and end scrolling.
  const shows = frames.some((frame) => lines(frame).some((line) => !inSecond(line.ms) && fresh(frame, line)));
  return waiting || (failed && shows) ? inSecond : undefined;
}

// without returns a frame without the lines whose time drop matches.
function without(frame: DataFrame, drop: (ms: number) => boolean): DataFrame {
  const all = lines(frame);
  const keep = all.flatMap((line, i) => (drop(line.ms) ? [] : [[frame, i] as const]));
  return keep.length === all.length ? frame : withRows(frame, keep);
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
    // Mixed-datasource panels prefix each datasource's requestId; their scroll requests do not, and go
    // through the first query's instance. So a run is keyed by the data source that serves its queries,
    // resolved as the backend query resolves it.
    const mixed = /^mixed-\d+-/;
    const ref = request.targets[0]?.datasource;
    const served = ref ? getDataSourceSrv().getInstanceSettings(ref, request.scopedVars) : undefined;
    const key = `${(served?.rawRef ?? served)?.uid ?? this.uid}|${request.requestId.replace(mixed, '')}`;
    const explore = request.app === CoreApp.Explore;
    // Explore keeps one requestId per pane for every run, so only its scroll pages, which Grafana marks,
    // continue a run.
    const scroll =
      !explore ||
      request.targets.some((t) => (t as { supportingQueryType?: string }).supportingQueryType === 'infiniteScroll');
    const from = request.range.from.valueOf();
    const to = request.range.to.valueOf();
    const last = runs.get(key);
    const run: Run = scroll && last ? last : { from, mixed: false, queries: new Map() };
    run.mixed ||= mixed.test(request.requestId);
    if (run !== last) {
      runs.delete(key);
    }
    const newer = from > run.from;
    const bound = newer ? from : to;
    const targets = request.targets.map((target): PageQuery => {
      const q = run.queries.get(target.refId);
      if (run !== last || !q || q.query !== querySignature(target)) {
        return target;
      }
      return { ...target, scrollId: continuesFrom(run, q, newer, bound)?.id, scrollNewer: newer || undefined };
    });
    return super.query({ ...request, targets }).pipe(
      map((response) => {
        const repeated = explore && scroll && run.bound === `${newer} ${bound}`;
        run.bound = `${newer} ${bound}`;
        const logs: DataFrame[] = response.data.filter(isLog);
        // Explore's page merge loses alignment when only some pages carry nanos.
        logs.forEach((frame) =>
          frame.fields.forEach((field) => {
            if (field.type === FieldType.time && !field.nanos) {
              field.nanos = new Array(field.values.length).fill(0);
            }
          })
        );
        // A query that cannot continue from its line on a repeated request lost its last page, or a filter
        // hides it, so the page reads that page again.
        const reread = repeated ? targets.flatMap((t) => (t.scrollId ? [] : [t.refId])) : [];
        const held =
          run === last
            ? lastSecond(run, logs, newer, newer ? to : from, Boolean(response.errors?.length), reread)
            : undefined;
        const data = response.data.map((frame: DataFrame) => (held && isLog(frame) ? without(frame, held) : frame));
        // Edges come from the response itself; a merged-in page past its edge would misplace them.
        const merged = data.map((frame: DataFrame) => {
          const page = isLog(frame) && run.queries.get(frame.refId!)?.page;
          return repeated && page ? withLines(frame, page) : frame;
        });
        // Explore drops a whole scroll page when any query in it errors, so that page must not move a run.
        if (!(explore && scroll && response.state === LoadingState.Error)) {
          const logFrames = data.filter(isLog);
          targets.forEach((target) =>
            recordPage(
              run,
              target,
              logFrames.find((f) => f.refId === target.refId),
              explore ? merged.find((f) => isLog(f) && f.refId === target.refId) : undefined
            )
          );
          // Only a run that recorded a log page is kept: requests without logs would evict the runs of
          // Logs panels that are mid-scroll. Re-inserting moves the run to the end of the map, so the
          // cap evicts the least recently used. A late scroll page of a replaced Explore run puts that
          // run back on purpose: Grafana does not cancel it and shows it over the newer run.
          if (run.queries.size > 0) {
            runs.delete(key);
            runs.set(key, run);
            if (runs.size > maxRuns) {
              runs.delete(runs.keys().next().value!);
            }
          }
          // Bounds memory. Runs are per data source, so one open pane can hold several; 6 covers two panes
          // with several Nominal data sources each.
          const paged = [...runs.values()].filter((r) => [...r.queries.values()].some((q) => q.page));
          paged.slice(0, -6).forEach((r) => r.queries.forEach((q) => delete q.page));
        }
        return { ...response, data: merged };
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
