import { SelectableValue } from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';

export type WeakTimestampType = 'ABSOLUTE' | 'RELATIVE' | 'PENDING' | 'UNKNOWN';

export interface Duration {
  seconds: number;
  nanos: number;
  picos?: number;
}

export type DataSource =
  | { type: 'dataset'; dataset: string }
  | { type: 'connection'; connection: string }
  | { type: 'logSet'; logSet: string }
  | { type: 'video'; video: string };

export interface DataScope {
  dataScopeName: string;
  dataSource: DataSource;
  offset?: Duration;
  timestampType: WeakTimestampType;
  seriesTags: Record<string, string>;
}

export interface Asset {
  rid: string;
  title: string;
  description?: string;
  labels: string[];
  dataScopes: DataScope[];
  properties: Record<string, string>;
  createdBy?: string;
  createdAt?: string;
  updatedAt?: string;
}

export interface Channel {
  name: string;
  dataType?: string;
  description?: string;
}

/** Data source types that support channel queries */
export const SUPPORTED_DATA_SOURCE_TYPES = ['dataset', 'connection', 'logSet'];

/** Returns the rid for a data source, or undefined if the type is unsupported. */
export const getDataSourceRid = (ds: DataSource): string | undefined => {
  if (ds.type === 'dataset') {
    return ds.dataset;
  }
  if (ds.type === 'connection') {
    return ds.connection;
  }
  if (ds.type === 'logSet') {
    return ds.logSet;
  }
  return undefined;
};

/** Collects data source RIDs from an asset's dataScopes, optionally filtered to a single scope. */
export const resolveDataSourceRids = (asset: Asset, dataScopeName?: string): string[] => {
  const scopes = asset.dataScopes.filter(
    (scope) => !dataScopeName || scope.dataScopeName === dataScopeName
  );
  const rids: string[] = [];
  for (const scope of scopes) {
    const rid = getDataSourceRid(scope.dataSource);
    if (rid) {
      rids.push(rid);
    }
  }
  return rids;
};

/** Creates a minimal asset placeholder when the actual asset can't be fetched.
 *  dataScopes is intentionally empty — we don't fabricate scope data. */
export const createBasicAsset = (rid: string, title: string): Asset => ({
  rid,
  title,
  labels: [],
  dataScopes: [],
  properties: {},
});

/** Convert asset to dropdown option */
export const assetToOption = (asset: Asset): SelectableValue<string> => {
  const supportedScopes = asset.dataScopes.filter((scope) =>
    SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type)
  );
  return {
    label: asset.title,
    value: asset.rid,
    description: `${asset.labels.join(', ') || 'No labels'} - ${supportedScopes.length} data scope(s)`,
  };
};

/** Fetches a single asset by its exact RID using the batch lookup endpoint */
export const fetchAssetByRid = async (datasourceUrl: string, rid: string): Promise<Asset | null> => {
  if (!rid || !rid.startsWith('ri.')) {
    return null;
  }

  const response = await getBackendSrv().post(
    `${datasourceUrl}/scout/v1/asset/multiple`,
    [rid]
  );

  const asset = response?.[rid];
  if (asset && asset.dataScopes?.length > 0) {
    return asset;
  }
  return null;
};

/** Searches assets, returning only those with at least one supported-type dataScope. */
export const searchAssets = async (datasourceUrl: string, searchText: string): Promise<Asset[]> => {
  const response = await getBackendSrv().post(`${datasourceUrl}/scout/v1/search-assets`, {
    query: {
      searchText: searchText || '',
      type: 'searchText',
    },
    sort: {
      field: 'CREATED_AT',
      isDescending: false,
    },
    pageSize: 50,
  });

  if (!response?.results) {
    return [];
  }
  return response.results.filter((asset: Asset) =>
    asset.dataScopes &&
    asset.dataScopes.length > 0 &&
    asset.dataScopes.some((scope) => SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type))
  );
};

/** Searches channels for the given data source RIDs. Returns raw channel objects;
 *  callers are responsible for mapping to their display shape. */
export const searchChannels = async (
  datasourceUrl: string,
  dataSourceRids: string[],
  searchText: string
): Promise<Channel[]> => {
  if (dataSourceRids.length === 0) {
    return [];
  }
  const response = await getBackendSrv().post(`${datasourceUrl}/channels`, {
    dataSourceRids,
    searchText,
  });
  return response?.channels ?? [];
};
