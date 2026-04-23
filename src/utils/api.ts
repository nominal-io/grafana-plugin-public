import { SelectableValue } from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';

export interface Asset {
  rid: string;
  title: string;
  description?: string;
  labels: string[];
  dataScopes: Array<{
    dataScopeName: string;
    dataSource: {
      type: string;
      dataset?: string;
      video?: string;
      connection?: string;
      logSet?: string;
    };
    offset?: any;
    timestampType?: string;
    seriesTags?: Record<string, any>;
  }>;
  properties?: Record<string, any>;
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

/** Returns the rid for a data source, or undefined if the type is unsupported or the rid field is missing. */
export const getDataSourceRid = (
  ds: Asset['dataScopes'][number]['dataSource']
): string | undefined => {
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
  const scopes = (asset.dataScopes || []).filter(
    (scope) => !dataScopeName || scope.dataScopeName === dataScopeName
  );
  const rids: string[] = [];
  for (const scope of scopes) {
    if (!scope.dataSource) {
      continue;
    }
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
export const fetchAssetByRid = async (datasourceUrl: string | undefined, rid: string): Promise<Asset | null> => {
  if (!datasourceUrl || !rid || !rid.startsWith('ri.')) {
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
  console.warn('fetchAssetByRid: asset not found in response', { rid, responseKeys: Object.keys(response || {}) });
  return null;
};

/** Searches assets, returning only those with at least one supported-type dataScope. */
export const searchAssets = async (datasourceUrl: string | undefined, searchText: string): Promise<Asset[]> => {
  if (!datasourceUrl) {
    return [];
  }
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
  datasourceUrl: string | undefined,
  dataSourceRids: string[],
  searchText: string
): Promise<Channel[]> => {
  if (!datasourceUrl || dataSourceRids.length === 0) {
    return [];
  }
  const response = await getBackendSrv().post(`${datasourceUrl}/channels`, {
    dataSourceRids,
    searchText,
  });
  return response?.channels ?? [];
};
