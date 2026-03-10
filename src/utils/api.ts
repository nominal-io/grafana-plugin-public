import { getBackendSrv } from '@grafana/runtime';

interface Asset {
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

/** Fetches a single asset by its exact RID using the batch lookup endpoint */
export const fetchAssetByRid = async (datasourceUrl: string, rid: string): Promise<Asset | null> => {
  // Validate RID format - must start with "ri." to be a valid resource identifier
  if (!rid || !rid.startsWith('ri.')) {
    return null;
  }

  // Use the efficient batch lookup endpoint instead of searching all assets
  const response = await getBackendSrv().post(
    `${datasourceUrl}/scout/v1/asset/multiple`,
    [rid]  // API expects an array of RIDs
  );

  // Response is a map: { "ri.scout...": { rid, title, dataScopes, ... } }
  const asset = response?.[rid];
  if (asset && asset.dataScopes?.length > 0) {
    return asset;
  }
  // Log to help diagnose asset lookup failures (e.g. unexpected response format)
  console.warn('fetchAssetByRid: asset not found in response', { rid, responseKeys: Object.keys(response || {}) });
  return null;
};
