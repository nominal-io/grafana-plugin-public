import React, { useState, useEffect, ChangeEvent, useCallback } from 'react';
import { InlineField, Input, Stack, Select, RadioButtonGroup } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { DataSource } from '../datasource';
import { NominalDataSourceOptions, NominalQuery } from '../types';

type Props = QueryEditorProps<DataSource, NominalQuery, NominalDataSourceOptions>;

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

interface Channel {
  name: string;
  type: string;
  description?: string;
  dataScopeName: string;
}


type AssetInputMethod = 'search' | 'direct';

/** Data source types that support channel queries */
const SUPPORTED_DATA_SOURCE_TYPES = ['dataset', 'connection'];

/** Creates a basic asset placeholder when the actual asset can't be found */
const createBasicAsset = (rid: string, title: string): Asset => ({
  rid,
  title,
  labels: [],
  dataScopes: [{ dataScopeName: 'dataset', dataSource: { type: 'dataset' } }]
});

/** Fetches a single asset by its exact RID using the batch lookup endpoint */
const fetchAssetByRid = async (datasourceUrl: string, rid: string): Promise<Asset | null> => {
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
  if (asset?.dataScopes?.length > 0) {
    return asset;
  }
  return null;
};

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);
  const [channels, setChannels] = useState<Channel[]>([]);
  const [dataScopes, setDataScopes] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [assetInputMethod, setAssetInputMethod] = useState<AssetInputMethod>('search');
  const [directRID, setDirectRID] = useState('');
  const [hasManuallySetMethod, setHasManuallySetMethod] = useState(false);
  const [showCopiedMessage, setShowCopiedMessage] = useState(false);
  
  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      console.log('Copied to clipboard:', text);
      
      // Show ephemeral "copied" message
      setShowCopiedMessage(true);
      setTimeout(() => {
        setShowCopiedMessage(false);
      }, 2000); // Hide after 2 seconds
      
    } catch (err) {
      console.error('Failed to copy to clipboard:', err);
      // Fallback for browsers that don't support clipboard API
      const textArea = document.createElement('textarea');
      textArea.value = text;
      document.body.appendChild(textArea);
      textArea.select();
      // eslint-disable-next-line @typescript-eslint/no-deprecated
      document.execCommand('copy');
      document.body.removeChild(textArea);
      
      // Show ephemeral "copied" message for fallback too
      setShowCopiedMessage(true);
      setTimeout(() => {
        setShowCopiedMessage(false);
      }, 2000);
    }
  };

  const loadAssets = useCallback(async () => {
    setIsLoading(true);
    try {
      // Use the backend proxy to search for assets
      const response = await getBackendSrv().post(
        `${datasource.url}/scout/v1/search-assets`,
        {
          query: {
            searchText: searchQuery || '',
            type: 'searchText'
          },
          sort: {
            field: 'CREATED_AT',
            isDescending: false
          },
          pageSize: 50
        }
      );
      
      if (response && response.results) {
        // Filter assets to only include those with supported data source types
        const filteredAssets = response.results.filter((asset: Asset) => {
          return asset.dataScopes && asset.dataScopes.length > 0 &&
                 asset.dataScopes.some(scope =>
                   SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type)
                 );
        });
        
        setAssets(filteredAssets);
      } else {
        setAssets([]);
      }
    } catch (error) {
      console.error('Failed to load assets:', error);
      // Fallback to example assets if API fails
      setAssets([
        {
          rid: 'ri.scout.cerulean-staging.asset.0f4b18c9-7876-44ab-bfde-b6147a3a10f9',
          title: 'Car Asset Example',
          description: 'Example car asset with GPS and video data',
          labels: ['vehicle', 'test'],
          dataScopes: [
            {
              dataScopeName: 'car_driv',
              dataSource: { type: 'dataset' }
            },
            {
              dataScopeName: 'car_video',
              dataSource: { type: 'video' }
            }
          ]
        }
      ]);
    } finally {
      setIsLoading(false);
    }
  }, [searchQuery, datasource]);

  const loadChannelsForAsset = useCallback(async (asset: Asset) => {
    try {
      // Build a map from data source RID to data scope name
      const dataSourceToScope: Record<string, string> = {};
      const dataSourceRids: string[] = [];

      for (const scope of asset.dataScopes || []) {
        const ds = scope.dataSource;
        if (!ds || !SUPPORTED_DATA_SOURCE_TYPES.includes(ds.type)) {
          continue;
        }
        let rid: string | undefined;
        if (ds.type === 'dataset' && ds.dataset) {
          rid = ds.dataset;
        } else if (ds.type === 'connection' && ds.connection) {
          rid = ds.connection;
        }
        if (rid) {
          dataSourceRids.push(rid);
          dataSourceToScope[rid] = scope.dataScopeName;
        }
      }

      if (dataSourceRids.length === 0) {
        setChannels([]);
        return;
      }

      // Call backend channels search endpoint
      const response = await getBackendSrv().post(
        `${datasource.url}/channels`,
        {
          dataSourceRids: dataSourceRids,
          searchText: ''
        }
      );

      if (response && response.channels) {
        // Transform API response to our Channel interface
        // Map each channel back to its correct data scope using the dataSource RID
        const apiChannels: Channel[] = response.channels.map((ch: any) => {
          // ch.dataSource is the RID of the data source this channel belongs to
          const channelDataSourceRid = ch.dataSource;
          const matchingScopeName = dataSourceToScope[channelDataSourceRid] || 'dataset';

          return {
            name: ch.name,
            type: ch.type || 'numeric',
            description: ch.description || `Channel: ${ch.name}`,
            dataScopeName: matchingScopeName
          };
        });

        setChannels(apiChannels);
      } else {
        setChannels([]);
      }
    } catch (error) {
      console.error('Failed to load channels from API:', error);
      setChannels([]);
    }
  }, [datasource]);

  // Initialize state from existing query (handles duplicated queries)
  useEffect(() => {
    let mounted = true;

    if (query && query.assetRid && !selectedAsset && !hasManuallySetMethod) {
      // Resolve template variables if present
      const resolvedAssetRid = getTemplateSrv().replace(query.assetRid);
      const isVariable = query.assetRid.includes('$');
      const searchRid = resolvedAssetRid;

      // If variable couldn't be resolved, skip
      if (!searchRid.includes('$')) {
        // Always try search mode first - check if asset exists in search results
        const asset = assets.find(a => a.rid === searchRid);
        if (asset) {
          // Found in search results - use search mode
          setAssetInputMethod('search');
          setSelectedAsset(asset);
        } else if (assets.length > 0) {
          // Assets are loaded but asset not found - must be direct RID input
          setAssetInputMethod('direct');
          setDirectRID(query.assetRid); // Keep original (with variable) for display
          // Fetch asset by exact RID match
          (async () => {
            try {
              const foundAsset = await fetchAssetByRid(datasource.url!, searchRid);
              if (!mounted) {
                return;
              }
              if (foundAsset) {
                setSelectedAsset(foundAsset);
                const validScopes = foundAsset.dataScopes.filter(
                  (scope) => SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type)
                );
                setDataScopes(validScopes.map((scope) => scope.dataScopeName));
                loadChannelsForAsset(foundAsset);
              } else {
                const title = isVariable ? `Asset (${query.assetRid})` : 'Asset (Direct RID)';
                setSelectedAsset(createBasicAsset(searchRid, title));
                setDataScopes(['dataset']);
                setChannels([]);
              }
            } catch (error) {
              if (!mounted) {
                return;
              }
              console.error('Failed to fetch asset by RID:', error);
              const title = isVariable ? `Asset (${query.assetRid})` : 'Asset (Direct RID)';
              setSelectedAsset(createBasicAsset(searchRid, title));
              setDataScopes(['dataset']);
              setChannels([]);
            }
          })();
        }
      }
    }

    return () => { mounted = false; };
  }, [query, selectedAsset, assets, hasManuallySetMethod, loadChannelsForAsset, datasource]);

  // Compute resolved asset RID on every render - this changes when template variables change
  const resolvedAssetRid = query?.assetRid ? getTemplateSrv().replace(query.assetRid) : '';

  // Update dropdown options when the resolved asset RID changes
  useEffect(() => {
    // Skip if not fully resolved (still contains $) or empty
    if (!resolvedAssetRid || resolvedAssetRid.includes('$')) {
      return;
    }

    // Skip if selectedAsset already matches the resolved RID
    if (selectedAsset?.rid === resolvedAssetRid) {
      return;
    }

    // Resolved RID changed - fetch new asset and update dropdowns
    const controller = new AbortController();
    (async () => {
      try {
        const foundAsset = await fetchAssetByRid(datasource.url!, resolvedAssetRid);
        if (controller.signal.aborted) {
          return;
        }
        if (foundAsset) {
          setSelectedAsset(foundAsset);
          const validScopes = foundAsset.dataScopes.filter(
            (scope) => SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type)
          );
          setDataScopes(validScopes.map((scope) => scope.dataScopeName));
          loadChannelsForAsset(foundAsset);
        } else {
          // Asset not found - create basic placeholder
          const title = query?.assetRid?.includes('$') ? `Asset (${query.assetRid})` : 'Asset (Direct RID)';
          setSelectedAsset(createBasicAsset(resolvedAssetRid, title));
          setDataScopes(['dataset']);
          setChannels([]);
        }
      } catch (error) {
        if (controller.signal.aborted) {
          return;
        }
        console.error('Failed to fetch asset for variable change:', error);
        // Set placeholder to prevent infinite retry loop
        const title = query?.assetRid?.includes('$') ? `Asset (${query.assetRid})` : 'Asset (Direct RID)';
        setSelectedAsset(createBasicAsset(resolvedAssetRid, title));
        setDataScopes(['dataset']);
        setChannels([]);
      }
    })();
    return () => controller.abort();
  }, [resolvedAssetRid, selectedAsset?.rid, datasource, loadChannelsForAsset, query?.assetRid]);

  // Load assets on component mount and when search query changes
  useEffect(() => {
    loadAssets();
  }, [loadAssets]);

  // After assets are loaded, check if we need to restore a selected asset (for duplicated queries)
  useEffect(() => {
    if (query && query.assetRid && !selectedAsset && assets.length > 0 && assetInputMethod === 'search') {
      const asset = assets.find(a => a.rid === query.assetRid);
      if (asset) {
        setSelectedAsset(asset);
      }
    }
  }, [query, selectedAsset, assets, assetInputMethod]);

  // Update dependent fields when asset changes
  useEffect(() => {
    if (selectedAsset) {
      // Extract data scope names from supported data source types
      const validScopes = selectedAsset.dataScopes.filter(
        (scope) => SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type)
      );
      const scopeNames = validScopes.map((scope) => scope.dataScopeName);
      setDataScopes(scopeNames);

      // Fetch channels from API
      loadChannelsForAsset(selectedAsset);

      // Auto-select data scope if only one available
      if (scopeNames.length === 1 && query?.dataScopeName !== scopeNames[0]) {
        onChange({ ...query, dataScopeName: scopeNames[0], queryType: 'decimation', buckets: 1000 });
      }
      // Update query with selected asset only if it has changed (search mode)
      else if (assetInputMethod === 'search' && query && query.assetRid !== selectedAsset.rid) {
        onChange({ ...query, assetRid: selectedAsset.rid });
      }
    }
  }, [selectedAsset, loadChannelsForAsset, onChange, query, assetInputMethod]);

  // Trigger graph update when query is complete
  useEffect(() => {
    const isQueryComplete = query && query.assetRid && query.channel && query.dataScopeName;
    if (isQueryComplete) {
      onRunQuery();
    }
  }, [query?.assetRid, query?.channel, query?.dataScopeName, query, onRunQuery]);

  const onAssetSelect = (selection: SelectableValue<string>) => {
    const asset = assets.find(a => a.rid === selection.value);
    setSelectedAsset(asset || null);
  };

  const onSearchQueryChange = (event: ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(event.target.value);
  };

  const handleSearchKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      loadAssets();
    }
  };


  // Prepare asset options for dropdown
  const assetOptions: Array<SelectableValue<string>> = assets.map(asset => {
    const datasetScopes = asset.dataScopes.filter(scope => scope.dataSource.type === 'dataset');
    return {
      label: asset.title,
      value: asset.rid,
      description: `${asset.labels.join(', ') || 'No labels'} - ${datasetScopes.length} dataset(s)`
    };
  });

  // Prepare channel options for dropdown - only show channels for the selected data scope
  const channelOptions: Array<SelectableValue<string>> = query?.dataScopeName
    ? channels
        .filter(ch => ch.dataScopeName === query.dataScopeName)
        .map(ch => ({
          label: ch.name,
          value: ch.name,
          description: ch.description
        }))
    : [];

  // Prepare data scope options for dropdown
  const dataScopeOptions: Array<SelectableValue<string>> = dataScopes.map(scope => ({
    label: scope,
    value: scope
  }));

  const handleAssetInputMethodChange = (method: AssetInputMethod) => {
    setAssetInputMethod(method);
    setHasManuallySetMethod(true); // Mark as manually set to prevent automatic overrides
    // Clear current selections when switching methods
    setSelectedAsset(null);
    setChannels([]);
    setDataScopes([]);
    setDirectRID(''); // Clear direct RID input
    onChange({ ...query, assetRid: '', channel: '', dataScopeName: '' });
  };

  const handleDirectRIDChange = useCallback(async (rid: string) => {
    setDirectRID(rid);

    if (rid.trim()) {
      try {
        // Resolve template variables (e.g., $asset or ${asset}) to actual RID
        const resolvedRid = getTemplateSrv().replace(rid);
        const isVariable = rid.includes('$');

        // If it's still a variable (not resolved), skip the search but still update query
        if (resolvedRid.includes('$')) {
          if (query && query.assetRid !== rid) {
            onChange({ ...query, assetRid: rid, queryType: 'decimation', buckets: 1000 });
          }
          return;
        }

        // Fetch asset by exact RID match
        const foundAsset = await fetchAssetByRid(datasource.url!, resolvedRid);
        if (foundAsset) {
          setSelectedAsset(foundAsset);
          const validScopes = foundAsset.dataScopes.filter(
            (scope) => SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type)
          );
          setDataScopes(validScopes.map((scope) => scope.dataScopeName));
          loadChannelsForAsset(foundAsset);
          onChange({ ...query, assetRid: rid, queryType: 'decimation', buckets: 1000 });
        } else {
          // Asset not found - create fallback
          const title = isVariable ? `Asset (${rid})` : 'Asset (Direct RID)';
          setSelectedAsset(createBasicAsset(resolvedRid, title));
          setDataScopes(['dataset']);
          setChannels([]);
          onChange({ ...query, assetRid: rid, queryType: 'decimation', buckets: 1000 });
        }
      } catch (error) {
        console.error('Failed to fetch asset by RID:', error);
        const resolvedRid = getTemplateSrv().replace(rid);
        const title = rid.includes('$') ? `Asset (${rid})` : 'Asset (Direct RID)';
        setSelectedAsset(createBasicAsset(resolvedRid.includes('$') ? rid : resolvedRid, title));
        setDataScopes(['dataset']);
        setChannels([]);
        onChange({ ...query, assetRid: rid, queryType: 'decimation', buckets: 1000 });
      }
    } else {
      setSelectedAsset(null);
      setDataScopes([]);
      setChannels([]);
    }
  }, [query, onChange, loadChannelsForAsset, datasource]);

  // Step completion status
  const assetComplete = assetInputMethod === 'search' ? selectedAsset !== null : directRID.trim() !== '';
  const configComplete = assetComplete && query && query.dataScopeName && query.channel;

  const singleBoxStyle = {
    padding: '8px 12px',
    backgroundColor: configComplete ? '#0d2818' : '#1f1f1f',
    borderRadius: '4px',
    border: configComplete ? '1px solid #28a745' : '1px solid #333',
    marginBottom: '4px',
    width: '100%'
  };

  return (
    <>
      <style>{`
        @keyframes fadeInOut {
          0% { opacity: 0; transform: translateY(5px); }
          20% { opacity: 1; transform: translateY(0); }
          80% { opacity: 1; transform: translateY(0); }
          100% { opacity: 0; transform: translateY(-5px); }
        }
      `}</style>
      <div style={{ width: '100%', padding: '4px' }}>
        <div style={singleBoxStyle}>
        <Stack gap={1} direction="row" wrap alignItems="center">
          {/* Asset Input Method */}
          <div style={{ marginRight: '8px' }}>
            <RadioButtonGroup
              options={[
                { label: 'Asset Search', value: 'search' },
                { label: 'Asset RID', value: 'direct' }
              ]}
              value={assetInputMethod}
              onChange={handleAssetInputMethodChange}
              size="sm"
            />
          </div>
          
          {/* Asset Selection */}
          {assetInputMethod === 'search' ? (
            <>
              <InlineField label="Search" labelWidth={8}>
                <Input
                  placeholder="Search assets"
                  value={searchQuery}
                  onChange={onSearchQueryChange}
                  onKeyDown={handleSearchKeyDown}
                  width={20}
                />
              </InlineField>
              
              {assets.length > 0 && (
                <InlineField label="Asset" labelWidth={8}>
                  {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                  <Select
                    key={`asset-select-${assets.length}`}
                    options={assetOptions}
                    value={assetOptions.some(opt => opt.value === query?.assetRid) ? query?.assetRid : ''}
                    onChange={onAssetSelect}
                    width={25}
                    placeholder="Choose asset..."
                    isLoading={isLoading}
                    isClearable={false}
                  />
                </InlineField>
              )}
            </>
          ) : (
            <InlineField label="Asset RID" labelWidth={12}>
              <Input
                placeholder="ri.scout.cerulean-staging.asset..."
                value={directRID}
                onChange={(e) => handleDirectRIDChange(e.currentTarget.value)}
                width={40}
              />
            </InlineField>
          )}
          
          {/* Channel Selection - only show if asset is selected */}
          {assetComplete && (
            <>
              <InlineField label="Data scope" labelWidth={12}>
                {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                <Select
                  value={dataScopeOptions.some(opt => opt.value === query?.dataScopeName) ? query?.dataScopeName : ''}
                  onChange={(value) => {
                    const newScope = value?.value;
                    // Check if current channel exists in the new scope
                    const channelExistsInNewScope = channels.some(
                      ch => ch.dataScopeName === newScope && ch.name === query?.channel
                    );
                    onChange({
                      ...query,
                      dataScopeName: newScope,
                      // Only clear channel if it doesn't exist in the new scope
                      channel: channelExistsInNewScope ? query?.channel : '',
                      queryType: 'decimation',
                      buckets: 1000
                    });
                  }}
                  options={dataScopeOptions}
                  placeholder="Choose scope..."
                  width={18}
                  isClearable={false}
                />
              </InlineField>
              
              <InlineField label="Channel" labelWidth={8}>
                {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                <Select
                  key={`channel-select-${query?.dataScopeName ?? 'none'}-${channelOptions.length}`}
                  value={channelOptions.some(opt => opt.value === query?.channel) ? query?.channel : ''}
                  onChange={(value) => {
                    onChange({
                      ...query,
                      channel: value?.value || '',
                      queryType: 'decimation',
                      buckets: 1000
                    });
                  }}
                  options={channelOptions}
                  placeholder="Choose channel..."
                  width={40}
                  isClearable={false}
                />
              </InlineField>
            </>
          )}
        </Stack>
        
        {/* Asset info display - compact single line */}
        {selectedAsset && (
          <div style={{ 
            marginTop: '6px',
            padding: '6px 10px', 
            backgroundColor: '#1f2937', 
            borderRadius: '4px',
            fontSize: '11px',
            border: '1px solid #374151',
            color: '#e5e7eb',
            lineHeight: '1.4'
          }}>
            <span style={{ color: '#9ca3af' }}>Asset:</span>
            <span style={{ 
              fontFamily: 'Monaco, "Lucida Console", monospace',
              color: '#d1d5db',
              backgroundColor: '#374151',
              padding: '2px 5px',
              borderRadius: '3px',
              marginLeft: '6px',
              marginRight: '8px'
            }}>
              {selectedAsset.title}
            </span>
            <span style={{ color: '#9ca3af' }}>RID:</span>
            <span style={{ position: 'relative', display: 'inline-block' }}>
              <span 
                onClick={() => copyToClipboard(selectedAsset.rid)}
                title="Click to copy RID"
                style={{ 
                  fontFamily: 'Monaco, "Lucida Console", monospace',
                  color: '#a78bfa',
                  cursor: 'pointer',
                  textDecoration: 'underline',
                  textDecorationStyle: 'dotted',
                  textDecorationColor: '#6b46c1',
                  marginLeft: '6px',
                  marginRight: '8px',
                  fontSize: '10px',
                  transition: 'background-color 0.15s ease, padding 0.15s ease'
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.backgroundColor = '#312e8120';
                  e.currentTarget.style.borderRadius = '2px';
                  e.currentTarget.style.padding = '1px 3px';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = 'transparent';
                  e.currentTarget.style.padding = '0';
                }}
              >
                {selectedAsset.rid}
              </span>
              {showCopiedMessage && (
                <span style={{
                  position: 'absolute',
                  top: '-25px',
                  left: '6px',
                  backgroundColor: '#065f46',
                  color: '#a7f3d0',
                  padding: '2px 6px',
                  borderRadius: '3px',
                  fontSize: '9px',
                  whiteSpace: 'nowrap',
                  border: '1px solid #047857',
                  zIndex: 1000,
                  animation: showCopiedMessage ? 'fadeInOut 2s ease-in-out' : 'none'
                }}>
                  ✓ Copied to clipboard
                </span>
              )}
            </span>
            <span style={{ color: '#9ca3af' }}>Dataset Scopes:</span>
            <span style={{ 
              color: '#34d399',
              fontWeight: '600',
              marginLeft: '4px'
            }}>
              {selectedAsset.dataScopes.filter(s => s.dataSource.type === 'dataset').length}
            </span>
          </div>
        )}
        </div>
      </div>
    </>
  );
}
