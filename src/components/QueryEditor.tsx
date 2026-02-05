import React, { useState, useEffect, ChangeEvent, useCallback } from 'react';
import { InlineField, Input, Stack, Select, RadioButtonGroup } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';
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
    console.log('🔍 loadAssets called with searchQuery:', searchQuery);
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
        // Filter assets to only include those with dataset data sources
        const filteredAssets = response.results.filter((asset: Asset) => {
          return asset.dataScopes && asset.dataScopes.length > 0 && 
                 asset.dataScopes.some(scope => scope.dataSource.type === 'dataset');
        });
        
        setAssets(filteredAssets);
        console.log('Loaded assets:', response.results.length, 'total →', filteredAssets.length, 'with datasets');
        console.log('First filtered asset:', filteredAssets[0]);
      } else {
        console.log('No results in response:', response);
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
    console.log('🔍 loadChannelsForAsset called for asset:', asset.title);
    try {
      // Extract data source RIDs from asset's data scopes using actual API-provided RIDs
      // Prefer dataset data sources for time-series channels; include other supported types when present
      const dataSourceRids: string[] = [];
      for (const scope of asset.dataScopes || []) {
        const ds = scope.dataSource;
        if (!ds) {
          continue;
        }
        if (ds.type === 'dataset' && ds.dataset) {
          dataSourceRids.push(ds.dataset);
        } else if (ds.type === 'connection' && (ds as any).connection) {
          dataSourceRids.push((ds as any).connection);
        } else if (ds.type === 'logSet' && (ds as any).logSet) {
          dataSourceRids.push((ds as any).logSet);
        } else if (ds.type === 'video' && (ds as any).video) {
          // Some backends may not return channels for video; include for completeness
          dataSourceRids.push((ds as any).video);
        }
      }

      if (dataSourceRids.length === 0) {
        console.log('No dataset scopes found for asset');
        setChannels([]);
        return;
      }

      console.log('Searching channels for data source RIDs:', dataSourceRids);

      // Call backend channels search endpoint
      const response = await getBackendSrv().post(
        `${datasource.url}/channels`,
        {
          dataSourceRids: dataSourceRids,
          // Provide empty fuzzy search to fetch as many channels as allowed by default page size
          searchText: ''
        }
      );

      if (response && response.channels) {
        // Transform API response to our Channel interface
        const datasetScopes = (asset.dataScopes || []).filter(scope => scope.dataSource.type === 'dataset');
        console.log('Available dataset scopes for channels:', datasetScopes);
        
        const apiChannels: Channel[] = response.channels.map((ch: any) => {
          // Try to match channel to appropriate data scope, or use first dataset scope
          const matchingScope = datasetScopes[0]; // For now, use first dataset scope
          console.log(`Channel ${ch.name} assigned to data scope:`, matchingScope?.dataScopeName);
          
          return {
            name: ch.name,
            type: ch.type || 'numeric',
            description: ch.description || `Channel: ${ch.name}`,
            dataScopeName: matchingScope?.dataScopeName || 'dataset'
          };
        });

        setChannels(apiChannels);
        console.log('Loaded channels from API:', apiChannels.length);
        console.log('Available channels:', apiChannels);
      } else {
        console.log('No channels returned from API');
        setChannels([]);
      }
    } catch (error) {
      console.error('Failed to load channels from API:', error);
      // Fallback to empty channels instead of hardcoded ones
      setChannels([]);
    }
  }, [datasource]);

  // Initialize state from existing query (handles duplicated queries)
  useEffect(() => {
    if (query && query.assetRid && !selectedAsset && !hasManuallySetMethod) {
      // Always try search mode first - check if asset exists in search results
      const asset = assets.find(a => a.rid === query.assetRid);
      if (asset) {
        // Found in search results - use search mode
        setAssetInputMethod('search');
        setSelectedAsset(asset);
      } else if (assets.length > 0) {
        // Assets are loaded but asset not found - must be direct RID input
        setAssetInputMethod('direct');
        setDirectRID(query.assetRid);
        // Manually search for the specific asset using the search API
        (async () => {
          try {
            const response = await getBackendSrv().post(
              `${datasource.url}/scout/v1/search-assets`,
              {
                query: {
                  searchText: query.assetRid,
                  type: 'searchText'
                },
                sort: {
                  field: 'CREATED_AT',
                  isDescending: false
                },
                pageSize: 100 // Search more assets to ensure we find the exact RID
              }
            );
            
            if (response && response.results) {
              // Find the exact asset by RID
              const asset = response.results.find((a: Asset) => a.rid === query.assetRid);
              if (asset && asset.dataScopes && asset.dataScopes.length > 0) {
                setSelectedAsset(asset);
                const datasetScopes = asset.dataScopes.filter((scope: any) => scope.dataSource.type === 'dataset');
                const scopeNames = datasetScopes.map((scope: any) => scope.dataScopeName);
                setDataScopes(scopeNames);
                loadChannelsForAsset(asset);
              } else {
                // Asset not found or no data scopes - create basic asset
                const basicAsset: Asset = {
                  rid: query.assetRid || '',
                  title: 'Asset (Direct RID)',
                  labels: [],
                  dataScopes: [{ dataScopeName: 'dataset', dataSource: { type: 'dataset' } }]
                };
                setSelectedAsset(basicAsset);
                setDataScopes(['dataset']);
                setChannels([]);
              }
            } else {
              const basicAsset: Asset = {
                rid: query.assetRid || '',
                title: 'Asset (Direct RID)',
                labels: [],
                dataScopes: [{ dataScopeName: 'dataset', dataSource: { type: 'dataset' } }]
              };
              setSelectedAsset(basicAsset);
              setDataScopes(['dataset']);
              setChannels([]);
            }
          } catch (error) {
            console.error('Failed to search for asset by RID:', error);
            const basicAsset: Asset = {
              rid: query.assetRid || '',
              title: 'Asset (Direct RID)',
              labels: [],
              dataScopes: [{ dataScopeName: 'dataset', dataSource: { type: 'dataset' } }]
            };
            setSelectedAsset(basicAsset);
            setDataScopes(['dataset']);
            setChannels([]);
          }
        })();
      }
    }
  }, [query, selectedAsset, assets, hasManuallySetMethod, loadChannelsForAsset, datasource]);

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
      // Extract data scope names - only dataset types
      const datasetScopes = selectedAsset.dataScopes.filter(scope => scope.dataSource.type === 'dataset');
      const scopeNames = datasetScopes.map(scope => scope.dataScopeName);
      setDataScopes(scopeNames);
      
      // Fetch channels from API instead of generating hardcoded ones
      loadChannelsForAsset(selectedAsset);
      
      console.log('Loading channels for asset:', selectedAsset.title);
      console.log('Data scopes:', scopeNames);
      console.log('Full asset data scopes:', selectedAsset.dataScopes);
      console.log('Selected asset details:', selectedAsset);
      
      // Update query with selected asset only if it has changed
      if (query && query.assetRid !== selectedAsset.rid) {
        onChange({ ...query, assetRid: selectedAsset.rid });
      }
    }
  }, [selectedAsset, loadChannelsForAsset, onChange, query]);

  // Trigger graph update when query is complete
  useEffect(() => {
    const isQueryComplete = query && query.assetRid && query.channel && query.dataScopeName;
    if (isQueryComplete) {
      console.log('Query is complete, triggering graph update');
      console.log('Final query configuration:', {
        assetRid: query.assetRid,
        channel: query.channel,
        dataScopeName: query.dataScopeName,
        queryType: query.queryType,
        buckets: query.buckets
      });
      onRunQuery();
    }
  }, [query?.assetRid, query?.channel, query?.dataScopeName, query, onRunQuery]);


  const onAssetSelect = (selection: SelectableValue<string>) => {
    const asset = assets.find(a => a.rid === selection.value);
    setSelectedAsset(asset || null);
    console.log('Selected asset:', asset);
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

  // Prepare channel options for dropdown
  const channelOptions: Array<SelectableValue<string>> = channels.map(ch => ({
    label: ch.name,
    value: ch.name,
    description: ch.description
  }));

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
    if (query && query.assetRid !== rid) {
      onChange({ ...query, assetRid: rid, queryType: 'decimation', buckets: 1000 });
    }
    
    if (rid.trim()) {
      try {
        // Search for the asset using the search API
        const response = await getBackendSrv().post(
          `${datasource.url}/scout/v1/search-assets`,
          {
            query: {
              searchText: rid,
              type: 'searchText'
            },
            sort: {
              field: 'CREATED_AT',
              isDescending: false
            },
            pageSize: 100 // Search more assets to ensure we find the exact RID
          }
        );
        
        if (response && response.results) {
          // Find the exact asset by RID
          const asset = response.results.find((a: Asset) => a.rid === rid);
          if (asset && asset.dataScopes && asset.dataScopes.length > 0) {
            // Use the found asset data
            setSelectedAsset(asset);
            
            // Extract data scope names - only dataset types
            const datasetScopes = asset.dataScopes.filter((scope: any) => scope.dataSource.type === 'dataset');
            const scopeNames = datasetScopes.map((scope: any) => scope.dataScopeName);
            setDataScopes(scopeNames);
            
            // Load channels using the actual asset data
            loadChannelsForAsset(asset);
          } else {
            // Asset not found or no data scopes - create basic asset
            const basicAsset: Asset = {
              rid: rid,
              title: 'Asset (Direct RID)',
              labels: [],
              dataScopes: [
                {
                  dataScopeName: 'dataset',
                  dataSource: { type: 'dataset' }
                }
              ]
            };
            setSelectedAsset(basicAsset);
            setDataScopes(['dataset']);
            setChannels([]); // Clear channels since we can't fetch them properly
          }
        } else {
          // No results from search API
          const basicAsset: Asset = {
            rid: rid,
            title: 'Asset (Direct RID)',
            labels: [],
            dataScopes: [
              {
                dataScopeName: 'dataset',
                dataSource: { type: 'dataset' }
              }
            ]
          };
          setSelectedAsset(basicAsset);
          setDataScopes(['dataset']);
          setChannels([]); // Clear channels since we can't fetch them properly
        }
      } catch (error) {
        console.error('Failed to search for asset by RID:', error);
        // Fallback to basic asset
        const basicAsset: Asset = {
          rid: rid,
          title: 'Asset (Direct RID)',
          labels: [],
          dataScopes: [
            {
              dataScopeName: 'dataset',
              dataSource: { type: 'dataset' }
            }
          ]
        };
        setSelectedAsset(basicAsset);
        setDataScopes(['dataset']);
        setChannels([]); // Clear channels since we can't fetch them properly
      }
    } else {
      setSelectedAsset(null);
      setDataScopes([]);
      setChannels([]);
    }
  }, [query, onChange, loadChannelsForAsset, datasource]);

  // Step completion status
  const assetSelected = assetInputMethod === 'search' ? selectedAsset !== null : directRID.trim() !== '';
  const assetComplete = assetSelected;
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
                    options={assetOptions}
                    value={query?.assetRid || ''}
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
                  value={query?.dataScopeName || ''}
                  onChange={(value) => onChange({ ...query, dataScopeName: value?.value, queryType: 'decimation', buckets: 1000 })}
                  options={dataScopeOptions}
                  placeholder="Choose scope..."
                  width={18}
                  isClearable={false}
                />
              </InlineField>
              
              <InlineField label="Channel" labelWidth={8}>
                {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                <Select
                  value={query?.channel || ''}
                  onChange={(value) => {
                    const selectedChannel = channels.find(ch => ch.name === value?.value);
                    onChange({ 
                      ...query, 
                      channel: value?.value || '',
                      dataScopeName: selectedChannel?.dataScopeName || query?.dataScopeName,
                      queryType: 'decimation',
                      buckets: 1000
                    });
                  }}
                  options={channelOptions}
                  placeholder="Choose channel..."
                  width={20}
                  allowCustomValue
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
