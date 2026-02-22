import React, { useState, useEffect, useRef, ChangeEvent, useCallback } from 'react';
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

/** Creates a minimal asset placeholder when the actual asset can't be fetched.
 *  dataScopes is intentionally empty — we don't fabricate scope data. */
const createBasicAsset = (rid: string, title: string): Asset => ({
  rid,
  title,
  labels: [],
  dataScopes: []
});

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
  if (asset?.dataScopes?.length > 0) {
    return asset;
  }
  // Log to help diagnose asset lookup failures (e.g. unexpected response format)
  console.warn('fetchAssetByRid: asset not found or has no dataScopes', { rid, responseKeys: Object.keys(response || {}), asset });
  return null;
};

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);
  const [channels, setChannels] = useState<Channel[]>([]);
  const [dataScopes, setDataScopes] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  // Initialize input method from saved query, defaulting to 'search'
  const [assetInputMethod, setAssetInputMethod] = useState<AssetInputMethod>(
    query?.assetInputMethod || 'search'
  );
  // Initialize directRID from saved query if using direct mode
  const [directRID, setDirectRID] = useState(
    query?.assetInputMethod === 'direct' ? (query?.assetRid || '') : ''
  );
  const [hasManuallySetMethod, setHasManuallySetMethod] = useState(false);
  const [showCopiedMessage, setShowCopiedMessage] = useState(false);
  // Track whether the user has interacted with query fields - prevents auto-clearing on initial load
  const [hasUserInteracted, setHasUserInteracted] = useState(false);

  // Ref to latest query — used by effects and callbacks that need fresh query values
  // without re-triggering when query changes (avoids onChange→query→effect cycles)
  const queryRef = useRef(query);
  queryRef.current = query;

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);

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
      setAssets([]);
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

  /** Fetch an asset by resolved RID and update selectedAsset/dataScopes/channels state.
   *  Returns early without updating state if `signal` is aborted. */
  const applyAssetFromRid = useCallback(async (
    resolvedRid: string,
    displayLabel: string,
    signal?: AbortSignal,
  ) => {
    if (!datasource.url) {
      return;
    }
    try {
      const foundAsset = await fetchAssetByRid(datasource.url, resolvedRid);
      if (signal?.aborted) {
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
        setSelectedAsset(createBasicAsset(resolvedRid, displayLabel));
        setDataScopes([]);
        setChannels([]);
      }
    } catch (error) {
      if (signal?.aborted) {
        return;
      }
      console.error('Failed to fetch asset by RID:', error);
      setSelectedAsset(createBasicAsset(resolvedRid, displayLabel));
      setDataScopes([]);
      setChannels([]);
    }
  }, [datasource.url, loadChannelsForAsset]);

  // Restore UI state from a saved query on mount / duplication.
  //
  // Decision tree:
  //  1. Skip if no assetRid, asset already selected, or user manually switched modes.
  //  2. Resolve template variables — skip if unresolvable (still contains '$').
  //  3. If saved method is 'direct' → fetch asset by RID.
  //  4. Else (search mode or no saved method):
  //     a. If asset is in current search results → select it in search mode.
  //     b. If assets are loaded but not found, and no saved method → infer direct mode.
  useEffect(() => {
    const controller = new AbortController();

    if (query && query.assetRid && !selectedAsset && !hasManuallySetMethod) {
      const resolved = getTemplateSrv().replace(query.assetRid);
      const displayLabel = query.assetRid.includes('$') ? `Asset (${query.assetRid})` : 'Asset (Direct RID)';

      if (!resolved.includes('$')) {
        if (query.assetInputMethod === 'direct') {
          applyAssetFromRid(resolved, displayLabel, controller.signal);
        } else {
          const asset = assets.find(a => a.rid === resolved);
          if (asset) {
            setAssetInputMethod('search');
            setSelectedAsset(asset);
          } else if (assets.length > 0 && !query.assetInputMethod) {
            setAssetInputMethod('direct');
            setDirectRID(query.assetRid);
            applyAssetFromRid(resolved, displayLabel, controller.signal);
          }
        }
      }
    }

    return () => controller.abort();
  }, [query, selectedAsset, assets, hasManuallySetMethod, applyAssetFromRid]);

  // Compute resolved asset RID on every render - this changes when template variables change
  const resolvedAssetRid = query?.assetRid ? getTemplateSrv().replace(query.assetRid) : '';

  // Compute resolved datascope name - this changes when template variables change
  const resolvedDataScopeName = query?.dataScopeName ? getTemplateSrv().replace(query.dataScopeName) : '';

  // Compute resolved channel name - this changes when template variables change
  const resolvedChannel = query?.channel ? getTemplateSrv().replace(query.channel) : '';

  // Update dropdown options when the resolved asset RID changes (e.g. template variable changed)
  useEffect(() => {
    if (!resolvedAssetRid || resolvedAssetRid.includes('$')) {
      return;
    }
    if (selectedAsset?.rid === resolvedAssetRid) {
      return;
    }
    const displayLabel = queryRef.current?.assetRid?.includes('$') ? `Asset (${queryRef.current.assetRid})` : 'Asset (Direct RID)';
    const controller = new AbortController();
    applyAssetFromRid(resolvedAssetRid, displayLabel, controller.signal);
    return () => controller.abort();
    // Only depend on selectedAsset?.rid (not the full object) to avoid aborting in-flight
    // fetches when the object reference changes but the RID stays the same.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resolvedAssetRid, selectedAsset?.rid, applyAssetFromRid]);

  // Load assets on component mount and when search query changes
  useEffect(() => {
    loadAssets();
  }, [loadAssets]);

  // After assets are loaded, check if we need to restore a selected asset (for duplicated queries)
  useEffect(() => {
    if (query && query.assetRid && !selectedAsset && assets.length > 0 && assetInputMethod === 'search') {
      // Resolve template variables to match against actual asset RIDs
      const resolvedRid = getTemplateSrv().replace(query.assetRid);
      const asset = assets.find(a => a.rid === resolvedRid);
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

      // Only auto-update query if user has interacted with the query builder
      // This prevents unwanted resets when just editing panel display settings
      if (hasUserInteracted) {
        const q = queryRef.current;
        // Check if current dataScopeName is valid for the new asset
        const resolvedCurrentScope = q?.dataScopeName
          ? getTemplateSrv().replace(q.dataScopeName)
          : '';
        const scopeIsValid = scopeNames.includes(resolvedCurrentScope);

        // Auto-select data scope if only one available (channel validated separately when channels load)
        if (scopeNames.length === 1 && q?.dataScopeName !== scopeNames[0]) {
          onChange({ ...q, dataScopeName: scopeNames[0], assetInputMethod, queryType: 'decimation', buckets: 1000 });
        }
        // Clear invalid data scope when asset changes (channel validated separately when channels load)
        else if (!scopeIsValid && q?.dataScopeName) {
          onChange({ ...q, dataScopeName: '', assetInputMethod, queryType: 'decimation', buckets: 1000 });
        }
        // Update query with selected asset only if it has changed (search mode)
        // Compare resolved values to preserve template variables that resolve to the same asset
        else if (assetInputMethod === 'search') {
          const resolvedCurrentRid = getTemplateSrv().replace(q?.assetRid || '');
          if (resolvedCurrentRid !== selectedAsset.rid) {
            onChange({ ...q, assetRid: selectedAsset.rid, assetInputMethod: 'search' });
          }
        }
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedAsset, loadChannelsForAsset, onChange, assetInputMethod, hasUserInteracted]);

  // Validate channel when channels are loaded - clear if invalid for current scope
  useEffect(() => {
    const q = queryRef.current;
    const currentChannel = q?.channel;
    if (!hasUserInteracted || !currentChannel || !channels.length) {
      return;
    }
    // Don't clear template variables - they are resolved at query time
    if (currentChannel.includes('$')) {
      return;
    }
    const channelExistsInScope = channels.some(
      ch => ch.dataScopeName === resolvedDataScopeName && ch.name === currentChannel
    );
    if (!channelExistsInScope) {
      onChange({ ...q, channel: '' });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [channels, resolvedDataScopeName, hasUserInteracted, onChange]);

  // Trigger graph update when query is complete
  useEffect(() => {
    const q = queryRef.current;
    const isQueryComplete = q && q.assetRid && q.channel && q.dataScopeName;
    if (isQueryComplete) {
      onRunQuery();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query?.assetRid, query?.channel, query?.dataScopeName, onRunQuery]);

  const onAssetSelect = (selection: SelectableValue<string>) => {
    setHasUserInteracted(true);
    const value = selection.value || '';
    const isVariable = value.includes('$');

    // Resolve to actual RID for asset lookup (variables need resolution)
    const ridToFind = isVariable ? getTemplateSrv().replace(value) : value;
    const asset = assets.find(a => a.rid === ridToFind);
    setSelectedAsset(asset || null);

    // Store variable syntax if variable, otherwise store the asset RID
    if (isVariable || asset) {
      onChange({ ...query, assetRid: isVariable ? value : asset!.rid, assetInputMethod: 'search' });
    }
  };

  const onSearchQueryChange = (event: ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(event.target.value);
  };

  const handleSearchKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      loadAssets();
    }
  };


  // Convert asset to dropdown option
  const assetToOption = (asset: Asset): SelectableValue<string> => {
    const supportedScopes = asset.dataScopes.filter(scope =>
      SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type)
    );
    return {
      label: asset.title,
      value: asset.rid,
      description: `${asset.labels.join(', ') || 'No labels'} - ${supportedScopes.length} data scope(s)`
    };
  };

  // Prepare asset options for dropdown
  const assetOptions: Array<SelectableValue<string>> = (() => {
    const options = assets.map(assetToOption);

    // Include the currently selected asset if it's not in the search results
    // This ensures the dropdown shows the current selection when switching modes
    if (selectedAsset && !assets.some(a => a.rid === selectedAsset.rid)) {
      options.unshift(assetToOption(selectedAsset));
    }

    // If the current assetRid is a template variable, add it as an option
    const currentValue = query?.assetRid || '';
    if (currentValue.includes('$') && !options.some(opt => opt.value === currentValue)) {
      // Show both the variable syntax and resolved asset title if available
      const resolvedTitle = selectedAsset?.title;
      const label = resolvedTitle && !resolvedTitle.includes('$')
        ? `${currentValue} → ${resolvedTitle}`
        : currentValue;
      options.unshift({
        label: label,
        value: currentValue,
        description: 'Template variable'
      });
    }

    return options;
  })();

  // Compute asset select value - show variable if set, otherwise match by resolved RID
  const assetSelectValue = (() => {
    const currentValue = query?.assetRid || '';
    if (currentValue.includes('$')) {
      return currentValue;
    }
    return assetOptions.some(opt => opt.value === resolvedAssetRid) ? resolvedAssetRid : '';
  })();

  // Prepare channel options for dropdown - only show channels for the selected data scope
  // Use resolved datascope name for filtering (handles template variables)
  const channelOptions: Array<SelectableValue<string>> = (() => {
    const options: Array<SelectableValue<string>> = resolvedDataScopeName
      ? channels
          .filter(ch => ch.dataScopeName === resolvedDataScopeName)
          .map(ch => ({
            label: ch.name,
            value: ch.name,
            description: ch.description
          }))
      : [];

    // If the current channel is a template variable, add it as an option
    const currentValue = query?.channel || '';
    if (currentValue.includes('$') && !options.some(opt => opt.value === currentValue)) {
      const resolved = resolvedChannel;
      const label = resolved && resolved !== currentValue && !resolved.includes('$')
        ? `${currentValue} → ${resolved}`
        : currentValue;
      options.unshift({
        label: label,
        value: currentValue
      });
    }

    return options;
  })();

  // Prepare data scope options for dropdown
  // Include template variable option if the current value is a variable
  const dataScopeOptions: Array<SelectableValue<string>> = (() => {
    const options = dataScopes.map(scope => ({
      label: scope,
      value: scope
    }));

    // If the current dataScopeName is a template variable, add it as an option
    const currentValue = query?.dataScopeName || '';
    if (currentValue.includes('$') && !dataScopes.includes(currentValue)) {
      // Show both the variable syntax and resolved value if different
      const resolved = resolvedDataScopeName;
      const label = resolved && resolved !== currentValue && !resolved.includes('$')
        ? `${currentValue} → ${resolved}`
        : currentValue;
      options.unshift({
        label: label,
        value: currentValue
      });
    }

    return options;
  })();

  const handleAssetInputMethodChange = (method: AssetInputMethod) => {
    setHasUserInteracted(true);
    setAssetInputMethod(method);
    setHasManuallySetMethod(true); // Mark as manually set to prevent automatic overrides
    setSelectedAsset(null);
    setChannels([]);
    setDataScopes([]);
    // Populate directRID from existing query when switching to direct mode
    setDirectRID(method === 'direct' ? (query?.assetRid || '') : '');
    // Only update input method, preserve other query values (assetRid, channel, dataScopeName)
    // so users don't lose their selection when quickly toggling between modes.
    onChange({ ...query, assetInputMethod: method });
  };

  // Debounced asset lookup for direct RID input — fires after user stops typing
  const directRidTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const directRidControllerRef = useRef<AbortController>(undefined);

  const handleDirectRIDChange = useCallback((rid: string) => {
    setHasUserInteracted(true);
    setDirectRID(rid);

    // Update query model immediately so the value is persisted
    if (rid.trim()) {
      onChange({ ...queryRef.current, assetRid: rid, assetInputMethod: 'direct', queryType: 'decimation', buckets: 1000 });
    }

    // Cancel any in-flight debounce / fetch
    clearTimeout(directRidTimerRef.current);
    directRidControllerRef.current?.abort();

    if (!rid.trim()) {
      setSelectedAsset(null);
      setDataScopes([]);
      setChannels([]);
      return;
    }

    // Resolve template variables
    const resolvedRid = getTemplateSrv().replace(rid);
    // If still unresolved, nothing more to do (query was already updated above)
    if (resolvedRid.includes('$')) {
      return;
    }

    const displayLabel = rid.includes('$') ? `Asset (${rid})` : 'Asset (Direct RID)';
    const controller = new AbortController();
    directRidControllerRef.current = controller;

    directRidTimerRef.current = setTimeout(() => {
      applyAssetFromRid(resolvedRid, displayLabel, controller.signal);
    }, 300);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [onChange, applyAssetFromRid]);

  // Clean up debounce timer on unmount
  useEffect(() => {
    return () => {
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();
    };
  }, []);

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
                    key={`asset-select-${assets.length}-${selectedAsset?.rid || ''}`}
                    options={assetOptions}
                    value={assetSelectValue}
                    onChange={onAssetSelect}
                    width={25}
                    placeholder="Choose asset..."
                    isLoading={isLoading}
                    isClearable={false}
                    allowCustomValue={true}
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
                  onChange={(value) => {
                    setHasUserInteracted(true);
                    const newScope = value?.value || '';
                    // Resolve to check if channel exists - use resolved value for comparison
                    const resolvedNewScope = newScope.includes('$')
                      ? getTemplateSrv().replace(newScope)
                      : newScope;
                    // Check if current channel exists in the new scope
                    const channelExistsInNewScope = channels.some(
                      ch => ch.dataScopeName === resolvedNewScope && ch.name === query?.channel
                    );
                    onChange({
                      ...query,
                      dataScopeName: newScope,
                      // Only clear channel if it doesn't exist in the new scope
                      channel: channelExistsInNewScope ? query?.channel : '',
                      assetInputMethod,
                      queryType: 'decimation',
                      buckets: 1000
                    });
                  }}
                  options={dataScopeOptions}
                  placeholder="Choose scope or use $variable..."
                  width={22}
                  isClearable={false}
                  allowCustomValue={true}
                />
              </InlineField>

              <InlineField label="Channel" labelWidth={8}>
                {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                <Select
                  key={`channel-select-${resolvedDataScopeName || 'none'}-${channelOptions.length}`}
                  value={(() => {
                    const currentValue = query?.channel || '';
                    if (currentValue.includes('$')) {
                      return currentValue;
                    }
                    return channelOptions.some(opt => opt.value === currentValue) ? currentValue : '';
                  })()}
                  onChange={(value) => {
                    setHasUserInteracted(true);
                    onChange({
                      ...query,
                      channel: value?.value || '',
                      assetInputMethod,
                      queryType: 'decimation',
                      buckets: 1000
                    });
                  }}
                  options={channelOptions}
                  placeholder="Choose channel or use $variable..."
                  width={40}
                  isClearable={false}
                  allowCustomValue={true}
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
