import React, { useState, useEffect, useRef, ChangeEvent, useCallback } from 'react';
import { debounce } from 'lodash';
import { InlineField, Input, Stack, Select, RadioButtonGroup } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { DataSource } from '../datasource';
import { NominalDataSourceOptions, NominalQuery } from '../types';

type Props = QueryEditorProps<DataSource, NominalQuery, NominalDataSourceOptions>;

/** Channel option with typed dataType field, extending Grafana's SelectableValue. */
type ChannelOption = SelectableValue<string> & { dataType?: string };

/** Extensible mapping from Nominal channel dataType to Grafana icon name.
 *  Add entries here for future types (e.g., log: 'gf-logs'). */
const DATA_TYPE_ICONS: Record<string, string> = {
  string: 'font',           // Grafana's standard icon for FieldType.string
  numeric: 'calculator-alt', // Grafana's standard icon for FieldType.number
};

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

/** TTL cache for fetchAssetByRid — collapses concurrent requests for the same RID
 *  (e.g. 70 panels rendering simultaneously) into a single API call. */
const assetCache = new Map<string, { promise: Promise<Asset | null>; expiresAt: number }>();
const ASSET_CACHE_TTL_MS = 30_000; // 30 seconds

/** Clears the asset cache. Exported for testing only. */
export const clearAssetCache = () => assetCache.clear();

/** Fetches a single asset by its exact RID using the batch lookup endpoint.
 *  Results are cached for 30s so that multiple panels sharing the same asset
 *  (common with template variables) don't each trigger a separate HTTP request. */
export const fetchAssetByRid = async (datasourceUrl: string, rid: string): Promise<Asset | null> => {
  // Validate RID format - must start with "ri." to be a valid resource identifier
  if (!rid || !rid.startsWith('ri.')) {
    return null;
  }

  const cacheKey = `${datasourceUrl}|${rid}`;
  const cached = assetCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.promise;
  }

  // Use the efficient batch lookup endpoint instead of searching all assets
  const promise = getBackendSrv().post(
    `${datasourceUrl}/scout/v1/asset/multiple`,
    [rid]  // API expects an array of RIDs
  ).then((response): Asset | null => {
    // Response is a map: { "ri.scout...": { rid, title, dataScopes, ... } }
    const asset = response?.[rid];
    if (asset && asset.dataScopes?.length > 0) {
      return asset;
    }
    // Log to help diagnose asset lookup failures (e.g. unexpected response format)
    console.warn('fetchAssetByRid: asset not found in response', { rid, responseKeys: Object.keys(response || {}) });
    return null;
  });

  assetCache.set(cacheKey, { promise, expiresAt: Date.now() + ASSET_CACHE_TTL_MS });

  // On failure, evict so the next call retries immediately
  promise.catch(() => {
    assetCache.delete(cacheKey);
  });

  return promise;
};

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);
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
  // Derive whether user has ever saved an explicit input method (persisted in query model).
  // Initialising from query rather than defaulting to false prevents the restore effect
  // from running unnecessary branches after a panel reload.
  const [hasManuallySetMethod, setHasManuallySetMethod] = useState(!!query?.assetInputMethod);
  const [showCopiedMessage, setShowCopiedMessage] = useState(false);
  // Track whether the user has interacted with query fields - prevents auto-clearing on initial load
  const [hasUserInteracted, setHasUserInteracted] = useState(false);

  // Ref to latest query — used by effects and callbacks that need fresh query values
  // without re-triggering when query changes (avoids onChange→query→effect cycles)
  const queryRef = useRef(query);
  queryRef.current = query;

  // Ref for the "copied" tooltip hide-timer so it can be cleared on unmount.
  const copiedTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  const copyToClipboard = async (text: string) => {
    // Clear any existing hide-timer before starting a new one
    clearTimeout(copiedTimerRef.current);
    try {
      await navigator.clipboard.writeText(text);

      // Show ephemeral "copied" message
      setShowCopiedMessage(true);
      copiedTimerRef.current = setTimeout(() => {
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
      copiedTimerRef.current = setTimeout(() => {
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

  // Dynamically search channels via backend; called by the Select loadOptions prop.
  const loadChannelOptions = useCallback(async (searchText: string): Promise<ChannelOption[]> => {
    if (!selectedAsset) {
      return [];
    }
    const resolvedScope = query?.dataScopeName ? getTemplateSrv().replace(query.dataScopeName) : '';
    const scopes = (selectedAsset.dataScopes || []).filter(
      (scope) => !resolvedScope || scope.dataScopeName === resolvedScope
    );
    const dataSourceRids: string[] = [];
    for (const scope of scopes) {
      const ds = scope.dataSource;
      if (!ds) { continue; }
      // Only collect RIDs for supported data source types (dataset, connection)
      if (ds.type === 'dataset' && ds.dataset) { dataSourceRids.push(ds.dataset); }
      else if (ds.type === 'connection' && (ds as any).connection) { dataSourceRids.push((ds as any).connection); }
    }
    if (dataSourceRids.length === 0) {
      return [];
    }
    try {
      const response = await getBackendSrv().post(
        `${datasource.url}/channels`,
        { dataSourceRids, searchText }
      );
      if (response?.channels) {
        return response.channels.map((ch: any) => ({
          label: ch.name,
          value: ch.name,
          description: ch.description || `Channel: ${ch.name}`,
          dataType: ch.dataType || '',
          icon: ch.dataType ? DATA_TYPE_ICONS[ch.dataType] : undefined,
        }));
      }
      return [];
    } catch (error) {
      console.error('Failed to load channel options:', error);
      return [];
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedAsset, datasource, query?.dataScopeName]);

  // Keep a ref so the stable debounce below always calls the latest closure without
  // needing to be recreated (and without leaving stale pending timeouts behind).
  const loadChannelOptionsRef = useRef(loadChannelOptions);
  loadChannelOptionsRef.current = loadChannelOptions;

  // Lazily initialized ref — created once on first render with no side effects.
  // Wraps the debounce in a Promise so loadOptions always returns a Promise
  // (lodash debounce itself returns undefined, not the inner async result).
  // Superseded Promises are resolved immediately with [] so they never hang.
  type DebouncedChannelLoader = {
    loadOptions: (searchText: string) => Promise<ChannelOption[]>;
    cancel: () => void;
  };
  const debouncedRef = useRef<DebouncedChannelLoader | null>(null);
  if (debouncedRef.current === null) {
    let pendingResolve: ((opts: ChannelOption[]) => void) | null = null;
    let pendingSearchText = '';
    const debounced = debounce(() => {
      const resolve = pendingResolve;
      pendingResolve = null;
      if (resolve) {
        loadChannelOptionsRef.current(pendingSearchText).then(resolve).catch(() => resolve([]));
      }
    }, 300);
    debouncedRef.current = {
      loadOptions: (searchText: string) => {
        pendingResolve?.([]);
        pendingSearchText = searchText;
        return new Promise(resolve => {
          pendingResolve = resolve;
          debounced();
        });
      },
      cancel: () => {
        debounced.cancel();
        pendingResolve?.([]);
        pendingResolve = null;
      },
    };
  }
  const debouncedLoadChannelOptions = debouncedRef.current.loadOptions;

  useEffect(() => () => debouncedRef.current!.cancel(), []);

  /** Fetch an asset by resolved RID and update selectedAsset/dataScopes state.
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
      } else {
        setSelectedAsset(createBasicAsset(resolvedRid, displayLabel));
        setDataScopes([]);
      }
    } catch (error) {
      if (signal?.aborted) {
        return;
      }
      console.error('Failed to fetch asset by RID:', error);
      setSelectedAsset(createBasicAsset(resolvedRid, displayLabel));
      setDataScopes([]);
    }
  }, [datasource.url]);

  // Restore UI state from a saved query on mount / duplication.
  //
  // Decision tree:
  //  1. Skip if no assetRid or asset already selected.
  //  2. Skip if user manually switched modes UNLESS the saved method is 'direct'
  //     (direct-mode queries initialize hasManuallySetMethod=true from the saved value,
  //     so we must still allow them through here for the initial asset fetch to happen).
  //  3. Resolve template variables — skip if unresolvable (still contains '$').
  //  4. If saved method is 'direct' → fetch asset by RID.
  //  5. Else (search mode or no saved method):
  //     a. If asset is in current search results → select it in search mode.
  //     b. If assets are loaded but not found, and no saved method → infer direct mode.
  useEffect(() => {
    const controller = new AbortController();

    if (query && query.assetRid && !selectedAsset && (!hasManuallySetMethod || query.assetInputMethod === 'direct')) {
      const resolved = getTemplateSrv().replace(query.assetRid);
      const displayLabel = query.assetRid.includes('$') ? `Asset (${query.assetRid})` : 'Asset (Direct RID)';

      // For direct mode, always ensure the input field shows the saved RID (including
      // template variable syntax) regardless of whether the variable is currently resolved.
      // This prevents a blank input when reloading a panel with a $variable-based direct RID.
      if (query.assetInputMethod === 'direct') {
        setDirectRID(prev => prev || query.assetRid || '');
      }

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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query?.assetRid, query?.assetInputMethod, selectedAsset, assets, hasManuallySetMethod, applyAssetFromRid]);

  // Compute resolved asset RID on every render - this changes when template variables change
  const resolvedAssetRid = query?.assetRid ? getTemplateSrv().replace(query.assetRid) : '';

  // Compute resolved datascope name - this changes when template variables change
  const resolvedDataScopeName = query?.dataScopeName ? getTemplateSrv().replace(query.dataScopeName) : '';

  // Compute resolved channel name - this changes when template variables change
  const resolvedChannel = query?.channel ? getTemplateSrv().replace(query.channel) : '';

  // Update dropdown options when the resolved asset RID changes (e.g. template variable changed).
  // Skipped in direct mode because handleDirectRIDChange manages its own debounced fetch —
  // running both would cause two concurrent requests racing to update selectedAsset.
  useEffect(() => {
    if (!resolvedAssetRid || resolvedAssetRid.includes('$')) {
      return;
    }
    if (selectedAsset?.rid === resolvedAssetRid) {
      return;
    }
    // In direct mode the handler's debounced timer owns the fetch lifecycle; skip here.
    if (queryRef.current?.assetInputMethod === 'direct' && !queryRef.current?.assetRid?.includes('$')) {
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

  // After assets are loaded, restore a selected asset for search-mode queries when the user
  // has already confirmed their input method (hasManuallySetMethod). This covers the case
  // where the asset list loads after the restore effect has already run and found nothing.
  // Guarded by hasManuallySetMethod to avoid overlapping with the restore effect above
  // when both are eligible to set selectedAsset simultaneously (React 18 strict mode concern).
  useEffect(() => {
    if (query && query.assetRid && !selectedAsset && assets.length > 0 && assetInputMethod === 'search' && hasManuallySetMethod) {
      // Resolve template variables to match against actual asset RIDs
      const resolvedRid = getTemplateSrv().replace(query.assetRid);
      const asset = assets.find(a => a.rid === resolvedRid);
      if (asset) {
        setSelectedAsset(asset);
      }
    }
  }, [query, selectedAsset, assets, assetInputMethod, hasManuallySetMethod]);

  // Update dependent fields when asset changes
  useEffect(() => {
    if (selectedAsset) {
      // Extract data scope names from supported data source types
      const validScopes = selectedAsset.dataScopes.filter(
        (scope) => SUPPORTED_DATA_SOURCE_TYPES.includes(scope.dataSource.type)
      );
      const scopeNames = validScopes.map((scope) => scope.dataScopeName);
      setDataScopes(scopeNames);

      // Only auto-update query if user has interacted with the query builder
      // This prevents unwanted resets when just editing panel display settings
      if (hasUserInteracted) {
        const q = queryRef.current;
        // Check if current dataScopeName is valid for the new asset
        const resolvedCurrentScope = q?.dataScopeName
          ? getTemplateSrv().replace(q.dataScopeName)
          : '';
        const scopeIsValid = scopeNames.includes(resolvedCurrentScope);

        // Preserve template variables — don't overwrite $variable with resolved scope
        if (q?.dataScopeName?.includes('$')) {
          // skip — variable will be resolved at query time
        }
        // Auto-select data scope if only one available
        else if (scopeNames.length === 1 && q?.dataScopeName !== scopeNames[0]) {
          onChange({ ...q, dataScopeName: scopeNames[0], assetInputMethod, queryType: 'decimation', buckets: 1000 });
        }
        // Clear invalid data scope when asset changes
        else if (!scopeIsValid && q?.dataScopeName) {
          onChange({ ...q, dataScopeName: '', assetInputMethod, queryType: 'decimation', buckets: 1000 });
        }
        // Update query with selected asset only if it has changed (search mode)
        // Preserve template variables — don't overwrite $variable with resolved RID
        else if (assetInputMethod === 'search' && !q?.assetRid?.includes('$')) {
          const resolvedCurrentRid = getTemplateSrv().replace(q?.assetRid || '');
          if (resolvedCurrentRid !== selectedAsset.rid) {
            onChange({ ...q, assetRid: selectedAsset.rid, assetInputMethod: 'search' });
          }
        }
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedAsset, onChange, assetInputMethod, hasUserInteracted]);

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

    if (asset) {
      // Abort any in-flight search-mode fetch from a previous selection
      assetSelectControllerRef.current?.abort();
      setSelectedAsset(asset);
    } else if (ridToFind && !ridToFind.includes('$')) {
      // Asset not in search results — fetch it directly instead of nulling selectedAsset.
      // This avoids a UI flash where channel/scope selectors unmount during the fetch.
      // Abort any previous in-flight fetch before starting a new one.
      assetSelectControllerRef.current?.abort();
      const controller = new AbortController();
      assetSelectControllerRef.current = controller;
      const displayLabel = isVariable ? `Asset (${value})` : 'Asset (Direct RID)';
      applyAssetFromRid(ridToFind, displayLabel, controller.signal);
    } else {
      assetSelectControllerRef.current?.abort();
      setSelectedAsset(null);
    }

    // Store variable syntax if variable, resolved RID if fetched directly, or asset RID from search
    if (isVariable) {
      onChange({ ...query, assetRid: value, assetInputMethod: 'search' });
    } else if (asset) {
      onChange({ ...query, assetRid: asset.rid, assetInputMethod: 'search' });
    } else if (ridToFind && !ridToFind.includes('$')) {
      onChange({ ...query, assetRid: ridToFind, assetInputMethod: 'search' });
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
      // Only show resolution if it's valid for the current asset's scopes
      const resolved = resolvedDataScopeName;
      const resolvedIsValid = resolved && resolved !== currentValue && !resolved.includes('$')
        && (!dataScopes.length || dataScopes.includes(resolved));
      const label = resolvedIsValid
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
    setDataScopes([]);
    // Populate directRID from existing query when switching to direct mode
    setDirectRID(method === 'direct' ? (query?.assetRid || '') : '');
    // Only update input method, preserve other query values (assetRid, channel, dataScopeName)
    // so users don't lose their selection when quickly toggling between modes.
    onChange({ ...query, assetInputMethod: method });
  };

  // AbortController for search-mode asset selection — cancels in-flight fetch on rapid re-selection
  const assetSelectControllerRef = useRef<AbortController>(undefined);

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
      onChange({ ...queryRef.current, assetRid: '', assetInputMethod: 'direct' });
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

  // Clean up timers and in-flight fetches on unmount
  useEffect(() => {
    return () => {
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();
      assetSelectControllerRef.current?.abort();
      clearTimeout(copiedTimerRef.current);
    };
  }, []);

  // Step completion status
  const assetComplete = assetInputMethod === 'search'
    ? (resolvedAssetRid !== '' && !resolvedAssetRid.includes('$'))
    : directRID.trim() !== '';
  const configComplete = assetComplete && query && query.dataScopeName && query.channel;
  // Show the channel selector whenever an asset is selected (even if dataScopes is empty).
  // With empty dataScopes, loadChannelOptions returns [] — the user can still type a channel
  // name manually via allowCustomValue. Previously, empty dataScopes from createBasicAsset
  // (used when the asset fetch fails) would silently hide the channel selector entirely.
  const hasChannelSearch = selectedAsset !== null;

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
                    width={30}
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
                    onChange({
                      ...query,
                      dataScopeName: newScope,
                      assetInputMethod,
                      queryType: 'decimation',
                      buckets: 1000
                    });
                  }}
                  options={dataScopeOptions}
                  placeholder="Choose scope or use $variable..."
                  width={30}
                  isClearable={false}
                  allowCustomValue={true}
                  isLoading={!selectedAsset && assetComplete}
                />
              </InlineField>

              {hasChannelSearch && <InlineField label="Channel" labelWidth={8}>
                {/* eslint-disable-next-line @typescript-eslint/no-deprecated */}
                <Select
                  key={`${resolvedAssetRid || 'no-asset'}-${resolvedDataScopeName}`}
                  value={query?.channel ? {
                    label: query.channel.includes('$') && resolvedChannel && resolvedChannel !== query.channel && !resolvedChannel.includes('$')
                      ? `${query.channel} → ${resolvedChannel}`
                      : query.channel,
                    value: query.channel
                  } : null}
                  onChange={(value: ChannelOption) => {
                    setHasUserInteracted(true);
                    // NOTE: channelDataType is captured at selection time from the dropdown option.
                    // If channel is later overridden by a template variable that resolves to a
                    // different channel, the stored channelDataType may be stale. The backend will
                    // fall back to numeric for an unknown type, but mismatches can cause query errors.
                    // Mitigation: the backend error message hints the user to re-select the channel.
                    onChange({
                      ...query,
                      channel: value?.value || '',
                      channelDataType: value?.dataType || '',
                      dataScopeName: query?.dataScopeName || '',
                      assetInputMethod,
                      queryType: 'decimation',
                      buckets: 1000
                    });
                  }}
                  loadOptions={debouncedLoadChannelOptions}
                  defaultOptions
                  placeholder="Search for channel..."
                  width={50}
                  allowCustomValue
                  isClearable={false}
                />
              </InlineField>}
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
