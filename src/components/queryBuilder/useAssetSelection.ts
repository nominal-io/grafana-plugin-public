import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AppEvents, type SelectableValue } from '@grafana/data';
import { getAppEvents, getTemplateSrv } from '@grafana/runtime';
import type { NominalQuery } from '../../types';
import {
  createBasicAsset,
  fetchAssetByRid,
  getSupportedScopeNames,
  getSupportedScopes,
  searchAssets,
  type Asset,
} from '../../utils/api';
import { buildAssetOptions, buildDataScopeOptions, getAssetSelectValue } from './queryBuilderOptions';
import type { AssetInputMethod } from './queryBuilderTypes';

interface UseAssetSelectionArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  datasourceUrl: string;
  resolvedAssetRid: string;
  resolvedDataScopeName: string;
  hasUserInteracted: boolean;
  markInteracted: () => void;
}

export interface AssetSelectionModel {
  assetInputMethod: AssetInputMethod;
  directRID: string;
  searchQuery: string;
  selectedAsset: Asset | null;
  assetSearchResultCount: number;
  selectedAssetSupportedScopeCount: number;
  assetOptions: Array<SelectableValue<string>>;
  assetSelectValue: string;
  dataScopeOptions: Array<SelectableValue<string>>;
  isLoadingAssets: boolean;
  changeAssetInputMethod: (method: AssetInputMethod) => void;
  changeAssetSearchQuery: (value: string) => void;
  runAssetSearch: () => void;
  selectAsset: (selection: SelectableValue<string>) => void;
  changeDirectRID: (rid: string) => void;
  selectDataScope: (dataScopeName: string) => void;
}

const notifyError = (title: string, message: string) => {
  getAppEvents().publish({
    type: AppEvents.alertError.name,
    payload: [title, message],
  });
};

export function useAssetSelection({
  query,
  onChange,
  datasourceUrl,
  resolvedAssetRid,
  resolvedDataScopeName,
  hasUserInteracted,
  markInteracted,
}: UseAssetSelectionArgs): AssetSelectionModel {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);
  const [dataScopes, setDataScopes] = useState<string[]>([]);
  const [isLoadingAssets, setIsLoadingAssets] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  // Initialize input method from saved query, defaulting to 'search'
  const [assetInputMethod, setAssetInputMethod] = useState<AssetInputMethod>(query?.assetInputMethod || 'search');
  // Initialize directRID from saved query if using direct mode
  const [directRID, setDirectRID] = useState(query?.assetInputMethod === 'direct' ? query?.assetRid || '' : '');
  // Derive whether user has ever saved an explicit input method (persisted in query model).
  // Initialising from query rather than defaulting to false prevents the restore effect
  // from running unnecessary branches after a panel reload.
  const [hasManuallySetMethod, setHasManuallySetMethod] = useState(!!query?.assetInputMethod);

  // Ref to latest query - used by effects and callbacks that need fresh query values
  // without re-triggering when query changes (avoids onChange -> query -> effect cycles)
  const queryRef = useRef(query);
  queryRef.current = query;

  // AbortController for search-mode asset selection - cancels in-flight fetch on rapid re-selection
  const assetSelectControllerRef = useRef<AbortController>(undefined);

  // Tracks the resolved RID currently being fetched by applyAssetFromRid so that concurrent
  // effects (mount restore + resolved-asset) don't both fetch the same asset.
  const pendingAssetRidRef = useRef<string | undefined>(undefined);

  // Debounced asset lookup for direct RID input - fires after user stops typing
  const directRidTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const directRidControllerRef = useRef<AbortController>(undefined);

  const loadAssets = useCallback(async () => {
    setIsLoadingAssets(true);
    try {
      setAssets(await searchAssets(datasourceUrl, searchQuery));
    } catch {
      notifyError('Unable to load Nominal assets', 'Check the data source configuration and try again.');
      setAssets([]);
    } finally {
      setIsLoadingAssets(false);
    }
  }, [searchQuery, datasourceUrl]);

  /** Fetch an asset by resolved RID and update selectedAsset/dataScopes state.
   *  Returns early without updating state if `signal` is aborted. */
  const applyAssetFromRid = useCallback(
    async (resolvedRid: string, displayLabel: string, signal?: AbortSignal) => {
      // Mark this RID as in-flight so concurrent effects skip a duplicate fetch.
      pendingAssetRidRef.current = resolvedRid;
      try {
        const foundAsset = await fetchAssetByRid(datasourceUrl, resolvedRid);
        if (signal?.aborted) {
          return;
        }
        if (foundAsset) {
          setSelectedAsset(foundAsset);
          setDataScopes(getSupportedScopeNames(foundAsset));
        } else {
          setSelectedAsset(createBasicAsset(resolvedRid, displayLabel));
          setDataScopes([]);
        }
      } catch {
        if (signal?.aborted) {
          return;
        }
        notifyError('Unable to load Nominal asset', 'The RID was kept, but data scopes could not be loaded automatically.');
        setSelectedAsset(createBasicAsset(resolvedRid, displayLabel));
        setDataScopes([]);
      } finally {
        // Clear only if no newer fetch has since claimed the in-flight slot for a different RID.
        if (pendingAssetRidRef.current === resolvedRid) {
          pendingAssetRidRef.current = undefined;
        }
      }
    },
    [datasourceUrl]
  );

  // Restore a saved DIRECT-mode asset on mount / duplication.
  // Deliberately does NOT depend on `assets`: the by-RID fetch path never reads the
  // search list, so letting search-assets resolution (setAssets) re-run this effect
  // would only abort and re-issue an identical in-flight fetch (Caveat B). Keeping
  // `assets` out of the deps removes that redundant cancelled request.
  // MUST stay ordered before the resolved-asset effect so pendingAssetRidRef is set
  // first (see plan Decision 2 / the "fetches a saved direct template RID only once" test).
  useEffect(() => {
    if (!query?.assetRid || selectedAsset || query.assetInputMethod !== 'direct') {
      return;
    }
    // Always show the saved RID (incl. unresolved $variable) in the direct input.
    setDirectRID((prev) => prev || query.assetRid || '');

    const resolved = getTemplateSrv().replace(query.assetRid);
    if (resolved.includes('$')) {
      return;
    }
    const displayLabel = query.assetRid.includes('$') ? `Asset (${query.assetRid})` : 'Asset (Direct RID)';
    const controller = new AbortController();
    applyAssetFromRid(resolved, displayLabel, controller.signal);
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query?.assetRid, query?.assetInputMethod, selectedAsset, applyAssetFromRid]);

  // Restore a SEARCH-mode asset / infer direct mode for a saved RID, once assets are known.
  // This branch legitimately depends on `assets` (it matches against the loaded list).
  // Only entered when the user never saved an explicit method (hasManuallySetMethod=false);
  // the hasManuallySetMethod search case is handled by the search-restore effect below.
  useEffect(() => {
    if (
      !query?.assetRid ||
      selectedAsset ||
      hasManuallySetMethod ||
      query.assetInputMethod === 'direct'
    ) {
      return;
    }
    const resolved = getTemplateSrv().replace(query.assetRid);
    if (resolved.includes('$')) {
      return;
    }
    const asset = assets.find((a) => a.rid === resolved);
    if (asset) {
      setAssetInputMethod('search');
      setSelectedAsset(asset);
    } else if (assets.length > 0 && !query.assetInputMethod) {
      const displayLabel = query.assetRid.includes('$') ? `Asset (${query.assetRid})` : 'Asset (Direct RID)';
      setAssetInputMethod('direct');
      setDirectRID(query.assetRid);
      const controller = new AbortController();
      applyAssetFromRid(resolved, displayLabel, controller.signal);
      return () => controller.abort();
    }
    return undefined;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query?.assetRid, query?.assetInputMethod, selectedAsset, assets, hasManuallySetMethod, applyAssetFromRid]);

  // Update dropdown options when the resolved asset RID changes (e.g. template variable changed).
  // Skipped in direct mode because changeDirectRID manages its own debounced fetch -
  // running both would cause two concurrent requests racing to update selectedAsset.
  useEffect(() => {
    if (!resolvedAssetRid || resolvedAssetRid.includes('$')) {
      return;
    }
    if (selectedAsset?.rid === resolvedAssetRid) {
      return;
    }
    // Skip if another path (e.g. the mount restore effect) is already fetching this RID.
    // Prevents a saved direct query with a template RID ($asset) from being fetched twice.
    // Invariant: every path that fetches an asset for selection starts from the current
    // query/source RID and either has already persisted that RID (changeDirectRID) or is
    // driven by an existing saved query. Stale completions are ignored by their abort
    // signals, so the abort-then-skip over-suppression window is unreachable without a
    // generation counter.
    if (pendingAssetRidRef.current === resolvedAssetRid) {
      return;
    }
    // In direct mode the handler's debounced timer owns the fetch lifecycle; skip here.
    if (queryRef.current?.assetInputMethod === 'direct' && !queryRef.current?.assetRid?.includes('$')) {
      return;
    }
    const displayLabel = queryRef.current?.assetRid?.includes('$')
      ? `Asset (${queryRef.current.assetRid})`
      : 'Asset (Direct RID)';
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
    if (
      query &&
      query.assetRid &&
      !selectedAsset &&
      assets.length > 0 &&
      assetInputMethod === 'search' &&
      hasManuallySetMethod
    ) {
      // Resolve template variables to match against actual asset RIDs
      const resolvedRid = getTemplateSrv().replace(query.assetRid);
      const asset = assets.find((a) => a.rid === resolvedRid);
      if (asset) {
        setSelectedAsset(asset);
      }
    }
  }, [query, selectedAsset, assets, assetInputMethod, hasManuallySetMethod]);

  // Update dependent fields when asset changes
  useEffect(() => {
    if (selectedAsset) {
      const scopeNames = getSupportedScopeNames(selectedAsset);
      setDataScopes(scopeNames);

      // Only auto-update query if user has interacted with the query builder
      // This prevents unwanted resets when just editing panel display settings
      if (hasUserInteracted) {
        const q = queryRef.current;
        // Check if current dataScopeName is valid for the new asset
        const resolvedCurrentScope = q?.dataScopeName ? getTemplateSrv().replace(q.dataScopeName) : '';
        const scopeIsValid = scopeNames.includes(resolvedCurrentScope);

        // Preserve template variables - don't overwrite $variable with resolved scope
        if (q?.dataScopeName?.includes('$')) {
          // skip - variable will be resolved at query time
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
        // Preserve template variables - don't overwrite $variable with resolved RID
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

  const changeAssetInputMethod = useCallback(
    (method: AssetInputMethod) => {
      markInteracted();
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();
      assetSelectControllerRef.current?.abort();
      setAssetInputMethod(method);
      setHasManuallySetMethod(true); // Mark as manually set to prevent automatic overrides
      setSelectedAsset(null);
      setDataScopes([]);
      // Populate directRID from existing query when switching to direct mode
      setDirectRID(method === 'direct' ? query?.assetRid || '' : '');
      // Only update input method, preserve other query values (assetRid, channel, dataScopeName)
      // so users don't lose their selection when quickly toggling between modes.
      onChange({ ...query, assetInputMethod: method });
    },
    [markInteracted, onChange, query]
  );

  const changeAssetSearchQuery = useCallback((value: string) => {
    setSearchQuery(value);
  }, []);

  const selectAsset = useCallback(
    (selection: SelectableValue<string>) => {
      markInteracted();
      const value = selection.value || '';
      const isVariable = value.includes('$');

      // Resolve to actual RID for asset lookup (variables need resolution)
      const ridToFind = isVariable ? getTemplateSrv().replace(value) : value;
      const asset = assets.find((a) => a.rid === ridToFind);

      if (asset) {
        // Abort any in-flight search-mode fetch from a previous selection
        assetSelectControllerRef.current?.abort();
        setSelectedAsset(asset);
      } else if (ridToFind && !ridToFind.includes('$')) {
        // Asset not in search results - fetch it directly instead of nulling selectedAsset.
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
    },
    [applyAssetFromRid, assets, markInteracted, onChange, query]
  );

  const changeDirectRID = useCallback(
    (rid: string) => {
      markInteracted();
      setDirectRID(rid);

      // Update query model immediately so the value is persisted
      if (rid.trim()) {
        onChange({
          ...queryRef.current,
          assetRid: rid,
          assetInputMethod: 'direct',
          queryType: 'decimation',
          buckets: 1000,
        });
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
    },
    [markInteracted, onChange, applyAssetFromRid]
  );

  const selectDataScope = useCallback(
    (dataScopeName: string) => {
      markInteracted();
      onChange({
        ...query,
        dataScopeName,
        assetInputMethod,
        queryType: 'decimation',
        buckets: 1000,
      });
    },
    [assetInputMethod, markInteracted, onChange, query]
  );

  // Clean up timers and in-flight fetches on unmount
  useEffect(() => {
    return () => {
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();
      assetSelectControllerRef.current?.abort();
    };
  }, []);

  const assetOptions = useMemo(
    () => buildAssetOptions({ assets, selectedAsset, currentAssetRid: query?.assetRid || '' }),
    [assets, selectedAsset, query?.assetRid]
  );

  const assetSelectValue = useMemo(
    () => getAssetSelectValue({ currentAssetRid: query?.assetRid || '', resolvedAssetRid, assetOptions }),
    [query?.assetRid, resolvedAssetRid, assetOptions]
  );

  const dataScopeOptions = useMemo(
    () =>
      buildDataScopeOptions({
        dataScopes,
        currentDataScopeName: query?.dataScopeName || '',
        resolvedDataScopeName,
      }),
    [dataScopes, query?.dataScopeName, resolvedDataScopeName]
  );

  return {
    assetInputMethod,
    directRID,
    searchQuery,
    selectedAsset,
    assetSearchResultCount: assets.length,
    selectedAssetSupportedScopeCount: selectedAsset ? getSupportedScopes(selectedAsset).length : 0,
    assetOptions,
    assetSelectValue,
    dataScopeOptions,
    isLoadingAssets,
    changeAssetInputMethod,
    changeAssetSearchQuery,
    runAssetSearch: loadAssets,
    selectAsset,
    changeDirectRID,
    selectDataScope,
  };
}
