import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AppEvents, type SelectableValue } from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
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
import {
  changeAssetInputMethodQuery,
  changeDirectAssetRidQuery,
  changeSearchAssetRidQuery,
  changeSelectedDataScopeQuery,
} from './queryMutations';
import type { TemplateValueResolution } from './templateResolution';
import type { AssetInputMethod } from './queryBuilderTypes';

interface UseAssetSelectionArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  datasourceUrl: string;
  assetRidResolution: TemplateValueResolution;
  dataScopeResolution: TemplateValueResolution;
  resolveTemplateText: (value: string) => TemplateValueResolution;
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
  assetRidResolution,
  dataScopeResolution,
  resolveTemplateText,
  hasUserInteracted,
  markInteracted,
}: UseAssetSelectionArgs): AssetSelectionModel {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);
  const [dataScopes, setDataScopes] = useState<string[]>([]);
  const [isLoadingAssets, setIsLoadingAssets] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [assetInputMethod, setAssetInputMethod] = useState<AssetInputMethod>(query?.assetInputMethod || 'search');
  const [directRID, setDirectRID] = useState(query?.assetInputMethod === 'direct' ? query?.assetRid || '' : '');
  // Derive whether user has ever saved an explicit input method (persisted in query model).
  // Initialising from query rather than defaulting to false prevents the restore effect
  // from running unnecessary branches after a panel reload.
  const [hasManuallySetMethod, setHasManuallySetMethod] = useState(!!query?.assetInputMethod);

  // Latest query for effects/callbacks, read via ref to avoid onChange -> query -> effect cycles.
  const queryRef = useRef(query);
  queryRef.current = query;

  // AbortController for search-mode asset selection - cancels in-flight fetch on rapid re-selection
  const assetSelectControllerRef = useRef<AbortController>(undefined);

  // Tracks the in-flight by-RID fetch (its resolved RID + abort signal) so concurrent effects
  // (mount restore + resolved-asset) don't both fetch the same asset. Stored as a token rather
  // than a bare RID so the guard can distinguish a *live* fetch from one already aborted, and
  // so the cleanup clears only the slot belonging to *this* fetch — never a same-RID successor.
  const pendingAssetFetchRef = useRef<{ rid: string; signal?: AbortSignal } | undefined>(undefined);

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
      // Mark this fetch as in-flight (RID + signal) so concurrent effects skip a duplicate.
      pendingAssetFetchRef.current = { rid: resolvedRid, signal };
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
        notifyError(
          'Unable to load Nominal asset',
          'The RID was kept, but data scopes could not be loaded automatically.'
        );
        setSelectedAsset(createBasicAsset(resolvedRid, displayLabel));
        setDataScopes([]);
      } finally {
        // Clear only the slot belonging to THIS fetch (matched by signal identity). A newer
        // fetch — even for the same RID — owns a different signal, so a stale completion can
        // never wipe a live successor's slot.
        if (pendingAssetFetchRef.current?.signal === signal) {
          pendingAssetFetchRef.current = undefined;
        }
      }
    },
    [datasourceUrl]
  );

  // Restore a saved DIRECT-mode asset on mount / duplication.
  // Deliberately does NOT depend on `assets`: the by-RID fetch path never reads the
  // search list, so letting search-assets resolution (setAssets) re-run this effect
  // would only abort and re-issue an identical in-flight fetch. Keeping
  // `assets` out of the deps removes that redundant cancelled request.
  // MUST stay ordered before the resolved-asset effect so pendingAssetFetchRef is set
  // before that effect considers the same resolved template RID. Otherwise a saved
  // direct query like "$asset" can schedule duplicate by-RID fetches on mount.
  useEffect(() => {
    if (!query?.assetRid || selectedAsset || query.assetInputMethod !== 'direct') {
      return;
    }
    // Always show the saved RID (incl. unresolved $variable) in the direct input.
    setDirectRID((prev) => prev || query.assetRid || '');

    if (!assetRidResolution.isResolved) {
      return;
    }
    const displayLabel = assetRidResolution.hasTemplate ? `Asset (${assetRidResolution.raw})` : 'Asset (Direct RID)';
    const controller = new AbortController();
    applyAssetFromRid(assetRidResolution.resolved, displayLabel, controller.signal);
    return () => controller.abort();
  }, [
    query?.assetRid,
    query?.assetInputMethod,
    selectedAsset,
    assetRidResolution.resolved,
    assetRidResolution.isResolved,
    assetRidResolution.hasTemplate,
    assetRidResolution.raw,
    applyAssetFromRid,
  ]);

  // Restore a SEARCH-mode asset / infer direct mode for a saved RID, once assets are known.
  // This branch legitimately depends on `assets` (it matches against the loaded list).
  // Only entered when the user never saved an explicit method (hasManuallySetMethod=false);
  // the hasManuallySetMethod search case is handled by the search-restore effect below.
  useEffect(() => {
    if (!query?.assetRid || selectedAsset || hasManuallySetMethod || query.assetInputMethod === 'direct') {
      return;
    }
    if (!assetRidResolution.isResolved) {
      return;
    }
    const asset = assets.find((a) => a.rid === assetRidResolution.resolved);
    if (asset) {
      setAssetInputMethod('search');
      setSelectedAsset(asset);
    } else if (assets.length > 0 && !query.assetInputMethod) {
      const displayLabel = assetRidResolution.hasTemplate ? `Asset (${assetRidResolution.raw})` : 'Asset (Direct RID)';
      setAssetInputMethod('direct');
      setDirectRID(query.assetRid);
      const controller = new AbortController();
      applyAssetFromRid(assetRidResolution.resolved, displayLabel, controller.signal);
      return () => controller.abort();
    }
    return undefined;
  }, [
    query?.assetRid,
    query?.assetInputMethod,
    selectedAsset,
    assets,
    hasManuallySetMethod,
    assetRidResolution.resolved,
    assetRidResolution.isResolved,
    assetRidResolution.hasTemplate,
    assetRidResolution.raw,
    applyAssetFromRid,
  ]);

  // Update dropdown options when the resolved asset RID changes (e.g. template variable changed).
  // Skipped in direct mode because changeDirectRID manages its own debounced fetch -
  // running both would cause two concurrent requests racing to update selectedAsset.
  useEffect(() => {
    if (!assetRidResolution.resolved || !assetRidResolution.isResolved) {
      return;
    }
    if (selectedAsset?.rid === assetRidResolution.resolved) {
      return;
    }
    // Skip only if a *live* (not-yet-aborted) by-RID fetch for this same RID is already in
    // flight, so a saved direct query with a template RID ($asset) isn't fetched twice by both
    // the mount-restore effect and this one.
    // The abort-signal check is load-bearing: under React 18 StrictMode the mount runs
    // setup -> cleanup -> setup, and the cleanup aborts the first fetch. A bare-RID guard would
    // still see the in-flight RID and suppress the second setup's fetch forever, leaving
    // selectedAsset null and the channel picker hidden. Gating on a live signal (and clearing
    // the slot by signal identity in applyAssetFromRid's finally) lets the legitimate re-run
    // proceed while still ignoring stale completions.
    const pendingFetch = pendingAssetFetchRef.current;
    if (pendingFetch && pendingFetch.rid === assetRidResolution.resolved && !pendingFetch.signal?.aborted) {
      return;
    }
    // In direct mode the handler's debounced timer owns the fetch lifecycle; skip here.
    if (queryRef.current?.assetInputMethod === 'direct' && !assetRidResolution.hasTemplate) {
      return;
    }
    const displayLabel = assetRidResolution.hasTemplate ? `Asset (${assetRidResolution.raw})` : 'Asset (Direct RID)';
    const controller = new AbortController();
    applyAssetFromRid(assetRidResolution.resolved, displayLabel, controller.signal);
    return () => controller.abort();
    // Only depend on selectedAsset?.rid (not the full object) to avoid aborting in-flight
    // fetches when the object reference changes but the RID stays the same.
  }, [
    assetRidResolution.resolved,
    assetRidResolution.isResolved,
    assetRidResolution.hasTemplate,
    assetRidResolution.raw,
    selectedAsset?.rid,
    applyAssetFromRid,
  ]);

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
      const asset = assets.find((a) => a.rid === assetRidResolution.resolved);
      if (asset) {
        setSelectedAsset(asset);
      }
    }
  }, [query, selectedAsset, assets, assetInputMethod, hasManuallySetMethod, assetRidResolution.resolved]);

  // Update dependent fields when asset changes
  useEffect(() => {
    if (selectedAsset) {
      const scopeNames = getSupportedScopeNames(selectedAsset);
      setDataScopes(scopeNames);

      // Only auto-update query if user has interacted with the query builder
      // This prevents unwanted resets when just editing panel display settings
      if (hasUserInteracted) {
        const q = queryRef.current;
        // NOTE: resolution (dataScopeResolution/assetRidResolution) is read from the closure of
        // the render that scheduled this effect, while `q` is the latest query via ref. They are
        // consistent because the effect fires on selectedAsset/assetInputMethod/hasUserInteracted
        // changes, and at that render the resolution is derived from the same query. The original
        // re-resolved live via getTemplateSrv() here; this is intentionally render-time instead.
        // Check if current dataScopeName is valid for the new asset
        const resolvedCurrentScope = dataScopeResolution.resolved;
        const scopeIsValid = scopeNames.includes(resolvedCurrentScope);

        // Preserve template variables - don't overwrite $variable with resolved scope
        if (q?.dataScopeName?.includes('$')) {
          // skip - variable will be resolved at query time
        }
        // Auto-select data scope if only one available
        else if (scopeNames.length === 1 && q?.dataScopeName !== scopeNames[0]) {
          onChange(changeSelectedDataScopeQuery(q, scopeNames[0], assetInputMethod));
        }
        // Clear invalid data scope when asset changes
        else if (!scopeIsValid && q?.dataScopeName) {
          onChange(changeSelectedDataScopeQuery(q, '', assetInputMethod));
        }
        // Update query with selected asset only if it has changed (search mode)
        // Preserve template variables - don't overwrite $variable with resolved RID
        else if (assetInputMethod === 'search' && !q?.assetRid?.includes('$')) {
          const resolvedCurrentRid = assetRidResolution.resolved;
          if (resolvedCurrentRid !== selectedAsset.rid) {
            onChange(changeSearchAssetRidQuery(q, selectedAsset.rid));
          }
        }
      }
    }
    // Intentionally omit asset/data-scope resolution deps here. This effect mutates
    // query fields in response to selected-asset/user-interaction changes, while the
    // latest query is read through queryRef.current. Running it on template-resolution
    // changes would broaden when dataScopeName/assetRid can be auto-cleared or rewritten.
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
      onChange(changeAssetInputMethodQuery(query, method));
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
      const selectedRidResolution = resolveTemplateText(value);
      const ridToFind = isVariable ? selectedRidResolution.resolved : value;
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
        onChange(changeSearchAssetRidQuery(query, value));
      } else if (asset) {
        onChange(changeSearchAssetRidQuery(query, asset.rid));
      } else if (ridToFind && !ridToFind.includes('$')) {
        onChange(changeSearchAssetRidQuery(query, ridToFind));
      }
    },
    [applyAssetFromRid, assets, markInteracted, onChange, query, resolveTemplateText]
  );

  const changeDirectRID = useCallback(
    (rid: string) => {
      markInteracted();
      setDirectRID(rid);

      // Update query model immediately so the value is persisted
      if (rid.trim()) {
        onChange(changeDirectAssetRidQuery(queryRef.current, rid));
      }

      // Cancel any in-flight debounce / fetch
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();

      if (!rid.trim()) {
        setSelectedAsset(null);
        setDataScopes([]);
        onChange(changeDirectAssetRidQuery(queryRef.current, ''));
        return;
      }

      // Resolve template variables
      const ridResolution = resolveTemplateText(rid);
      // If still unresolved, nothing more to do (query was already updated above)
      if (!ridResolution.isResolved) {
        return;
      }

      const displayLabel = rid.includes('$') ? `Asset (${rid})` : 'Asset (Direct RID)';
      const controller = new AbortController();
      directRidControllerRef.current = controller;

      directRidTimerRef.current = setTimeout(() => {
        applyAssetFromRid(ridResolution.resolved, displayLabel, controller.signal);
      }, 300);
    },
    [markInteracted, onChange, applyAssetFromRid, resolveTemplateText]
  );

  const selectDataScope = useCallback(
    (dataScopeName: string) => {
      markInteracted();
      onChange(changeSelectedDataScopeQuery(query, dataScopeName, assetInputMethod));
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
    () =>
      buildAssetOptions({
        assets,
        selectedAsset,
        assetRid: {
          raw: assetRidResolution.raw,
          resolved: assetRidResolution.resolved,
          hasTemplate: assetRidResolution.hasTemplate,
          isResolved: assetRidResolution.isResolved,
        },
      }),
    [
      assets,
      selectedAsset,
      assetRidResolution.raw,
      assetRidResolution.resolved,
      assetRidResolution.hasTemplate,
      assetRidResolution.isResolved,
    ]
  );

  const assetSelectValue = useMemo(
    () =>
      getAssetSelectValue({
        assetRid: {
          raw: assetRidResolution.raw,
          resolved: assetRidResolution.resolved,
          hasTemplate: assetRidResolution.hasTemplate,
          isResolved: assetRidResolution.isResolved,
        },
        assetOptions,
      }),
    [
      assetRidResolution.raw,
      assetRidResolution.resolved,
      assetRidResolution.hasTemplate,
      assetRidResolution.isResolved,
      assetOptions,
    ]
  );

  const dataScopeOptions = useMemo(
    () =>
      buildDataScopeOptions({
        dataScopes,
        dataScopeName: {
          raw: dataScopeResolution.raw,
          resolved: dataScopeResolution.resolved,
          hasTemplate: dataScopeResolution.hasTemplate,
          isResolved: dataScopeResolution.isResolved,
        },
      }),
    [
      dataScopes,
      dataScopeResolution.raw,
      dataScopeResolution.resolved,
      dataScopeResolution.hasTemplate,
      dataScopeResolution.isResolved,
    ]
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
