import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AppEvents } from '@grafana/data';
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
import { buildAssetOptions, buildDataScopeOptions, getAssetPickerValue } from './queryBuilderOptions';
import {
  changeAssetInputMethodQuery,
  changeDirectAssetRidQuery,
  changeSearchAssetRidQuery,
  changeSelectedDataScopeQuery,
} from './queryMutations';
import { decideAssetReconcile } from './assetReconcile';
import type { TemplateValueResolution } from './templateResolution';
import type { AssetInputMethod, AssetOption, DataScopeOption } from './queryBuilderTypes';

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
  assetOptions: AssetOption[];
  assetSelectValue: string;
  dataScopeOptions: DataScopeOption[];
  isLoadingAssets: boolean;
  changeAssetInputMethod: (method: AssetInputMethod) => void;
  changeAssetSearchQuery: (value: string) => void;
  runAssetSearch: () => void;
  selectAsset: (assetRid: string) => void;
  changeDirectRID: (rid: string) => void;
  selectDataScope: (dataScopeName: string) => void;
}

const notifyError = (title: string, message: string) => {
  getAppEvents().publish({
    type: AppEvents.alertError.name,
    payload: [title, message],
  });
};

const ASSET_LOOKUP_DEBOUNCE_MS = 300;

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
  const [hasLoadedAssets, setHasLoadedAssets] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [assetInputMethod, setAssetInputMethod] = useState<AssetInputMethod>(query?.assetInputMethod || 'search');
  const [directRID, setDirectRID] = useState(query?.assetInputMethod === 'direct' ? query?.assetRid || '' : '');

  // Latest query for effects/callbacks, read via ref to avoid onChange -> query -> effect cycles.
  const queryRef = useRef(query);
  queryRef.current = query;

  // AbortController for search-mode asset selection - cancels in-flight fetch on rapid re-selection
  const assetSelectControllerRef = useRef<AbortController>(undefined);

  // Tracks the exact concrete RID whose by-RID fetch is owned by a user event handler
  // (`selectAsset` custom values or `changeDirectRID` debounced input). Query-driven
  // reconciliation skips only this RID, so saved/query-driven concrete RIDs can still restore.
  const eventOwnedConcreteAssetRidRef = useRef<string | undefined>(undefined);

  // Debounced asset lookup for direct RID input - fires after user stops typing
  const directRidTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const directRidControllerRef = useRef<AbortController>(undefined);

  // Debounced asset lookup for search-mode typing. Search-mode entry and Enter run immediately.
  const assetSearchTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const assetSearchControllerRef = useRef<AbortController>(undefined);
  const previousAssetSearchInputsRef = useRef({
    assetInputMethod,
    datasourceUrl,
    searchQuery,
  });

  const loadAssets = useCallback(async () => {
    assetSearchControllerRef.current?.abort();
    const controller = new AbortController();
    assetSearchControllerRef.current = controller;
    const { signal } = controller;

    setIsLoadingAssets(true);
    setHasLoadedAssets(false);
    try {
      const foundAssets = await searchAssets(datasourceUrl, searchQuery);
      if (signal.aborted) {
        return;
      }
      setAssets(foundAssets);
    } catch {
      if (signal.aborted) {
        return;
      }
      notifyError('Unable to load Nominal assets', 'Check the data source configuration and try again.');
      setAssets([]);
    } finally {
      if (!signal.aborted) {
        setIsLoadingAssets(false);
        setHasLoadedAssets(true);
      }
    }
  }, [searchQuery, datasourceUrl]);

  const runAssetSearch = useCallback(() => {
    clearTimeout(assetSearchTimerRef.current);
    if (assetInputMethod !== 'search') {
      return;
    }
    loadAssets();
  }, [assetInputMethod, loadAssets]);

  /** Fetch an asset by resolved RID and update selectedAsset/dataScopes state.
   *  Returns early without updating state if `signal` is aborted. */
  const applyAssetFromRid = useCallback(
    async (resolvedRid: string, displayLabel: string, signal?: AbortSignal) => {
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
      }
    },
    [datasourceUrl]
  );

  const assetRidRaw = assetRidResolution.raw;
  const assetRidResolved = assetRidResolution.resolved;
  const assetRidHasTemplate = assetRidResolution.hasTemplate;
  const assetRidIsResolved = assetRidResolution.isResolved;

  const queryReconcileUsesSearchResults = query?.assetInputMethod !== 'direct';
  const queryReconcileSearchHasLoaded = queryReconcileUsesSearchResults ? hasLoadedAssets : false;
  const queryReconcileSearchAsset = queryReconcileUsesSearchResults
    ? assets.find((asset) => asset.rid === assetRidResolved)
    : undefined;

  // Reconcile selected asset state from the saved/query-driven RID.
  //
  // This is the only query-driven path allowed to schedule a by-RID asset fetch. Concrete
  // user-entered/custom RIDs are owned by their event handlers; template-backed RIDs stay
  // query-driven so dashboard variable changes refetch the resolved asset.
  useEffect(() => {
    const controllers: AbortController[] = [];
    const actions = decideAssetReconcile({
      assetRid: query?.assetRid,
      assetInputMethod: query?.assetInputMethod,
      selectedAssetRid: selectedAsset?.rid,
      assetRidResolution: {
        raw: assetRidRaw,
        resolved: assetRidResolved,
        hasTemplate: assetRidHasTemplate,
        isResolved: assetRidIsResolved,
      },
      eventOwnedConcreteAssetRid: eventOwnedConcreteAssetRidRef.current,
      searchHasLoaded: queryReconcileSearchHasLoaded,
      searchAsset: queryReconcileSearchAsset,
    });

    for (const action of actions) {
      switch (action.kind) {
        case 'mirrorDirectRaw':
          setDirectRID((prev) => prev || action.raw);
          break;
        case 'fetchByRid': {
          const controller = new AbortController();
          controllers.push(controller);
          applyAssetFromRid(action.rid, action.label, controller.signal);
          break;
        }
        case 'selectSearchResult':
          setAssetInputMethod('search');
          setSelectedAsset(action.asset);
          break;
        case 'inferDirect': {
          setAssetInputMethod('direct');
          setDirectRID(action.raw);
          const controller = new AbortController();
          controllers.push(controller);
          applyAssetFromRid(action.rid, action.label, controller.signal);
          break;
        }
      }
    }

    return controllers.length > 0 ? () => controllers.forEach((controller) => controller.abort()) : undefined;
  }, [
    query?.assetRid,
    query?.assetInputMethod,
    selectedAsset?.rid,
    assetRidResolved,
    assetRidIsResolved,
    assetRidHasTemplate,
    assetRidRaw,
    queryReconcileSearchHasLoaded,
    queryReconcileSearchAsset,
    applyAssetFromRid,
  ]);

  // Load immediately when search mode opens; debounce typed search query changes.
  useEffect(() => {
    const cancelPendingAssetSearch = () => {
      clearTimeout(assetSearchTimerRef.current);
      assetSearchControllerRef.current?.abort();
    };

    clearTimeout(assetSearchTimerRef.current);

    const previous = previousAssetSearchInputsRef.current;
    const searchQueryChanged = previous.searchQuery !== searchQuery;
    const enteredSearchMode = previous.assetInputMethod !== 'search' && assetInputMethod === 'search';
    const datasourceChanged = previous.datasourceUrl !== datasourceUrl;

    // Keep this snapshot update before any early return. The next run compares
    // against every observed input state, including direct mode.
    previousAssetSearchInputsRef.current = {
      assetInputMethod,
      datasourceUrl,
      searchQuery,
    };

    if (assetInputMethod !== 'search') {
      return cancelPendingAssetSearch;
    }

    if (!searchQueryChanged || enteredSearchMode || datasourceChanged) {
      loadAssets();
      return cancelPendingAssetSearch;
    }

    assetSearchTimerRef.current = setTimeout(() => {
      loadAssets();
    }, ASSET_LOOKUP_DEBOUNCE_MS);

    return cancelPendingAssetSearch;
  }, [assetInputMethod, datasourceUrl, loadAssets, searchQuery]);

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
      clearTimeout(assetSearchTimerRef.current);
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();
      assetSelectControllerRef.current?.abort();
      assetSearchControllerRef.current?.abort();
      eventOwnedConcreteAssetRidRef.current = undefined;
      setIsLoadingAssets(false);
      setHasLoadedAssets(false);
      setAssetInputMethod(method);
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
    (value: string) => {
      markInteracted();
      const isVariable = value.includes('$');

      // Resolve to actual RID for asset lookup (variables need resolution)
      const selectedRidResolution = resolveTemplateText(value);
      const ridToFind = isVariable ? selectedRidResolution.resolved : value;
      const asset = assets.find((a) => a.rid === ridToFind);

      if (asset) {
        // Abort any in-flight search-mode fetch from a previous selection
        assetSelectControllerRef.current?.abort();
        eventOwnedConcreteAssetRidRef.current = undefined;
        setSelectedAsset(asset);
      } else if (ridToFind && !ridToFind.includes('$') && !isVariable) {
        // Concrete custom RIDs are event-owned: fetch immediately instead of waiting
        // for query reconciliation, and avoid a UI flash while the fetch is in flight.
        assetSelectControllerRef.current?.abort();
        const controller = new AbortController();
        assetSelectControllerRef.current = controller;
        eventOwnedConcreteAssetRidRef.current = ridToFind;
        applyAssetFromRid(ridToFind, 'Asset (Direct RID)', controller.signal);
      } else if (isVariable && selectedRidResolution.isResolved) {
        // Template-backed selections are query-owned so future variable resolution changes
        // refetch through the same reconcile path. Keep the current selected asset until
        // reconciliation replaces it.
        assetSelectControllerRef.current?.abort();
        eventOwnedConcreteAssetRidRef.current = undefined;
      } else {
        assetSelectControllerRef.current?.abort();
        eventOwnedConcreteAssetRidRef.current = undefined;
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
        eventOwnedConcreteAssetRidRef.current = undefined;
        setSelectedAsset(null);
        setDataScopes([]);
        onChange(changeDirectAssetRidQuery(queryRef.current, ''));
        return;
      }

      // Resolve template variables
      const ridResolution = resolveTemplateText(rid);
      // Template-backed RIDs are query-owned so dashboard variable changes use the
      // same reconcile path. Concrete direct RIDs stay event-owned and debounced.
      if (!ridResolution.isResolved) {
        eventOwnedConcreteAssetRidRef.current = undefined;
        return;
      }
      if (ridResolution.hasTemplate) {
        eventOwnedConcreteAssetRidRef.current = undefined;
        return;
      }

      eventOwnedConcreteAssetRidRef.current = ridResolution.resolved;

      const displayLabel = 'Asset (Direct RID)';
      const controller = new AbortController();
      directRidControllerRef.current = controller;

      directRidTimerRef.current = setTimeout(() => {
        applyAssetFromRid(ridResolution.resolved, displayLabel, controller.signal);
      }, ASSET_LOOKUP_DEBOUNCE_MS);
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
      clearTimeout(assetSearchTimerRef.current);
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();
      assetSelectControllerRef.current?.abort();
      assetSearchControllerRef.current?.abort();
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
      getAssetPickerValue({
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
    runAssetSearch,
    selectAsset,
    changeDirectRID,
    selectDataScope,
  };
}
