import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react';
import { AppEvents } from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
import type { NominalQuery } from '../../types';
import { fetchAssetByRid, searchAssets, type Asset } from '../../utils/api';
import { buildAssetOptions, buildDataScopeOptions, getAssetSelectValue } from './queryBuilderOptions';
import {
  changeAssetInputMethodQuery,
  changeDirectAssetRidQuery,
  changeSearchAssetRidQuery,
  changeSelectedDataScopeQuery,
} from './queryMutations';
import { classifyAssetRidEcho, decideAssetReconcile, DIRECT_ASSET_RID_LABEL } from './assetReconcile';
import {
  assetIdentityReducer,
  createEmptyAssetIdentityState,
  getVisibleAssetIdentity,
  isAssetFullyResolved,
} from './assetIdentity';
import type { TemplateValueResolution } from './templateResolution';
import type { AssetInputMethod, AssetOption, AssetOptionsLoader, DataScopeOption } from './queryBuilderTypes';

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
  selectedAsset: Asset | null;
  selectedAssetSupportedScopeCount: number;
  assetOptions: AssetOptionsLoader;
  assetSelectValue: AssetOption | null;
  dataScopeOptions: DataScopeOption[];
  changeAssetInputMethod: (method: AssetInputMethod) => void;
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
  const [assetIdentity, dispatchAssetIdentity] = useReducer(
    assetIdentityReducer,
    undefined,
    createEmptyAssetIdentityState
  );
  const [assetInputMethod, setAssetInputMethod] = useState<AssetInputMethod>(query?.assetInputMethod || 'search');
  const [directRID, setDirectRID] = useState(query?.assetInputMethod === 'direct' ? query?.assetRid || '' : '');

  const queryRef = useRef(query);
  queryRef.current = query;

  const isMountedRef = useRef(true);
  const assetOptionsRequestId = useRef(0);
  const assetSelectControllerRef = useRef<AbortController>(undefined);
  // Tracks the exact concrete RID whose by-RID fetch is owned by a user event
  // handler (`selectAsset` custom values or `changeDirectRID` debounced input).
  // Reconcile skips only this RID; set it before onChange so the query update
  // does not schedule a second fetch for the same event-owned selection.
  const eventOwnedConcreteAssetRidRef = useRef<string | undefined>(undefined);
  // Raw assetRid values committed via onChange but not yet echoed back by the
  // query prop (oldest first), plus the last query value this hook reconciled.
  // The parent's query state moves monotonically through the values written to
  // it, so a lagging prop can only re-deliver one of these; anything else is a
  // genuine external change and must win immediately.
  const pendingEchoRawsRef = useRef<string[]>([]);
  const lastReconciledAssetRidRawRef = useRef(query?.assetRid ?? '');
  const directRidTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const directRidControllerRef = useRef<AbortController>(undefined);

  // Single owner of the cancel-in-flight-resolution sequence. Every path that
  // supersedes a pending by-RID fetch must stop the direct-RID debounce, abort
  // both fetch controllers, and release event ownership; forgetting a step
  // leaks a controller or strands a pending RID. Returns the RID that was
  // event-owned so callers can cancel its pending reducer state (the debounced
  // fetch may not have started yet, so its abort listener may not exist).
  const cancelInFlightResolution = useCallback(() => {
    const ownedRid = eventOwnedConcreteAssetRidRef.current;
    clearTimeout(directRidTimerRef.current);
    directRidControllerRef.current?.abort();
    directRidControllerRef.current = undefined;
    assetSelectControllerRef.current?.abort();
    assetSelectControllerRef.current = undefined;
    eventOwnedConcreteAssetRidRef.current = undefined;
    return ownedRid;
  }, []);

  const commitQueryWithAssetRid = useCallback(
    (nextQuery: NominalQuery, committedRaw: string) => {
      const echoes = pendingEchoRawsRef.current;
      echoes.push(committedRaw);
      if (echoes.length > 50) {
        echoes.shift();
      }
      onChange(nextQuery);
    },
    [onChange]
  );

  const applyAssetFromRid = useCallback(
    async (resolvedRid: string, displayLabel: string, signal?: AbortSignal) => {
      dispatchAssetIdentity({ type: 'beginResolving', rid: resolvedRid });
      const cancelResolving = () => {
        if (eventOwnedConcreteAssetRidRef.current === resolvedRid) {
          eventOwnedConcreteAssetRidRef.current = undefined;
        }
        if (isMountedRef.current) {
          dispatchAssetIdentity({ type: 'cancelResolving', rid: resolvedRid });
        }
      };

      if (signal?.aborted) {
        cancelResolving();
        return;
      }
      signal?.addEventListener('abort', cancelResolving, { once: true });

      // Release event ownership as soon as the resolution settles: from here on
      // the selectedAssetRid guard covers what the ref covered, and a stale ref
      // must not depend on reconcile's guard ordering to stay harmless. The
      // identity check protects a newer event-owned RID set while this fetch
      // was in flight.
      const resolveWith = (asset: Asset | null) => {
        if (eventOwnedConcreteAssetRidRef.current === resolvedRid) {
          eventOwnedConcreteAssetRidRef.current = undefined;
        }
        dispatchAssetIdentity({
          type: 'resolveAsset',
          rid: resolvedRid,
          asset,
          fallbackLabel: displayLabel,
        });
      };

      try {
        const foundAsset = await fetchAssetByRid(datasourceUrl, resolvedRid);
        if (signal?.aborted) {
          return;
        }
        resolveWith(foundAsset);
      } catch {
        if (signal?.aborted) {
          return;
        }
        notifyError(
          'Unable to load Nominal asset',
          'The RID was kept, but data scopes could not be loaded automatically.'
        );
        resolveWith(null);
      } finally {
        signal?.removeEventListener('abort', cancelResolving);
      }
    },
    [datasourceUrl]
  );

  const assetRidRaw = assetRidResolution.raw;
  const assetRidResolved = assetRidResolution.resolved;
  const assetRidHasTemplate = assetRidResolution.hasTemplate;
  const assetRidIsResolved = assetRidResolution.isResolved;

  const assetRidSnapshot = useMemo(
    () => ({
      raw: assetRidRaw,
      resolved: assetRidResolved,
      hasTemplate: assetRidHasTemplate,
      isResolved: assetRidIsResolved,
    }),
    [assetRidRaw, assetRidResolved, assetRidHasTemplate, assetRidIsResolved]
  );

  const selectedAsset = assetIdentity.selectedAsset;
  const visibleAssetIdentity = useMemo(() => getVisibleAssetIdentity(assetIdentity), [assetIdentity]);

  const assetOptions = useCallback<AssetOptionsLoader>(
    async (searchText: string): Promise<AssetOption[]> => {
      const requestId = ++assetOptionsRequestId.current;
      try {
        const found = await searchAssets(datasourceUrl, searchText);
        return buildAssetOptions({
          assets: found,
          selectedAsset: visibleAssetIdentity.selectedAsset,
          assetRid: assetRidSnapshot,
        });
      } catch {
        // Only the request id gates the alert: each new search bumps it (and so does a
        // method switch / unmount), so a superseded search stays quiet while a genuine
        // failure of the latest request always surfaces.
        if (isMountedRef.current && assetOptionsRequestId.current === requestId) {
          notifyError('Unable to load Nominal assets', 'Check the data source configuration and try again.');
        }
        return [];
      }
    },
    [datasourceUrl, visibleAssetIdentity.selectedAsset, assetRidSnapshot]
  );

  useEffect(() => {
    const controllers: AbortController[] = [];
    const queryAssetRidRaw = query?.assetRid ?? '';
    const echoStatus = classifyAssetRidEcho({
      raw: queryAssetRidRaw,
      lastReconciledRaw: lastReconciledAssetRidRawRef.current,
      pendingEchoRaws: pendingEchoRawsRef.current,
    });
    if (echoStatus === 'lag') {
      // The prop is re-delivering a value this hook committed itself (or still
      // holds the pre-commit value); reconciling against it would revert local
      // edits. The newer echo or a genuine external change re-runs this effect
      // through the query?.assetRid dependency.
      return undefined;
    }
    if (echoStatus !== 'none') {
      pendingEchoRawsRef.current = [];
    }
    lastReconciledAssetRidRawRef.current = queryAssetRidRaw;

    const eventOwnedConcreteAssetRid = eventOwnedConcreteAssetRidRef.current;
    if (eventOwnedConcreteAssetRid && assetRidResolved !== eventOwnedConcreteAssetRid) {
      cancelInFlightResolution();
      dispatchAssetIdentity({ type: 'cancelResolving', rid: eventOwnedConcreteAssetRid });
    }

    const actions = decideAssetReconcile({
      assetRid: query?.assetRid,
      assetInputMethod: query?.assetInputMethod,
      selectedAssetRid: selectedAsset?.rid,
      assetRidResolution: assetRidSnapshot,
      eventOwnedConcreteAssetRid: eventOwnedConcreteAssetRidRef.current,
    });

    for (const action of actions) {
      switch (action.kind) {
        case 'mirrorDirectRaw':
          setDirectRID(action.raw);
          break;
        case 'fetchByRid': {
          const controller = new AbortController();
          controllers.push(controller);
          applyAssetFromRid(action.rid, action.label, controller.signal);
          break;
        }
        case 'inferDirect': {
          setAssetInputMethod('direct');
          setDirectRID(action.raw);
          const controller = new AbortController();
          controllers.push(controller);
          applyAssetFromRid(action.rid, action.label, controller.signal);
          break;
        }
        case 'clearIdentity':
          dispatchAssetIdentity({ type: 'clear' });
          break;
      }
    }

    return controllers.length > 0 ? () => controllers.forEach((controller) => controller.abort()) : undefined;
  }, [
    query?.assetRid,
    query?.assetInputMethod,
    selectedAsset?.rid,
    assetRidResolved,
    assetRidSnapshot,
    applyAssetFromRid,
    cancelInFlightResolution,
  ]);

  useEffect(() => {
    if (selectedAsset) {
      if (!assetRidIsResolved || selectedAsset.rid !== assetRidResolved) {
        return;
      }

      const scopeNames = assetIdentity.dataScopes;

      if (hasUserInteracted) {
        const q = queryRef.current;
        const resolvedCurrentScope = dataScopeResolution.resolved;
        const scopeIsValid = scopeNames.includes(resolvedCurrentScope);

        if (q?.dataScopeName?.includes('$')) {
          // skip - variable will be resolved at query time
        } else if (scopeNames.length === 1 && q?.dataScopeName !== scopeNames[0]) {
          onChange(changeSelectedDataScopeQuery(q, scopeNames[0], assetInputMethod));
        } else if (!scopeIsValid && q?.dataScopeName) {
          onChange(changeSelectedDataScopeQuery(q, '', assetInputMethod));
        }
      }
    }
    // Intentionally omit data-scope resolution deps here. This effect mutates
    // dataScopeName in response to a selected asset becoming current after user
    // interaction; the latest query is read through queryRef.current. Template
    // scopes are preserved for query time, so running on each resolved $scope
    // change would broaden when concrete dataScopeName can be auto-cleared or
    // rewritten.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedAsset, onChange, assetInputMethod, hasUserInteracted, assetRidResolved, assetRidIsResolved]);

  const changeAssetInputMethod = useCallback(
    (method: AssetInputMethod) => {
      markInteracted();
      cancelInFlightResolution();
      assetOptionsRequestId.current += 1;
      setAssetInputMethod(method);
      dispatchAssetIdentity({ type: 'clear' });
      setDirectRID(method === 'direct' ? query?.assetRid || '' : '');
      // The commit carries assetRid (unchanged), so it must go through the echo
      // bookkeeping: a plain onChange would leave any un-echoed prior commit in
      // pendingEchoRaws, and this commit's own echo would classify as 'lag'.
      commitQueryWithAssetRid(changeAssetInputMethodQuery(query, method), query?.assetRid ?? '');
    },
    [cancelInFlightResolution, commitQueryWithAssetRid, markInteracted, query]
  );

  const selectAsset = useCallback(
    (value: string) => {
      markInteracted();

      if (!value.trim()) {
        // Mirror changeDirectRID: a blank/whitespace selection clears the asset rather
        // than committing whitespace as the assetRid and firing a doomed by-RID fetch.
        cancelInFlightResolution();
        dispatchAssetIdentity({ type: 'clear' });
        commitQueryWithAssetRid(changeSearchAssetRidQuery(query, ''), '');
        return;
      }

      const isVariable = value.includes('$');
      const selectedRidResolution = resolveTemplateText(value);
      const ridToFind = isVariable ? selectedRidResolution.resolved : value;

      if (!isVariable && isAssetFullyResolved(assetIdentity, ridToFind)) {
        // Re-picking the fully resolved current asset: nothing to fetch. A fallback
        // asset (empty dataScopes) still refetches so a failed lookup stays retryable.
        commitQueryWithAssetRid(changeSearchAssetRidQuery(query, ridToFind), ridToFind);
        return;
      }

      if (ridToFind && !ridToFind.includes('$') && !isVariable) {
        cancelInFlightResolution();
        const controller = new AbortController();
        assetSelectControllerRef.current = controller;
        eventOwnedConcreteAssetRidRef.current = ridToFind;
        applyAssetFromRid(ridToFind, DIRECT_ASSET_RID_LABEL, controller.signal);
      } else if (isVariable && selectedRidResolution.isResolved) {
        cancelInFlightResolution();
        dispatchAssetIdentity({ type: 'beginResolving', rid: selectedRidResolution.resolved });
      } else {
        cancelInFlightResolution();
        dispatchAssetIdentity({ type: 'clear' });
      }

      if (isVariable) {
        commitQueryWithAssetRid(changeSearchAssetRidQuery(query, value), value);
      } else if (ridToFind && !ridToFind.includes('$')) {
        commitQueryWithAssetRid(changeSearchAssetRidQuery(query, ridToFind), ridToFind);
      }
    },
    [applyAssetFromRid, assetIdentity, cancelInFlightResolution, commitQueryWithAssetRid, markInteracted, query, resolveTemplateText]
  );

  const changeDirectRID = useCallback(
    (rid: string) => {
      markInteracted();
      setDirectRID(rid);
      cancelInFlightResolution();

      if (!rid.trim()) {
        dispatchAssetIdentity({ type: 'clear' });
        commitQueryWithAssetRid(changeDirectAssetRidQuery(queryRef.current, ''), '');
        return;
      }

      const ridResolution = resolveTemplateText(rid);
      const isConcrete = ridResolution.isResolved && !ridResolution.hasTemplate;
      // Set the event-owned RID before committing the query so the update cannot
      // schedule a second fetch for the same selection (see the ref comment).
      eventOwnedConcreteAssetRidRef.current = isConcrete ? ridResolution.resolved : undefined;
      if (ridResolution.isResolved) {
        dispatchAssetIdentity({ type: 'beginResolving', rid: ridResolution.resolved });
      } else {
        dispatchAssetIdentity({ type: 'clear' });
      }
      commitQueryWithAssetRid(changeDirectAssetRidQuery(queryRef.current, rid), rid);

      if (!isConcrete) {
        return;
      }

      const controller = new AbortController();
      directRidControllerRef.current = controller;

      directRidTimerRef.current = setTimeout(() => {
        applyAssetFromRid(ridResolution.resolved, DIRECT_ASSET_RID_LABEL, controller.signal);
      }, 300);
    },
    [applyAssetFromRid, cancelInFlightResolution, commitQueryWithAssetRid, markInteracted, resolveTemplateText]
  );

  const selectDataScope = useCallback(
    (dataScopeName: string) => {
      markInteracted();
      onChange(changeSelectedDataScopeQuery(query, dataScopeName, assetInputMethod));
    },
    [assetInputMethod, markInteracted, onChange, query]
  );

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      assetOptionsRequestId.current += 1;
      cancelInFlightResolution();
    };
  }, [cancelInFlightResolution]);

  const assetSelectValue = useMemo(
    () => getAssetSelectValue({ assetRid: assetRidSnapshot, selectedAsset: visibleAssetIdentity.selectedAsset }),
    [assetRidSnapshot, visibleAssetIdentity.selectedAsset]
  );

  const dataScopeOptions = useMemo(
    () =>
      buildDataScopeOptions({
        dataScopes: visibleAssetIdentity.dataScopes,
        dataScopeName: {
          raw: dataScopeResolution.raw,
          resolved: dataScopeResolution.resolved,
          hasTemplate: dataScopeResolution.hasTemplate,
          isResolved: dataScopeResolution.isResolved,
        },
      }),
    [
      visibleAssetIdentity.dataScopes,
      dataScopeResolution.raw,
      dataScopeResolution.resolved,
      dataScopeResolution.hasTemplate,
      dataScopeResolution.isResolved,
    ]
  );

  return {
    assetInputMethod,
    directRID,
    selectedAsset: visibleAssetIdentity.selectedAsset,
    selectedAssetSupportedScopeCount: visibleAssetIdentity.selectedAssetSupportedScopeCount,
    assetOptions,
    assetSelectValue,
    dataScopeOptions,
    changeAssetInputMethod,
    selectAsset,
    changeDirectRID,
    selectDataScope,
  };
}
