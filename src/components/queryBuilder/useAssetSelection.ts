import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react';
import { AppEvents } from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
import type { NominalQuery } from '../../types';
import { fetchAssetByRid, getSupportedScopeNames, searchAssets, type Asset } from '../../utils/api';
import { buildAssetOptions, buildDataScopeOptions, getAssetSelectValue } from './queryBuilderOptions';
import {
  changeAssetInputMethodQuery,
  changeDirectAssetRidQuery,
  changeSearchAssetRidQuery,
  changeSelectedDataScopeQuery,
} from './queryMutations';
import { decideAssetReconcile, DIRECT_ASSET_RID_LABEL } from './assetReconcile';
import {
  assetIdentityReducer,
  createEmptyAssetIdentityState,
  getVisibleAssetIdentity,
  isAssetFullyResolved,
} from './assetIdentity';
import { AssetResolutionCoordinator } from './assetResolution';
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

  const resolutionCoordinatorRef = useRef<AssetResolutionCoordinator | null>(null);
  if (!resolutionCoordinatorRef.current) {
    resolutionCoordinatorRef.current = new AssetResolutionCoordinator(query?.assetRid ?? '');
  }
  const resolutionCoordinator = resolutionCoordinatorRef.current;

  const commitQueryWithAssetRid = useCallback(
    (nextQuery: NominalQuery, committedRaw: string) => {
      resolutionCoordinator.trackCommittedAssetRid(committedRaw);
      onChange(nextQuery);
    },
    [onChange, resolutionCoordinator]
  );

  const applyAssetFromRid = useCallback(
    async (resolvedRid: string, displayLabel: string, signal?: AbortSignal) => {
      dispatchAssetIdentity({ type: 'beginResolving', rid: resolvedRid });
      const cancelResolving = () => {
        resolutionCoordinator.clearEventOwnedConcreteAssetRidIfMatches(resolvedRid);
        if (resolutionCoordinator.isMounted) {
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
        resolutionCoordinator.clearEventOwnedConcreteAssetRidIfMatches(resolvedRid);
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
    [datasourceUrl, resolutionCoordinator]
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
      const requestId = resolutionCoordinator.startAssetOptionsRequest();
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
        if (resolutionCoordinator.shouldPublishAssetOptionsFailure(requestId)) {
          notifyError('Unable to load Nominal assets', 'Check the data source configuration and try again.');
        }
        return [];
      }
    },
    [datasourceUrl, resolutionCoordinator, visibleAssetIdentity.selectedAsset, assetRidSnapshot]
  );

  useEffect(() => {
    const controllers: AbortController[] = [];
    const queryAssetRidRaw = query?.assetRid ?? '';
    const echoStatus = resolutionCoordinator.consumeQueryAssetRidEcho(queryAssetRidRaw);
    if (echoStatus === 'lag') {
      // The prop is re-delivering a value this hook committed itself (or still
      // holds the pre-commit value); reconciling against it would revert local
      // edits. The newer echo or a genuine external change re-runs this effect
      // through the query?.assetRid dependency.
      return undefined;
    }
    const eventOwnedConcreteAssetRid = resolutionCoordinator.eventOwnedConcreteAssetRid;
    if (eventOwnedConcreteAssetRid && assetRidResolved !== eventOwnedConcreteAssetRid) {
      resolutionCoordinator.cancelInFlightResolution();
      dispatchAssetIdentity({ type: 'cancelResolving', rid: eventOwnedConcreteAssetRid });
    }

    const actions = decideAssetReconcile({
      assetRid: query?.assetRid,
      assetInputMethod: query?.assetInputMethod,
      selectedAssetRid: selectedAsset?.rid,
      assetRidResolution: assetRidSnapshot,
      eventOwnedConcreteAssetRid: resolutionCoordinator.eventOwnedConcreteAssetRid,
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
    resolutionCoordinator,
  ]);

  useEffect(() => {
    if (selectedAsset) {
      if (!assetRidIsResolved || selectedAsset.rid !== assetRidResolved) {
        return;
      }

      // Derive scopes from the raw selected asset, not the visible view: the
      // visible view masks to [] while a different asset's resolution is
      // pending, which would misclassify the saved scope as invalid and clear
      // it even though the guard above confirmed this asset is current.
      const scopeNames = getSupportedScopeNames(selectedAsset);

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
      resolutionCoordinator.cancelInFlightResolution();
      resolutionCoordinator.invalidateAssetOptionsRequests();
      setAssetInputMethod(method);
      dispatchAssetIdentity({ type: 'clear' });
      setDirectRID(method === 'direct' ? query?.assetRid || '' : '');
      // The commit carries assetRid (unchanged), so it must go through the echo
      // bookkeeping: a plain onChange would leave any un-echoed prior commit in
      // pendingEchoRaws, and this commit's own echo would classify as 'lag'.
      commitQueryWithAssetRid(changeAssetInputMethodQuery(query, method), query?.assetRid ?? '');
    },
    [commitQueryWithAssetRid, markInteracted, query, resolutionCoordinator]
  );

  const selectAsset = useCallback(
    (value: string) => {
      markInteracted();

      if (!value.trim()) {
        // Mirror changeDirectRID: a blank/whitespace selection clears the asset rather
        // than committing whitespace as the assetRid and firing a doomed by-RID fetch.
        resolutionCoordinator.cancelInFlightResolution();
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
        applyAssetFromRid(ridToFind, DIRECT_ASSET_RID_LABEL, resolutionCoordinator.beginSelectFetch(ridToFind));
      } else if (isVariable && selectedRidResolution.isResolved) {
        resolutionCoordinator.cancelInFlightResolution();
        dispatchAssetIdentity({ type: 'beginResolving', rid: selectedRidResolution.resolved });
      } else {
        resolutionCoordinator.cancelInFlightResolution();
        dispatchAssetIdentity({ type: 'clear' });
      }

      if (isVariable) {
        commitQueryWithAssetRid(changeSearchAssetRidQuery(query, value), value);
      } else if (ridToFind && !ridToFind.includes('$')) {
        commitQueryWithAssetRid(changeSearchAssetRidQuery(query, ridToFind), ridToFind);
      }
    },
    [
      applyAssetFromRid,
      assetIdentity,
      commitQueryWithAssetRid,
      markInteracted,
      query,
      resolutionCoordinator,
      resolveTemplateText,
    ]
  );

  const changeDirectRID = useCallback(
    (rid: string) => {
      markInteracted();
      setDirectRID(rid);
      resolutionCoordinator.cancelInFlightResolution();

      if (!rid.trim()) {
        dispatchAssetIdentity({ type: 'clear' });
        commitQueryWithAssetRid(changeDirectAssetRidQuery(queryRef.current, ''), '');
        return;
      }

      const ridResolution = resolveTemplateText(rid);
      const isConcrete = ridResolution.isResolved && !ridResolution.hasTemplate;
      // Set the event-owned RID before committing the query so the update cannot
      // schedule a second fetch for the same selection (see AssetResolutionCoordinator).
      resolutionCoordinator.setEventOwnedConcreteAssetRid(isConcrete ? ridResolution.resolved : undefined);
      if (ridResolution.isResolved) {
        dispatchAssetIdentity({ type: 'beginResolving', rid: ridResolution.resolved });
      } else {
        dispatchAssetIdentity({ type: 'clear' });
      }
      commitQueryWithAssetRid(changeDirectAssetRidQuery(queryRef.current, rid), rid);

      if (!isConcrete) {
        return;
      }

      resolutionCoordinator.scheduleDirectRidFetch((signal) => {
        applyAssetFromRid(ridResolution.resolved, DIRECT_ASSET_RID_LABEL, signal);
      }, 300);
    },
    [applyAssetFromRid, commitQueryWithAssetRid, markInteracted, resolutionCoordinator, resolveTemplateText]
  );

  const selectDataScope = useCallback(
    (dataScopeName: string) => {
      markInteracted();
      onChange(changeSelectedDataScopeQuery(query, dataScopeName, assetInputMethod));
    },
    [assetInputMethod, markInteracted, onChange, query]
  );

  useEffect(() => {
    resolutionCoordinator.markMounted();
    return () => {
      resolutionCoordinator.markUnmounted();
      resolutionCoordinator.cancelInFlightResolution();
    };
  }, [resolutionCoordinator]);

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
