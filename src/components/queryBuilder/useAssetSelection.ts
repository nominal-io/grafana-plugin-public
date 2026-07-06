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
import { useResolutionSnapshot, type TemplateValueResolution } from './templateResolution';
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

const isNominalRid = (value: string): boolean => value.trim().startsWith('ri.');

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
  const latestAssetOptionsAssetsRef = useRef<Asset[]>([]);

  const resolutionCoordinatorRef = useRef<AssetResolutionCoordinator | null>(null);
  if (!resolutionCoordinatorRef.current) {
    resolutionCoordinatorRef.current = new AssetResolutionCoordinator();
  }
  const resolutionCoordinator = resolutionCoordinatorRef.current;

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

  const assetRidResolved = assetRidResolution.resolved;
  const assetRidIsResolved = assetRidResolution.isResolved;
  const assetRidSnapshot = useResolutionSnapshot(assetRidResolution);
  const dataScopeResolutionSnapshot = useResolutionSnapshot(dataScopeResolution);

  const selectedAsset = assetIdentity.selectedAsset;
  const visibleAssetIdentity = useMemo(() => getVisibleAssetIdentity(assetIdentity), [assetIdentity]);
  const visibleAssetIdentityRef = useRef(visibleAssetIdentity);
  visibleAssetIdentityRef.current = visibleAssetIdentity;
  const assetRidSnapshotRef = useRef(assetRidSnapshot);
  assetRidSnapshotRef.current = assetRidSnapshot;
  const clearAssetSelection = useCallback(() => {
    resolutionCoordinator.cancelInFlightResolution();
    dispatchAssetIdentity({ type: 'clear' });
  }, [resolutionCoordinator]);

  const assetOptions = useCallback<AssetOptionsLoader>(
    async (searchText: string): Promise<AssetOption[]> => {
      const requestId = resolutionCoordinator.startAssetOptionsRequest();
      try {
        const found = await searchAssets(datasourceUrl, searchText);
        if (resolutionCoordinator.isCurrentAssetOptionsRequest(requestId)) {
          latestAssetOptionsAssetsRef.current = found;
        }
        return buildAssetOptions({
          assets: found,
          selectedAsset: visibleAssetIdentityRef.current.selectedAsset,
          assetRid: assetRidSnapshotRef.current,
        });
      } catch {
        // Only the request id gates the alert: each new search bumps it (and so does a
        // method switch / unmount), so a superseded search stays quiet while a genuine
        // failure of the latest request always surfaces.
        if (resolutionCoordinator.isCurrentAssetOptionsRequest(requestId)) {
          latestAssetOptionsAssetsRef.current = [];
          notifyError('Unable to load Nominal assets', 'Check the data source configuration and try again.');
        }
        return [];
      }
    },
    [datasourceUrl, resolutionCoordinator]
  );

  useEffect(() => {
    let reconcileSignal: AbortSignal | undefined;
    if (query?.assetInputMethod) {
      setAssetInputMethod(query.assetInputMethod);
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
          reconcileSignal = resolutionCoordinator.beginReconcileFetch();
          applyAssetFromRid(action.rid, action.label, reconcileSignal);
          break;
        }
        case 'inferDirect': {
          setAssetInputMethod('direct');
          setDirectRID(action.raw);
          reconcileSignal = resolutionCoordinator.beginReconcileFetch();
          applyAssetFromRid(action.rid, action.label, reconcileSignal);
          break;
        }
        case 'clearIdentity':
          dispatchAssetIdentity({ type: 'clear' });
          break;
      }
    }

    return reconcileSignal ? () => resolutionCoordinator.cancelReconcileFetch(reconcileSignal) : undefined;
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
      setDirectRID(method === 'direct' ? query?.assetRid || '' : '');
      onChange(changeAssetInputMethodQuery(query, method));
    },
    [markInteracted, onChange, query, resolutionCoordinator]
  );

  const selectAsset = useCallback(
    (value: string) => {
      markInteracted();
      const trimmedValue = value.trim();

      if (!trimmedValue) {
        // Mirror changeDirectRID: a blank/whitespace selection clears the asset rather
        // than committing whitespace as the assetRid and firing a doomed by-RID fetch.
        clearAssetSelection();
        onChange(changeSearchAssetRidQuery(query, ''));
        return;
      }

      const isVariable = trimmedValue.includes('$');
      if (!isVariable && !isNominalRid(trimmedValue)) {
        return;
      }

      const selectedRidResolution = resolveTemplateText(trimmedValue);
      const ridToFind = isVariable ? selectedRidResolution.resolved : trimmedValue;
      const isConcreteRid = Boolean(ridToFind) && !ridToFind.includes('$');

      if (!isVariable && isAssetFullyResolved(assetIdentity, ridToFind)) {
        // Re-picking the fully resolved current asset: nothing to fetch. A fallback
        // asset (empty dataScopes) still refetches so a failed lookup stays retryable.
        onChange(changeSearchAssetRidQuery(query, ridToFind));
        return;
      }

      const displayedAsset = !isVariable
        ? latestAssetOptionsAssetsRef.current.find((asset) => asset.rid === ridToFind)
        : undefined;
      if (isConcreteRid && !isVariable) {
        if (displayedAsset) {
          resolutionCoordinator.cancelInFlightResolution();
          dispatchAssetIdentity({ type: 'beginResolving', rid: ridToFind });
          dispatchAssetIdentity({
            type: 'resolveAsset',
            rid: ridToFind,
            asset: displayedAsset,
            fallbackLabel: DIRECT_ASSET_RID_LABEL,
          });
        } else {
          applyAssetFromRid(ridToFind, DIRECT_ASSET_RID_LABEL, resolutionCoordinator.beginSelectFetch(ridToFind));
        }
      } else if (isVariable && selectedRidResolution.isResolved) {
        resolutionCoordinator.cancelInFlightResolution();
        dispatchAssetIdentity({ type: 'beginResolving', rid: selectedRidResolution.resolved });
      } else {
        clearAssetSelection();
      }

      if (isVariable) {
        onChange(changeSearchAssetRidQuery(query, trimmedValue));
      } else if (isConcreteRid) {
        onChange(changeSearchAssetRidQuery(query, ridToFind));
      }
    },
    [
      applyAssetFromRid,
      assetIdentity,
      clearAssetSelection,
      markInteracted,
      onChange,
      query,
      resolutionCoordinator,
      resolveTemplateText,
    ]
  );

  const changeDirectRID = useCallback(
    (rid: string) => {
      markInteracted();
      setDirectRID(rid);

      if (!rid.trim()) {
        clearAssetSelection();
        onChange(changeDirectAssetRidQuery(queryRef.current, ''));
        return;
      }

      const ridResolution = resolveTemplateText(rid);
      if (!ridResolution.isResolved) {
        clearAssetSelection();
        onChange(changeDirectAssetRidQuery(queryRef.current, rid));
        return;
      }

      const isConcrete = !ridResolution.hasTemplate;
      resolutionCoordinator.cancelInFlightResolution();
      // Set the event-owned RID before committing the query so the update cannot
      // schedule a second fetch for the same selection (see AssetResolutionCoordinator).
      resolutionCoordinator.setEventOwnedConcreteAssetRid(isConcrete ? ridResolution.resolved : undefined);
      dispatchAssetIdentity({ type: 'beginResolving', rid: ridResolution.resolved });
      onChange(changeDirectAssetRidQuery(queryRef.current, rid));

      if (!isConcrete) {
        return;
      }

      resolutionCoordinator.scheduleDirectRidFetch((signal) => {
        applyAssetFromRid(ridResolution.resolved, DIRECT_ASSET_RID_LABEL, signal);
      }, 300);
    },
    [applyAssetFromRid, clearAssetSelection, markInteracted, onChange, resolutionCoordinator, resolveTemplateText]
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
        dataScopeName: dataScopeResolutionSnapshot,
      }),
    [visibleAssetIdentity.dataScopes, dataScopeResolutionSnapshot]
  );

  return {
    assetInputMethod,
    directRID,
    selectedAsset: visibleAssetIdentity.selectedAsset,
    assetOptions,
    assetSelectValue,
    dataScopeOptions,
    changeAssetInputMethod,
    selectAsset,
    changeDirectRID,
    selectDataScope,
  };
}
