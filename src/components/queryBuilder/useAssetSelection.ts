import { useCallback, useEffect, useMemo, useReducer, useRef } from 'react';
import { AppEvents } from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
import type { NominalQuery } from '../../types';
import { fetchAssetByRid, getSupportedScopeNames, searchAssets, type Asset } from '../../utils/api';
import { buildAssetOptions, buildDataScopeOptions, getAssetSelectValue } from './queryBuilderOptions';
import { changeAssetRidQuery, changeSelectedDataScopeQuery } from './queryMutations';
import { ASSET_RID_FALLBACK_LABEL, decideAssetReconcile } from './assetReconcile';
import {
  assetIdentityReducer,
  createEmptyAssetIdentityState,
  getVisibleAssetIdentity,
  isAssetFullyResolved,
} from './assetIdentity';
import { AssetResolutionCoordinator } from './assetResolution';
import { useResolutionSnapshot, type TemplateValueResolution } from './templateResolution';
import type { AssetOption, AssetOptionsLoader, DataScopeOption } from './queryBuilderTypes';

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
  selectedAsset: Asset | null;
  assetOptions: AssetOptionsLoader;
  assetSelectValue: AssetOption | null;
  dataScopeOptions: DataScopeOption[];
  selectAsset: (assetRid: string) => void;
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
        if (resolutionCoordinator.isMounted) {
          dispatchAssetIdentity({ type: 'cancelResolving', rid: resolvedRid });
        }
      };

      if (signal?.aborted) {
        cancelResolving();
        return;
      }
      signal?.addEventListener('abort', cancelResolving, { once: true });

      const resolveWith = (asset: Asset | null) => {
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
    resolutionCoordinator.cancelFetch();
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
        // Only the request id gates the alert: each new search bumps it (and so
        // does unmount), so a superseded search stays quiet while a genuine
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

    const action = decideAssetReconcile({
      assetRid: query?.assetRid,
      selectedAssetRid: selectedAsset?.rid,
      assetRidResolution: assetRidSnapshot,
    });

    if (action?.kind === 'fetchByRid') {
      reconcileSignal = resolutionCoordinator.beginFetch();
      applyAssetFromRid(action.rid, action.label, reconcileSignal);
    } else if (action?.kind === 'clearIdentity') {
      dispatchAssetIdentity({ type: 'clear' });
    }

    return reconcileSignal ? () => resolutionCoordinator.cancelFetch(reconcileSignal) : undefined;
  }, [query?.assetRid, selectedAsset?.rid, assetRidSnapshot, applyAssetFromRid, resolutionCoordinator]);

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
          onChange(changeSelectedDataScopeQuery(q, scopeNames[0]));
        } else if (!scopeIsValid && q?.dataScopeName) {
          onChange(changeSelectedDataScopeQuery(q, ''));
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
  }, [selectedAsset, onChange, hasUserInteracted, assetRidResolved, assetRidIsResolved]);

  const selectAsset = useCallback(
    (value: string) => {
      markInteracted();
      const trimmedValue = value.trim();

      if (!trimmedValue) {
        // A blank/whitespace selection clears the asset rather than committing
        // whitespace as the assetRid and firing a doomed by-RID fetch.
        clearAssetSelection();
        onChange(changeAssetRidQuery(query, ''));
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
        onChange(changeAssetRidQuery(query, ridToFind));
        return;
      }

      const displayedAsset = !isVariable
        ? latestAssetOptionsAssetsRef.current.find((asset) => asset.rid === ridToFind)
        : undefined;
      if (isConcreteRid && !isVariable) {
        if (displayedAsset) {
          // Displayed option: resolve immediately from the loaded asset, no fetch.
          resolutionCoordinator.cancelFetch();
          dispatchAssetIdentity({ type: 'beginResolving', rid: ridToFind });
          dispatchAssetIdentity({
            type: 'resolveAsset',
            rid: ridToFind,
            asset: displayedAsset,
            fallbackLabel: ASSET_RID_FALLBACK_LABEL,
          });
        } else if (ridToFind === query?.assetRid) {
          // Same-RID retry of a fallback asset: the saved RID is unchanged and
          // beginResolving does not change the raw selectedAsset, so neither
          // reconcile trigger fires. Fetch directly through the single
          // controller; this cannot race reconcile.
          applyAssetFromRid(ridToFind, ASSET_RID_FALLBACK_LABEL, resolutionCoordinator.beginFetch());
        } else {
          // New/changed RID: mask stale controls now; the onChange below changes
          // assetRid and the reconcile effect performs the by-RID fetch.
          dispatchAssetIdentity({ type: 'beginResolving', rid: ridToFind });
        }
      } else if (isVariable && selectedRidResolution.isResolved) {
        resolutionCoordinator.cancelFetch();
        dispatchAssetIdentity({ type: 'beginResolving', rid: selectedRidResolution.resolved });
      } else {
        clearAssetSelection();
      }

      if (isVariable) {
        onChange(changeAssetRidQuery(query, trimmedValue));
      } else if (isConcreteRid) {
        onChange(changeAssetRidQuery(query, ridToFind));
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

  const selectDataScope = useCallback(
    (dataScopeName: string) => {
      markInteracted();
      onChange(changeSelectedDataScopeQuery(query, dataScopeName));
    },
    [markInteracted, onChange, query]
  );

  useEffect(() => {
    resolutionCoordinator.markMounted();
    return () => {
      resolutionCoordinator.markUnmounted();
      resolutionCoordinator.cancelFetch();
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
    selectedAsset: visibleAssetIdentity.selectedAsset,
    assetOptions,
    assetSelectValue,
    dataScopeOptions,
    selectAsset,
    selectDataScope,
  };
}
