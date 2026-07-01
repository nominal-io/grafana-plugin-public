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
import { decideAssetReconcile } from './assetReconcile';
import { assetIdentityReducer, createEmptyAssetIdentityState, getVisibleAssetIdentity } from './assetIdentity';
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
  const directRidTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const directRidControllerRef = useRef<AbortController>(undefined);

  const applyAssetFromRid = useCallback(
    async (resolvedRid: string, displayLabel: string, signal?: AbortSignal) => {
      dispatchAssetIdentity({ type: 'beginResolving', rid: resolvedRid });

      try {
        const foundAsset = await fetchAssetByRid(datasourceUrl, resolvedRid);
        if (signal?.aborted) {
          return;
        }
        dispatchAssetIdentity({
          type: 'resolveAsset',
          rid: resolvedRid,
          asset: foundAsset,
          fallbackLabel: displayLabel,
        });
      } catch {
        if (signal?.aborted) {
          return;
        }
        notifyError(
          'Unable to load Nominal asset',
          'The RID was kept, but data scopes could not be loaded automatically.'
        );
        dispatchAssetIdentity({
          type: 'resolveAsset',
          rid: resolvedRid,
          asset: null,
          fallbackLabel: displayLabel,
        });
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
    if (eventOwnedConcreteAssetRidRef.current && assetRidResolved !== eventOwnedConcreteAssetRidRef.current) {
      clearTimeout(directRidTimerRef.current);
      assetSelectControllerRef.current?.abort();
      assetSelectControllerRef.current = undefined;
      directRidControllerRef.current?.abort();
      directRidControllerRef.current = undefined;
      eventOwnedConcreteAssetRidRef.current = undefined;
    }

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
    applyAssetFromRid,
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
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();
      assetSelectControllerRef.current?.abort();
      assetOptionsRequestId.current += 1;
      eventOwnedConcreteAssetRidRef.current = undefined;
      setAssetInputMethod(method);
      dispatchAssetIdentity({ type: 'clear' });
      setDirectRID(method === 'direct' ? query?.assetRid || '' : '');
      onChange(changeAssetInputMethodQuery(query, method));
    },
    [markInteracted, onChange, query]
  );

  const selectAsset = useCallback(
    (value: string) => {
      markInteracted();

      if (!value.trim()) {
        // Mirror changeDirectRID: a blank/whitespace selection clears the asset rather
        // than committing whitespace as the assetRid and firing a doomed by-RID fetch.
        assetSelectControllerRef.current?.abort();
        eventOwnedConcreteAssetRidRef.current = undefined;
        dispatchAssetIdentity({ type: 'clear' });
        onChange(changeSearchAssetRidQuery(query, ''));
        return;
      }

      const isVariable = value.includes('$');
      const selectedRidResolution = resolveTemplateText(value);
      const ridToFind = isVariable ? selectedRidResolution.resolved : value;

      if (ridToFind && !ridToFind.includes('$') && !isVariable) {
        assetSelectControllerRef.current?.abort();
        const controller = new AbortController();
        assetSelectControllerRef.current = controller;
        eventOwnedConcreteAssetRidRef.current = ridToFind;
        applyAssetFromRid(ridToFind, 'Asset (Direct RID)', controller.signal);
      } else if (isVariable && selectedRidResolution.isResolved) {
        assetSelectControllerRef.current?.abort();
        eventOwnedConcreteAssetRidRef.current = undefined;
        dispatchAssetIdentity({ type: 'beginResolving', rid: selectedRidResolution.resolved });
      } else {
        assetSelectControllerRef.current?.abort();
        eventOwnedConcreteAssetRidRef.current = undefined;
        dispatchAssetIdentity({ type: 'clear' });
      }

      if (isVariable) {
        onChange(changeSearchAssetRidQuery(query, value));
      } else if (ridToFind && !ridToFind.includes('$')) {
        onChange(changeSearchAssetRidQuery(query, ridToFind));
      }
    },
    [applyAssetFromRid, markInteracted, onChange, query, resolveTemplateText]
  );

  const changeDirectRID = useCallback(
    (rid: string) => {
      markInteracted();
      setDirectRID(rid);

      if (rid.trim()) {
        onChange(changeDirectAssetRidQuery(queryRef.current, rid));
      }

      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();

      if (!rid.trim()) {
        eventOwnedConcreteAssetRidRef.current = undefined;
        dispatchAssetIdentity({ type: 'clear' });
        onChange(changeDirectAssetRidQuery(queryRef.current, ''));
        return;
      }

      const ridResolution = resolveTemplateText(rid);
      if (!ridResolution.isResolved) {
        eventOwnedConcreteAssetRidRef.current = undefined;
        dispatchAssetIdentity({ type: 'clear' });
        return;
      }
      if (ridResolution.hasTemplate) {
        eventOwnedConcreteAssetRidRef.current = undefined;
        dispatchAssetIdentity({ type: 'beginResolving', rid: ridResolution.resolved });
        return;
      }

      eventOwnedConcreteAssetRidRef.current = ridResolution.resolved;
      dispatchAssetIdentity({ type: 'beginResolving', rid: ridResolution.resolved });

      const displayLabel = 'Asset (Direct RID)';
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

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      assetOptionsRequestId.current += 1;
      clearTimeout(directRidTimerRef.current);
      directRidControllerRef.current?.abort();
      assetSelectControllerRef.current?.abort();
    };
  }, []);

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
