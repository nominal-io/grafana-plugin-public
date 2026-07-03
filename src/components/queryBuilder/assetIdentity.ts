import { createBasicAsset, getSupportedScopeNames, type Asset } from '../../utils/api';

export interface AssetIdentityState {
  selectedAsset: Asset | null;
  pendingAssetRid: string | null;
}

export interface VisibleAssetIdentity {
  selectedAsset: Asset | null;
  dataScopes: string[];
  selectedAssetSupportedScopeCount: number;
}

export type AssetIdentityAction =
  | { type: 'beginResolving'; rid: string }
  | { type: 'resolveAsset'; rid: string; asset: Asset | null; fallbackLabel: string }
  | { type: 'cancelResolving'; rid: string }
  | { type: 'clear' };

export function createEmptyAssetIdentityState(): AssetIdentityState {
  return {
    selectedAsset: null,
    pendingAssetRid: null,
  };
}

function clearIdentity(state: AssetIdentityState): AssetIdentityState {
  const isAlreadyEmpty = state.selectedAsset === null && state.pendingAssetRid === null;
  return isAlreadyEmpty ? state : createEmptyAssetIdentityState();
}

export function assetIdentityReducer(state: AssetIdentityState, action: AssetIdentityAction): AssetIdentityState {
  switch (action.type) {
    case 'beginResolving':
      if (!action.rid) {
        return clearIdentity(state);
      }
      if (state.pendingAssetRid === action.rid) {
        return state;
      }
      return { ...state, pendingAssetRid: action.rid };
    case 'resolveAsset': {
      if (state.pendingAssetRid !== action.rid) {
        return state;
      }
      const selectedAsset = action.asset ?? createBasicAsset(action.rid, action.fallbackLabel);
      return {
        selectedAsset,
        pendingAssetRid: null,
      };
    }
    case 'cancelResolving':
      if (state.pendingAssetRid !== action.rid) {
        return state;
      }
      return {
        ...state,
        pendingAssetRid: null,
      };
    case 'clear':
      return clearIdentity(state);
  }
}

/**
 * True when `rid` is the settled current asset: no resolution in flight and not
 * a fallback placeholder (fallbacks carry empty dataScopes), so re-selecting it
 * has nothing to fetch. A fallback asset stays refetchable so a failed lookup
 * can be retried.
 */
export function isAssetFullyResolved(state: AssetIdentityState, rid: string): boolean {
  return (
    state.selectedAsset?.rid === rid && state.pendingAssetRid === null && state.selectedAsset.dataScopes.length > 0
  );
}

export function getVisibleAssetIdentity(state: AssetIdentityState): VisibleAssetIdentity {
  const isResolvingDifferentAsset =
    Boolean(state.pendingAssetRid) && state.selectedAsset?.rid !== state.pendingAssetRid;
  const selectedAsset = isResolvingDifferentAsset ? null : state.selectedAsset;
  const dataScopes = selectedAsset ? getSupportedScopeNames(selectedAsset) : [];

  return {
    selectedAsset,
    dataScopes,
    selectedAssetSupportedScopeCount: dataScopes.length,
  };
}
