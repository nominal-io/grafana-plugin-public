import { createBasicAsset, getSupportedScopeNames, getSupportedScopes, type Asset } from '../../utils/api';

export interface AssetIdentityState {
  selectedAsset: Asset | null;
  pendingAssetRid: string | null;
  dataScopes: string[];
}

export interface VisibleAssetIdentity {
  selectedAsset: Asset | null;
  dataScopes: string[];
  selectedAssetSupportedScopeCount: number;
}

export type AssetIdentityAction =
  | { type: 'beginResolving'; rid: string }
  | { type: 'resolveAsset'; rid: string; asset: Asset | null; fallbackLabel: string }
  | { type: 'clear' };

export function createEmptyAssetIdentityState(): AssetIdentityState {
  return {
    selectedAsset: null,
    pendingAssetRid: null,
    dataScopes: [],
  };
}

export function assetIdentityReducer(state: AssetIdentityState, action: AssetIdentityAction): AssetIdentityState {
  switch (action.type) {
    case 'beginResolving':
      return { ...state, pendingAssetRid: action.rid };
    case 'resolveAsset': {
      if (state.pendingAssetRid !== action.rid) {
        return state;
      }
      const selectedAsset = action.asset ?? createBasicAsset(action.rid, action.fallbackLabel);
      return {
        selectedAsset,
        pendingAssetRid: null,
        dataScopes: action.asset ? getSupportedScopeNames(action.asset) : [],
      };
    }
    case 'clear':
      return createEmptyAssetIdentityState();
  }
}

export function getVisibleAssetIdentity(state: AssetIdentityState): VisibleAssetIdentity {
  const isResolvingDifferentAsset =
    state.pendingAssetRid !== null && state.selectedAsset?.rid !== state.pendingAssetRid;
  const selectedAsset = isResolvingDifferentAsset ? null : state.selectedAsset;
  const dataScopes = isResolvingDifferentAsset ? [] : state.dataScopes;

  return {
    selectedAsset,
    dataScopes,
    selectedAssetSupportedScopeCount: selectedAsset ? getSupportedScopes(selectedAsset).length : 0,
  };
}
