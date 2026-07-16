import type { TemplateValueResolution } from './templateResolution';

// Shown when the RID is known but the asset name cannot be resolved
// (failed by-RID fetch): "RID known, name unresolvable".
export const ASSET_RID_FALLBACK_LABEL = 'Asset (RID)';

export type AssetReconcileAction =
  | { kind: 'fetchByRid'; rid: string; label: string }
  | { kind: 'clearIdentity' };

export interface AssetReconcileInputs {
  assetRid: string | undefined;
  selectedAssetRid: string | undefined;
  assetRidResolution: TemplateValueResolution;
}

export function decideAssetReconcile({
  assetRid,
  selectedAssetRid,
  assetRidResolution,
}: AssetReconcileInputs): AssetReconcileAction | null {
  if (assetRid === undefined) {
    return null;
  }

  if (assetRid === '') {
    return { kind: 'clearIdentity' };
  }

  if (!assetRidResolution.resolved) {
    if (assetRidResolution.isResolved) {
      return { kind: 'clearIdentity' };
    }
    return null;
  }

  if (!assetRidResolution.isResolved) {
    return null;
  }

  if (selectedAssetRid === assetRidResolution.resolved) {
    return null;
  }

  const label = assetRidResolution.hasTemplate ? `Asset (${assetRidResolution.raw})` : ASSET_RID_FALLBACK_LABEL;

  return { kind: 'fetchByRid', rid: assetRidResolution.resolved, label };
}
