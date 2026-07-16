import type { AssetInputMethod } from './queryBuilderTypes';
import type { TemplateValueResolution } from './templateResolution';

export const DIRECT_ASSET_RID_LABEL = 'Asset (Direct RID)';

export type AssetReconcileAction =
  | { kind: 'mirrorDirectRaw'; raw: string }
  | { kind: 'fetchByRid'; rid: string; label: string }
  | { kind: 'inferDirect'; raw: string; rid: string; label: string }
  | { kind: 'clearIdentity' };

export interface AssetReconcileInputs {
  assetRid: string | undefined;
  assetInputMethod: AssetInputMethod | undefined;
  selectedAssetRid: string | undefined;
  assetRidResolution: TemplateValueResolution;
  eventOwnedConcreteAssetRid: string | undefined;
}

export function decideAssetReconcile({
  assetRid,
  assetInputMethod,
  selectedAssetRid,
  assetRidResolution,
  eventOwnedConcreteAssetRid,
}: AssetReconcileInputs): AssetReconcileAction[] {
  const actions: AssetReconcileAction[] = [];

  if (assetRid === undefined) {
    return actions;
  }

  if (assetInputMethod === 'direct') {
    actions.push({ kind: 'mirrorDirectRaw', raw: assetRid });
  }

  if (assetRid === '') {
    actions.push({ kind: 'clearIdentity' });
    return actions;
  }

  if (!assetRidResolution.resolved) {
    if (assetRidResolution.isResolved) {
      actions.push({ kind: 'clearIdentity' });
    }
    return actions;
  }

  if (!assetRidResolution.isResolved) {
    return actions;
  }

  if (selectedAssetRid === assetRidResolution.resolved) {
    return actions;
  }

  if (!assetRidResolution.hasTemplate && eventOwnedConcreteAssetRid === assetRidResolution.resolved) {
    return actions;
  }

  const label = assetRidResolution.hasTemplate ? `Asset (${assetRidResolution.raw})` : DIRECT_ASSET_RID_LABEL;

  if (assetInputMethod === 'direct') {
    actions.push({ kind: 'fetchByRid', rid: assetRidResolution.resolved, label });
    return actions;
  }

  if (!assetInputMethod) {
    // A legacy query carrying a template variable ($asset) belongs to search mode, not
    // the direct-RID input; fetch it like the search-mode template path rather than
    // inferring 'direct' and stuffing the raw variable into the direct RID field.
    actions.push(
      assetRidResolution.hasTemplate
        ? { kind: 'fetchByRid', rid: assetRidResolution.resolved, label }
        : { kind: 'inferDirect', raw: assetRid, rid: assetRidResolution.resolved, label }
    );
    return actions;
  }

  if (assetInputMethod === 'search') {
    actions.push({ kind: 'fetchByRid', rid: assetRidResolution.resolved, label });
  }

  return actions;
}
