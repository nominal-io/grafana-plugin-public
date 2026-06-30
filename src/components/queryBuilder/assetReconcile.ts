import type { AssetInputMethod } from './queryBuilderTypes';
import type { TemplateValueResolution } from './templateResolution';

export type AssetReconcileAction =
  | { kind: 'mirrorDirectRaw'; raw: string }
  | { kind: 'fetchByRid'; rid: string; label: string }
  | { kind: 'inferDirect'; raw: string; rid: string; label: string };

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

  if (!assetRid) {
    return actions;
  }

  if (assetInputMethod === 'direct') {
    actions.push({ kind: 'mirrorDirectRaw', raw: assetRid });
  }

  if (!assetRidResolution.resolved || !assetRidResolution.isResolved) {
    return actions;
  }

  if (selectedAssetRid === assetRidResolution.resolved) {
    return actions;
  }

  if (!assetRidResolution.hasTemplate && eventOwnedConcreteAssetRid === assetRidResolution.resolved) {
    return actions;
  }

  const label = assetRidResolution.hasTemplate ? `Asset (${assetRidResolution.raw})` : 'Asset (Direct RID)';

  if (assetInputMethod === 'direct') {
    actions.push({ kind: 'fetchByRid', rid: assetRidResolution.resolved, label });
    return actions;
  }

  if (!assetInputMethod) {
    actions.push({ kind: 'inferDirect', raw: assetRid, rid: assetRidResolution.resolved, label });
    return actions;
  }

  if (assetInputMethod === 'search') {
    actions.push({ kind: 'fetchByRid', rid: assetRidResolution.resolved, label });
  }

  return actions;
}
