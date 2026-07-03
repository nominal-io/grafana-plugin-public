import type { AssetInputMethod } from './queryBuilderTypes';
import type { TemplateValueResolution } from './templateResolution';

export const DIRECT_ASSET_RID_LABEL = 'Asset (Direct RID)';

export type AssetRidEchoStatus = 'none' | 'sync' | 'lag' | 'external';

/**
 * Classify the query prop's assetRid against the raw values this editor has
 * committed via onChange but not yet seen echoed back. Because the parent's
 * query state moves monotonically through the values written to it, a lagging
 * prop can only ever deliver a value we committed ourselves (or the value it
 * held before the commits) — anything else is a genuine external change.
 *
 * Known limitation (accepted): if a second writer overwrites an un-echoed
 * commit with a value that collides with an older pending entry or the
 * last-reconciled value, that delivery is indistinguishable from lag and is
 * ignored; the editor stays on its local value until the next user edit
 * commits (any commit re-arms the queue, and a non-colliding external value
 * always classifies 'external' and wins). This requires a lagging or
 * multi-writer parent, which Grafana's synchronous single-writer onChange
 * flow does not produce.
 */
export function classifyAssetRidEcho({
  raw,
  lastReconciledRaw,
  pendingEchoRaws,
}: {
  raw: string;
  lastReconciledRaw: string;
  pendingEchoRaws: string[];
}): AssetRidEchoStatus {
  if (pendingEchoRaws.length === 0) {
    return 'none';
  }
  if (raw === pendingEchoRaws[pendingEchoRaws.length - 1]) {
    return 'sync';
  }
  if (pendingEchoRaws.includes(raw) || raw === lastReconciledRaw) {
    return 'lag';
  }
  return 'external';
}

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

  if (!assetRid) {
    return actions;
  }

  if (assetInputMethod === 'direct') {
    actions.push({ kind: 'mirrorDirectRaw', raw: assetRid });
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
