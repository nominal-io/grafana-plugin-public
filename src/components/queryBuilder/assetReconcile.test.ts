import type { Asset } from '../../utils/api';
import { decideAssetReconcile } from './assetReconcile';
import type { TemplateValueResolution } from './templateResolution';

const ASSET: Asset = {
  rid: 'ri.scout.main.asset.a',
  title: 'Asset A',
  labels: [],
  dataScopes: [],
  properties: {},
};

function resolution(overrides: Partial<TemplateValueResolution> = {}): TemplateValueResolution {
  return {
    raw: ASSET.rid,
    resolved: ASSET.rid,
    hasTemplate: false,
    isResolved: true,
    ...overrides,
  };
}

describe('decideAssetReconcile', () => {
  it('does nothing when no asset RID is saved', () => {
    expect(
      decideAssetReconcile({
        assetRid: undefined,
        assetInputMethod: undefined,
        selectedAssetRid: undefined,
        assetRidResolution: resolution({ raw: '', resolved: '', isResolved: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([]);
  });

  it('mirrors a direct raw RID but does not fetch an unresolved template', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        assetInputMethod: 'direct',
        selectedAssetRid: undefined,
        assetRidResolution: resolution({ raw: '$asset', resolved: '$asset', hasTemplate: true, isResolved: false }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([{ kind: 'mirrorDirectRaw', raw: '$asset' }]);
  });

  it('stops after mirroring when the resolved direct RID is already selected', () => {
    expect(
      decideAssetReconcile({
        assetRid: ASSET.rid,
        assetInputMethod: 'direct',
        selectedAssetRid: ASSET.rid,
        assetRidResolution: resolution(),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([{ kind: 'mirrorDirectRaw', raw: ASSET.rid }]);
  });

  it('skips a concrete search RID owned by an event handler', () => {
    expect(
      decideAssetReconcile({
        assetRid: ASSET.rid,
        assetInputMethod: 'search',
        selectedAssetRid: undefined,
        assetRidResolution: resolution(),
        eventOwnedConcreteAssetRid: ASSET.rid,
      })
    ).toEqual([]);
  });

  it('mirrors and fetches a saved direct RID', () => {
    expect(
      decideAssetReconcile({
        assetRid: ASSET.rid,
        assetInputMethod: 'direct',
        selectedAssetRid: undefined,
        assetRidResolution: resolution(),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([
      { kind: 'mirrorDirectRaw', raw: ASSET.rid },
      { kind: 'fetchByRid', rid: ASSET.rid, label: 'Asset (Direct RID)' },
    ]);
  });

  it('infers direct mode for an untyped saved RID', () => {
    expect(
      decideAssetReconcile({
        assetRid: ASSET.rid,
        assetInputMethod: undefined,
        selectedAssetRid: undefined,
        assetRidResolution: resolution(),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([{ kind: 'inferDirect', raw: ASSET.rid, rid: ASSET.rid, label: 'Asset (Direct RID)' }]);
  });

  it('fetches by RID for a saved search-mode template RID', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        assetInputMethod: 'search',
        selectedAssetRid: undefined,
        assetRidResolution: resolution({ raw: '$asset', hasTemplate: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([{ kind: 'fetchByRid', rid: ASSET.rid, label: 'Asset ($asset)' }]);
  });

  it('fetches a concrete search-mode RID not owned by an event handler', () => {
    expect(
      decideAssetReconcile({
        assetRid: ASSET.rid,
        assetInputMethod: 'search',
        selectedAssetRid: undefined,
        assetRidResolution: resolution(),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([{ kind: 'fetchByRid', rid: ASSET.rid, label: 'Asset (Direct RID)' }]);
  });
});
