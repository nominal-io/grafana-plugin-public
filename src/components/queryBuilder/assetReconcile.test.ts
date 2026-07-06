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
        selectedAssetRid: undefined,
        assetRidResolution: resolution({ raw: '', resolved: '', isResolved: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toBeNull();
  });

  it('clears asset identity when the saved RID is empty', () => {
    expect(
      decideAssetReconcile({
        assetRid: '',
        selectedAssetRid: ASSET.rid,
        assetRidResolution: resolution({ raw: '', resolved: '', isResolved: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual({ kind: 'clearIdentity' });
  });

  it('does not fetch an unresolved template', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        selectedAssetRid: undefined,
        assetRidResolution: resolution({ raw: '$asset', resolved: '$asset', hasTemplate: true, isResolved: false }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toBeNull();
  });

  it('does nothing when a template has not resolved to a RID yet', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        selectedAssetRid: ASSET.rid,
        assetRidResolution: resolution({ raw: '$asset', resolved: '', hasTemplate: true, isResolved: false }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toBeNull();
  });

  it('clears asset identity when a template resolves to an empty RID', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        selectedAssetRid: ASSET.rid,
        assetRidResolution: resolution({ raw: '$asset', resolved: '', hasTemplate: true, isResolved: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual({ kind: 'clearIdentity' });
  });

  it('does nothing when the resolved RID is already selected', () => {
    expect(
      decideAssetReconcile({
        assetRid: ASSET.rid,
        selectedAssetRid: ASSET.rid,
        assetRidResolution: resolution(),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toBeNull();
  });

  it('skips a concrete RID owned by an event handler', () => {
    expect(
      decideAssetReconcile({
        assetRid: ASSET.rid,
        selectedAssetRid: undefined,
        assetRidResolution: resolution(),
        eventOwnedConcreteAssetRid: ASSET.rid,
      })
    ).toEqual(null);
  });

  it('fetches a concrete saved RID', () => {
    expect(
      decideAssetReconcile({
        assetRid: ASSET.rid,
        selectedAssetRid: undefined,
        assetRidResolution: resolution(),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual({ kind: 'fetchByRid', rid: ASSET.rid, label: 'Asset (RID)' });
  });

  it('fetches a resolved template RID with the template label', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        selectedAssetRid: undefined,
        assetRidResolution: resolution({ raw: '$asset', hasTemplate: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual({ kind: 'fetchByRid', rid: ASSET.rid, label: 'Asset ($asset)' });
  });
});
