import type { Asset } from '../../utils/api';
import { classifyAssetRidEcho, decideAssetReconcile } from './assetReconcile';
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

  it('clears asset identity when a search template resolves to an empty RID', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        assetInputMethod: 'search',
        selectedAssetRid: ASSET.rid,
        assetRidResolution: resolution({ raw: '$asset', resolved: '', hasTemplate: true, isResolved: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([{ kind: 'clearIdentity' }]);
  });

  it('mirrors direct raw input and clears asset identity when a direct template resolves to an empty RID', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        assetInputMethod: 'direct',
        selectedAssetRid: ASSET.rid,
        assetRidResolution: resolution({ raw: '$asset', resolved: '', hasTemplate: true, isResolved: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([{ kind: 'mirrorDirectRaw', raw: '$asset' }, { kind: 'clearIdentity' }]);
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

  it('fetches by RID (not infer direct) for an untyped saved template RID', () => {
    expect(
      decideAssetReconcile({
        assetRid: '$asset',
        assetInputMethod: undefined,
        selectedAssetRid: undefined,
        assetRidResolution: resolution({ raw: '$asset', hasTemplate: true }),
        eventOwnedConcreteAssetRid: undefined,
      })
    ).toEqual([{ kind: 'fetchByRid', rid: ASSET.rid, label: 'Asset ($asset)' }]);
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

describe('classifyAssetRidEcho', () => {
  const B = 'ri.scout.main.asset.b';
  const C = 'ri.scout.main.asset.c';

  it('reports none when no commits are awaiting an echo', () => {
    expect(classifyAssetRidEcho({ raw: ASSET.rid, lastReconciledRaw: '', pendingEchoRaws: [] })).toBe('none');
  });

  it('reports sync when the query echoes the newest committed value', () => {
    expect(classifyAssetRidEcho({ raw: C, lastReconciledRaw: ASSET.rid, pendingEchoRaws: [B, C] })).toBe('sync');
  });

  it('reports lag when the query echoes an older committed value', () => {
    expect(classifyAssetRidEcho({ raw: B, lastReconciledRaw: ASSET.rid, pendingEchoRaws: [B, C] })).toBe('lag');
  });

  it('reports lag when the query still carries the pre-commit value', () => {
    expect(classifyAssetRidEcho({ raw: ASSET.rid, lastReconciledRaw: ASSET.rid, pendingEchoRaws: [B, C] })).toBe(
      'lag'
    );
  });

  it('reports external for a value this editor never committed', () => {
    expect(classifyAssetRidEcho({ raw: ASSET.rid, lastReconciledRaw: '', pendingEchoRaws: [B, C] })).toBe('external');
  });

  it('prefers sync when the newest commit repeats an older one', () => {
    expect(classifyAssetRidEcho({ raw: B, lastReconciledRaw: '', pendingEchoRaws: [B, C, B] })).toBe('sync');
  });
});
