import type { Asset } from '../../utils/api';
import { decideAssetReconcile, type AssetReconcileAction, type AssetReconcileInputs } from './assetReconcile';
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

type ReconcileCase = {
  name: string;
  input: AssetReconcileInputs;
  expected: AssetReconcileAction | null;
};

const reconcileCases: ReconcileCase[] = [
  {
    name: 'does nothing when no asset RID is saved',
    input: {
      assetRid: undefined,
      selectedAssetRid: undefined,
      assetRidResolution: resolution({ raw: '', resolved: '', isResolved: true }),
    },
    expected: null,
  },
  {
    name: 'clears asset identity when the saved RID is empty',
    input: {
      assetRid: '',
      selectedAssetRid: ASSET.rid,
      assetRidResolution: resolution({ raw: '', resolved: '', isResolved: true }),
    },
    expected: { kind: 'clearIdentity' },
  },
  {
    name: 'does not fetch an unresolved template',
    input: {
      assetRid: '$asset',
      selectedAssetRid: undefined,
      assetRidResolution: resolution({ raw: '$asset', resolved: '$asset', hasTemplate: true, isResolved: false }),
    },
    expected: null,
  },
  {
    name: 'does nothing when a template has not resolved to a RID yet',
    input: {
      assetRid: '$asset',
      selectedAssetRid: ASSET.rid,
      assetRidResolution: resolution({ raw: '$asset', resolved: '', hasTemplate: true, isResolved: false }),
    },
    expected: null,
  },
  {
    name: 'clears asset identity when a template resolves to an empty RID',
    input: {
      assetRid: '$asset',
      selectedAssetRid: ASSET.rid,
      assetRidResolution: resolution({ raw: '$asset', resolved: '', hasTemplate: true, isResolved: true }),
    },
    expected: { kind: 'clearIdentity' },
  },
  {
    name: 'does nothing when the resolved RID is already selected',
    input: {
      assetRid: ASSET.rid,
      selectedAssetRid: ASSET.rid,
      assetRidResolution: resolution(),
    },
    expected: null,
  },
  {
    name: 'fetches a concrete saved RID',
    input: {
      assetRid: ASSET.rid,
      selectedAssetRid: undefined,
      assetRidResolution: resolution(),
    },
    expected: { kind: 'fetchByRid', rid: ASSET.rid, label: 'Asset (RID)' },
  },
  {
    name: 'fetches a resolved template RID with the template label',
    input: {
      assetRid: '$asset',
      selectedAssetRid: undefined,
      assetRidResolution: resolution({ raw: '$asset', hasTemplate: true }),
    },
    expected: { kind: 'fetchByRid', rid: ASSET.rid, label: 'Asset ($asset)' },
  },
];

describe('decideAssetReconcile', () => {
  it.each(reconcileCases)('$name', ({ input, expected }) => {
    expect(decideAssetReconcile(input)).toEqual(expected);
  });
});
