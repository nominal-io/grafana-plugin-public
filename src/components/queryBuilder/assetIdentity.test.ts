import type { Asset } from '../../utils/api';
import { assetIdentityReducer, createEmptyAssetIdentityState, getVisibleAssetIdentity } from './assetIdentity';

const assetA: Asset = {
  rid: 'ri.scout.main.asset.a',
  title: 'Asset A',
  labels: [],
  dataScopes: [
    {
      dataScopeName: 'default',
      dataSource: { type: 'dataset', dataset: 'ri.scout.main.dataset.a' },
      timestampType: 'ABSOLUTE',
      seriesTags: {},
    },
  ],
  properties: {},
};

const assetB: Asset = {
  ...assetA,
  rid: 'ri.scout.main.asset.b',
  title: 'Asset B',
  dataScopes: [{ ...assetA.dataScopes[0], dataScopeName: 'new-scope' }],
};

describe('assetIdentity', () => {
  it('hides selected asset controls while a different RID is pending', () => {
    const visible = getVisibleAssetIdentity({
      selectedAsset: assetA,
      pendingAssetRid: assetB.rid,
      dataScopes: ['default'],
    });

    expect(visible).toEqual({
      selectedAsset: null,
      dataScopes: [],
      selectedAssetSupportedScopeCount: 0,
    });
  });

  it('keeps selected asset controls visible when the pending RID matches', () => {
    const visible = getVisibleAssetIdentity({
      selectedAsset: assetA,
      pendingAssetRid: assetA.rid,
      dataScopes: ['default'],
    });

    expect(visible).toEqual({
      selectedAsset: assetA,
      dataScopes: ['default'],
      selectedAssetSupportedScopeCount: 1,
    });
  });

  it('resolves a pending RID into the selected asset and supported data scopes', () => {
    const pending = assetIdentityReducer(
      { selectedAsset: assetA, pendingAssetRid: null, dataScopes: ['default'] },
      { type: 'beginResolving', rid: assetB.rid }
    );

    const resolved = assetIdentityReducer(pending, {
      type: 'resolveAsset',
      rid: assetB.rid,
      asset: assetB,
      fallbackLabel: 'Asset (Direct RID)',
    });

    expect(resolved).toEqual({
      selectedAsset: assetB,
      pendingAssetRid: null,
      dataScopes: ['new-scope'],
    });
  });

  it('ignores a stale resolution when another RID is pending', () => {
    const pending = {
      selectedAsset: assetA,
      pendingAssetRid: assetB.rid,
      dataScopes: ['default'],
    };

    expect(
      assetIdentityReducer(pending, {
        type: 'resolveAsset',
        rid: assetA.rid,
        asset: assetA,
        fallbackLabel: 'Asset (Direct RID)',
      })
    ).toBe(pending);
  });

  it('uses a basic asset when a pending RID cannot be fetched', () => {
    const pending = assetIdentityReducer(createEmptyAssetIdentityState(), {
      type: 'beginResolving',
      rid: assetB.rid,
    });

    expect(
      assetIdentityReducer(pending, {
        type: 'resolveAsset',
        rid: assetB.rid,
        asset: null,
        fallbackLabel: 'Asset (Direct RID)',
      })
    ).toEqual({
      selectedAsset: {
        rid: assetB.rid,
        title: 'Asset (Direct RID)',
        labels: [],
        dataScopes: [],
        properties: {},
      },
      pendingAssetRid: null,
      dataScopes: [],
    });
  });
});
