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
    });

    expect(visible).toEqual({
      selectedAsset: null,
      dataScopes: [],
    });
  });

  it('keeps selected asset controls visible when the pending RID matches', () => {
    const visible = getVisibleAssetIdentity({
      selectedAsset: assetA,
      pendingAssetRid: assetA.rid,
    });

    expect(visible).toEqual({
      selectedAsset: assetA,
      dataScopes: ['default'],
    });
  });

  it('treats an empty pending RID as clearing the selection', () => {
    expect(
      assetIdentityReducer({ selectedAsset: assetA, pendingAssetRid: null }, { type: 'beginResolving', rid: '' })
    ).toEqual(createEmptyAssetIdentityState());
  });

  it('returns the same state when clearing an already-empty identity', () => {
    const empty = createEmptyAssetIdentityState();

    expect(assetIdentityReducer(empty, { type: 'clear' })).toBe(empty);
    expect(assetIdentityReducer(empty, { type: 'beginResolving', rid: '' })).toBe(empty);
  });

  it('returns the same state when beginResolving repeats the pending RID', () => {
    const pending = {
      selectedAsset: assetA,
      pendingAssetRid: assetB.rid,
    };

    expect(assetIdentityReducer(pending, { type: 'beginResolving', rid: assetB.rid })).toBe(pending);
  });

  it('resolves a pending RID into the selected asset and supported data scopes', () => {
    const pending = assetIdentityReducer(
      { selectedAsset: assetA, pendingAssetRid: null },
      { type: 'beginResolving', rid: assetB.rid }
    );

    const resolved = assetIdentityReducer(pending, {
      type: 'resolveAsset',
      rid: assetB.rid,
      asset: assetB,
      fallbackLabel: 'Asset (RID)',
    });

    expect(resolved).toEqual({
      selectedAsset: assetB,
      pendingAssetRid: null,
    });
  });

  it('ignores a stale resolution when another RID is pending', () => {
    const pending = {
      selectedAsset: assetA,
      pendingAssetRid: assetB.rid,
    };

    expect(
      assetIdentityReducer(pending, {
        type: 'resolveAsset',
        rid: assetA.rid,
        asset: assetA,
        fallbackLabel: 'Asset (RID)',
      })
    ).toBe(pending);
  });

  it('cancels a matching pending RID without clearing the selected asset', () => {
    const pending = {
      selectedAsset: assetA,
      pendingAssetRid: assetB.rid,
    };

    expect(assetIdentityReducer(pending, { type: 'cancelResolving', rid: assetB.rid })).toEqual({
      selectedAsset: assetA,
      pendingAssetRid: null,
    });
  });

  it('ignores a stale pending RID cancellation', () => {
    const pending = {
      selectedAsset: assetA,
      pendingAssetRid: assetB.rid,
    };

    expect(assetIdentityReducer(pending, { type: 'cancelResolving', rid: assetA.rid })).toBe(pending);
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
        fallbackLabel: 'Asset (RID)',
      })
    ).toEqual({
      selectedAsset: {
        rid: assetB.rid,
        title: 'Asset (RID)',
        labels: [],
        dataScopes: [],
        properties: {},
      },
      pendingAssetRid: null,
    });
  });
});
