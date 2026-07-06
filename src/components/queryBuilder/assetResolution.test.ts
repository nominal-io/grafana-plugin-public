import { AssetResolutionCoordinator } from './assetResolution';

describe('AssetResolutionCoordinator', () => {
  it('beginSelectFetch supersedes in-flight work and takes ownership of the RID', () => {
    const coordinator = new AssetResolutionCoordinator();

    const reconcileSignal = coordinator.beginReconcileFetch();
    const signal = coordinator.beginSelectFetch('ri.scout.main.asset.a');

    expect(reconcileSignal.aborted).toBe(true);
    expect(signal.aborted).toBe(false);
    expect(coordinator.eventOwnedConcreteAssetRid).toBe('ri.scout.main.asset.a');
  });

  it('cancels select fetches, reconcile fetches, and event ownership in one place', () => {
    const coordinator = new AssetResolutionCoordinator();

    const selectSignal = coordinator.beginSelectFetch('ri.scout.main.asset.a');
    const reconcileSignal = coordinator.beginReconcileFetch();

    coordinator.cancelInFlightResolution();

    expect(selectSignal.aborted).toBe(true);
    expect(reconcileSignal.aborted).toBe(true);
    expect(coordinator.eventOwnedConcreteAssetRid).toBeUndefined();
  });

  it('stale reconcile cleanup does not abort the current reconcile fetch', () => {
    const coordinator = new AssetResolutionCoordinator();

    const firstSignal = coordinator.beginReconcileFetch();
    const secondSignal = coordinator.beginReconcileFetch();

    coordinator.cancelReconcileFetch(firstSignal);

    expect(firstSignal.aborted).toBe(true);
    expect(secondSignal.aborted).toBe(false);

    coordinator.cancelReconcileFetch(secondSignal);

    expect(secondSignal.aborted).toBe(true);
  });

  it('tracks the current asset options request and invalidates it on unmount', () => {
    const coordinator = new AssetResolutionCoordinator();

    const first = coordinator.startAssetOptionsRequest();
    const second = coordinator.startAssetOptionsRequest();

    expect(coordinator.isCurrentAssetOptionsRequest(first)).toBe(false);
    expect(coordinator.isCurrentAssetOptionsRequest(second)).toBe(true);

    coordinator.markUnmounted();

    expect(coordinator.isCurrentAssetOptionsRequest(second)).toBe(false);
  });
});
