import { AssetResolutionCoordinator } from './assetResolution';

describe('AssetResolutionCoordinator', () => {
  it('re-arming the direct fetch supersedes the previous one without leaking its timer', () => {
    jest.useFakeTimers();
    const coordinator = new AssetResolutionCoordinator();
    const first = jest.fn();
    const second = jest.fn();

    coordinator.scheduleDirectRidFetch(first, 300);
    coordinator.scheduleDirectRidFetch(second, 300);
    jest.advanceTimersByTime(300);

    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);
    expect((second.mock.calls[0][0] as AbortSignal).aborted).toBe(false);
  });

  it('beginSelectFetch supersedes in-flight work and takes ownership of the RID', () => {
    jest.useFakeTimers();
    const coordinator = new AssetResolutionCoordinator();
    const fetchDirectRid = jest.fn();

    coordinator.scheduleDirectRidFetch(fetchDirectRid, 300);
    const reconcileSignal = coordinator.beginReconcileFetch();
    const signal = coordinator.beginSelectFetch('ri.scout.main.asset.a');
    jest.advanceTimersByTime(300);

    expect(fetchDirectRid).not.toHaveBeenCalled();
    expect(reconcileSignal.aborted).toBe(true);
    expect(signal.aborted).toBe(false);
    expect(coordinator.eventOwnedConcreteAssetRid).toBe('ri.scout.main.asset.a');
  });

  it('cancels direct timers, fetch controllers, reconcile fetches, and event ownership in one place', () => {
    jest.useFakeTimers();
    const coordinator = new AssetResolutionCoordinator();
    const fetchDirectRid = jest.fn();

    const selectSignal = coordinator.beginSelectFetch('ri.scout.main.asset.a');
    coordinator.scheduleDirectRidFetch(fetchDirectRid, 300);
    const reconcileSignal = coordinator.beginReconcileFetch();

    coordinator.cancelInFlightResolution();

    jest.advanceTimersByTime(300);
    expect(fetchDirectRid).not.toHaveBeenCalled();
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
