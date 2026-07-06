import { AssetResolutionCoordinator } from './assetResolution';

describe('AssetResolutionCoordinator', () => {
  it('beginFetch supersedes an in-flight fetch', () => {
    const coordinator = new AssetResolutionCoordinator();

    const first = coordinator.beginFetch();
    const second = coordinator.beginFetch();

    expect(first.aborted).toBe(true);
    expect(second.aborted).toBe(false);
  });

  it('cancelFetch aborts the in-flight fetch', () => {
    const coordinator = new AssetResolutionCoordinator();

    const signal = coordinator.beginFetch();
    coordinator.cancelFetch();

    expect(signal.aborted).toBe(true);
  });

  it('stale cleanup does not abort the current fetch', () => {
    const coordinator = new AssetResolutionCoordinator();

    const firstSignal = coordinator.beginFetch();
    const secondSignal = coordinator.beginFetch();

    coordinator.cancelFetch(firstSignal);

    expect(firstSignal.aborted).toBe(true);
    expect(secondSignal.aborted).toBe(false);

    coordinator.cancelFetch(secondSignal);

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
