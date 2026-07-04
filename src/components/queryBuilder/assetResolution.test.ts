import { AssetResolutionCoordinator } from './assetResolution';

describe('AssetResolutionCoordinator', () => {
  it('tracks committed RID echoes and classifies lag versus external changes', () => {
    const coordinator = new AssetResolutionCoordinator('ri.scout.main.asset.a');

    coordinator.trackCommittedAssetRid('ri.scout.main.asset.b');
    coordinator.trackCommittedAssetRid('ri.scout.main.asset.c');

    expect(coordinator.consumeQueryAssetRidEcho('ri.scout.main.asset.a')).toBe('lag');
    expect(coordinator.consumeQueryAssetRidEcho('ri.scout.main.asset.b')).toBe('lag');
    expect(coordinator.consumeQueryAssetRidEcho('ri.scout.main.asset.c')).toBe('sync');

    coordinator.trackCommittedAssetRid('ri.scout.main.asset.d');

    expect(coordinator.consumeQueryAssetRidEcho('ri.scout.main.asset.external')).toBe('external');
  });

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
    const signal = coordinator.beginSelectFetch('ri.scout.main.asset.a');
    jest.advanceTimersByTime(300);

    expect(fetchDirectRid).not.toHaveBeenCalled();
    expect(signal.aborted).toBe(false);
    expect(coordinator.eventOwnedConcreteAssetRid).toBe('ri.scout.main.asset.a');
  });

  it('cancels direct timers, fetch controllers, and event ownership in one place', () => {
    jest.useFakeTimers();
    const coordinator = new AssetResolutionCoordinator();
    const fetchDirectRid = jest.fn();

    const selectSignal = coordinator.beginSelectFetch('ri.scout.main.asset.a');
    coordinator.scheduleDirectRidFetch(fetchDirectRid, 300);

    expect(coordinator.cancelInFlightResolution()).toBe('ri.scout.main.asset.a');

    jest.advanceTimersByTime(300);
    expect(fetchDirectRid).not.toHaveBeenCalled();
    expect(selectSignal.aborted).toBe(true);
    expect(coordinator.eventOwnedConcreteAssetRid).toBeUndefined();
  });

  it('invalidates superseded asset option failures and suppresses unmounted failures', () => {
    const coordinator = new AssetResolutionCoordinator();

    const first = coordinator.startAssetOptionsRequest();
    const second = coordinator.startAssetOptionsRequest();

    expect(coordinator.shouldPublishAssetOptionsFailure(first)).toBe(false);
    expect(coordinator.shouldPublishAssetOptionsFailure(second)).toBe(true);

    coordinator.markUnmounted();

    expect(coordinator.shouldPublishAssetOptionsFailure(second)).toBe(false);
  });
});
