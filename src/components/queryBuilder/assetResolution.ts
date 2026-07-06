export class AssetResolutionCoordinator {
  private assetOptionsRequestId = 0;
  private mounted = true;
  private assetSelectController: AbortController | undefined;
  private reconcileController: AbortController | undefined;
  private ownedConcreteAssetRid: string | undefined;

  startAssetOptionsRequest(): number {
    this.assetOptionsRequestId += 1;
    return this.assetOptionsRequestId;
  }

  isCurrentAssetOptionsRequest(requestId: number): boolean {
    return this.mounted && this.assetOptionsRequestId === requestId;
  }

  markMounted(): void {
    this.mounted = true;
  }

  get isMounted(): boolean {
    return this.mounted;
  }

  get eventOwnedConcreteAssetRid(): string | undefined {
    return this.ownedConcreteAssetRid;
  }

  markUnmounted(): void {
    this.mounted = false;
    // Invalidate any in-flight asset options request so a late failure cannot
    // alert after unmount.
    this.assetOptionsRequestId += 1;
  }

  clearEventOwnedConcreteAssetRidIfMatches(rid: string): void {
    if (this.ownedConcreteAssetRid === rid) {
      this.ownedConcreteAssetRid = undefined;
    }
  }

  /**
   * Begin an event-owned select fetch: supersedes all in-flight resolution and
   * takes ownership of `rid`. This is the only place event ownership is set;
   * it is released by clearEventOwnedConcreteAssetRidIfMatches or
   * cancelInFlightResolution.
   */
  beginSelectFetch(rid: string): AbortSignal {
    this.cancelInFlightResolution();
    const controller = new AbortController();
    this.assetSelectController = controller;
    this.ownedConcreteAssetRid = rid;
    return controller.signal;
  }

  // The select/reconcile two-controller split is deliberate: reconcile-effect
  // cleanup must abort only its own fetch (via the signal-identity check in
  // cancelReconcileFetch), never an event-owned select fetch.
  beginReconcileFetch(): AbortSignal {
    this.cancelReconcileFetch();
    const controller = new AbortController();
    this.reconcileController = controller;
    return controller.signal;
  }

  cancelReconcileFetch(signal?: AbortSignal): void {
    if (signal && this.reconcileController?.signal !== signal) {
      return;
    }
    this.reconcileController?.abort();
    this.reconcileController = undefined;
  }

  /**
   * Single owner of the cancel-in-flight-resolution sequence. Every path that
   * supersedes a pending by-RID fetch must abort the active select and
   * reconcile controllers and release event ownership; forgetting a step leaks
   * a controller or strands a pending RID.
   */
  cancelInFlightResolution(): void {
    this.assetSelectController?.abort();
    this.assetSelectController = undefined;
    this.cancelReconcileFetch();
    this.ownedConcreteAssetRid = undefined;
  }
}
