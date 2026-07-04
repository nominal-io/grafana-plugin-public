type TimerHandle = ReturnType<typeof setTimeout> | number;

export class AssetResolutionCoordinator {
  private assetOptionsRequestId = 0;
  private mounted = true;
  private directRidTimer: TimerHandle | undefined;
  private directRidController: AbortController | undefined;
  private assetSelectController: AbortController | undefined;
  private reconcileController: AbortController | undefined;
  private ownedConcreteAssetRid: string | undefined;

  startAssetOptionsRequest(): number {
    this.assetOptionsRequestId += 1;
    return this.assetOptionsRequestId;
  }

  invalidateAssetOptionsRequests(): void {
    this.assetOptionsRequestId += 1;
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
    this.invalidateAssetOptionsRequests();
  }

  setEventOwnedConcreteAssetRid(rid: string | undefined): void {
    this.ownedConcreteAssetRid = rid;
  }

  clearEventOwnedConcreteAssetRidIfMatches(rid: string): void {
    if (this.ownedConcreteAssetRid === rid) {
      this.ownedConcreteAssetRid = undefined;
    }
  }

  /**
   * Arm the debounced direct-RID fetch. Cancels any previously armed direct
   * fetch first, so re-typing can never leak the prior timer or controller.
   * Deliberately does NOT touch select fetches or event ownership — the
   * direct-RID path sets ownership before committing the query, and a full
   * cancel here would release it.
   */
  scheduleDirectRidFetch(run: (signal: AbortSignal) => void, delayMs: number): void {
    this.cancelDirectRidFetch();
    const controller = new AbortController();
    this.directRidController = controller;
    this.directRidTimer = setTimeout(() => run(controller.signal), delayMs);
  }

  /**
   * Begin an event-owned select fetch: supersedes all in-flight resolution
   * (direct debounce, active controllers, prior ownership), takes ownership of
   * `rid`, and returns the abort signal for the new fetch.
   */
  beginSelectFetch(rid: string): AbortSignal {
    this.cancelInFlightResolution();
    const controller = new AbortController();
    this.assetSelectController = controller;
    this.ownedConcreteAssetRid = rid;
    return controller.signal;
  }

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

  private cancelDirectRidFetch(): void {
    clearTimeout(this.directRidTimer);
    this.directRidTimer = undefined;
    this.directRidController?.abort();
    this.directRidController = undefined;
  }

  /**
   * Single owner of the cancel-in-flight-resolution sequence. Every path that
   * supersedes a pending by-RID fetch must stop the direct-RID debounce, abort
   * active fetch controllers, and release event ownership; forgetting a step
   * leaks a controller or strands a pending RID.
   */
  cancelInFlightResolution(): void {
    this.cancelDirectRidFetch();
    this.assetSelectController?.abort();
    this.assetSelectController = undefined;
    this.cancelReconcileFetch();
    this.ownedConcreteAssetRid = undefined;
  }
}
