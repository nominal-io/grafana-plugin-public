export class AssetResolutionCoordinator {
  private assetOptionsRequestId = 0;
  private mounted = true;
  private fetchController: AbortController | undefined;

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

  markUnmounted(): void {
    this.mounted = false;
    // Invalidate any in-flight asset options request so a late failure cannot
    // alert after unmount.
    this.assetOptionsRequestId += 1;
  }

  /**
   * Begin a by-RID fetch, superseding any in-flight one, and return its abort
   * signal. This is the single owner of by-RID fetch lifecycle: the reconcile
   * effect drives it for saved/changed RIDs, and selectAsset drives it directly
   * only for a same-RID fallback retry (which cannot re-trigger reconcile).
   */
  beginFetch(): AbortSignal {
    this.cancelFetch();
    const controller = new AbortController();
    this.fetchController = controller;
    return controller.signal;
  }

  /**
   * Abort the in-flight by-RID fetch. When a signal is passed, only abort if it
   * still owns the controller, so a stale reconcile-effect cleanup cannot abort
   * a newer fetch.
   */
  cancelFetch(signal?: AbortSignal): void {
    if (signal && this.fetchController?.signal !== signal) {
      return;
    }
    this.fetchController?.abort();
    this.fetchController = undefined;
  }
}
