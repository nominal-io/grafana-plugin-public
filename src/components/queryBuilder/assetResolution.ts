export type AssetRidEchoStatus = 'none' | 'sync' | 'lag' | 'external';
type TimerHandle = ReturnType<typeof setTimeout> | number;

/**
 * Classify the query prop's assetRid against the raw values this editor has
 * committed via onChange but not yet seen echoed back. Because the parent's
 * query state moves monotonically through the values written to it, a lagging
 * prop can only ever deliver a value we committed ourselves (or the value it
 * held before the commits) — anything else is a genuine external change.
 *
 * Known limitation (accepted): if a second writer overwrites an un-echoed
 * commit with a value that collides with an older pending entry or the
 * last-reconciled value, that delivery is indistinguishable from lag and is
 * ignored; the editor stays on its local value until the next user edit
 * commits (any commit re-arms the queue, and a non-colliding external value
 * always classifies 'external' and wins). This requires a lagging or
 * multi-writer parent, which Grafana's synchronous single-writer onChange
 * flow does not produce.
 */
export function classifyAssetRidEcho({
  raw,
  lastReconciledRaw,
  pendingEchoRaws,
}: {
  raw: string;
  lastReconciledRaw: string;
  pendingEchoRaws: string[];
}): AssetRidEchoStatus {
  if (pendingEchoRaws.length === 0) {
    return 'none';
  }
  if (raw === pendingEchoRaws[pendingEchoRaws.length - 1]) {
    return 'sync';
  }
  if (pendingEchoRaws.includes(raw) || raw === lastReconciledRaw) {
    return 'lag';
  }
  return 'external';
}

export class AssetResolutionCoordinator {
  private pendingEchoRaws: string[] = [];
  private lastReconciledRaw: string;
  private assetOptionsRequestId = 0;
  private mounted = true;
  private directRidTimer: TimerHandle | undefined;
  private directRidController: AbortController | undefined;
  private assetSelectController: AbortController | undefined;
  private ownedConcreteAssetRid: string | undefined;

  constructor(initialAssetRidRaw = '') {
    this.lastReconciledRaw = initialAssetRidRaw;
  }

  trackCommittedAssetRid(committedRaw: string): void {
    this.pendingEchoRaws.push(committedRaw);
    if (this.pendingEchoRaws.length > 50) {
      this.pendingEchoRaws.shift();
    }
  }

  /**
   * Classify AND consume: on 'sync'/'external' this clears the pending echo
   * queue and advances lastReconciledRaw; 'lag' deliberately mutates nothing
   * so the newer echo (or a genuine external change) can still be recognized
   * on a later delivery. Because it consumes the queue, only the single
   * reconcile effect may call it — a second caller peeking at echo status
   * would drain the queue and make a real lagging echo classify 'external',
   * reverting the user's newer local edit.
   */
  consumeQueryAssetRidEcho(raw: string): AssetRidEchoStatus {
    const status = classifyAssetRidEcho({
      raw,
      lastReconciledRaw: this.lastReconciledRaw,
      pendingEchoRaws: this.pendingEchoRaws,
    });
    if (status === 'lag') {
      return status;
    }
    if (status !== 'none') {
      this.pendingEchoRaws = [];
    }
    this.lastReconciledRaw = raw;
    return status;
  }

  startAssetOptionsRequest(): number {
    this.assetOptionsRequestId += 1;
    return this.assetOptionsRequestId;
  }

  invalidateAssetOptionsRequests(): void {
    this.assetOptionsRequestId += 1;
  }

  shouldPublishAssetOptionsFailure(requestId: number): boolean {
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
   * (direct debounce, both controllers, prior ownership), takes ownership of
   * `rid`, and returns the abort signal for the new fetch.
   */
  beginSelectFetch(rid: string): AbortSignal {
    this.cancelInFlightResolution();
    const controller = new AbortController();
    this.assetSelectController = controller;
    this.ownedConcreteAssetRid = rid;
    return controller.signal;
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
   * both fetch controllers, and release event ownership; forgetting a step
   * leaks a controller or strands a pending RID. Returns the RID that was
   * event-owned so callers can cancel its pending reducer state (the debounced
   * fetch may not have started yet, so its abort listener may not exist).
   */
  cancelInFlightResolution(): string | undefined {
    const ownedRid = this.ownedConcreteAssetRid;
    this.cancelDirectRidFetch();
    this.assetSelectController?.abort();
    this.assetSelectController = undefined;
    this.ownedConcreteAssetRid = undefined;
    return ownedRid;
  }
}
