# Review Findings: Remove Asset Input Toggle (de/consolidate-asset-selection)

Review of the branch implementing `docs/superpowers/plans/2026-07-06-remove-asset-input-toggle.md`
(diff `19c9d45..HEAD`). No correctness bugs in production code; all findings are
test-quality/cleanup level. Findings 1 and 2 trace back to the plan itself, which the
implementation followed verbatim.

## 1. E2E test never committed the pasted RID — FIXED

**File:** `tests/queryEditor.spec.ts` (data-query test)

**Issue:** The rewritten test filled the asset Combobox and asserted the echoed input text.
Grafana's Combobox only fires `onChange` on option selection (Enter / custom-value pick), never
on typing, so `commands.selectAsset` never ran. The paste-a-RID commit path — the exact flow
that replaced the deleted direct-RID `<Input>` — had zero e2e coverage, and the test was a
near-duplicate of the search test above it.

**Fix (applied):** After `fill()`, press Enter to select the highlighted custom-value option,
then assert committed-state signals instead of input text:
- the Data scope field appears (only renders when an `assetRid` is committed), and
- the asset summary line shows the committed RID (renders after the by-RID fetch settles,
  whether the RID resolves to a real asset or the `Asset (RID)` fallback).

Both signals are agnostic to whether the test RID exists in the CI backend. No local gate runs
this file; verify via the CI e2e job.

## 2. Tautological legacy-key unit test — FIXED

**File:** `src/components/queryBuilder/assetReconcile.test.ts`

**Issue:** `'fetches a concrete saved RID even when legacy inputs still carry an input-method
key'` duplicated the test above it plus an inert `assetInputMethod` key. `decideAssetReconcile`
destructures four fixed fields, so the extra key structurally cannot influence the result — the
test could never fail independently of its twin. Real legacy-key coverage lives where the key
actually flows through `NominalQuery`: `useAssetSelection.test.ts` (hook) and
`QueryEditor.test.tsx` (editor).

**Fix (applied):** Deleted the test. Suite: 9 tests, green.

## 3. Single-use test helper `renderSavedAsset` — NOT FIXING

**File:** `src/components/queryBuilder/useAssetSelection.test.ts:115`

**Issue:** The diff deleted one of the helper's two callers, leaving it single-use.

**Why we're leaving it:** The helper predates this branch (it was only renamed), and it
correctly encapsulates the build-args-once `renderHook` pattern — the known footgun where
constructing `args()` inside the `renderHook` closure infinite-loops the selection effect and
hangs jest. Inlining buys only locality (arrange step visible in the test body) and risks
someone re-inlining the pattern wrong later. Marginal churn; fold it in only if the file is
being touched anyway.

## 4. Derivable `assetRid === ''` guard in `decideAssetReconcile` — NOT FIXING

**File:** `src/components/queryBuilder/assetReconcile.ts`

**Issue:** The `assetRid === ''` → `clearIdentity` branch is derivable: the sole producer
(`resolveTemplateValue` via `useResolutionSnapshot`) returns `{resolved: '', isResolved: true}`
for an empty raw value, which the later `!resolved && isResolved` branch already maps to
`clearIdentity`. Two branches encode one condition.

**Why we're leaving it:** Collapsing would couple the pure, independently-tested decision
function's correctness to a producer invariant three files away — with the guard, the empty-RID
behavior is locally provable from the function body alone; without it, an inconsistent input
(`assetRid: ''` with a non-empty `resolved`) silently becomes `fetchByRid`. The plan also kept
the guards verbatim from the old body deliberately, so each survives review as "unchanged".
The redundancy is defensive documentation, not waste.
