# Query Execution Deepening Working Log

Date started: 2026-05-04

## Purpose

This is the living working log for deepening the backend query execution path in
the Nominal Grafana data source plugin.

The goal is not to move code into more files. The goal is to make query
execution a deeper Module: a small, clear Interface with the complicated
Implementation kept behind it. The result should improve Locality for future
changes and give tests more Leverage.

## Operating Rule

For future `NominalQueryExecution` work, use focused subagents before making or
changing an architectural recommendation.

At minimum, use subagents for three independent reads when the work touches
behavior, tests, or module shape:

1. One subagent maps the current behavior.
2. One subagent inspects the testing story.
3. One subagent challenges or improves the proposed direction.

The main agent owns the final judgment. It should reconcile disagreements,
record uncertainty, and only proceed when the recommendation survives that
pressure test.

For tiny follow-up edits, subagents can be skipped, but the reason should be
written in this log.

## Current Understanding

The current query execution center is `Datasource.QueryData` in
`pkg/plugin/datasource.go`.

Today it is responsible for all of this:

- loading plugin settings
- parsing Grafana query JSON into `NominalQueryModel`
- applying backend template variable interpolation
- handling connection-test queries
- validating query shape
- repairing stale or missing channel type metadata
- applying numeric aggregation defaults
- rejecting unsupported aggregation names
- separating legacy queries from asset/channel queries
- partitioning log queries away from numeric and string queries
- running log and non-log batches
- chunking compute requests at the backend subrequest limit
- building Nominal compute requests
- calling Nominal compute
- turning compute results into Grafana frames
- mapping every result back to its original RefID

This makes the current Module shallow. A caller or test has to understand too
much of the Implementation at once.

The most important current Seam is query preparation: raw Grafana input becomes
a normalized runtime query with variables applied, channel type known where
possible, aggregation state decided, and batch eligibility known.

## What Good Looks Like

A good end state is a `NominalQueryExecution` Module, initially inside
`pkg/plugin`.

`Datasource` should stay the Grafana Adapter. It should keep lifecycle,
settings loading, health checks, resource routing, and proxying. It should hand
query work to the deeper Module rather than own the whole query path.

The deeper Module should own:

- prepared-query creation
- query validation
- template variable application
- channel type inference
- aggregation defaulting and validation
- numeric, string, and log query planning
- log vs non-log partitioning
- batch chunking
- compute request construction
- compute execution
- response-to-frame rendering
- compute error mapping
- legacy query fallback, if keeping `Datasource.QueryData` thin is the priority

The Interface should let callers say, in effect:

> Here are loaded Nominal settings and Grafana queries. Return Grafana responses
> by RefID.

The caller should not need to know whether a query becomes numeric Arrow output,
enum/table output, paged log output, or a legacy frame.

## Non-Goals

- Do not create a new package first. Start inside `pkg/plugin` to avoid churn
  and import-cycle pressure.
- Do not wrap `executeBatchQuery` as-is. That would preserve the same shallow
  Interface in a different place.
- Do not split first by channel type. Numeric, string, and log differ later,
  but they share preparation, validation, type repair, and batching concerns.
- Do not make tests weaker by only asserting that mocks were called.

## Proposed File Shape

Start with files inside `pkg/plugin`:

- `query_execution.go`: the Module entry point and orchestration
- `query_model.go`: query model, preparation, validation, and defaults
- `compute_request.go`: compute request planning and construction
- `response_transform.go`: result and frame transformation
- `aggregation.go`: keep as-is for now because it already has good Locality

This file shape is a starting point, not a mandate. The real test is whether
the Interface becomes smaller while the Implementation becomes easier to reason
about.

## Migration Process

Move behavior in small slices.

1. Add the `NominalQueryExecution` Module inside `pkg/plugin`.
2. Add prepared-query contract tests before moving behavior.
3. Move template variable application, validation, and aggregation defaults.
4. Move compute request construction.
5. Move result-to-frame transformation.
6. Move channel type inference and cache ownership.
7. Move batch planning, chunking, and compute calls.
8. Thin `Datasource.QueryData` to settings loading plus one Module call.
9. Reassess whether a subpackage is worth it after the Module earns its shape.

Each step should leave the backend tests passing.

## Testing Story

The existing backend tests are already strong. They cover:

- bad input and missing datasource settings
- legacy query routing
- batch grouping
- 300-query chunking
- chunk-level failure isolation
- partial compute errors
- missing compute results
- channel type inference
- stale saved channel types from template-variable expansion
- asset and channel type cache reuse
- numeric, string, and log request shapes
- Arrow aggregation decoding
- FIRST/LAST point timestamps
- log frame shape and sorting
- mixed log and numeric batching

The refactor should preserve those protections while moving some tests closer
to the new Interface.

Add or sharpen these tests before or during the migration:

- prepared-query contract:
  given saved query input and template variables, the backend produces resolved
  asset/channel/scope values, corrected channel type where possible, and clear
  explicit-vs-default aggregation state
- aggregation defaults:
  missing numeric aggregations become `MEAN`, explicit aggregations keep display
  labels, duplicates are removed, invalid names are rejected, and string/log
  queries skip numeric aggregation validation
- channel type inference failures:
  asset lookup failure, missing datasource RID, channel search failure, and
  channel not found should fall back predictably
- planner behavior:
  log and non-log work are separated, large requests are chunked, and result
  mapping does not depend on execution order
- mixed QueryData contract:
  one request containing numeric, string, log, legacy, invalid, and partially
  failing queries still returns the right response for every RefID
- extra compute results:
  current tests cover missing results, but not more results than requested
- realistic dashboard refresh:
  include `MaxDataPoints`, mixed channel types, explicit aggregations, and a
  stale saved channel type

## Validation Ledger

Known backend verification command:

```bash
go test ./pkg/...
```

Recent result from this investigation: passed when the Go build cache was
writable.

Recent result after the first prepared-query implementation slice:

```bash
go test ./pkg/...
```

Result: passed on 2026-05-04.

Recent result after moving the execution flow behind `NominalQueryExecution`:

```bash
go test -count=1 ./pkg/...
```

Result: passed on 2026-05-04.

Frontend verification commands are expected to be:

```bash
pnpm run test:ci
pnpm run typecheck
```

In this worktree, those frontend checks previously failed at setup because
`node_modules` was missing. Treat that as an environment/setup failure, not a
plugin behavior failure.

## Decision Log

### 2026-05-04: Start with prepared query, not batch execution

Decision: the first deepening move should be a prepared-query shape.

Why: `executeBatchQuery` is already downstream of too many decisions. Moving it
first would keep the Interface shallow.

Evidence checked:

- `QueryData` currently owns parse, validate, type repair, aggregation defaults,
  legacy routing, partitioning, batching, execution, and response attachment.
- Tests are strongest where behavior has a focused surface, such as
  `aggregation.go`.

Rejected alternative: extracting `executeBatchQuery` directly.

Remaining doubt: how much of result-to-frame rendering should be inside the
first `NominalQueryExecution` Module versus split into a second deeper Module
after preparation is stable.

### 2026-05-04: Keep the first Module inside `pkg/plugin`

Decision: start inside the current package instead of creating
`pkg/plugin/queryexec` immediately.

Why: the repo currently has a small backend package layout, and many tests
construct `Datasource` with mocked services. A package split would force type
movement before the Interface has proved itself.

Rejected alternative: create a subpackage first.

Remaining doubt: a subpackage may become useful later if the Module stabilizes
and package-level cycles are not a risk.

### 2026-05-04: Add the prepared-query helper before moving batching

Decision: add a private prepared-query helper in `pkg/plugin` and route
`Datasource.QueryData` through it.

Why: this creates a real Seam around the first set of query execution decisions
without moving batching, compute request construction, or response rendering
yet.

Evidence checked:

- The fragile order is preserved: apply variables, short-circuit connection
  tests, validate, infer channel type, then normalize aggregations.
- New tests cover template-variable preparation, aggregation defaults and
  validation, connection-test classification, legacy classification, and channel
  type inference.
- `go test ./pkg/...` passed after the change.

Rejected alternative: move `executeBatchQuery`, `buildComputeRequest`, or
`transformBatchResult` in the same patch.

Remaining doubt: future tests should keep moving from private helper details
toward behavior-level contracts as the Module deepens.

### 2026-05-04: Move query execution ownership behind the Module

Decision: add `NominalQueryExecution` in `pkg/plugin/query_execution.go` and
make `Datasource.QueryData` hand loaded settings and Grafana queries to it.

Why: after preparation was separated, the next useful Interface is the whole
query execution path: prepare, plan, execute, render, and return responses by
RefID. This keeps `Datasource` as the Grafana Adapter instead of the owner of
Nominal query behavior.

Evidence checked:

- Subagents agreed that query/model pairing, per-query errors, cache lifetime,
  partition-before-chunking, chunk-level failure isolation, and log sorting are
  the behavior that must not move accidentally.
- The implementation keeps service clients and TTL caches on `Datasource`, so
  cache lifetime across dashboard refreshes is preserved while channel
  inference is called through `NominalQueryExecution`.
- A planner test now checks that log and non-log partitioning preserves
  query/model pairs.
- `go test -count=1 ./pkg/...` passed after the change.

Rejected alternative: create a new subpackage now.

Rejected alternative: copy cache ownership into the Module. The Module owns the
behavior, but `Datasource` remains the lifetime owner for shared clients and
caches.

Remaining doubt: file organization can improve further. The Module owns the
behavior, but compute request planning and response transformation still live in
`datasource.go` as Module methods. Splitting them into `compute_request.go` and
`response_transform.go` is now mostly a file-locality cleanup, not a behavior
move.

## Investigation Log

### 2026-05-04: Query execution architecture review

Question: where should the backend query execution path deepen first?

Files read:

- `pkg/plugin/datasource.go`
- `pkg/plugin/datasource_test.go`
- `pkg/plugin/aggregation.go`
- `pkg/plugin/aggregation_test.go`
- `pkg/models/settings.go`

Subagent findings:

- Current behavior is too concentrated in `QueryData`.
- Existing tests are strong but some are coupled to raw serialized request JSON
  and exact batch-call counts.
- The best first Seam is prepared-query creation, followed by planning,
  execution, and rendering.
- A document should keep a validation ledger and require focused subagents for
  behavior-shaping architecture work.

Conclusion: proceed with a working log and future prepared-query design.

Confidence: high for the first move. Medium for the final file split.

### 2026-05-04: First prepared-query implementation slice

Question: can the first Seam be added without changing runtime behavior?

Files changed:

- `pkg/plugin/query_model.go`
- `pkg/plugin/datasource.go`
- `pkg/plugin/datasource_test.go`
- this working log

Subagent findings:

- Keep the helper private and inside `pkg/plugin`.
- Preserve the ordering of variable application, connection-test bypass,
  validation, channel type inference, and aggregation normalization.
- Treat channel type inference as a side-effecting part of preparation because
  it can call Nominal channel metadata and write the cache.
- Use fresh datasource instances in preparation tests unless the test is about
  cache reuse.

Conclusion: the first slice is in place. `Datasource.QueryData` now delegates
per-query preparation while retaining batching and response rendering in the
existing flow.

Validation:

```bash
go test ./pkg/...
```

Result: passed.

Confidence: high for behavior preservation in the backend path covered by the
current suite.

### 2026-05-04: Expanded module extraction before PR

Question: should this PR stop at prepared-query, or go through the full
requested migration sequence?

Files changed:

- `pkg/plugin/query_execution.go`
- `pkg/plugin/query_model.go`
- `pkg/plugin/datasource.go`
- `pkg/plugin/datasource_test.go`
- `pkg/plugin/query_execution_test_helpers_test.go`
- this working log

Subagent findings:

- One read recommended giving the remaining flow a single execution owner, then
  pulling planning, batching, compute request building, and rendering behind
  that owner.
- One read warned that moving every behavior at once is risky unless cache
  lifetime, per-query result mapping, partitioning, chunking, and log sorting
  stay explicitly protected.
- One read identified existing tests that protect request building, response
  rendering, channel inference/cache reuse, batching, chunking, and partial
  failures.

Conclusion: proceed with the expanded extraction, but keep cache storage and
service client lifetime on `Datasource`. The Module owns behavior. `Datasource`
owns plugin lifecycle resources.

Validation:

```bash
go test -count=1 ./pkg/...
```

Result: passed.

Confidence: high for behavior preservation in the backend path covered by the
current suite. The focused helper tests now call `NominalQueryExecution`
directly.

## Next Agent Handoff

Before changing code, the next agent should:

1. Re-read this document.
2. Re-read `Datasource.QueryData`.
3. Re-run `go test ./pkg/...`.
4. Re-read `NominalQueryExecution.Execute`.
5. Use focused subagents unless the change is tiny and local.

The next concrete implementation move is cleanup, not another behavior move:
reassess whether `compute_request.go` and `response_transform.go` are worth
splitting out as separate files.
