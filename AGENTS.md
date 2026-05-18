# Repository Agent Instructions

This file is the source of truth for agent behavior in this repository.

## Communication

- Report results in plain, clear English.
- Keep implementation details out of final user summaries unless they are needed to explain risk, testing, or a decision.
- Before reporting back, verify the work whenever practical and say exactly what was checked.

## Definition Of Done

Before considering a task complete:

- Understand the requested outcome and the relevant branch or PR context.
- Inspect the actual diff before deciding what tests are enough.
- Keep the change scoped to the request.
- Run the most relevant checks available locally.
- Fix failures that are caused by the change, then re-run the affected checks.
- Separate real product failures from local setup, credential, sandbox, or network blockers.
- Leave a clear note about any untested behavior or remaining weak points.

## Testing Story For PRs

Every PR should include a concrete testing story. The PR description or handoff should state:

- What changed.
- Why the change was made.
- What user-facing behavior changed, if any.
- Which automated tests were run and whether they passed.
- Which manual or live checks were run and what they proved.
- Which checks were intentionally skipped and why.
- Any residual risk or follow-up work.

Do not describe a PR as verified only because code was written. Verification should come from commands, fixtures, mocks, local runs, browser checks, or live integration checks that match the change.

## Choosing Tests

Use the diff to choose tests:

- Backend Go changes: run focused package tests first, then broader `go test -count=1 ./pkg/...` when practical.
- Frontend changes: run the relevant typecheck, unit tests, build, and browser verification for the touched surface.
- Documentation-only changes: run lightweight validation such as `git diff --check`; run broader tests only when the docs change executable examples or test instructions.
- Cross-cutting changes: include both focused regression tests and a broader package or project check.
- PR review follow-ups: add or update tests when the comment reveals a behavior gap, not only when it asks for a test directly.

Prefer behavior-level tests over tests that merely lock in private implementation details.

## Nominal API And Live Integration Tests

Live Nominal API checks should be opt-in so normal local and CI runs do not require credentials.

Use live tests when the change affects real request construction, response parsing, query execution, or authentication behavior and credentials are available. If credentials are not available, make sure the live test skips explicitly and report that the live path was not exercised.

Do not commit Nominal API keys or print them in logs, shell output, PR bodies, or final summaries. Prefer explicit opt-in environment variables over implicit secret discovery.

For writeful query integration coverage, prefer self-provisioned test data over known staging fixtures. Create temporary assets, data scopes, datasets, and channels inside the test, assert against data the test ingested, and archive temporary resources during cleanup. Known existing assets are acceptable for manual diagnostics, but they should not be the default proof for PR verification.

Known local credential sources may exist in or near this worktree:

- `./.env` can provide `NOMINAL_API_KEY`.
- `~/.config/nominal/config.yml` can provide Nominal client profiles.

These sources are not automatically correct for every API environment. Pair the key with the matching base URL before treating a live test failure as a product failure. For the local Grafana plugin `.env` key, the verified staging base URL is:

```sh
NOMINAL_BASE_URL=https://api-staging.gov.nominal.io/api
```

The default backend base URL is production GovCloud. If a local key fails there with an authentication error, retry against the intended staging URL before concluding the key is bad.

For query execution work, the testing story should cover the relevant parts of the flow:

- Query preparation and validation.
- Compute request planning.
- Response rendering into Grafana frames.
- Channel type inference and caching.
- Batch, chunk, and parallel execution behavior.
- Per-query errors.
- Extra or missing compute results.
- Log, string, numeric, and other supported response shapes.

## PR Creation Process

Before opening or updating a PR:

- Confirm the branch and base branch.
- Check `git status --short`.
- Review the diff for unrelated changes.
- Run the selected checks.
- Commit with a conventional commit message.
- Push the branch.
- Create or update the PR description with the testing story and known weak points.

If a PR is stacked on another branch, make that relationship explicit in the PR body.
