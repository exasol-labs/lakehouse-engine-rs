# Decisions: azure-e2e-ci-scope-simplification

## ADR: Drop the fork-coverage goal for the Azure E2E job

**ID:** azure-e2e-ci-no-fork-coverage-goal
**Plan:** azure-e2e-ci-scope-simplification
**Status:** Accepted
**Supersedes:** azure-e2e-ci-job-non-release-gating

### Context

Issue #277 specified exactly one CI addition: an `E2E (Azure)` job reading
five credential variables from a repository secret. The implementation
additionally invented a same-repository `if:` guard on `e2e-azure` and an
`azure_offline_` test-naming convention with a count-guard step inside
`unit-tests`, re-running nine pure tests there so fork PRs would still see
coverage. A maintainer review comment on PR #292 flagged both as unscoped:
#277 says nothing about forks, and the two existing E2E jobs (`e2e`,
`e2e-lakekeeper`) carry neither a fork guard nor a mirrored offline-subset
step.

### Decision

Remove the `unit-tests` job's Azure offline-checks step, the
`azure_offline_` naming convention and its count guard, and `e2e-azure`'s
same-repository `if:` guard. The nine pure tests keep running, unprefixed,
inside `e2e_azure_test` via `make test-e2e-azure` in the `e2e-azure` job —
the same pattern `lakekeeper_*` and seed-catalog pure tests already use.
`e2e-azure` schedules on the same events as `e2e` and `e2e-lakekeeper`, forks
included; a draft PR is still excluded, unchanged, via the cascade-skip
through `needs: [build-so]`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drop the fork-coverage goal: one job, no guard, no naming convention | ✓ Chosen — restores consistency with `e2e` and `e2e-lakekeeper`; matches #277's actual scope |
| Keep the guard, drop only the `unit-tests` offline-checks step | ✗ Rejected as a half-measure — the review comment's rationale (#277 says nothing about forks) applies equally to the job-scheduling guard |
| Keep both as-is | ✗ Rejected — that is the state being changed |
| Move the pure helpers out of the E2E-gated `common/` tree for genuine fork-visible coverage without an offline filter | ✗ Rejected for this plan — a larger structural change flagged as a separate follow-up, not folded into a scope-reduction plan |

### Consequences

A non-draft fork PR now schedules `E2E (Azure)` and sees it fail loudly,
naming the missing credential variable, rather than being silently skipped.
Verified
via the repository's branch-protection ruleset that `E2E (Azure)` is not a
required status check, so this does not block merge. `unit-tests` no longer
compiles the `azure-e2e` feature or its Azure SDK dev-dependencies on every
PR.
