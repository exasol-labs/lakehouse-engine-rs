# Decisions: azure-e2e-ci-scope-simplification

## ADR: Drop the fork-coverage goal for the Azure E2E job

**ID:** azure-e2e-ci-no-fork-coverage-goal
**Plan:** azure-e2e-ci-scope-simplification
**Status:** Accepted
**Supersedes:** azure-e2e-ci-job-non-release-gating

### Context

Issue #277 asked for one `E2E (Azure)` CI job and said nothing about forks. A same-repository guard on the job and an offline-test step in `unit-tests` were added beyond that scope, and the two existing E2E jobs have neither (maintainer review on PR #292).

### Decision

The Azure E2E job has no same-repository guard, and `unit-tests` has no Azure offline-checks step, naming convention, or count guard. The nine pure tests run unprefixed inside the Azure E2E test target, as the Lakekeeper pure tests do. The job schedules on the same events as `e2e` and `e2e-lakekeeper`, forks included, and draft PRs stay excluded through the `build-so` dependency.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the guard and drop only the offline-checks step | Rejected: the review rationale applies equally to the guard |
| Move the pure helpers out of the E2E-gated tree for fork-visible coverage | Rejected for this plan: a larger structural change, flagged as a separate follow-up |

### Consequences

A non-draft fork PR schedules `E2E (Azure)` and sees it fail loudly, naming the missing credential variable. It is not a required status check per the branch-protection ruleset, so merge is not blocked. `unit-tests` no longer compiles the `azure-e2e` feature or its SDK dev-dependencies.
