# Decisions: azure-e2e-ci-scope-simplification

## ADR: Drop the fork-coverage goal for the Azure E2E job

**ID:** azure-e2e-ci-no-fork-coverage-goal
**Plan:** azure-e2e-ci-scope-simplification
**Status:** Accepted

### Context

The Azure E2E CI job needs no fork-specific handling, and the two existing E2E jobs have none. A same-repository guard and an offline-test step in `unit-tests` would exceed the job's scope.

### Decision

The Azure E2E job has no same-repository guard, and `unit-tests` has no Azure offline-checks step. The pure tests run inside the Azure E2E test target, as the Lakekeeper pure tests do. The job runs on the same events as the other E2E jobs, forks included, and draft PRs stay excluded.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the guard and drop only the offline-checks step | Rejected: the same rationale applies to the guard |
| Move the pure helpers out of the E2E-gated tree for fork-visible coverage | Rejected: a larger structural change |

### Consequences

- A non-draft fork PR runs `E2E (Azure)` and fails loudly, naming the missing credential variable. The job is not a required check, so merge is not blocked.
- `unit-tests` does not compile the Azure feature or its SDK dependencies.
