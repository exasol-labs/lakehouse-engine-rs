# Decisions: add-unity-e2e-ci-job

## ADR: Verify Unity Catalog continuously in a dedicated CI job

**ID:** verify-unity-catalog-continuously-dedicated-ci-job
**Plan:** `add-unity-e2e-ci-job`
**Status:** Accepted

### Context

The Unity Catalog E2E suite ran only on developer machines. Its stack is all local Docker containers, so continuous CI costs one runner and no external credentials.

### Decision

An always-on `e2e-unity` CI job runs the suite, mirrors `e2e-lakekeeper`, and gates `release` alongside `e2e` and `e2e-lakekeeper`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Opt-in only, as `cloud-e2e` | Rejected: the goal is continuous verification, and the Unity stack needs no credentials |
| Fold into the existing `e2e` job | Rejected: couples the baseline job to a large image pull, as with Lakekeeper |
| Add the job but leave `release` ungated, as `e2e-azure` | Rejected: `e2e-azure` depends on a live third-party account, but the Unity stack is local on a pinned image |

### Consequences

Unity interop is tested on every CI run at the cost of one extra runner for up to 45 minutes. A red Unity job blocks `release` on a `main` push but does not yet block a pull-request merge, which needs a repository-settings change tracked separately.
