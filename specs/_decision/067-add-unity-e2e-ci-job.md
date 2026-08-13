# Decisions: add-unity-e2e-ci-job

## ADR: Verify Unity Catalog continuously in a dedicated CI job

**ID:** verify-unity-catalog-continuously-dedicated-ci-job
**Plan:** `add-unity-e2e-ci-job`
**Status:** Accepted

### Context

The Unity Catalog E2E suite shipped in #327 ran only on a developer machine. MinIO, Exasol, and
Unity Catalog OSS all run as local Docker containers, so continuous CI verification costs one
runner and no external dependency — unlike `cloud-e2e`, which needs real AWS credentials and stays
opt-in-only. Without a dedicated job, a regression in UC-native `createVirtualSchema` listing or
column mapping could reach `main` unnoticed.

### Decision

Add an always-on `e2e-unity` job mirroring `e2e-lakekeeper` — `needs: [build-so]`,
`runs-on: ubuntu-latest`, `timeout-minutes: 45`, `LH_EXASOL_CPUS: "2"`, the shared
`./.github/actions/e2e-setup` composite, a phased bring-up with an exit-code-checked `minio-init`,
`scripts/unity/seed.sh` as its own step, failure log dumping, artifact upload, and `down -v`
cleanup — and gate `release` on it alongside `e2e` and `e2e-lakekeeper`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Dedicated always-on CI job `e2e-unity`, gating `release` | ✓ Chosen — the stack is all local containers, so continuous verification catches regressions; gating `release` matches Lakekeeper and makes the verification load-bearing rather than advisory |
| Opt-in only, never in CI, as `cloud-e2e` is | ✗ Rejected — the whole point of #328 is continuous verification, and unlike `cloud-e2e` the Unity stack needs no credentials |
| Fold the suite into the existing `e2e` job | ✗ Rejected — couples the baseline job's runtime and stability to a ~5.7 GB image pull, the same reasoning that gave Lakekeeper its own job |
| Add the job but leave `release` ungated, as `e2e-azure` is | ✗ Rejected — `e2e-azure`'s exclusion turns on a live third-party account; Unity's stack is entirely local containers on a pinned image (`unitycatalog/unitycatalog:v0.5.0`), so no external party can block a release |

### Consequences

Unity Catalog interop is regression-tested on every CI run, at the cost of one additional
`ubuntu-latest` runner for up to 45 minutes, including a ~5.7 GB image pull. A red `E2E (Unity)`
now blocks the `release` job on a `main` push. It does not yet block a pull-request merge: adding
`E2E (Unity)` to `main`'s required status checks is a repository-settings change tracked as a
separate follow-up, not a file change this plan could make.
