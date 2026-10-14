# Decisions: add-azure-e2e-static-target

## ADR: The issue's proposed orphan sweep is not implementable as written

**ID:** azure-e2e-orphan-sweep-out-of-band
**Plan:** `add-azure-e2e-static-target`
**Status:** Superseded by azure-orphan-sweep-in-repo-workflow

### Context

A killed process leaves its container behind. The originating issue proposed an Azure Storage lifecycle-management rule, but those policies act on blobs, versions, and snapshots and cannot delete a container.

### Decision

Orphan cleanup requires an out-of-band scheduled sweep (Azure CLI or a Function) owned outside this repository. The `lhrs-e2e-<user>-<millis>` container name keeps an orphan attributable to this suite, a user, and a run. Issue #291 tracks it.

### Options Considered

| Option | Verdict |
|--------|---------|
| A storage-account lifecycle rule, as the issue states | Rejected: such rules cannot target containers, so the mitigation does not exist |

### Consequences

Orphan removal needs tooling outside this repository. The account already holds leftovers from earlier spike runs.

## ADR: The CI job runs but does not gate releases, and does not run on fork pull requests

**ID:** azure-e2e-ci-job-non-release-gating
**Plan:** `add-azure-e2e-static-target`
**Status:** Accepted

### Context

The suite depends on a live third-party Azure account and cannot skip by its fail-loud contract. `E2E (Lakekeeper)` and `E2E` gate `release`. A fork pull request cannot read the account-key secret.

### Decision

`E2E (Azure)` mirrors `E2E (Lakekeeper)`'s bring-up, log dumping, and teardown, is guarded to the same repository so it skips fork pull requests, and is left out of the `release` dependencies.

### Options Considered

| Option | Verdict |
|--------|---------|
| Gate releases on it | Rejected: an Azure incident or rotated secret would block every release, and the suite cannot degrade |
| Run it on every pull request, forks included | Rejected: a fork PR cannot read the secret, so the job would fail on every external contribution |

### Consequences

An Azure regression can reach a release past the gate. The accepted mitigation is that the job still fails visibly on every `main` push.
