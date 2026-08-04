# Decisions: add-azure-orphan-sweep-workflow

## ADR: Host the orphan sweep in this repository as a scheduled GitHub Actions workflow

**ID:** azure-orphan-sweep-in-repo-workflow
**Plan:** `add-azure-orphan-sweep-workflow`
**Status:** Accepted
**Supersedes:** azure-e2e-orphan-sweep-out-of-band

### Context

The `azure-e2e` suite's per-run container is deleted by a `Drop` guard that runs
on normal return and on panic unwind but never on `SIGKILL`, CI cancellation, or
OOM, so a killed run orphans its container permanently. Azure Blob
lifecycle-management policies act on blobs, not containers, so no lifecycle rule
can reclaim an orphan — the finding ADR `azure-e2e-orphan-sweep-out-of-band`
already recorded. That ADR left the mitigation as an out-of-band sweep "owned
outside this repository". Issue #291 revisited that placement.

### Decision

Add `.github/workflows/azure-orphan-sweep.yml`, a scheduled (`0 2 * * 1`) plus
`workflow_dispatch` workflow that reclaims stale `lhrs-e2e-` containers whose
`last_modified` is older than 24 hours, authenticating with the existing Entra ID
service principal and never the account key. This supersedes
`azure-e2e-orphan-sweep-out-of-band`'s placement of the mitigation as tooling
owned outside this repository; that ADR's finding — a storage-account lifecycle
rule cannot reclaim a container — stands unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| In-repo scheduled GitHub Actions workflow | ✓ Chosen — lives beside the suite that leaks the containers, versioned and reviewed in the same repo; no separate Azure resource to provision or own |
| Azure Function (timer trigger) | ✗ Rejected — a separate cloud resource to provision and own for a low-frequency cleanup |
| Keep the mitigation owned outside the repository (prior ADR wording) | ✗ Rejected — issue #291 asked exactly this placement question and this plan answers it |

### Consequences

The mitigation is now versioned and reviewable in this repository. The
still-valid finding that a lifecycle rule cannot target a container carries
forward unchanged from the superseded ADR.
