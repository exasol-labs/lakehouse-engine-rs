# Decisions: add-azure-orphan-sweep-workflow

## ADR: Host the orphan sweep in this repository as a scheduled GitHub Actions workflow

**ID:** azure-orphan-sweep-in-repo-workflow
**Plan:** `add-azure-orphan-sweep-workflow`
**Status:** Accepted
**Supersedes:** azure-e2e-orphan-sweep-out-of-band

### Context

The Azure E2E container guard does not run on `SIGKILL`, CI cancellation, or OOM, so a killed run orphans its container. Azure lifecycle-management policies cannot delete containers. The superseded ADR left the sweep owned outside this repository, and issue #291 revisited that placement.

### Decision

A scheduled (weekly) and manually dispatchable GitHub Actions workflow in this repository deletes `lhrs-e2e-` containers last modified more than 24 hours ago. It authenticates with the existing Entra ID service principal, never the account key. The superseded ADR's finding stands: a lifecycle rule cannot reclaim a container.

### Options Considered

| Option | Verdict |
|--------|---------|
| Azure Function with a timer trigger | Rejected: a separate cloud resource to provision and own for a low-frequency cleanup |
| Keep the sweep owned outside the repository | Rejected: issue #291 asked for exactly this placement and this plan answers it |

### Consequences

The mitigation is versioned and reviewed beside the suite that leaks the containers.
