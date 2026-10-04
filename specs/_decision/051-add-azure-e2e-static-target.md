# Decisions: add-azure-e2e-static-target

## ADR: Container lifecycle runs on the official crate under an Entra ID service principal

**ID:** azure-e2e-container-lifecycle-entra-id-service-principal
**Plan:** `add-azure-e2e-static-target`
**Status:** Accepted

### Context

The harness needs to create and delete a per-run Azure blob container. The
official `azure_storage_blob` 1.0 line authenticates only with Entra ID, while
the legacy `azure_storage_blobs` 0.21 line supports account-key auth but is
published from the `legacy` branch under an explicit "no longer under active
development" notice. A single long-lived container with a per-run `key-prefix`
over the already-present `object_store` was also considered.

### Decision

Take `azure_storage_blob` 1.0.0, `azure_identity` 1.0.0, and `azure_core` 1.1.0
as dev-dependencies, and authenticate container create and delete with an
Entra ID service principal via `ClientSecretCredential`. Default features
resolve to reqwest 0.13 + rustls and tokio `^1.49`, all already in the
lockfile, so no new major version or second TLS stack enters the graph.

### Options Considered

| Option | Verdict |
|--------|---------|
| Official `azure_storage_blob` 1.0 + Entra ID service principal | ✓ Chosen — maintained, official dependency; verified surface (`create`/`delete`, `TokenCredential`, `StorageErrorCode::{ContainerAlreadyExists,ContainerNotFound}`) is complete |
| Legacy `azure_storage_blobs` 0.21 (account-key auth) | ✗ Rejected — unmaintained line, explicit "no longer under active development" notice |
| Single long-lived container with a per-run `key-prefix` over `object_store` | ✗ Rejected by the user together with the legacy crate |
| Hand-rolled Shared Key REST signing over `reqwest` | ✗ Rejected in the first interview |

### Consequences

Three extra CI secrets and a second credential concept inside the harness,
alongside the account-key path under test.

---

## ADR: The harness's credential and the credential under test are kept strictly apart

**ID:** azure-e2e-credential-segregation-by-purpose
**Plan:** `add-azure-e2e-static-target`
**Status:** Accepted

### Context

The harness now holds two credentials: an Entra ID service principal for
container lifecycle, and the account key that is the actual subject of this
slice (`AdlsCred::AccountKey`). Letting the service principal reach the
Lakekeeper warehouse storage credential, the seed `FileIO`, or the Exasol
CONNECTION would let the suite pass without exercising the account-key path
slice C shipped.

### Decision

Only the container guard uses the service principal. The Lakekeeper warehouse
storage credential, the seed `FileIO`, and the Exasol CONNECTION the scan
reads through all carry the account key; the CONNECTION carries no Entra ID
field, asserted normatively in the spec's scan scenario.

### Options Considered

| Option | Verdict |
|--------|---------|
| Segregate by purpose: service principal only for container lifecycle | ✓ Chosen — a correctness property of the test, not a hygiene preference |
| Let the service principal serve both purposes | ✗ Rejected — collapses five variables to three but would let a green run prove the harness works while exercising nothing slice C shipped |

### Consequences

Five credential variables instead of three, split across two purposes and
stated as a normative separation in the spec rather than left to the
implementation.

---

## ADR: The issue's proposed orphan sweep is not implementable as written

**ID:** azure-e2e-orphan-sweep-out-of-band
**Plan:** `add-azure-e2e-static-target`
**Status:** Superseded by azure-orphan-sweep-in-repo-workflow

### Context

The container guard's cleanup depends on unwinding; a killed process leaves
its container behind. The originating issue proposed sweeping orphans via an
Azure Storage account lifecycle-management rule. Azure Blob lifecycle-management
policies act on blobs, blob versions, and snapshots — they cannot delete a
container.

### Decision

Record the known ceiling as requiring an out-of-band scheduled sweep (Azure
CLI or a Function) owned outside this repository, not a storage-account
lifecycle rule. The `lhrs-e2e-<user>-<millis>` container name keeps an orphan
attributable to this suite, to a user, and to one run. Tracked as a follow-up
issue (#291).

### Options Considered

| Option | Verdict |
|--------|---------|
| Document an out-of-band scheduled sweep as the real mitigation | ✓ Chosen — states a mitigation that actually exists |
| State the ceiling as the issue does (an account lifecycle rule) | ✗ Rejected — lifecycle-management policies cannot target containers; this would record a mitigation that does not exist |

### Consequences

Orphan removal needs tooling outside this repository. The account already
holds leftovers from earlier spike runs, so the sweep is a real operational
need, not a hypothetical.

---

## ADR: The CI job runs but does not gate releases, and does not run on fork pull requests

**ID:** azure-e2e-ci-job-non-release-gating
**Plan:** `add-azure-e2e-static-target`
**Status:** Accepted

### Context

The suite depends on a live third-party Azure account and, by its fail-loud
contract, cannot skip. `E2E (Lakekeeper)` and `E2E` both gate `release`. A fork
pull request cannot read the account-key repository secret.

### Decision

Add `E2E (Azure)` mirroring `E2E (Lakekeeper)`'s bring-up, log dumping, and
teardown, guarded to the same repository so it does not schedule on fork pull
requests, and leave it out of `release`'s `needs`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Run the job, but exclude it from the release gate and from fork PRs | ✓ Chosen — accepted cost: an Azure regression can reach a release, mitigated by the job still running and failing visibly on every `main` push |
| Gate releases on it, as `e2e` and `e2e-lakekeeper` are | ✗ Rejected — an Azure incident or a rotated secret would block every release, and the suite has no way to degrade |
| Run it on every pull request, forks included | ✗ Rejected — a fork PR cannot read the account-key secret, so the job would fail-loud on every external contribution |

### Consequences

An Azure regression can reach a release undetected by the gate; visibility on
`main` pushes is the accepted mitigation.
