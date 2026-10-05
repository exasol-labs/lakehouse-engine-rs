# Decisions: add-azure-e2e-static-target

## ADR: Container lifecycle runs on the official crate under an Entra ID service principal

**ID:** azure-e2e-container-lifecycle-entra-id-service-principal
**Plan:** `add-azure-e2e-static-target`
**Status:** Accepted

### Context

The harness creates and deletes a per-run Azure blob container. The official `azure_storage_blob` crate authenticates only with Entra ID. The legacy `azure_storage_blobs` crate supports account-key auth but is marked "no longer under active development".

### Decision

The harness uses the official `azure_storage_blob`, `azure_identity`, and `azure_core` crates as dev-dependencies. Container create and delete authenticate with an Entra ID service principal. These resolve to reqwest and rustls versions already in the lockfile, so no new major version or second TLS stack enters.

### Options Considered

| Option | Verdict |
|--------|---------|
| Legacy `azure_storage_blobs` with account-key auth | Rejected: unmaintained |
| One long-lived container with a per-run key prefix over `object_store` | Rejected by the user together with the legacy crate |
| Hand-rolled Shared Key REST signing over `reqwest` | Rejected in the first interview |

### Consequences

The harness needs three extra CI secrets and a second credential concept next to the account-key path under test.

## ADR: The harness's credential and the credential under test are kept strictly apart

**ID:** azure-e2e-credential-segregation-by-purpose
**Plan:** `add-azure-e2e-static-target`
**Status:** Accepted

### Context

The harness holds an Entra ID service principal for container lifecycle and the account key that is the subject of this slice. If the service principal reached the Lakekeeper warehouse credential, the seed `FileIO`, or the Exasol CONNECTION, the suite could pass without exercising the account-key path.

### Decision

Only the container guard uses the service principal. The Lakekeeper warehouse credential, the seed `FileIO`, and the Exasol CONNECTION carry the account key, and the CONNECTION carries no Entra ID field, as the spec's scan scenario asserts.

### Options Considered

| Option | Verdict |
|--------|---------|
| Let the service principal serve both purposes | Rejected: needs fewer variables (three instead of five) but a green run would prove the harness works while exercising nothing this slice shipped |

### Consequences

The suite uses five credential variables across two purposes, and the spec states the separation as a normative rule.

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
