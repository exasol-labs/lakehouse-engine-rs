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

## ADR: Cleanup is anchored to a test function's stack, never to the OnceLock setup

**ID:** azure-e2e-cleanup-anchored-to-stack-not-onceLock
**Plan:** `add-azure-e2e-static-target`
**Status:** Accepted

### Context

The shared harness pattern parks cleanup-free setup (readiness waits,
Lakekeeper bootstrap, SLC install, `.so` upload, shared scripts) in a
`OnceLock`. The Azure fixture additionally owns a container, a warehouse, a
seeded table, a CONNECTION, and a Virtual Schema, all of which need
deterministic cleanup. Rust never drops values held in a `OnceLock` static at
process exit.

### Decision

Split provisioning in two: cleanup-free work stays in the shared `OnceLock`;
the container, warehouse, seed, CONNECTION, and Virtual Schema live on an
`AzureFixture` held as a local by the test that uses them, so unwinding (this
workspace sets no `panic = "abort"`) still runs `Drop`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fixture-with-guard held as a test-local stack value | ✓ Chosen — unwinding runs `Drop`; a panicking test still cleans up |
| Park the whole fixture, guard included, in the shared `OnceLock` | ✗ Rejected — a value inside a `OnceLock` static is never dropped, so the suite would look correct while leaking a container on every run |

### Consequences

Everything with nothing to clean up stays in the shared `OnceLock`; only the
container-owning fixture pays the per-test-local cost.

---

## ADR: Drop cannot await; teardown moves to its own thread and its own runtime

**ID:** azure-e2e-container-guard-own-thread-teardown
**Plan:** `add-azure-e2e-static-target`
**Status:** Accepted

### Context

The container guard's `Drop` must delete the container, but `Drop::drop(&mut
self)` is synchronous, and the guard is ordinarily dropped inside the
fixture's `rt.block_on(…)`. Driving the async delete via `Handle::block_on`
there panics with "Cannot start a runtime from within a runtime" — on the
panic path the guard is meant to cover, that is a panic while unwinding, which
aborts the process instead of cleaning up. A first repair moved only the
runtime to a spawned thread but kept the construction-time `BlobContainerClient`;
that client's pooled connection is bound to the runtime that opened it, so
reusing it dispatches the delete to a task on the fixture's runtime — which is
at that exact moment blocked in the guard's own `thread::join()` and polling
nothing, deadlocking the suite instead of cleaning up.

### Decision

`Drop` spawns a `std::thread`, builds a fresh `tokio::runtime::Runtime` inside
it, and **reconstructs** `ClientSecretCredential`, `BlobServiceClient`, and
`BlobContainerClient` inside that thread rather than reusing the
construction-time client, then `block_on`s the delete there and joins.
`AzureContainer` stores only plain owned data (account name, container name,
three Entra ID values) and holds no client and no runtime handle; the five
`String`s are cloned out of `self` into the thread, settling the `'static`
requirement `std::thread::spawn` needs against `Drop`'s `&mut self`. Failures
are reported through `eprintln!` and never panic from `Drop`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Own-thread, own-runtime teardown, rebuilding the client there | ✓ Chosen — behaves identically inside and outside a runtime context; costs one extra token acquisition |
| Spawn a thread and a fresh runtime, but reuse the construction-time client | ✗ Rejected — the client's pooled connection is bound to the fixture's runtime, which is blocked in `join()` at delete time; the delete never completes and the suite deadlocks |
| Drive the delete via `Handle::block_on` directly in `Drop` | ✗ Rejected — panics with "Cannot start a runtime from within a runtime"; fatal while unwinding |

### Consequences

Container teardown always succeeds or reports a non-fatal error, on both the
normal-return and panic-unwind paths, at the cost of rebuilding three cheap
client values per teardown.

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
