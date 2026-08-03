# Plan: add-azure-e2e-static-target

## Summary

Stand up `make test-e2e-azure` as the single home for every Azure E2E case —
test-only, no production crate changes (issue #277, slice E of 6). Fill it with the
static-credential path: a per-run container, an `adls` warehouse with
`sas-enabled: false`, and an `abfss://` scan on the CONNECTION account key.

## Design

### Context

Slice C already shipped the production Azure read path on this branch: the `Adls`
storage backend, its CONNECTION shape, and the `MicrosoftAzureBuilder` object-store
registration. Nothing has run it against real Azure. This slice supplies that proof.

Azure forces three structural departures from every existing E2E suite:

1. **Storage cannot be local.** Azurite's `dfs` endpoint is incomplete, and
   Lakekeeper v0.13.1's `adls` profile builds `https://<account-name>.<host>` from a
   bare hostname, so an Azurite endpoint with a port cannot be expressed through the
   profile at all. Storage is real cloud; catalog and Exasol stay local Docker.
2. **The storage container has a lifecycle.** MinIO's warehouse bucket is created
   once by a compose init job and lives forever. Lakekeeper creates no ADLS
   filesystem and validates physical access at warehouse-creation time, so the
   harness must create a container first — and must delete it, or the shared account
   accumulates garbage.
3. **Managing that container needs a second, different credential.** The official
   Azure blob crate authenticates only with Entra ID, so container create and delete
   run under a service principal. The data path under test still uses the account
   key in the CONNECTION — that is the whole point of the slice. The two credentials
   serve different purposes and must not leak into each other.

- **Goals** — one Make target that both credential modes will live under; a live
  Azure scan proving slice C's account-key path; a container lifecycle that cleans up
  on success and on test failure; credentials that reach the suite identically from a
  developer laptop and from CI, with the harness's own credential kept out of the
  path under test.
- **Non-Goals** — the vended/SAS-delegated case (slice F, same target and stack);
  configuring anything inside the Azure account (an out-of-band operational task);
  consolidating `seed.rs`'s per-table writers (#169, deferred — this slice targets
  the current structure); any change to production crates.

### Decision

Extend the three shared harness seams the MinIO Lakekeeper suite already owns, and
add exactly one new harness module for what is genuinely Azure-specific.

#### Architecture

```
tests/common — THREE gating files, not one:
  │  mod.rs ······················ crate-level #![cfg(any(exasol-e2e, cloud-e2e,
  │                                lakekeeper-e2e))] plus per-module #[cfg]s.
  │  stack.rs ····················· its own file-level #![cfg(any(exasol-e2e,
  │                                cloud-e2e, lakekeeper-e2e))] plus item-level
  │                                #[cfg]s on lakehouse_engine_so_path and
  │                                local_stack_connection_password. mod.rs declares
  │                                `pub mod stack;` ungated, so the file-level
  │                                attribute is the ONLY thing admitting this module.
  │  lakekeeper.rs ················ its own file-level #![cfg(feature =
  │                                "lakekeeper-e2e")], on top of mod.rs's gate.
  │  `azure-e2e` must join every one of them, or `common` compiles to nothing
  │  under the new Make target.
  ▼
tests/e2e_azure_test.rs
  │  OnceLock: wait_for_* → lakekeeper_bootstrap → install_slc → upload_so → scripts
  │  (shared, idempotent, nothing to clean up)
  │
  └─ AzureFixture (per test, owns Drop)
       ├─ common/azure.rs ······· 5 credential vars, container name,
       │                          AzureContainer guard ──▶ Entra ID service principal
       ├─ common/lakekeeper.rs ·· ADLS warehouse body ─┐  (account key)
       │                          MinIO warehouse body ─┼─▶ one private post_warehouse()
       │                          Azure CONNECTION password (account key)
       ├─ common/seed.rs ········ seed-catalog config carries a storage backend
       │                          (Default = static MinIO) → Azdls FileIO (account key)
       └─ common/stack.rs ······· CatalogConnectionPassword gains account_name/account_key
```

Only the container guard authenticates with the service principal. Every other arrow
— warehouse storage credential, seed `FileIO`, and the CONNECTION the scan reads
through — carries the account key, so the credential path under test is exactly
slice C's.

Cleanup is anchored to a value on a test function's stack, not to the `OnceLock`
setup. A guard parked in a `OnceLock` static would never drop — statics are not
dropped at process exit — so the container lifecycle has to sit with the test that
owns it. Everything with nothing to clean up stays in the shared `OnceLock`.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| RAII guard | `common/azure.rs` `AzureContainer` | Unwinding runs `Drop`, so a panicking test still deletes its container; this workspace sets no `panic = "abort"` |
| Own-thread, own-runtime teardown | `AzureContainer::drop` | `Drop` is synchronous and ordinarily fires inside the fixture's `rt.block_on(…)`, where `Runtime::block_on` panics with "Cannot start a runtime from within a runtime" — a panic while unwinding, which aborts the process instead of cleaning up. The guard therefore holds no runtime handle: it spawns a `std::thread`, builds a fresh runtime there, and joins, so teardown behaves identically inside and outside a runtime context — **and holds no client either. Both the runtime and the HTTP stack are built inside the teardown thread, because a client's pooled connection is bound to the runtime that opened it**: reusing the construction-time `BlobContainerClient` would hand the delete to a task on the fixture's runtime, which is at that moment blocked in the guard's own `join()` and polling nothing, so the delete would never complete and the suite would deadlock instead of cleaning up |
| Credential segregation by purpose | `common/azure.rs` vs the other three seams | The service principal exists only because the official crate cannot use an account key; letting it reach the scan would make the suite pass without ever exercising `AdlsCred::AccountKey` |
| Shared body, thin per-storage callers | `common/lakekeeper.rs` `post_warehouse` | Keeps one owner of the endpoint, the 400-`StorageProfileOverlap` idempotency rule, and the never-echo-the-response-body contract, instead of copying all three into an Azure fork |
| Defaulted configuration value | `common/seed.rs` seed-catalog config | Adding a storage backend beside the catalog-auth token leaves the seed table-create-write-commit path single-owner; `Default` reproduces the static-MinIO baseline exactly |
| Absent-means-unset | Azure CONNECTION password | `validate_creds` reads any non-empty S3 field beside an Azure field as an ambiguous credential set, so the Azure password must leave `endpoint`/`region`/`access_key`/`secret_key` empty |

Design-philosophy check on the one new module: `common/azure.rs` is responsible for
everything needed to reach the test Azure account — the five credential variables,
the per-run container name, and the container's create/delete lifecycle. Calling it
is materially easier than reimplementing it (the guard hides client construction,
tolerated HTTP error codes, and unwind safety). Container naming has exactly one
owner there; the ADLS warehouse body has exactly one owner in `lakekeeper.rs`; the
seed storage choice has exactly one owner in `seed.rs`. Nothing in production
depends on any of it. The tactical shortcut it carries is named in the spec's known
ceiling: cleanup depends on unwinding, so a killed process still orphans a container.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| The official `azure_storage_blob` 1.0 + `azure_identity` 1.0, authenticating the container lifecycle with an Entra ID service principal | The legacy `azure_storage_blobs` 0.21 (account-key auth, one credential, but an unmaintained unofficial line); a single long-lived container with a per-run `key-prefix` over the already-present `object_store`; hand-rolled Shared Key REST signing | Chosen by the user after both alternatives were put to them. Buys a maintained, official dependency and a narrowly-scoped RBAC identity; costs three extra secrets and a second credential concept in the harness |
| Read the three Entra ID variables explicitly and construct `ClientSecretCredential` | `DefaultAzureCredential`, which historically scanned `AZURE_*` from the environment | `DefaultAzureCredential` was removed in `azure_identity` 0.28.0 and replaced by `DeveloperToolsCredential`, which tries only the Azure CLI and Developer CLI. `azure_identity` 1.x has no environment-scanning credential at all, so nothing picks those variables up implicitly — the suite must read them and fail loudly on an absent one |
| One container per run, shared by all Azure tests in the run | One container per test | Halves the live-Azure cost and halves the orphan surface; `--test-threads=1` removes any concurrency argument for isolation |
| Per-run suffix on the warehouse name as well as the container | Fixed warehouse name with idempotent creation | A fixed name survives locally after its container is deleted, and the existing already-exists handler would silently bind the next run to a warehouse whose storage is gone |
| CI job not added to `release`'s `needs` | Gate releases on it | A live third-party account in the release gate turns an Azure incident or a rotated secret into a blocked release, and the suite's fail-loud contract gives it no way to degrade. Accepted cost: an Azure regression can reach a release, mitigated by the job still running and failing visibly on every `main` push |
| Same-repository guard on the CI job | Run on every pull request | A fork pull request cannot read the account-key secret, so the job would fail-loud on every external contribution. Not scheduling a job is not the suite skipping |
| Add `account_name`/`account_key` only | Add `sas_token` now for slice F | An unused field is dead code today; slice F adds it with its first user |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| azure-e2e-harness | NEW | `e2e-harness/azure-e2e-harness/spec.md` |

## Impact

Adds a test target. No product-code change, no CONNECTION or Virtual Schema surface
change, nothing existing behaves differently.

Four operator-facing additions. Running the Azure suite locally requires five values
in a gitignored `test.env`, split by purpose:

- **Data path (under test):** `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`
- **Container lifecycle (harness only):** `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`

CI needs the same five configured on the repository — the account name as a variable,
the other four as secrets — before the new job can pass. The service principal must
hold **Storage Blob Data Contributor** on the test storage account; a narrower
blob-data-only role cannot create or delete containers. The account itself must be
StorageV2 with **hierarchical namespace enabled**, which Lakekeeper requires for ADLS
warehouses and which cannot be turned on reversibly.

`AZURE_STORAGE_ACCOUNT_KEY` deserves separate note: unlike the service principal it
cannot be scoped to one container or rotated per consumer, and it grants full
data-plane control over every container in the account. The slice requires it — it is
the credential under test — and it will live as a repository secret readable by
workflow runs. Point the five variables at a dedicated test-only storage account
holding no other data.

The suite consumes real Azure storage on every run and leaves a container behind when
a run is killed rather than failed — see the spec's known ceiling and its follow-up
issue.

## Dependencies

| Dependency | Kind | Note |
|------------|------|------|
| #275 (slice C — static Azure read path) | Prerequisite | Implemented on this branch (`feat/add-azure-static-storage-backend`); the PR is still open |
| `azure_storage_blob` 1.0.0, `azure_identity` 1.0.0, `azure_core` 1.1.0 | New dev-dependencies | Official azure-sdk-for-rust 1.x line, dev-only, never linked into the `.so`. Default features are correct: they resolve to reqwest 0.13 + rustls and tokio `^1.49`, all of which the lockfile already carries (reqwest 0.13.4, rustls 0.23.40, tokio 1.52.3), so no new major version or second TLS stack enters the graph. `azure_core` is needed directly for `credentials::Secret` and `http::Url` |
| An Entra ID service principal with **Storage Blob Data Contributor** on the test storage account | External service identity | Container create and delete are the `containers/write` and `containers/delete` actions that role carries. A custom role holding only the `containers/blobs/*` data actions cannot manage containers and returns 403; `Storage Blob Data Owner` and account-level `Contributor` are both over-privileged |
| `iceberg-storage-opendal` feature `opendal-azdls` | Existing dependency, new feature | Reaches `lakehouse-engine`'s tests today only through Cargo feature unification via `lakehouse-catalog`; declare it explicitly on the dev-dependency rather than relying on another crate's choice |
| Lakekeeper v0.13.1 (`quay.io/lakekeeper/catalog:v0.13.1`) | Existing stack service | `adls` profile and `az`/`shared-access-key` credential shapes are pinned to this tag |
| A **StorageV2 account with hierarchical namespace enabled**, plus its account key | External service | Lakekeeper v0.13.1 requires HNS for ADLS warehouses (`docs/docs/storage.md`) and its ADLS backend has no non-HNS fallback. Enabling HNS on an existing account is a **one-way** upgrade with prerequisites (no page blobs; blob and container soft delete, snapshots, encryption scopes, and immutable storage all disabled first), so a non-HNS account blocks task 4.2 behind an Azure admin action with real lead time. Verify with `az storage account show -n <account> --query isHnsEnabled` before starting task 4.2 |
| Role assignment on the existing `APP_EXA_RND_LAKEKEEPER_DEV` service principal | Precondition to verify | The principal and a filled-in local `test.env` already exist. What is unverified is whether it actually holds Storage Blob Data Contributor on the test account: `az role assignment list --assignee <client-id> --scope <account-id>` |
| Four repository secrets and one repository variable | Precondition to verify | `AZURE_STORAGE_ACCOUNT_NAME` as a variable; `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` as secrets. Task 4.1's job cannot pass until all five exist |
| #169 (`seed.rs` consolidation) | Explicitly deferred | This slice targets the current `seed.rs` structure |

## Implementation Tasks

### 1. Target, gating, and pure helpers

- [ ] 1.1 Declare the `azure-e2e` cargo feature in `crates/lakehouse-engine/Cargo.toml` (documenting the fail-not-skip contract alongside `exasol-e2e` and `lakekeeper-e2e`) **and** add `feature = "azure-e2e"` to every gate that stands between that feature and a compiled `common` module. `mod.rs` is not the only gate — three files gate this tree, and missing any one leaves the build broken under `--features azure-e2e`. The exact edit set:
  - `tests/common/mod.rs` — the crate-level `#![cfg(any(...))]`, and the per-module gates on `e2e_harness`, `exasol_ws`, `lakekeeper`, and `seed`; then declare `#[cfg(feature = "azure-e2e")] pub mod azure;`.
  - `tests/common/lakekeeper.rs` (line 18) — its own file-level `#![cfg(feature = "lakekeeper-e2e")]`. `mod.rs` admitting `pub mod lakekeeper;` is not enough: the file would still compile to nothing, leaving tasks 2.2, 2.3, and 3.1 no module to extend.
  - `tests/common/stack.rs` (lines 2-6) — its own file-level `#![cfg(any(feature = "exasol-e2e", feature = "cloud-e2e", feature = "lakekeeper-e2e"))]`. **`stack` has no `mod.rs`-level gate at all** (`mod.rs:33` is a bare `pub mod stack;`), so this file-level attribute is the only thing admitting the module. Left untouched, `stack` is empty under `--features azure-e2e`, which deletes `CatalogConnectionPassword` (the type task 1.4 extends) and `wait_for_url` (imported by `lakekeeper.rs:22`).
  - `tests/common/stack.rs` item-level `#[cfg(any(feature = "exasol-e2e", feature = "lakekeeper-e2e"))]` at line 263 (`lakehouse_engine_so_path`) and line 377 (`local_stack_connection_password`). Both are imported unconditionally by `tests/common/e2e_harness.rs:13-17`, the module task 3.1 provisions the scan path from, so both must admit `azure-e2e` even once `stack` itself is admitted.

  This wiring is not optional bookkeeping: without it the whole `common` module — `stack`, `seed`, `lakekeeper`, `e2e_harness`, `exasol_ws` — compiles to nothing, making Group C unbuildable. Every other task in Group A depends on this one. Task 4.3's `cargo test --features azure-e2e --no-run` is the check that the gate set is complete.
- [ ] 1.2 Add the `test-e2e-azure` Makefile target: `cross-musl-udf-build` prerequisite, then a **single recipe line** loading `./test.env` only when it exists and invoking cargo in that same shell (`if [ -f ./test.env ]; then set -a; . ./test.env; set +a; fi; cargo test --features azure-e2e --test e2e_azure_test -- --test-threads=1`). Make runs each recipe line in its own shell, so splitting the sourcing onto its own line discards every variable before cargo starts and the failure then looks like a credential problem rather than a recipe problem. Add the target to `.PHONY`.
- [ ] 1.3 Commit `test.env.example` naming all five variables with placeholder values only, grouped and commented so the two data-path variables are visibly distinct from the three container-lifecycle ones. `.gitignore` already lists `test.env` (line 5, under "# Test environment properties") — no edit needed there; the spec's gitignore scenario is a regression guard on that existing state.
- [ ] 1.4 Add `account_name: Option<String>` and `account_key: Option<String>` to `CatalogConnectionPassword`, emitted from `to_sql_password_json` only when present; extend the existing `catalog_connection_password_tests`.
- [ ] 1.5 Create `crates/lakehouse-engine/tests/common/azure.rs` with the five credential-variable readers and the per-run container-name derivation, plus pure unit tests. Split each reader into a pure `fn require_var(name: &str, value: Option<&str>) -> String` that panics naming `name` and never the value, plus a thin `std::env::var` caller — the unit test then exercises `require_var` with `None` and `Some("")` and never mutates the process environment, which matters because this crate is `edition = "2024"` (`std::env::remove_var` is `unsafe` and process-global) and every other test in the binary needs those same variables set. Nothing reads the variables implicitly: `azure_identity` 1.x ships no environment-scanning credential. Cover the container-name derivation with pure unit tests over adversarial `$USER` values. The derived name must satisfy Lakekeeper's filesystem rules — 3 to 63 characters of `[a-z0-9-]`, no consecutive hyphens, no leading or trailing hyphen — for inputs including empty, `Antoni.Reus`, `a..b`, `---`, and a 90-character name; truncation must not leave a trailing hyphen. [expert]
- [ ] 1.6 Open the follow-up GitHub issue for the out-of-band orphaned-container sweep (Azure lifecycle policies delete blobs, not containers) and replace `(#TBD)` in the spec's known-ceiling bullet with its number.

### 2. Harness seams

- [ ] 2.1 Add the `azure_storage_blob` 1.0 / `azure_identity` 1.0 / `azure_core` 1.1 dev-dependencies and the `AzureContainer` guard in `common/azure.rs`.
  - **Client construction (a private helper, called twice).** Build the credential with `ClientSecretCredential::new(&tenant_id, client_id, Secret::new(client_secret), None)`, then `BlobServiceClient::new(https://<account>.blob.core.windows.net/, Some(credential), None)?` and `.blob_container_client(name)`. Create with `container_client.create(None).await`; a `ContainerAlreadyExists` response fails the run, because the millisecond suffix makes a collision a defect.
  - **`AzureContainer` stores only plain owned data** — the account name, the container name, and the three Entra ID values (tenant id, client id, client secret) — and **no client and no runtime handle**. It reconstructs `ClientSecretCredential`, `BlobServiceClient`, and `BlobContainerClient` **inside the teardown thread, on that thread's own runtime**. Do NOT move the construction-time client across runtimes. An `azure_core` HTTP client is a `reqwest`/`hyper_util` client whose pooled keep-alive connection is driven by a task on the runtime that opened it — and `create()` ran seconds earlier on the fixture's runtime, so such a connection is normally sitting idle in the pool. Handing the delete to that client dispatches it to a task on the fixture's runtime, which is at that exact moment blocked in the guard's own `thread::join()` and polling nothing: the delete future never completes, `join()` never returns, and the suite deadlocks with no timeout to break it — orphaning the container anyway. Rebuilding the three cheap client values in the teardown thread costs one extra token acquisition and removes the deadlock entirely. Storing plain owned data also settles the `'static` requirement: `Drop` takes `&mut self` while `std::thread::spawn` needs `'static`, so clone the five `String`s out of `self` into the thread rather than moving `self`.
  - **Teardown.** `Drop` MUST NOT drive the delete on the ambient runtime: it is synchronous, and it ordinarily fires inside the fixture's `rt.block_on(…)`, where `Runtime::block_on`/`Handle::block_on` panics with "Cannot start a runtime from within a runtime" — while unwinding, that aborts the process and deletes nothing. Instead spawn a `std::thread`, build a fresh `tokio::runtime::Runtime` inside it, **build the container client in that thread** and `block_on` its `delete(None)` there, then join; treat an already-absent container as success. Report a join or delete failure through `eprintln!` and never panic from `Drop`, so an unwinding test keeps its original failure.
  - **Error handling.** Detect Azure error codes by `TryFrom`-ing `azure_core::Error` into `azure_storage_blob::StorageError` and matching `StorageErrorCode::{ContainerAlreadyExists, ContainerNotFound}`, with arms for the catch-all `StorageErrorCode::UnknownValue` and for a failed `TryFrom` conversion (which returns the original error). [expert]
- [ ] 2.2 In `common/lakekeeper.rs`, extract the existing warehouse POST, its 400-`StorageProfileOverlap` idempotency handling, and its response-body-free panic message into one private helper; leave `lakekeeper_create_warehouse`'s signature unchanged and delegate to it. Add the ADLS warehouse profile plus its creation function building the `adls` storage profile (`account-name`, `filesystem`, `key-prefix`, `sas-enabled: false`) and the `az`/`shared-access-key` credential, delegating to the same helper. [expert]
- [ ] 2.3 Add the Azure CONNECTION-password builder in `common/lakekeeper.rs`: `warehouse` plus the OAuth2 catalog fields plus `account_name`/`account_key`, leaving `endpoint`, `region`, `access_key`, `secret_key`, and `session_token` empty so `validate_creds` reads it as an Azure CONNECTION rather than an ambiguous one. `warehouse` is not optional and is not copyable from the MinIO builder: `crates/lakehouse-engine/src/adapter/connection.rs:112` rejects an empty `warehouse` outright with "CONNECTION '{name}' password is missing required field: warehouse" as `validate_creds`'s very first check, before any Azure validation runs — and the value carries the per-run suffix (decision [5]), so the builder must take the per-run ADLS warehouse name as a parameter. No Entra ID value goes near it — the CONNECTION is the account-key path under test.
- [ ] 2.4 Carry a storage backend on the shared seed-catalog configuration in `common/seed.rs`, defaulting to the current static-MinIO baseline, and dispatch `build_seed_catalog_with_auth` on it: MinIO keeps today's S3 factory and static credential loader, ADLS uses `OpenDalStorageFactory::Azdls` with the `adls.account-name`/`adls.account-key` props and no credential loader. Do NOT rename the configuration value or its entry points: `SeedCatalogAuth`, `build_seed_catalog_with_auth`, and `seed_events_table_with_auth` have 18 occurrences across a 117 KB `seed.rs` reached by 12 test binaries, and a cosmetic rename buys no behavior while enlarging the diff in the module whose breakage is hardest to catch. Declare `features = ["opendal-azdls"]` explicitly on the `iceberg-storage-opendal` dev-dependency. [expert]

### 3. The suite

- [ ] 3.1 Write `crates/lakehouse-engine/tests/e2e_azure_test.rs`: a `OnceLock` setup holding only cleanup-free provisioning (readiness waits, Lakekeeper bootstrap, SLC install, `.so` upload, shared scripts), and an `AzureFixture` that creates the container guard, the per-run ADLS warehouse, the seeded events table, the CONNECTION, and the Virtual Schema — held as a local so `Drop` fires. [expert]
- [ ] 3.2 Add the end-to-end test asserting, in order: the warehouse's storage profile as Lakekeeper reports it, that every seeded data-file path is an `abfss://<container>@<account>.dfs.core.windows.net/` location, that the projection/filter/LIMIT query returns exactly the expected seeded rows, and that the script DDL came from the shared harness definition.
- [ ] 3.3 Add the container-guard test: create a container inside `futures::FutureExt::catch_unwind` over an `AssertUnwindSafe(async { … })` block awaited inside `rt.block_on(…)`, panicking inside that block, then assert the container's absence with a fresh `container_client.exists()` call after the scope, failing loudly if it survived. Use that form specifically — `std::panic::catch_unwind` is NOT usable here: it takes a synchronous closure, the guard's construction is `async`, and driving it with `Handle::block_on` inside the closure re-enters the "Cannot start a runtime from within a runtime" panic this test exists to prove was fixed. `futures` is already a dev-dependency, so the workable form costs nothing. Running inside `rt.block_on` is the point: it exercises the runtime-nested `Drop` path, which is where the naive implementation aborts the process. This is the one place cleanup is verified rather than assumed, and it costs one extra short-lived container per run.
- [ ] 3.4 Add the remaining tests: stack-unavailable fail-loud, credentials-never-in-output over a deliberately invalid Azure CONNECTION DDL, `test.env` gitignore hygiene, and the Makefile target's properties — `cross-musl-udf-build` prerequisite, `--test-threads=1`, and the `test.env` sourcing sharing one recipe line with the cargo invocation. The missing-credential-variable test belongs to task 1.5 as a pure unit test, not here.

### 4. CI and live verification

- [ ] 4.1 Add the `E2E (Azure)` job to `.github/workflows/ci.yml`, mirroring `E2E (Lakekeeper)`'s bring-up, log dumping, and teardown; set `AZURE_STORAGE_ACCOUNT_NAME` from a repository variable and `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` from repository secrets; guard it so a fork pull request does not schedule it; run `make test-e2e-azure`. Do NOT add it to `release`'s `needs`. Document the required **Storage Blob Data Contributor** role assignment in the job's comment, so the next person configuring the secrets does not reach for an over-privileged role or an under-privileged custom one. [expert]
- [ ] 4.2 Run `make test-e2e-azure` against the real Azure account with a real service principal. Confirm the suite passes, the container is created and then deleted, no container from the run survives, and the scan genuinely read through the account key rather than the service principal. This is definition-of-done, not a compile check. [expert]
- [ ] 4.3 Run `cargo test --features azure-e2e --no-run` first, as a compile-only completeness check on task 1.1's gate set — it fails fast if any of the three gating files or two item-level gates was missed, before any Azure account or Docker stack is involved. Then run `cargo clippy --all-targets --features exasol-e2e,lakekeeper-e2e,cloud-e2e,azure-e2e` and `cargo fmt`, then re-run **both** MinIO suites — `make test-e2e-lakekeeper` and `make test-e2e` — to prove the shared-seam edits left them intact. Both are required: tasks 2.2 and 2.4 restructure `common/lakekeeper.rs` and `common/seed.rs`, and `seed::` is reached from 12 test files including the `exasol-e2e`-only `int96_fixtures` and `pos_delete_fixtures`. Each binary is `#![cfg(feature = …)]`-gated at its own top, so a single-feature clippy run type-checks none of the others.
- [ ] 4.4 Add the four offline checks to an always-run CI job. The container-name legality, missing-variable, gitignore, and Makefile-shape tests need no Azure account and no Docker stack — only the feature enabled — yet they otherwise execute only under `make test-e2e-azure`, which is guarded to this repository and excluded from the release gate. Name all four with the shared prefix `azure_offline_` (`azure_offline_container_name_is_azure_and_lakekeeper_legal`, `azure_offline_missing_credential_variable_fails_loud`, `azure_offline_local_credential_file_is_gitignored`, `azure_offline_make_target_rebuilds_so_and_runs_serially`) and add a step to the existing `unit-tests` job running `cargo test --features azure-e2e --test e2e_azure_test -- azure_offline_`. **The step MUST fail when fewer than four tests ran.** A libtest name filter that matches nothing exits 0 — it prints "0 passed; 0 failed; N filtered out" and the step goes green — so a renamed test or a fifth offline check would silently stop being covered, which is the same "green and proves nothing" outcome this task exists to prevent, only invisible instead of red. Assert the count in the step (parse the `N passed` line and require `N >= 4`), or add a fifth test that fails when the prefix set is incomplete. With that guard the slice's one piece of real logic — the container-name sanitizer — is covered on every pull request, forks included, and inside the release gate.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 first, then 1.2, 1.3, 1.4, 1.5, 1.6 in parallel |
| Group B | Two chains, parallel to each other: 2.2 → 2.3, and 2.1 → 2.4 |
| Group C | 3.1 → 3.2, 3.3, 3.4 |
| Group D | 4.1, 4.2, 4.3, 4.4 |

Sequential dependencies:
- 1.1 → everything else in Group A. Until the feature is declared and `tests/common/mod.rs` admits it, nothing under `tests/` compiles with `--features azure-e2e`, so 1.5's unit tests cannot even build
- Group A → Group B (2.1 needs the container name from 1.5; 2.3 needs the fields from 1.4)
- 2.2 → 2.3 (both edit `crates/lakehouse-engine/tests/common/lakekeeper.rs`; 2.2's extraction moves the code 2.3 extends, so running them concurrently is a write conflict on a file the other is mid-edit in)
- 2.1 → 2.4 (both edit `crates/lakehouse-engine/Cargo.toml` — 2.1 adds three dev-dependencies, 2.4 adds `features = ["opendal-azdls"]` to an existing one)
- Group B → Group C (the fixture composes all four seams)
- 3.1 → 3.2 (the fixture must exist before the test that asserts through it)
- Group C → Group D (4.2 runs the finished suite)
- 3.3, 3.4, 4.1 are independent of each other

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | Purely additive: a new test binary, a new harness module, and new arms on three existing harness helpers. Task 2.2 and 2.4 restructure existing helpers in place without leaving a superseded copy behind |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Harness provisions a per-run container and a delegation-disabled ADLS warehouse | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` |
| End-to-end scan over the static-credential ADLS warehouse returns correct rows | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` |
| Per-run container is deleted when its owning scope ends, including on panic | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_container_guard_deletes_on_panic` — panics inside `rt.block_on`, then asserts absence via a fresh `exists()` |
| Container name is legal for Azure and Lakekeeper whatever the user name contains | Unit | `crates/lakehouse-engine/tests/common/azure.rs` | `azure_offline_container_name_is_azure_and_lakekeeper_legal` |
| Azure suite fails when a required credential variable is absent | Unit | `crates/lakehouse-engine/tests/common/azure.rs` | `azure_offline_missing_credential_variable_fails_loud` — exercises the pure `require_var` with `None` and `Some("")`; no process-environment mutation |
| Azure suite fails when the local stack is unavailable | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_suite_fails_when_stack_unavailable` |
| The Azure Make target rebuilds the .so before running the suite | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_offline_make_target_rebuilds_so_and_runs_serially` |
| Local credential file cannot be committed | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_offline_local_credential_file_is_gitignored` |
| Azure binary provisions the scan path from the shared harness definition | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` |
| No Azure credential value appears in output when credential-bearing DDL fails | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_credentials_never_appear_in_output` |

The first, second, and ninth scenarios map to one test: they are three sets of
assertions over one provisioned Azure fixture, and splitting them would triple the
live-Azure cost and the orphan surface for no added coverage.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| azure-e2e-harness | `cp test.env.example test.env`, fill all five values, then `make test-e2e-azure` | Suite passes; `az storage container list --account-name <account>` shows no `lhrs-e2e-*` container from the run |
| azure-e2e-harness | `mv test.env test.env.bak && unset AZURE_CLIENT_SECRET && make test-e2e-azure` | Fails, naming `AZURE_CLIENT_SECRET`; no test reported as skipped or passed; no credential value in the output |
| azure-e2e-harness | Fill `test.env` with a service principal holding only `Storage Blob Data Reader`, then `make test-e2e-azure` | Fails on container creation with a 403 authorization error, not on the query — confirms the documented role is the required one |
| azure-e2e-harness | `docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml stop lakekeeper && make test-e2e-azure` | Fails on the readiness wait; no test reported as skipped |
| azure-e2e-harness | `git status --porcelain test.env` after filling it in | Empty — the file is ignored |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test (unit) | `cargo test` | 0 failures |
| Test (Azure offline checks) | `cargo test --features azure-e2e --test e2e_azure_test -- azure_offline_` | 4 passed, 0 failures — fewer than 4 is a failure, not a pass; no Azure account or Docker stack needed |
| Compile (Azure gate set) | `cargo test --features azure-e2e --no-run` | Exit 0 — proves task 1.1's three gating files and two item-level gates are all wired |
| Test (Azure E2E) | `make test-e2e-azure` | 0 failures, container deleted |
| Test (regression, Lakekeeper MinIO) | `make test-e2e-lakekeeper` | 0 failures |
| Test (regression, REST MinIO) | `make test-e2e` | 0 failures — `seed.rs` is reached by 12 test files, including `exasol-e2e`-only ones |
| Precondition | `az storage account show -n <account> --query isHnsEnabled` | `true` — required before `make test-e2e-azure` can pass |
| Lint | `cargo clippy --all-targets --features exasol-e2e,lakekeeper-e2e,cloud-e2e,azure-e2e` | 0 errors/warnings — each test binary is feature-gated at its own top, so a single-feature run checks none of the others |
| Format | `cargo fmt` | No changes |
