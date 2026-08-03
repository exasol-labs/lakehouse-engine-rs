# Decision Log: add-azure-e2e-static-target

## Interview

**Q:** `object_store`'s azure feature (already a dependency, used in production `register_object_store`) has no create/delete-container API — only blob I/O within an existing container. How should the harness create and drop the per-test Azure container?
**A:** Add the official `azure_storage_blobs` SDK (from the azure-sdk-for-rust project) as a dev-dependency on `crates/lakehouse-engine`, matching the existing pattern where `reqwest` is a dev-dependency used only by the E2E harness. The SDK was chosen explicitly over hand-rolling Shared-Key REST signing with `reqwest` + `hmac` + `sha2`; both were offered.

**Q:** The issue's "known ceiling" mentions sweeping orphaned containers via an Azure Storage account lifecycle-management rule. Is configuring that rule in scope?
**A:** No — document only. Record the ceiling in the spec and reference a follow-up GitHub issue for someone with Azure account access to configure the sweep manually. This plan must not touch real Azure account configuration.

**Q:** Is a real Azure Storage account and account key available now, to verify the harness locally before this is considered done?
**A:** Yes. Local `test.env` can be filled with real credentials, so live verification is possible and should be exercised at implement time per the project's verification-discipline norm — the implementer must actually run `make test-e2e-azure` against the real account, not just get it to compile.

**Q:** (Follow-up, after research showed the crate named in the first answer is the unofficial, unmaintained line and that the official crate cannot use an account key.) Three options: keep the legacy `azure_storage_blobs` 0.21; drop the container lifecycle entirely in favour of a single long-lived container with a per-run `key-prefix` over the already-present `object_store`; or take the official `azure_storage_blob` 1.0 and authenticate the container lifecycle with an Entra ID service principal.
**A:** Use the official `azure_storage_blob` 1.0 crate, accepting Entra ID auth for the container lifecycle. Both other options rejected. The harness then has two separate credential paths that must not be conflated: container setup and teardown authenticate with a service principal (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, new secrets, role scoped as narrowly as can be documented), while the data path under test keeps the account key carried in the Exasol CONNECTION exactly as issue #277 specified. The Virtual Schema and the query never use Entra ID.

**Q:** Is configuring the Azure lifecycle-management sweep still out of scope, given that such a rule cannot target containers?
**A:** Yes — confirmed. There is no real second option; document the ceiling plainly and open a follow-up issue for a manual or separately-scripted sweep.

## Design Decisions

### [1] Container lifecycle runs on the official crate under an Entra ID service principal

- **Decision:** Take `azure_storage_blob` 1.0.0, `azure_identity` 1.0.0, and `azure_core` 1.1.0 as dev-dependencies, and authenticate container create and delete with an Entra ID service principal via `ClientSecretCredential`. Default features are correct: they resolve to reqwest 0.13 + rustls and tokio `^1.49`, all already in the lockfile, so no new major version or second TLS stack enters the graph.
- **Alternatives:** The legacy `azure_storage_blobs` 0.21 does account-key auth, needing no second credential, but is published from the `legacy` branch under an explicit "no longer under active development" notice. A single long-lived container with a per-run `key-prefix` over the already-present `object_store` would have added no dependency at all and made the orphan ceiling sweepable by a real prefix-filtered blob lifecycle rule. Hand-rolled Shared Key REST signing over the existing `reqwest` was rejected in the first interview.
- **Rationale:** Both alternatives were put to the user with this evidence and both were rejected in favour of an official, maintained dependency. Verified before committing to it: `BlobContainerClient` exposes `create(None)` and `delete(None)`, its constructor takes `Option<Arc<dyn TokenCredential>>`, and `StorageErrorCode` carries both `ContainerAlreadyExists` and `ContainerNotFound`, so the surface is complete and the error handling is structured. The cost is three extra CI secrets and a second credential concept inside the harness.
- **Promotes to ADR:** yes

### [1a] The harness's credential and the credential under test are kept strictly apart

- **Decision:** Only the container guard uses the service principal. The Lakekeeper warehouse storage credential, the seed `FileIO`, and the Exasol CONNECTION the scan reads through all carry the account key. The CONNECTION carries no Entra ID field, asserted in the spec's scan scenario.
- **Alternatives:** Let the service principal serve both purposes, which would have collapsed five variables to three.
- **Rationale:** The account-key path IS the deliverable — this slice exists to prove `StorageBackend::Adls` with `AdlsCred::AccountKey` against live Azure. A suite that reached Azure through the service principal would pass while exercising nothing slice C shipped: a green run would prove the harness works, not the product. The separation is therefore a correctness property of the test, not a hygiene preference, which is why it is stated normatively in the spec rather than left to the implementation.
- **Promotes to ADR:** yes

### [1b] The three Entra ID variables are read explicitly, never picked up implicitly

- **Decision:** Read `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET` in the harness and construct `ClientSecretCredential` from them, failing loudly and by name on any absent one.
- **Alternatives:** `DefaultAzureCredential`, which in older versions scanned those exact variables from the environment.
- **Rationale:** `DefaultAzureCredential` was removed in `azure_identity` 0.28.0 and replaced by `DeveloperToolsCredential`, which tries only the Azure CLI and the Azure Developer CLI. `azure_identity` 1.x ships no environment-scanning credential at all, so an absent variable would otherwise surface as a confusing authorization failure against Azure instead of a named local one — and on a developer machine with the Azure CLI logged in, the suite could silently authenticate as the wrong identity.
- **Promotes to ADR:** no

### [1c] Storage Blob Data Contributor, not a narrower custom role

- **Decision:** Document **Storage Blob Data Contributor** on the test storage account as the service principal's role.
- **Alternatives:** A custom role holding only the `containers/blobs/*` data actions; `Storage Blob Data Owner`; account-level `Contributor`.
- **Rationale:** Container create and delete are `Microsoft.Storage/storageAccounts/blobServices/containers/write` and `/delete`, which sit in the role's `Actions`, not among the `DataActions` that cover `containers/blobs/*`. A blob-data-only custom role therefore returns 403 on container management — the trap worth documenting, since it looks like the tighter, more correct choice. `Storage Blob Data Owner` and `Contributor` are both over-privileged for creating and deleting a container.
- **Promotes to ADR:** no

### [2] The issue's proposed orphan sweep is not implementable as written

- **Decision:** Record the known ceiling as requiring an out-of-band scheduled sweep (Azure CLI or a Function), not a storage-account lifecycle rule.
- **Alternatives:** State the ceiling as the issue does — "sweep via an account lifecycle rule (auto-delete containers older than N days)".
- **Rationale:** Azure Blob lifecycle-management policies act on blobs, blob versions, and snapshots; they cannot delete a container. Writing the issue's wording into a spec would record a mitigation that does not exist. The accurate statement is that `lhrs-e2e-<user>-<millis>` naming makes an orphan attributable, and that removing one needs tooling outside this repository.
- **Promotes to ADR:** yes

### [3] Cleanup is anchored to a test function's stack, never to the OnceLock setup

- **Decision:** Split provisioning in two: cleanup-free work (readiness waits, Lakekeeper bootstrap, SLC install, `.so` upload, shared scripts) stays in the shared `OnceLock`; the container, warehouse, seed, CONNECTION, and Virtual Schema live on an `AzureFixture` held as a local by the test that uses them.
- **Alternatives:** Park the whole fixture, guard included, in the `OnceLock` the MinIO Lakekeeper suite uses for everything.
- **Rationale:** A value inside a `OnceLock` static is never dropped — Rust does not drop statics at process exit — so a guard placed there would silently never delete anything, and the suite would look correct while leaking a container on every single run. Anchoring the guard to a stack frame makes unwinding do the work: this workspace sets no `panic = "abort"`, so a panicking test still cleans up.
- **Promotes to ADR:** yes

### [4] One container per run, shared by all Azure tests in that run

- **Decision:** Provision one Azure fixture and assert the provisioning, seed-location, query-result, and shared-provisioning scenarios against it in a single test.
- **Alternatives:** One fixture per test, giving every scenario full isolation.
- **Rationale:** Each fixture costs a real container creation, a warehouse creation that Lakekeeper validates by writing and deleting a probe object in Azure, and two Parquet writes. Splitting four sets of assertions across four fixtures would triple the live-Azure cost and the orphan surface without covering anything extra. `--test-threads=1` is already mandatory for the shared Exasol provisioning, so isolation buys no concurrency safety either. The plan-authoring rule permits combining scenarios that share setup and assertions. One exception is deliberate: the container-guard test (task 3.3) creates a second, short-lived container per run, on the code path where cleanup is least certain. That is the point — it is the one place cleanup is verified rather than assumed, so the extra container buys the evidence behind every other test's cleanup claim.
- **Promotes to ADR:** no

### [5] The per-run suffix goes on the warehouse name too, not only the container

- **Decision:** Derive both the container name and the Lakekeeper warehouse name from the same `<sanitized-user>-<millis>` suffix.
- **Alternatives:** A fixed warehouse name relying on the existing already-exists idempotency handling.
- **Rationale:** Warehouses outlive containers. A second local run with a fixed name would hit the existing 409/`StorageProfileOverlap` handler, return early, and bind to a warehouse whose container the previous run deleted — failing later, at scan time, with a misleading error. Unique names also make Lakekeeper's overlap detection a no-op rather than something the harness has to reason about.
- **Promotes to ADR:** no

### [6] The Azure CONNECTION must leave every static S3 field empty

- **Decision:** The Azure CONNECTION-password builder sets `warehouse` (the per-run ADLS warehouse name), `account_name`, and `account_key`, and leaves `endpoint`, `region`, `access_key`, `secret_key`, and `session_token` empty.
- **Alternatives:** Reuse `lakekeeper_connection_password`, which sets the MinIO endpoint and region on the static path.
- **Rationale:** `validate_creds` rejects a CONNECTION naming both an Azure and an S3 storage field as an ambiguous credential set, and `supplied_s3_fields` counts `endpoint` and `region` as S3 fields. `to_sql_password_json` always serializes those keys, but `parse_creds` reads every string field through `nonempty_str`, so an empty string is correctly seen as absent. The constraint is therefore "leave empty", not "omit the key". `warehouse` is the one field that must be non-empty: `validate_creds`'s first check (`connection.rs:112`) rejects an empty one before any Azure validation runs, and its value carries the per-run suffix from decision [5], so it is a parameter rather than a constant copied from the MinIO builder.
- **Promotes to ADR:** no

### [7] Extend the warehouse-creation helper by extracting its body, not by adding an enum or forking it

- **Decision:** Extract the POST, the 400-`StorageProfileOverlap` idempotency rule, and the response-body-free panic message into one private helper. `lakekeeper_create_warehouse` keeps its signature and delegates; the ADLS variant builds only its own request body and delegates to the same helper.
- **Alternatives:** Turn `WarehouseProfile` into a storage enum (its fields are `&'static str`, and the Azure container name is computed at runtime, so every existing call site would churn); or add a standalone ADLS creation function with its own copy of the POST and error handling.
- **Rationale:** A second copy would duplicate the credential-safety contract — the request body carries the account key, so "never echo the response body" must hold for both arms — which is exactly the leaked decision the shared helper exists to prevent. Extracting the body keeps one owner while touching no existing call site.
- **Promotes to ADR:** no

### [8] No delta on lakekeeper-e2e-harness

- **Decision:** Ship one new feature spec and leave `e2e-harness/lakekeeper-e2e-harness` untouched.
- **Alternatives:** Add a CHANGED delta bullet noting that warehouse creation is now storage-parameterized.
- **Rationale:** Every normative statement in that spec stays true: its warehouses are still created with an `s3` profile of `flavor` `s3-compat`, still one with delegation off and one `sts-enabled`. Task 2.2 and 2.4 restructure shared helpers without changing any behavior that spec describes, and the new spec states the shared-helper contract for a future storage backend to find. A delta asserting only that a helper was refactored would be churn.
- **Promotes to ADR:** no

### [9] The CI job runs but does not gate releases, and does not run on fork pull requests

- **Decision:** Add `E2E (Azure)` mirroring `E2E (Lakekeeper)`, guarded to the same repository, and leave it out of `release`'s `needs`.
- **Alternatives:** Gate releases on it, as `e2e` and `e2e-lakekeeper` are; or run it on every pull request including forks.
- **Rationale:** The suite depends on a live third-party account and, by contract, cannot skip. In the release gate, an Azure incident or a rotated secret becomes a blocked release; on fork pull requests, an unreadable secret becomes a red check on every external contribution. Declining to schedule a job is a different thing from the suite skipping — whenever it runs, an absent variable or an unreachable service fails it. Accepted cost, stated in the plan: an Azure regression can reach a release, mitigated by the job still running and failing visibly on every `main` push.
- **Promotes to ADR:** yes

### [10] Add only the CONNECTION fields this slice uses

- **Decision:** `CatalogConnectionPassword` gains `account_name` and `account_key`; `sas_token` is left for slice F.
- **Alternatives:** Add all three now, since slice F is the next scheduled slice.
- **Rationale:** An unused field is dead code and would be flagged as such in review. Slice F adds it together with its first user, for two lines.
- **Promotes to ADR:** no

### [11] Declare opendal-azdls explicitly on the dev-dependency

- **Decision:** Add `features = ["opendal-azdls"]` to `crates/lakehouse-engine`'s `iceberg-storage-opendal` dev-dependency.
- **Alternatives:** Rely on the current situation, where the feature reaches the test build only because `lakehouse-catalog` enables it and Cargo unions features across the graph.
- **Rationale:** The seed harness's own use of `OpenDalStorageFactory::Azdls` would break the moment another crate stopped asking for that feature, with no local declaration explaining why. Depending on another crate's feature choice for a capability this crate directly needs is back-door coupling; one line removes it.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Drop cannot await, and awaiting on the ambient runtime aborts the process

- **Finding:** Task 2.1 prescribed `container_client.delete(None).await` inside `Drop`. `Drop::drop(&mut self)` is synchronous, so that cannot compile; and because the guard is ordinarily dropped inside the fixture's `rt.block_on(…)`, the obvious repair — `Handle::block_on` — panics with "Cannot start a runtime from within a runtime". On the panic path task 3.3 deliberately exercises, that is a panic while unwinding, which aborts the process: no delete, no original failure message. The plan's stated pattern would have produced the exact opposite of its claim.
- **Direction change:** `Drop` now spawns a `std::thread`, builds a fresh `tokio::runtime::Runtime` inside it, `block_on`s the delete there, and joins, so teardown behaves identically inside and outside a runtime context; failures are reported through `eprintln!` and never panic. Added as a § Design › Patterns row naming why the guard holds no runtime handle, as an `*AND*` clause on the spec's panic-cleanup scenario, and as an assertion in task 3.3 (which now runs its `catch_unwind` scope inside `rt.block_on` so the nested case is actually covered).
- **Promotes to ADR:** yes

### [2] [plan-review] Hierarchical namespace was an unstated hard prerequisite

- **Finding:** The plan enumerated every other out-of-band Azure prerequisite — service principal, exact RBAC role, which values are secrets versus variables — but never said the storage account must have hierarchical namespace enabled. Lakekeeper v0.13.1 requires it for ADLS warehouses (`docs/docs/storage.md`) and its ADLS backend performs no HNS detection or fallback. Since task 4.2 is definition-of-done, a non-HNS account would have blocked the plan behind an Azure admin action the operator had no warning about — and enabling HNS on an existing account is a one-way upgrade with its own prerequisites.
- **Direction change:** § Dependencies now requires a StorageV2 account with HNS enabled and names the one-way-upgrade caveat; `az storage account show -n <account> --query isHnsEnabled` is a precondition row in the Checklist and a stated precondition of task 4.2; § Impact and a spec § Background bullet carry the same requirement, citing Lakekeeper's docs.
- **Promotes to ADR:** no

### [3] [plan-review] The guard's collision clauses contradicted each other

- **Finding:** The panic-cleanup scenario asserted both "a container that already exists SHALL be adopted rather than failing the run" and "the guard MUST NOT delete any container it did not itself create". An adopted container is by definition not one the guard created, so the second clause forbade cleaning up the first — no pass/fail test could be written from the pair. Task 2.1 contradicted itself in the same way, prescribing `.exists()`-guarded creation *and* `StorageErrorCode` matching as if both were the collision strategy.
- **Direction change:** Adoption is gone. A name collision at create time now fails the run — the millisecond suffix makes a collision a defect, not a tolerable state — and a container already absent at delete time counts as deleted. Task 2.1 keeps only the `StorageErrorCode` matching, with added arms for the catch-all `UnknownValue` and for a failed `TryFrom<azure_core::Error>` conversion.
- **Promotes to ADR:** no

### [4] [plan-review] The feature was never wired into tests/common/mod.rs

- **Finding:** No task touched `crates/lakehouse-engine/tests/common/mod.rs`, and no artifact named it. Verified against the file: it carries a crate-level `#![cfg(any(feature = "exasol-e2e", feature = "cloud-e2e", feature = "lakekeeper-e2e"))]` plus per-module gates. Under `cargo test --features azure-e2e --test e2e_azure_test` — exactly what the new Make target runs — that gate evaluates false and the whole `common` module compiles to nothing, making Group C unbuildable and 1.5's unit tests uncompilable.
- **Direction change:** Task 1.1 now covers both halves — declaring the feature in `Cargo.toml` and adding it to the crate-level gate, the `e2e_harness`/`exasol_ws`/`lakekeeper`/`seed` per-module gates, and a new `#[cfg(feature = "azure-e2e")] pub mod azure;`. `tests/common/mod.rs` is named in § Design › Architecture, and § Parallelization now makes 1.1 a sequential predecessor of everything else in Group A rather than a peer. Folded into 1.1 rather than added as a separate "1.0" so the numbering stays contiguous; the required ordering is unchanged.
- **Promotes to ADR:** no

### [5] [plan-review] Advisory findings applied

- **Finding:** Ten of the eleven advisories were correct and cheap.
- **Direction change:** Dropped task 2.4's cosmetic rename (18 occurrences across a 117 KB `seed.rs` reached by 12 binaries, buying no behavior). Dropped the `.gitignore` edit from task 1.3 — verified `test.env` is already ignored at line 5 and a filled-in five-variable `test.env` already exists — and replaced the stale "service principal may need creating" dependency note with two verifiable preconditions (role assignment on `APP_EXA_RND_LAKEKEEPER_DEV`, and the four secrets plus one variable existing). Made task 1.2's `test.env` sourcing and cargo invocation one recipe line, since Make gives each line its own shell. Split the credential readers into a pure `require_var` so the fail-loud test needs no `unsafe` process-environment mutation under edition 2024, and aligned the coverage table with it. Added `exists()`-based absence assertion to task 3.3. Widened task 4.3 to both MinIO suites and an all-features clippy run. Added task 4.4 putting the four offline checks into the always-run `unit-tests` CI job, so the container-name sanitizer is covered on fork PRs and in the release gate. Recorded the account key's unscopable blast radius in § Impact and § Background. Fixed "two structural departures" (three) and cut the feature description to two sentences.
- **Promotes to ADR:** no

The one advisory not taken as written proposed a new ungated `tests/azure_harness_helpers.rs` target so the four offline checks run under plain `cargo test`. The goal is right, the mechanism costs a file and a sharing seam; task 4.4 reaches the same coverage — every PR including forks, plus the release gate — with one CI step and no new file.

### [6] [plan-review] The own-thread teardown moved the runtime but left the client behind

- **Finding:** Round 2, raised against the repair for finding [1]. Task 2.1 built `BlobServiceClient`/`BlobContainerClient` in the async constructor, used them for `create(None).await` on the fixture's runtime, then prescribed `block_on(container_client.delete(None))` on a *different* runtime inside the teardown thread. An `azure_core` HTTP client is a `reqwest`/`hyper_util` client whose pooled keep-alive connection is driven by a task on the runtime that opened it — and `create()` leaves exactly such an idle connection behind. Reusing the client dispatches the delete to a task on the fixture's runtime, which is at that moment blocked in the guard's own `thread::join()` and polling nothing: the delete never completes, `join()` never returns, and the suite deadlocks with no timeout. The container is orphaned regardless. Task 3.3 hits it first, and on the normal path too, not only on panic. The load-bearing belief — that the client is runtime-portable — was never stated and is false. A second, smaller gap: `Drop` takes `&mut self` while `std::thread::spawn` needs `'static`, and the plan never said what is moved into the thread.
- **Direction change:** `AzureContainer` now stores only plain owned data — account name, container name, and the three Entra ID values — and holds no client and no runtime handle. It reconstructs `ClientSecretCredential`, `BlobServiceClient`, and `BlobContainerClient` inside the teardown thread on that thread's own runtime, rather than moving the construction-time client across runtimes; the five `String`s are cloned out of `self` into the thread, settling the `'static` requirement. Task 2.1 was restructured into four labelled bullets stating the reason inline, and § Design › Patterns' "Own-thread, own-runtime teardown" row now says the guard holds no client either, and why.
- **Promotes to ADR:** yes

### [7] [plan-review] The feature was wired into mod.rs, but three further gates stood behind it

- **Finding:** Round 2, raised as the unresolved half of round-1 finding [4] — the same failure mode one level deeper. Task 1.1 covered `tests/common/mod.rs` correctly, but `mod.rs` is not the only gate. Verified against the tree: `tests/common/lakekeeper.rs:18` carries its own file-level `#![cfg(feature = "lakekeeper-e2e")]`; `tests/common/stack.rs:2-6` carries its own file-level `#![cfg(any(exasol-e2e, cloud-e2e, lakekeeper-e2e))]` and has **no** `mod.rs`-level gate at all (`mod.rs:33` is a bare `pub mod stack;`), so that attribute is the only thing admitting the module; and `stack.rs:263`/`stack.rs:377` carry item-level `#[cfg(any(exasol-e2e, lakekeeper-e2e))]` on `lakehouse_engine_so_path` and `local_stack_connection_password`, both imported unconditionally by `e2e_harness.rs:13-17`. With only task 1.1's original edits the build still fails under `--features azure-e2e`: `stack` compiles to nothing, deleting `CatalogConnectionPassword` (the type task 1.4 extends) and `wait_for_url` (imported by `lakekeeper.rs:22`). Round-1 finding [4] recorded "Verified against the file" — singular, and that was the gap: `mod.rs` was verified, the modules it admits were not.
- **Direction change:** Task 1.1 now carries the exact edit set as a four-item list covering `mod.rs`, `lakekeeper.rs`'s file-level gate, `stack.rs`'s file-level gate (with the no-`mod.rs`-gate fact stated), and `stack.rs`'s two item-level gates. § Design › Architecture's box names all three gating files instead of `mod.rs` alone. Task 4.3 opens with `cargo test --features azure-e2e --no-run` as a compile-only completeness check before any Azure account or Docker stack is involved, and the Checklist gained a matching row.
- **Promotes to ADR:** no

Findings [6] and [7] were fixed in a manual, user-directed pass rather than an automatic round-3 iteration: the review loop is capped at two rounds, and the user asked for this round explicitly after reading `review/round-2.md`. The same pass applied all five of round 2's advisories — the `futures::FutureExt::catch_unwind` form in task 3.3 (plain `std::panic::catch_unwind` re-enters the nested-runtime panic, because the guard's construction is async); the omitted `warehouse` field in task 2.3, decision [6], and the spec's scan scenario; the `azure_offline_` name prefix plus a fewer-than-four-tests failure condition in task 4.4, the Checklist, and the coverage table; the Group B split into 2.2 → 2.3 and 2.1 → 2.4; and the § Summary and § Impact sentence-length splits.
