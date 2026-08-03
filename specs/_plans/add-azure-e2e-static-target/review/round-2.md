# Plan Review Findings: add-azure-e2e-static-target (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 7 (Blockers: 2, Advisory: 5)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- **Resolved: [UNSTATED_ASSUMPTION] Drop can't `.await`; `Handle::block_on` aborts mid-unwind** —
  plan.md § Design › Patterns now carries the "Own-thread, own-runtime teardown" row naming the
  nested-runtime panic; task 2.1 replaces the `.await` with "spawn a `std::thread`, build a fresh
  `tokio::runtime::Runtime` inside it, `block_on(container_client.delete(None))` there, and join",
  plus "Report a join or delete failure through `eprintln!` and never panic from `Drop`"; spec.md's
  panic-cleanup scenario gained "*THEN* the guard SHALL delete that container, including when the
  scope ends inside an active Tokio runtime context"; task 3.3 now runs its `catch_unwind` scope
  inside `rt.block_on(…)`. The round-1 defect is gone. A **new**, distinct defect in the replacement
  mechanism is raised below (BLOCKER, Feasibility) — the round-1 finding itself is closed.

- **Resolved: [HIDDEN_DEPENDENCY] hierarchical namespace was never stated** — plan.md § Dependencies
  has a dedicated row requiring "A **StorageV2 account with hierarchical namespace enabled**", the
  one-way-upgrade caveat, and `az storage account show -n <account> --query isHnsEnabled` named as a
  precondition of task 4.2; § Impact carries the same requirement; the Checklist has a `Precondition`
  row; spec.md § Background quotes Lakekeeper v0.13.1 `docs/docs/storage.md` verbatim.

- **Resolved: [REQUIREMENT_CONFLICT] adopt-on-exists vs never-delete-what-you-didn't-create** —
  spec.md's panic-cleanup scenario no longer contains either clause. It now reads "*AND* a name
  collision at create time SHALL fail the run … *AND* a container already absent at delete time
  SHALL be treated as deleted", which is a writable pass/fail pair. Task 2.1 dropped
  "guarding with `.exists()`" and keeps only `StorageErrorCode` matching, with the required
  `UnknownValue` and failed-`TryFrom` arms.

- **Not resolved (partially): [TRACEABILITY_GAP] the `azure-e2e` feature is not wired through the
  gates that actually exist** — task 1.1 now covers `tests/common/mod.rs`, and that half is correct
  and verified. But `mod.rs` is not the only gate. `tests/common/lakekeeper.rs:18` carries its own
  file-level `#![cfg(feature = "lakekeeper-e2e")]`, and `tests/common/stack.rs:2-6` carries its own
  file-level `#![cfg(any(feature = "exasol-e2e", feature = "cloud-e2e", feature =
  "lakekeeper-e2e"))]`, plus item-level gates at `stack.rs:263` and `stack.rs:377`. None of these is
  named in any artifact, and with only task 1.1's edits the build still fails under
  `--features azure-e2e`. Raised as a BLOCKER under Task Breakdown with the full edit set.

## Premortem

Two ways the *revised* plan fails:

1. **The suite hangs instead of aborting — a worse failure than the one just fixed.** Round 1's
   process-abort is gone, replaced by an own-thread teardown that reuses an HTTP client whose
   connection pool is pinned to the outer current-thread runtime. That runtime is blocked in
   `thread::join()` while the teardown thread waits on a connection only that runtime can drive.
   `make test-e2e-azure` never returns; CI burns the 45-minute timeout; the container is orphaned
   anyway. Task 3.3, the test written to prove cleanup, is the first thing to hang. → BLOCKER 1.
2. **Implementation stalls on day one at a compile error the plan says it fixed.** Task 1.1 is
   executed exactly as written, and `cargo test --features azure-e2e` still fails: `common::stack`
   compiles to nothing behind its own inner `#![cfg]`, so `e2e_harness`'s
   `use super::stack::{… lakehouse_engine_so_path, local_stack_connection_password …}` cannot
   resolve, and `common::lakekeeper` is empty behind a second inner `#![cfg]`. The implementer
   patches gates ad hoc across three files, and whichever item-level gate they miss surfaces later
   as an unrelated-looking error. → BLOCKER 2.

## Intent Fidelity

[no objection — axis checked: the deliverable is still `make test-e2e-azure` over the static
account-key path with slice F excluded, matching the intent verbatim. The round-1 SCOPE_CREEP
advisory is closed: task 2.4 now reads "Do NOT rename the configuration value or its entry points"
with the 18-occurrence cost stated inline. Credential segregation still holds — plan.md
§ Architecture routes only the container guard through the service principal, and spec.md's scan
scenario asserts the CONNECTION carries "neither a static S3 storage field nor any Entra ID
service-principal field". No decision settled in the interview is re-litigated here]

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks task 2.1; plan.md § Design › Patterns ("Own-thread,
  own-runtime teardown" row)
- Issue: the round-1 repair moves the *runtime* into a fresh thread but leaves the *client* behind.
  Task 2.1 builds `BlobServiceClient::new(…)` / `.blob_container_client(name)` in the async
  constructor, uses it for `create(None).await` on the fixture's runtime, then prescribes
  "`block_on(container_client.delete(None))`" on a different runtime inside the teardown thread.
  An `azure_core` HTTP client is a `reqwest`/`hyper_util` client, and a pooled keep-alive connection
  is driven by a task spawned on the runtime that opened it. Reusing that client from a second
  runtime hands the request to a task on runtime A — which, at that exact moment, is blocked in the
  guard's own `thread::join()` and polling nothing. The delete future never completes, `join()`
  never returns, and the whole suite deadlocks with no timeout to break it. The container is
  orphaned regardless. Task 3.3 is the first test to hit it, and it hits it on the normal path too,
  not only on panic. The plan never states the load-bearing belief — that the client is
  runtime-portable — and it is false in the common case, because `create()` runs seconds earlier and
  leaves an idle pooled connection. A second, smaller gap: `Drop` takes `&mut self` while
  `std::thread::spawn` needs `'static`, so the plan must say what is moved into the thread.
- Fix: In plan.md § Implementation Tasks task 2.1, state that `AzureContainer` stores only plain
  owned data — account name, container name, and the three Entra ID values — and constructs
  `ClientSecretCredential`, `BlobServiceClient`, and `BlobContainerClient` **inside** the teardown
  thread, on that thread's own runtime, rather than moving the construction-time client across
  runtimes; state the reason (a pooled connection is driven by the runtime that opened it, and that
  runtime is blocked in the guard's `join()`, so a reused client deadlocks instead of deleting).
  Replace "`block_on(container_client.delete(None))`" with "build the container client in the thread
  and `block_on` its `delete(None)` there". In plan.md § Design › Patterns, extend the "Own-thread,
  own-runtime teardown" row's Why with "and holds no client either — both the runtime and the HTTP
  stack are built inside the teardown thread, because a client's pooled connection is bound to the
  runtime that opened it".

[no objection on EFFORT_MISESTIMATION — axis checked: the two `[expert]`-tagged multi-file tasks
(2.1, 2.4) each carry their own scope statement, and task 2.4 now quantifies its blast radius
(18 occurrences, 117 KB `seed.rs`, 12 test binaries)]

[no objection on HIDDEN_DEPENDENCY — axis checked: HNS, the RBAC role, the four secrets plus one
variable, and the `opendal-azdls` feature are all § Dependencies rows with verification commands.
Verified independently: `crates/lakehouse-engine/Cargo.toml`'s features are all `= []` with no
optional dev-dependencies, so `azure-e2e = []` pulls in no further declaration; and CI's
`unit-tests` job (`.github/workflows/ci.yml:176`) both runs `cargo test --workspace` and sits in
`release`'s `needs` (line 641), so task 4.4's claim about reaching the release gate holds]

[no objection on NFR_IGNORED — axis checked: the account key's unscopable blast radius is now a
§ Impact paragraph and a spec § Background bullet, and the release-gate/fork-PR exposure is
decision [9] with its accepted cost stated]

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: plan.md § Implementation Tasks task 3.3
- Issue: task 3.3 says "create a container inside a `catch_unwind` scope that panics" and "Run the
  `catch_unwind` scope from inside an `rt.block_on(…)`", without saying which `catch_unwind`. The
  two readings are not interchangeable. `std::panic::catch_unwind` takes a synchronous closure, and
  container creation is `async` — so an implementer taking the obvious reading must reach for
  `Handle::block_on` inside that closure to construct the guard, which panics with "Cannot start a
  runtime from within a runtime": the exact trap round 1 blocked, re-entered through the test
  written to prove it was fixed. The workable form is `futures::FutureExt::catch_unwind` over an
  `AssertUnwindSafe(async { … })` block, awaited inside `rt.block_on`. `futures` is already a
  dev-dependency, so this costs nothing but must be named.
- Fix: In plan.md § Implementation Tasks task 3.3, replace "a `catch_unwind` scope" with
  "`futures::FutureExt::catch_unwind` over an `AssertUnwindSafe(async { … })` block awaited inside
  `rt.block_on(…)`", and state that `std::panic::catch_unwind` is not usable here because the guard's
  construction is async and driving it with `Handle::block_on` re-enters the nested-runtime panic.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks task 2.3; decision-log.md § Design Decisions [6];
  spec.md § Scenarios "End-to-end scan over the static-credential ADLS warehouse returns correct rows"
- Issue: task 2.3 and decision [6] both enumerate the Azure CONNECTION password field by field —
  "OAuth2 catalog fields plus `account_name`/`account_key`, leaving `endpoint`, `region`,
  `access_key`, `secret_key`, and `session_token` empty" — and both omit `warehouse`. That is the
  one field the adapter hard-rejects when empty:
  `crates/lakehouse-engine/src/adapter/connection.rs:112` returns
  "CONNECTION '{name}' password is missing required field" before any Azure validation runs. The
  omission matters more than usual here because the warehouse name carries the per-run suffix
  (decision [5]), so it is not a constant an implementer can copy from the MinIO builder. The spec
  scenario has the same hole: its CONNECTION precondition lists OAuth2 auth, `account_name`,
  `account_key`, and `use_vended_credentials`, but never the warehouse.
- Fix: In plan.md § Implementation Tasks task 2.3, add that the builder sets `warehouse` to the
  per-run ADLS warehouse name, and note that `validate_creds` rejects an empty `warehouse` outright.
  In decision-log.md § Design Decisions [6], add `warehouse` to the "sets" list. In spec.md's
  end-to-end scan scenario, extend the CONNECTION `*AND*` clause to "a CONNECTION naming the per-run
  warehouse and supplying OAuth2 catalog authentication plus `account_name` and `account_key`".

[no objection on REQUIREMENT_CONFLICT — axis checked: the round-1 adopt-versus-never-delete conflict
is gone; the stale "same **two** variables" in the Make-target scenario now reads "the same five
variables directly", matching § Background's five; and `speq plan validate
add-azure-e2e-static-target` passes. The cited `vs-adapter/storage-backend-enum` exists in the
recorded library, and nothing in the new spec contradicts `e2e-harness/lakekeeper-e2e-harness`,
which still describes `s3`/`s3-compat` warehouses only — decision [8]'s no-delta call holds]

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md § Implementation Tasks task 1.1; plan.md § Design › Architecture;
  decision-log.md § Review Findings [4]
- Issue: task 1.1's wiring list stops at `tests/common/mod.rs`, but three of the four modules it
  admits are gated a second time inside their own files, and none of those gates is named anywhere
  in plan.md, decision-log.md, or spec.md. Verified against the tree:
  - `tests/common/lakekeeper.rs:18` — file-level `#![cfg(feature = "lakekeeper-e2e")]`. With only
    task 1.1's edits, `mod.rs` admits `pub mod lakekeeper;` and the file then compiles to nothing
    under `--features azure-e2e`, so tasks 2.2, 2.3, and 3.1 have no module to extend.
  - `tests/common/stack.rs:2-6` — file-level `#![cfg(any(feature = "exasol-e2e", feature =
    "cloud-e2e", feature = "lakekeeper-e2e"))]`. `stack` has no per-module gate in `mod.rs` at all
    (`mod.rs:33` is bare `pub mod stack;`), so task 1.1 touches nothing that reaches it. Under
    `--features azure-e2e` the module is empty — which deletes `CatalogConnectionPassword`, the very
    type task 1.4 is written to extend, and `wait_for_url`, which `lakekeeper.rs:22` imports.
  - `tests/common/stack.rs:263` (`lakehouse_engine_so_path`) and `tests/common/stack.rs:377`
    (`local_stack_connection_password`) — item-level `#[cfg(any(feature = "exasol-e2e", feature =
    "lakekeeper-e2e"))]`. Both are imported unconditionally by `tests/common/e2e_harness.rs:13-17`,
    the module task 3.1 provisions the scan path from, so both must admit `azure-e2e` or
    `e2e_harness` fails to compile even once `stack` itself is admitted.
  decision-log [4] states the fix was "Verified against the file" — singular, and that is the gap:
  `mod.rs` was verified, the modules it admits were not. This is round-1 BLOCKER 4's failure mode
  intact, one level deeper.
- Fix: In plan.md § Implementation Tasks task 1.1, extend the wiring beyond `mod.rs` with the exact
  edit set: add `feature = "azure-e2e"` to `tests/common/lakekeeper.rs`'s file-level
  `#![cfg(feature = "lakekeeper-e2e")]`, to `tests/common/stack.rs`'s file-level `#![cfg(any(…))]`,
  and to the item-level `#[cfg(any(…))]` on `stack.rs`'s `lakehouse_engine_so_path` and
  `local_stack_connection_password`; state that `stack` has no `mod.rs`-level gate, so its file-level
  attribute is the only thing admitting it. In plan.md § Design › Architecture, change the
  `tests/common/mod.rs` box to name all three gating files — "`mod.rs` gates, plus `stack.rs`'s and
  `lakekeeper.rs`'s own file-level `#![cfg]`" — rather than `mod.rs` alone. In plan.md
  § Implementation Tasks task 4.3, add `cargo test --features azure-e2e --no-run` as the check that
  the gate set is complete before any Azure account is involved.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks task 4.4; plan.md § Verification › Checklist (row "Test
  (Azure offline checks)")
- Issue: task 4.4 fixes the round-1 coverage gap by "running just those tests by name filter under
  `--features azure-e2e`", but a libtest name filter that matches nothing exits 0 — it reports
  "0 passed; 0 failed; N filtered out" and the CI step is green. So the moment one of the four test
  names is renamed, or a fifth offline check is added, the step silently stops covering it, with no
  signal. That is the same "green and proves nothing" outcome task 4.4 exists to prevent, and it is
  invisible rather than red. The Checklist row compounds it: its command is
  `cargo test --features azure-e2e --test e2e_azure_test -- <the four offline test names>`, where
  `<the four offline test names>` is an unresolved placeholder, so neither the task nor the checklist
  pins the filter to anything.
- Fix: In plan.md § Implementation Tasks task 4.4, require the four offline tests to share a common
  name prefix (`azure_offline_`) and the CI step to run `cargo test --features azure-e2e --test
  e2e_azure_test -- azure_offline_`, and require the step to fail when fewer than four tests ran —
  either by asserting the count in the step or by adding a fifth test that fails if the prefix set
  is incomplete. In plan.md § Verification › Checklist, replace `<the four offline test names>` with
  the literal `azure_offline_` filter. In plan.md § Verification › Scenario Coverage, rename the four
  affected Test Name entries to carry that prefix.

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Parallelization (Group B)
- Issue: Group B lists 2.1, 2.2, 2.3, 2.4 as parallel, and two pairs collide on one file each.
  Tasks 2.2 and 2.3 both edit `crates/lakehouse-engine/tests/common/lakekeeper.rs` — 2.2 extracts the
  shared POST helper and adds the ADLS warehouse function, 2.3 adds the Azure CONNECTION-password
  builder — and 2.2's extraction moves the code 2.3 sits next to. Tasks 2.1 and 2.4 both edit
  `crates/lakehouse-engine/Cargo.toml` — 2.1 adds three dev-dependencies, 2.4 adds
  `features = ["opendal-azdls"]` to an existing one. Run concurrently by separate implementers,
  each pair is a write conflict on a file the other is mid-edit in; the § Parallelization
  "Sequential dependencies" list records neither.
- Fix: In plan.md § Parallelization, split Group B into "2.2 → 2.3 (same file; 2.2's extraction moves
  the code 2.3 extends)" and "2.1 → 2.4 (both edit `crates/lakehouse-engine/Cargo.toml`)", and add
  both orderings to the "Sequential dependencies" list.

[no objection on remaining TRACEABILITY — axis checked: all ten spec scenarios appear in
§ Verification › Scenario Coverage with a task implementing each, the round-1 location disagreement
on `missing_azure_credential_variable_fails_loud` is resolved in favour of the pure `require_var`
unit test in `common/azure.rs` (task 3.4 explicitly disclaims it), and no task implements anything
outside the spec]

## Design Depth

[no objection — axis checked. The revision does not shift the module boundaries round 1 certified:
`common/azure.rs` still owns the five credential readers, the container-name derivation, and the
container lifecycle; the ADLS warehouse body still has one owner in `lakekeeper.rs`; the seed storage
choice still has one owner in `seed.rs`; the CONNECTION field shape still has one owner in
`stack.rs`. Two revision-introduced structures were checked against the Quick Diagnostic. The
`require_var(name, value) -> String` split (task 1.5) looks like a pass-through pair, but the thin
`std::env::var` caller exists to keep the pure half testable without `unsafe` process-environment
mutation under edition 2024 — a stated reason, not classitis. The own-thread teardown (task 2.1)
deepens `AzureContainer` rather than leaking: applying BLOCKER 1's fix pushes client construction
inside the guard too, so no caller learns that teardown has a runtime at all. Task 2.4's dropped
rename removes churn without changing any boundary. The one tactical shortcut — cleanup depending on
unwinding — still has its scheduled follow-up in spec.md's known-ceiling bullet and task 1.6. No
production module depends on any of it]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary, lines 5-9; plan.md § Impact, lines 129-133
- Issue: § Summary honours the two-sentence cap but not the 25-word sentence cap — its first sentence
  runs about 50 words ("Stand up `make test-e2e-azure` as the single home for every Azure E2E case
  and fill it with the static-credential path: a per-run Azure container, a Lakekeeper `adls`
  warehouse with `sas-enabled: false`, and an end-to-end scan reading `abfss://` with the account key
  carried in the Exasol CONNECTION."), packing the target, the container, the warehouse profile, and
  the scan into one breath. § Impact's opening operator sentence runs about 33 words and inlines five
  variable names mid-clause. Both are governed prose, and both are the sections a PR reviewer reads
  first.
- Fix: In plan.md § Summary, split the first sentence in two under 25 words each — one naming the
  deliverable (`make test-e2e-azure` as the single home for every Azure E2E case), one naming what
  fills it this slice (a per-run container, an `adls` warehouse with `sas-enabled: false`, an
  `abfss://` scan on the CONNECTION account key). In plan.md § Impact, move the five variable names
  out of the opening sentence into a two-line list split by purpose (data path versus container
  lifecycle).

[no objection on PROSE_UNCLEAR — axis checked: the round-1 "two structural departures" miscount now
reads "three structural departures" against its three-item list, and spec.md's feature description
is two sentences of 16 words each]
