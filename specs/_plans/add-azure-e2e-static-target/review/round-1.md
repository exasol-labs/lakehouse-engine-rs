# Plan Review Findings: add-azure-e2e-static-target (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 15 (Blockers: 4, Advisory: 11)
- Intent Fidelity blockers: 0

## Premortem

Three ways this plan fails six months out:

1. **`make test-e2e-azure` never runs green in CI, and nobody notices.** It merges after one
   laptop run. The first CI execution fails at compile because `tests/common/mod.rs` was never
   taught the `azure-e2e` feature. That gets patched; the next run fails because the four
   repository secrets were never provisioned. Because the job neither gates releases nor runs on
   fork pull requests, the red check scrolls past. Slice C ships permanently unverified — the exact
   outcome this slice exists to prevent. → BLOCKER 1, ADVISORY 12, ADVISORY 9.
2. **Every panicking or cancelled run orphans a container, and the guard makes it worse.** `Drop`
   is implemented the obvious way — `Handle::current().block_on(delete)` — which panics inside the
   suite's existing `rt.block_on(...)` context. Panic-while-panicking aborts the process: nothing
   is deleted, and the operator sees an abort with no diagnosis instead of the original assertion
   failure. The storage account fills with `lhrs-e2e-*` containers and the sweep issue stays open.
   → BLOCKER 2, ADVISORY 8.
3. **The suite is green and proves nothing.** The test account turns out to be non-HNS, so a
   replacement account is provisioned mid-implementation. Meanwhile the four offline checks
   (container-name legality, missing-variable fail-loud, gitignore, Makefile shape) live behind
   `azure-e2e` and have not executed since the day they were written; a later refactor breaks the
   container-name sanitizer and the failure surfaces as an unattributable Lakekeeper
   filesystem-name rejection. → BLOCKER 4, ADVISORY 9.

## Intent Fidelity

[no objection on INTENT_DRIFT — the plan's deliverable is `make test-e2e-azure` over the static
account-key path with slice F excluded, matching the intent verbatim; every credential arrow in
plan.md § Architecture except the container guard carries the account key, and task 1.4's
`CatalogConnectionPassword` change is in `tests/common/stack.rs`, so "test-only, no production-code
changes" holds]

[no objection on SCOPE_REDUCTION — the lifecycle-policy sweep and slice F were both renegotiated
with the user in the interview and are recorded as such in decision-log.md § Interview; not
re-litigated here]

#### [SCOPE_CREEP] ADVISORY
- Location: plan.md § Implementation Tasks task 2.4; decision-log.md § Design Decisions (no entry)
- Issue: task 2.4 carries a cosmetic rename beyond the functional change: "Rename the configuration
  value and its entry points so the names no longer claim to be auth-only." The functional need is
  only to dispatch on a storage backend. `SeedCatalogAuth` / `build_seed_catalog_with_auth` /
  `seed_events_table_with_auth` have 18 occurrences across `tests/common/seed.rs` (117 KB) and
  `tests/e2e_lakekeeper_test.rs`, and `seed.rs` is compiled into 12 other test files. The rename
  traces to no user need, buys no behavior, and enlarges the diff in the one module whose breakage
  the plan's own checklist cannot detect (see ADVISORY under Task Breakdown). No decision-log entry
  justifies it.
- Fix: In plan.md § Implementation Tasks task 2.4, delete the sentence "Rename the configuration
  value and its entry points so the names no longer claim to be auth-only." Keep the storage-backend
  field and the `build_seed_catalog_with_auth` dispatch. If the rename is retained instead, add a
  decision-log.md § Design Decisions entry justifying it against the diff cost and list every
  affected call site.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks task 2.1; plan.md § Design › Patterns (RAII guard row);
  spec.md § Scenarios "Per-run container is deleted when its owning scope ends, including on panic"
- Issue: task 2.1 specifies "delete in `Drop` with `container_client.delete(None).await`".
  `Drop::drop(&mut self)` is synchronous — that line cannot compile. The plan never states how the
  async delete is driven from `Drop`, and the mechanism is not a detail: this suite's established
  pattern is a sync `#[test]` that builds `tokio::runtime::Builder::new_current_thread()` and calls
  `rt.block_on(async { … })` (`tests/e2e_lakekeeper_test.rs:122-137`). Task 3.1's `AzureFixture`
  composes async work (container create, `build_seed_catalog_with_auth`), so the guard will
  ordinarily be constructed and dropped inside a `block_on`. Calling `Runtime::block_on` or
  `Handle::block_on` from there panics with "Cannot start a runtime from within a runtime"; on the
  panic path exercised by task 3.3 that is a panic while unwinding, which aborts the process. The
  net result is the opposite of the stated pattern ("a panicking test still deletes its
  container"): no delete, no original failure message, and a process abort.
- Fix: In plan.md § Implementation Tasks task 2.1, replace "delete in `Drop` with
  `container_client.delete(None).await`" with an explicit runtime-safe mechanism — state that
  `Drop` spawns a dedicated `std::thread`, builds its own `tokio::runtime::Runtime` inside that
  thread, and `block_on`s the delete there, then joins, so the delete works whether or not `Drop`
  fires inside an existing runtime context. State that a join failure or delete failure is reported
  via `eprintln!`/`tracing` and never panics. Add to plan.md § Design › Patterns a row naming this
  as the reason the guard holds no runtime handle. Add an `*AND*` clause to spec.md's "Per-run
  container is deleted when its owning scope ends, including on panic" scenario requiring that the
  guard delete the container when `Drop` fires inside a Tokio runtime context, and add that case to
  the assertions of task 3.3.

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Dependencies ("A real Azure Storage account + account key"); plan.md § Impact;
  spec.md § Background
- Issue: the plan enumerates every other out-of-band Azure prerequisite in detail — service
  principal, the exact RBAC role, five secrets, which are variables and which are secrets — but
  never states that the storage account must have **hierarchical namespace (HNS)** enabled.
  Lakekeeper v0.13.1's own storage documentation makes this a setup instruction for ADLS
  warehouses, verbatim: "Make sure to select 'Enable hierarchical namespace' in the 'Advanced'
  section. For existing Storage Accounts make sure 'Hierarchical namespace: Enabled' is shown in
  the 'Overview' page" (`docs/docs/storage.md:633` at tag `v0.13.1`); Lakekeeper's ADLS backend
  drives the DFS `data_lake_client` surface throughout and performs no HNS detection or fallback.
  Enabling HNS on an existing account is a documented **one-way** upgrade with prerequisites (no
  page blobs; blob soft delete, container soft delete, snapshots, encryption scopes and immutable
  storage all disabled first). Task 4.2 is definition-of-done, so if the account named in the
  already-present local `test.env` is non-HNS, the plan cannot complete and the fix is an Azure
  admin action with a lead time the plan gives the operator no warning about.
- Fix: In plan.md § Dependencies, change the "A real Azure Storage account + account key" row to
  require a **StorageV2 account with hierarchical namespace enabled**, note that enabling HNS on an
  existing account is a one-way upgrade, and add the verification command
  (`az storage account show -n <account> --query isHnsEnabled`) as a precondition of task 4.2. Add
  the same requirement to plan.md § Impact's operator-facing paragraph and as a `spec.md`
  § Background bullet citing Lakekeeper v0.13.1 `docs/docs/storage.md`.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: plan.md § Implementation Tasks task 1.2
- Issue: task 1.2 describes the recipe as three sequential steps — "then load `./test.env` only
  when it exists (`if [ -f ./test.env ]; then set -a; . ./test.env; set +a; fi` …), then
  `cargo test --features azure-e2e --test e2e_azure_test -- --test-threads=1`" — without stating
  that the sourcing and the `cargo test` invocation must execute in **one** shell. Make runs each
  recipe line in its own shell, so written as two lines the sourced variables are discarded before
  `cargo test` starts and the suite fails on a missing variable even with a correct `test.env`.
  Every existing recipe in this Makefile is a single line, which makes the split reading the likely
  one, and the failure looks like a credential problem rather than a recipe problem.
- Fix: In plan.md § Implementation Tasks task 1.2, state that the `test.env` sourcing and the
  `cargo test` invocation MUST be a single recipe line (`&&`-joined, or one `if …; fi; cargo test …`
  line), and add that property to the assertions of `azure_make_target_rebuilds_so_and_runs_serially`
  in task 3.4.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md § Impact; plan.md § Consequences; decision-log.md § Design Decisions [1c]
- Issue: the plan analyses the service principal's blast radius in depth — decision [1c] weighs
  three role options and rejects two as over-privileged — but says nothing about
  `AZURE_STORAGE_ACCOUNT_KEY`, which is the more powerful of the two credentials. A storage account
  key cannot be scoped, rotated per-consumer, or restricted to one container; it grants full
  data-plane control over every container in the account and is about to be stored as a repository
  secret readable by every workflow run on the default branch. The plan's threat surface discussion
  is therefore inverted: careful about the least-privileged credential, silent about the
  unscopable one.
- Fix: In plan.md § Impact, add one paragraph stating that `AZURE_STORAGE_ACCOUNT_KEY` is an
  unscopable full-account credential, that the slice's design requires it (it is the path under
  test), and that the account SHOULD therefore be a dedicated test-only storage account holding no
  other data. Add a spec.md § Background bullet recording the same constraint.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: spec.md § Scenarios "Per-run container is deleted when its owning scope ends, including
  on panic"; plan.md § Implementation Tasks task 2.1
- Issue: the scenario asserts two clauses that cannot both hold:
  "*AND* a container that already exists SHALL be adopted rather than failing the run" and
  "*AND* the guard MUST NOT delete any container it did not itself create". An adopted container is
  by definition one the guard did not create, so the second clause forbids cleaning up the first —
  yet deletion on scope end is the guard's entire contract. No pass/fail test can be written from
  this. Task 2.1 contradicts itself on the same point: it prescribes "Create with
  `container_client.create(None).await`, guarding with `.exists()` rather than matching a 409" and
  then, one sentence later, "matching `StorageErrorCode::{ContainerAlreadyExists,
  ContainerNotFound}`" — two mutually exclusive collision strategies in one task line. The
  adopt-on-exists behavior is also unreachable by construction: the name carries a millisecond
  suffix, so a collision means a bug, not a condition to tolerate. Additional API facts the task
  must accommodate if it keeps structured matching: `StorageErrorCode` carries a catch-all
  `UnknownValue(String)` variant, and `TryFrom<azure_core::Error> for StorageError` is fallible
  (returning the original error), so both need arms.
- Fix: In spec.md § Scenarios, delete the clause "*AND* a container that already exists SHALL be
  adopted rather than failing the run, and a container already gone at delete time SHALL be treated
  as deleted" and replace it with two clauses: "*AND* a name collision at create time SHALL fail the
  run, because the millisecond-suffixed name makes a collision a defect rather than a tolerable
  state" and "*AND* a container already absent at delete time SHALL be treated as deleted". Delete
  the clause "*AND* the guard MUST NOT delete any container it did not itself create". In plan.md
  task 2.1, delete "guarding with `.exists()` rather than matching a 409" and keep only the
  `StorageErrorCode` matching, adding that the match MUST have arms for `UnknownValue` and for a
  failed `TryFrom` conversion.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: spec.md § Scenarios "The Azure Make target rebuilds the .so before running the suite"
- Issue: the scenario reads "*AND* the target SHALL load `test.env` into the environment when that
  file exists and SHALL NOT fail when it does not, because CI supplies the same **two** variables
  directly." There are five variables, as spec.md § Background states two bullets earlier ("in CI
  the job sets the same five"). The "two" is stale text from a pre-service-principal draft and
  contradicts the rest of the spec.
- Fix: In spec.md § Scenarios, change "because CI supplies the same two variables directly" to
  "because CI supplies the same five variables directly".

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: plan.md § Verification › Scenario Coverage (row "Azure suite fails when a required
  credential variable is absent"); plan.md § Implementation Tasks task 3.4
- Issue: the coverage table places `missing_azure_credential_variable_fails_loud` as a **Unit** test
  in `crates/lakehouse-engine/tests/common/azure.rs`, while task 3.4 places the
  missing-credential-variable fail-loud test in `e2e_azure_test.rs`. The two disagree on location
  and test type. The mechanism is also unstated and non-obvious: this crate is `edition = "2024"`,
  so `std::env::remove_var` is `unsafe` and process-global, and the suite's other tests require
  those same variables to be set — a test that unsets one mutates shared state for every test in
  the binary. Nothing in the plan says whether the reader under test is a pure function taking an
  `Option<&str>` (trivially testable) or reads the process environment (requiring unsafe mutation).
- Fix: In plan.md § Implementation Tasks task 1.5, state that each credential reader is split into
  a pure `fn require_var(name: &str, value: Option<&str>) -> String` that panics naming `name`, plus
  a thin `std::env::var` caller, and that the fail-loud unit test exercises the pure function with
  `None` and with `Some("")` — no process-environment mutation. Remove the
  missing-credential-variable item from task 3.4 and align plan.md § Verification › Scenario
  Coverage to that single location.

#### [COMPLETENESS_GAP] ADVISORY
- Location: decision-log.md § Design Decisions [4]; plan.md § Implementation Tasks task 3.3
- Issue: decision [4] justifies one shared fixture on the grounds that it "Halves the live-Azure
  cost and halves the orphan surface", but task 3.3 creates a second real container inside a scope
  that is made to panic deliberately. That container is created on every run of the suite, and it is
  created on precisely the code path where cleanup is least certain — so the stated orphan-surface
  rationale does not hold, and if the `Drop` mechanism is wrong (see the BLOCKER above) this test
  orphans a container on every single run. Neither the decision nor the spec's known-ceiling bullet
  accounts for it.
- Fix: In decision-log.md § Design Decisions [4], add a sentence stating that the container-guard
  test creates one additional short-lived container per run, and that this is the one place cleanup
  is exercised rather than assumed. In plan.md task 3.3, require the test to assert the container's
  absence via a fresh `exists()` call after the `catch_unwind` scope and to fail loudly (not
  silently pass) if the container survives.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md § Implementation Tasks (all groups); plan.md § Design › Architecture;
  spec.md § Scenarios "Azure binary provisions the scan path from the shared harness definition"
- Issue: no task wires the new `azure-e2e` feature into `crates/lakehouse-engine/tests/common/mod.rs`,
  and that file is never named anywhere in plan.md, decision-log.md, or spec.md. `mod.rs` carries a
  crate-level `#![cfg(any(feature = "exasol-e2e", feature = "cloud-e2e", feature =
  "lakekeeper-e2e"))]` plus per-module gates: `e2e_harness` and `seed` on
  `any(exasol-e2e, lakekeeper-e2e)`, `exasol_ws` on the three-way `any(...)`, and `lakekeeper` on
  `lakekeeper-e2e`. Under `cargo test --features azure-e2e --test e2e_azure_test` — exactly what
  task 1.2's Makefile target runs — the crate-level gate evaluates false and the whole `common`
  module compiles to nothing: no `stack`, no `seed`, no `lakekeeper`, no `e2e_harness`, no
  `exasol_ws`. Tasks 3.1 and 3.2 and the spec scenario above are unbuildable as written. The
  Parallelization table compounds this: Group A lists 1.1 and 1.5 as parallel, but 1.5's unit tests
  cannot compile until 1.1 has added the feature and the gates admit the module.
- Fix: Add a task 1.0 to plan.md § Implementation Tasks, ordered before 1.5, reading: add
  `feature = "azure-e2e"` to `tests/common/mod.rs`'s crate-level `#![cfg(any(...))]` and to the
  per-module gates on `e2e_harness`, `exasol_ws`, `lakekeeper`, and `seed`, and declare
  `#[cfg(feature = "azure-e2e")] pub mod azure;`. Name `tests/common/mod.rs` in plan.md § Design ›
  Architecture. In plan.md § Parallelization, move 1.1 and 1.0 ahead of 1.5 as a sequential
  dependency and record it in the "Sequential dependencies" list.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Verification › Checklist; plan.md § Implementation Tasks task 4.3
- Issue: task 4.3 claims the regression run proves "the shared-seam edits left the MinIO path
  intact", but it runs only `make test-e2e-lakekeeper`. There are two MinIO paths: the
  `lakekeeper-e2e` binary and the eight `exasol-e2e` binaries that `make test-e2e` runs. Tasks 2.2
  and 2.4 restructure `tests/common/lakekeeper.rs` and `tests/common/seed.rs`; `seed.rs` is reached
  by `seed::` from 12 test files, including the `exasol-e2e`-only `int96_fixtures` and
  `pos_delete_fixtures`. Neither `cargo test` (which compiles no feature-gated test binary) nor
  `cargo clippy --all-targets --features azure-e2e` type-checks that surface, because each binary
  is `#![cfg(feature = "...")]`-gated at its own top. The plan's checklist therefore cannot support
  the claim it makes, and a compile break in the `exasol-e2e` arm reaches CI unseen.
- Fix: In plan.md § Verification › Checklist, change the Lint row's command to
  `cargo clippy --all-targets --features exasol-e2e,lakekeeper-e2e,cloud-e2e,azure-e2e` and add a
  row "Test (regression, MinIO REST) | `make test-e2e` | 0 failures". Add the same two commands to
  plan.md § Implementation Tasks task 4.3 and narrow its claim to name both MinIO suites.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Verification › Scenario Coverage; crates/lakehouse-engine/Cargo.toml
- Issue: four of the ten scenarios need no Azure account and no Docker stack to check — container
  name legality, missing-credential-variable fail-loud, `test.env` gitignore hygiene, and the
  Makefile target's shape — yet all four are placed in files compiled only under `azure-e2e`. That
  feature is exercised by exactly one command, `make test-e2e-azure`, which requires a live Azure
  account, a `cross-musl-udf-build`, and the full local stack; the CI job running it is guarded to
  the same repository and deliberately excluded from `release`'s `needs`. These four checks
  therefore never run in `cargo test`, never run in the release gate, and never run on a fork pull
  request — they are documentation, not regression protection, and the container-name sanitizer is
  the one piece of real logic in this slice.
- Fix: In plan.md § Implementation Tasks task 1.5, place the container-name derivation and its
  reader helpers in an ungated location that plain `cargo test` compiles (a `#[cfg(test)]` module
  under `crates/lakehouse-engine/src/` is not appropriate for test-only code — use an ungated
  `crates/lakehouse-engine/tests/azure_harness_helpers.rs` integration target that the
  `azure-e2e` binary also uses, or drop the `azure-e2e` gate from those specific tests). Move the
  gitignore and Makefile-shape assertions into that same ungated target. Update plan.md
  § Verification › Scenario Coverage's Test Location column for those four rows and add a Checklist
  row asserting they run under plain `cargo test`.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks task 1.3; plan.md § Dependencies (last-but-one row);
  spec.md § Scenarios "Local credential file cannot be committed"
- Issue: task 1.3 says "Add `test.env` to `.gitignore`" — `.gitignore` already contains it, under
  the comment "# Test environment properties". A filled-in `test.env` also already exists in the
  working tree carrying all five variables, its comment naming the service principal
  `APP_EXA_RND_LAKEKEEPER_DEV`. So the plan states already-complete work as new, the spec's
  gitignore scenario is vacuously true before implementation starts, and plan.md § Dependencies'
  "The service principal may still need creating and role-assigning before task 4.2 can run" is
  stale about the half that is done while silent about what genuinely is unverified: whether that
  principal actually holds Storage Blob Data Contributor, and whether the four CI secrets and one CI
  variable exist. Those are the real gates on tasks 4.1 and 4.2.
- Fix: In plan.md § Implementation Tasks task 1.3, drop the `.gitignore` edit (state that `test.env`
  is already ignored) and keep only the `test.env.example` deliverable. In plan.md § Dependencies,
  replace the service-principal-may-need-creating sentence with the two verifiable preconditions:
  confirm the existing `APP_EXA_RND_LAKEKEEPER_DEV` principal's role assignment on the test account
  (`az role assignment list --assignee <client-id> --scope <account-id>`), and confirm the four
  repository secrets plus one repository variable exist before task 4.1's job can pass.

## Design Depth

[no objection — axis checked. `common/azure.rs` is deep against the complexity it hides (client
construction, credential wiring, name derivation, unwind-safe lifecycle) and its interface is
materially cheaper than reimplementing it. Single ownership holds per the Quick Diagnostic: the
container-name derivation lives only in `azure.rs`, the `adls` profile body only in
`lakekeeper.rs`, the seed storage choice only in `seed.rs`, and the CONNECTION field shape only in
`stack.rs`; the account name and key travel as parameters rather than being re-read per module.
Decision [7]'s extract-a-private-helper choice is the correct fix for the leaked
credential-safety decision a forked POST would have created, and decision [1a]'s
credential segregation is stated normatively in the spec rather than left to implementation. The
one tactical shortcut — cleanup depending on unwinding — is named in spec.md's known-ceiling bullet
with a follow-up issue, satisfying the scheduled-revisit test. No production module depends on any
of it, so the dependency rule is not engaged]

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Design › Context, line 19
- Issue: "Azure forces **two** structural departures from every existing E2E suite:" is followed by
  a numbered list of **three** items. A reader counting the list against the sentence has to stop
  and work out which item was meant to be subsumed.
- Fix: In plan.md § Design › Context, change "two structural departures" to "three structural
  departures".

#### [PROSE_BLOAT] ADVISORY
- Location: spec.md, the feature-description paragraph under `# Feature:` (lines 3-11)
- Issue: the governed feature description runs three sentences of roughly 35, 45, and 40 words,
  against the 25-word cap, and packs three ideas — what the suite verifies, which components are
  local versus cloud, and which credential axis this slice covers. The local-versus-cloud split and
  the fail-loud/credential-axis framing are both already stated as § Background bullets, so the
  description repeats them rather than leading with the conclusion.
- Fix: In spec.md, cut the feature description to two sentences under 25 words each — one naming
  what the suite verifies (`abfss://` reads against a real ADLS Gen2 account through the lakehouse
  VS), one naming the credential path in scope (static account key in the Exasol CONNECTION,
  Lakekeeper delegation off). Leave the local-versus-cloud split to the existing § Background
  bullet.
