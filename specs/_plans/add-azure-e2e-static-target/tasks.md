# Tasks: add-azure-e2e-static-target

## Phase 2: Implementation (Group A — target, gating, pure helpers)
- [x] 1.1 Declare `azure-e2e` cargo feature; gate `tests/common/mod.rs`, `tests/common/lakekeeper.rs`, `tests/common/stack.rs` (file-level + two item-level gates) for it
- [x] 1.2 Add `test-e2e-azure` Makefile target (single recipe line sourcing test.env + cargo invocation)
- [x] 1.3 Commit `test.env.example` naming all five variables, grouped by purpose
- [x] 1.4 Add `account_name`/`account_key` to `CatalogConnectionPassword`, extend existing tests
- [x] 1.5 Create `tests/common/azure.rs`: credential-variable readers (pure `require_var` + thin env caller) and container-name derivation, with unit tests [expert]
- [x] 1.6 Open follow-up GitHub issue for orphaned-container sweep; replace `(#TBD)` in spec with its number (#291)

## Phase 2: Implementation (Group B — harness seams)
- [x] 2.2 Extract shared warehouse-POST helper in `common/lakekeeper.rs`; add ADLS warehouse profile + creation function [expert]
- [x] 2.3 Add Azure CONNECTION-password builder in `common/lakekeeper.rs`
- [x] 2.1 Add azure_storage_blob/azure_identity/azure_core dev-deps; `AzureContainer` guard in `common/azure.rs` (own-thread teardown, no client/runtime stored) [expert]
- [x] 2.4 Carry storage backend on shared seed-catalog config in `common/seed.rs`; dispatch on it (MinIO vs ADLS) [expert]

## Phase 2: Implementation (Group C — the suite)
- [x] 3.1 Write `tests/e2e_azure_test.rs`: OnceLock setup + `AzureFixture` [expert]
- [x] 3.2 End-to-end test: storage profile, abfss:// paths, projection/filter/LIMIT query correctness, script DDL shared-harness check
- [x] 3.3 Container-guard panic test (catch_unwind + AssertUnwindSafe inside rt.block_on)
- [x] 3.4 Remaining tests: stack-unavailable fail-loud, credentials-never-in-output, test.env gitignore hygiene, Makefile target shape

## Phase 2: Implementation (Group D — CI and live verification)
- [x] 4.1 Add `E2E (Azure)` CI job (fork-PR guarded, not in release needs) [expert]
- [x] 4.3 `cargo test --features azure-e2e --no-run`; clippy all suites; `cargo fmt`; re-run both MinIO suites
- [x] 4.4 Add four offline checks to always-run CI job with a `N >= 4` count guard
- [ ] 4.2 Run `make test-e2e-azure` against real Azure account; confirm pass + container cleanup + account-key path [expert]

## Phase 4: Review Fixes
- [x] 4.5 Split `delete_reached_desired_state`/`is_name_collision` out of the two container-guard matches in `tests/common/azure.rs`, cover both spec clauses with an `azure_offline_` unit test, and raise the CI offline-check floor to the new count [expert]
- [x] 4.6 Fix test.env.example's header comment to point at specs/e2e-harness/azure-e2e-harness/spec.md instead of the in-flight plan dir
- [x] 4.7 Move the "Lakekeeper management API" banner + `lakekeeper_warehouse_storage_profile` above the "Unit tests" banner in `common/lakekeeper.rs` so it again sits immediately before `#[cfg(test)] mod tests`
- [x] 4.8 Rewrite the "Azure offline checks" CI step's prose comment block in `.github/workflows/ci.yml` to name the real count (9) and composition of offline checks (leave the already-correct `-lt 9` guard and its inline comment untouched)
- [x] 4.9 Remove `pub` from `tenant_id`/`client_id`/`client_secret` in `common/azure.rs` (keep `account_name`/`account_key` public)
- [x] 4.10 Add `MIN_CONTAINER_NAME_LEN: usize = 3` const in `common/azure.rs` and use it in `assert_legal_container_name`'s range check and message in place of the bare `3`
- [x] 4.11 Replace the `body.contains("liblakehouse_engine.so") || body.contains("udf")` assertion in `e2e_azure_test.rs` with `body.contains(SO_UDF_OBJECT_PATH)` (imported from `common::e2e_harness`)
- [x] 4.12 Add `pub fn panic_payload_message` to `common/stack.rs`; route `common/azure.rs`'s `panic_message`, `e2e_azure_test.rs`'s `azure_credentials_never_appear_in_output`, and `e2e_lakekeeper_test.rs`'s equivalent block through it (leave `cloud_e2e_test.rs` untouched)

## Phase 3: Verification
- [~] 5a Automated checks — all green except live `make test-e2e-azure` (blocked on external 403, see verification-report.md)
- [x] 5b Scenario coverage audit — 7/10 rows pass; 3 blocked on the same external precondition
- [~] 5c Manual verification steps — all pass except the live-account run (blocked)
- [x] 6 Generate verification-report.md
