# Tasks: add-pushdown-plan-execution-boundary

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A)
- [x] 2.1 Run `make cross-udf-build`, bring up the local stack, and run `e2e_credential_exposure_test` once so the SLC, the `.so`, the scripts, and the two users exist.
- [x] 2.2 Verify live, against that running container, the five facts the plan depends on (cell layout, error text, DBA view spellings, necessary EXECUTE ON SCRIPT grants, CREATE OR REPLACE SCRIPT grant-survival). Record verbatim in task notes.
- [x] 2.3 Add a helper beside `explain_virtual_sql` in `crates/lakehouse-engine/tests/common/e2e_harness.rs` that returns only the adapter-generated pushdown statement for a single-table query, selected by cell per 2.2's layout.
- [x] 2.4 Add `the_reader_cannot_execute_the_pushdown_plan_it_captured` to `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs`; capture the isolated statement via the 2.3 helper against `reader_conn()`.
- [x] 2.5 Assert the isolated statement is executable-shaped (qualified script names, table root, projection, filter, VALUES file list) before asserting denial.
- [x] 2.6 Assert the reader's absent script-execute authority from the DBA views verified in 2.2 (object privilege, EXECUTE ANY SCRIPT, roles).
- [x] 2.7 Submit the isolated statement as the reader; assert `denied["status"] == "error"` with the wording captured in 2.2.
- [x] 2.8 Assert the denial error carries no `access_key`/`secret_key` VALUE, and that the reader's ordinary VS query still returns `SEED_ROWS_SCORE_GT_15` rows.
- [x] 2.9 Add the plan visibility versus plan execution section to `docs/security.md` per R1, after `## Privilege model`; amend the existing `EXPLAIN VIRTUAL` line to point to it.
- [x] 2.10 Amend the non-DBA note in `docs/install.md` per R2, naming only the necessary grants and the re-install outcome from 2.2.
- [x] 2.11 Run the gates in Verification § Checklist, including the feature-gated clippy pass.

## Phase 3: Verification
- [x] 3.1 Run automated checks (build, test, E2E, lint, lint E2E-feature, format)
- [x] 3.2 Scenario coverage audit
- [x] 3.3 Manual verification per plan's Manual Testing table

## Phase 4: Review Fixes
- [x] 4.1 In `crates/lakehouse-engine/tests/common/e2e_harness.rs`, rewrite `isolated_pushdown_statement` to bind `cols` before indexing and assert the `EXPLAIN VIRTUAL` layout (`cols.len() == 4`, `cols[0].len() == 1`) with a panic message carrying `query_sql` and the observed shape when either check fails; replace `.expect("PUSHDOWN_SQL cell is a string")` with a `panic!` naming `query_sql` and the observed cell value.
- [x] 4.2 In `crates/lakehouse-engine/tests/common/e2e_harness.rs`, add `const PUSHDOWN_SQL_COLUMN: usize = 1;` beside `isolated_pushdown_statement`, index with it in place of the bare literal `1`, and replace the four-line doc comment with a single line leading with the promise and naming the cell by name (`PUSHDOWN_SQL`) rather than by ambiguous ordinal.
- [x] 4.3 In `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs`, in `the_reader_cannot_execute_the_pushdown_plan_it_captured`, replace the single filter assertion at lines 261-264 with two assertions — a `"filter":` key-presence check and a `\"SCORE\"` filtered-column check — dropping the now-redundant `isolated.contains("SCORE")` conjunct; re-run the E2E test against the local stack and confirm all four tests pass.
- [x] 4.4 Probe live whether `LAKEHOUSE_DISTRIBUTE_FILES`-alone execute access is sufficient to defeat an adapter-injected predicate (grant it to `CREDEXP_READER`, submit the isolated `PUSHDOWN_SQL` statement, record the outcome, revoke, reconfirm 17 rows); append the outcome to `specs/_plans/add-pushdown-plan-execution-boundary/notes/A.md` as a sixth section, then rewrite `docs/security.md` line 26 to state only what the probe supports.
