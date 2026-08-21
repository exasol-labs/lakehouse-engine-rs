# Tasks: fix-concat-null-semantics

## Phase 2: Implementation (Group A)
- [x] 1.1 Branch the `"CONCAT"` arm of `render_expression_inner` (`crates/vs-expression/src/lib.rs:1155-1187`) on `dialect`: `Dialect::DataFusion` renders `nullif(concat(<args>), '')`, `Dialect::Exasol` keeps `("A" || "B")`. Add an arity floor erroring on an empty `arguments` list in both dialects.
- [x] 1.3 Correct the refuted "Exasol's `||` propagates NULL" / "CONCAT renders identically in both dialects" claim in the CONCAT arm's comment (`lib.rs:1155-1164`), the `Dialect` module doc CONCAT bullet (`:47-48`), the `TRANSLATED_SCALAR_FNS` row comment (`:134-136`), and the neighbouring `FLOAT_DIV` bullet (`:41-44`, which claims `FLOAT_DIV` alone diverges by dialect).

## Phase 2: Implementation (Group B, depends on Group A)
- [x] 1.2 Update `renders_concat_as_chained_pipe_operator` and `renders_concat_bool_operand_as_exasol_case` in `crates/vs-expression/src/lib_tests.rs` (~:1522-1571) to the new DataFusion rendering; add tests for the Exasol dialect byte-identical form, a nested `CONCAT` argument, a single-argument call, and an empty-argument-list error (raising + safe variants, both dialects). Every assertion is an exact rendered string.
- [x] 2.1 Add `test_concat_null_operand_concatenates_non_null_parts` to `crates/lakehouse-engine/tests/e2e_scan_test.rs`, following existing `setup_e2e()`/`exa_conn()`/`vs_table()`/`explain_virtual_pushdown_sql` conventions. Assert the VALUE, FILTER, and all-NULL-FILTER positions per the plan's task 2.1, plus an `EXPLAIN VIRTUAL` assertion that the pushed spec carries `nullif(concat(`. Ran against a live Exasol container: RED against the pre-fix `.so` (VALUE assertion failed, `None` vs `Some("event-01-suffix")`), rebuilt the `.so` via `make cross-musl-udf-build`, GREEN on rerun (`test_concat_null_operand_concatenates_non_null_parts ... ok`).

## Phase 3: Code Review
- [x] 3.1 Review all changed files for guardrail violations, dead code, test quality, bad comments, YAGNI/over-engineering, error handling, design depth. 3 findings (2 standard, 1 expert), all fixed. Standard: corrected TRANSLATED_SCALAR_FNS CONCAT row comment (was still misattributing NULL-propagation to DataFusion's concat()); added concat_missing_arguments_or_null_argument_errors_in_both_dialects test. Expert: added explain_virtual_pushdown_sql delegation assertions to the two FILTER-position checks in the E2E test — confirmed live that both filter shapes are delegated pre- and post-fix, and the new test passes against the rebuilt .so (1 passed, 0 failed).

## Phase 4: Verification
- [x] 4.1 Confirm `crates/lakehouse-engine/tests/boolean_to_string_casing_test.rs` passes with ZERO edits (`git diff --exit-code` clean) — this is task 2.2 of the plan, folded into orchestrator verification since it is a no-edit confirmation, not implementation work. Confirmed: diff clean, both tests pass.
- [x] 4.2 Run build, full test suite, clippy, fmt per plan's Verification > Checklist. All green (build via `make cross-musl-udf-build`, `cargo test` full workspace 0 failed, `cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo fmt --all -- --check` clean after one mechanical fixup). Full `make test-e2e` also run once as the single authoritative gate: 12 binaries, 0 failed.
- [x] 4.3 Scenario coverage audit against plan's Verification > Scenario Coverage table. All 13 rows confirmed present and passing.
- [x] 4.4 Manual testing per plan's Verification > Manual Testing (live Exasol container). All 6 commands run live, all matched expected output exactly.
- [x] 4.5 Generate verification-report.md. Done: specs/_plans/fix-concat-null-semantics/verification-report.md.
