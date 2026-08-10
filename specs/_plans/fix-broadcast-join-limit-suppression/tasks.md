# Tasks: fix-broadcast-join-limit-suppression

## Phase 2: Implementation (Group A)
- [x] 2.1 Classify the join window in one owner, from the request alone — replace `join_requires_exasol_postprocessing` with `classify_join_window(&Json) -> JoinWindowPlan` in `joins/planning.rs`; dispatch in `plan_join` at the boolean's current position; add unit test `aggregate_over_join_classifies_before_the_render_that_would_error`. [expert]

## Phase 2: Implementation (Group B)
- [x] 2.2 Dispatch `build_broadcast_join_sql` on the classification — add `JoinSpec.post_join_limit`, move the scan-side read to `join.post_join_limit`, return `Option<String>` from the builder, wire all four arms (Unbounded / BareLimit / Ordered / decline), add serde + builder unit tests. [expert]

## Phase 2: Implementation (Group C)
- [x] 2.3 Update the join unit-test suite in `joins/sql_builders_tests.rs` — repoint imports off the deleted symbol; convert every `join_requires_exasol_postprocessing` call to a `JoinWindowPlan` assertion; rewrite `post_processing_predicate_covers_every_forcing_clause`; keep the CAST-to-CHAR coverage on a still-forcing fixture.
- [x] 2.4 Pin the post-join guarantee in `scan_join_test.rs` — migrate the `join_spec` helper's `limit` param onto `post_join_limit`; add `join_limit_bounds_joined_output_not_scanned_input` with a first-`n`-rows-match-nothing fixture and a plan-shape assertion.
- [x] 2.5 Extend the E2E join suite in `e2e_join_test.rs` — add bare-LIMIT, ORDER BY+LIMIT, bare-ORDER-BY / ORDER BY+LIMIT+OFFSET, and still-fallback (offset-no-order, aggregate) tests; delete `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan`; rewrite the `:107-116` comment.
- [x] 2.6 Update the doc comments the change invalidates — `join_fan_out_scan_spec`, `build_join_sql`, `CommonScanSpec.limit`, `build_broadcast_join_sql`, `topn::wrap_declined_order_by`, E2E harness `capped_result_sets`, `declared_cap_truncates_returned_row_count`; state the pre/post-join asymmetry once in `JoinSpec::post_join_limit`.

## Phase 3: Verification
- [x] 3.1 cargo fmt (no changes)
- [x] 3.2 cargo clippy --all-targets (0 errors/warnings)
- [x] 3.3 cargo test (1091 passed, 0 failed, 2 pre-existing ignored)
- [x] 3.4 make test-e2e (244 passed, 0 failed; live Exasol Docker stack)
- [x] 3.5 speq plan validate fix-broadcast-join-limit-suppression (pass)

## Phase 4: Review Fixes
- [x] 4.1 In crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs, remove `detected_join` from the `use super::super::tests::{…}` import list on line 2, and delete the trailing blank line at the end of the file so it ends with a single newline after the final `}`. Then run `cargo fmt` and confirm `cargo clippy -p lakehouse-engine --all-targets` no longer warns about this file.
- [x] 4.2 In crates/lakehouse-engine/tests/scan_join_test.rs, delete the blank line at line 241 so the `fn join_spec` declaration immediately follows its `///` doc comment, resolving clippy's `empty_line_after_doc_comments` warning.
- [x] 4.3 In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs, run `cargo fmt` to collapse the multi-line `planning` import at line 1 onto one line and to reduce the double blank line near line 1675 to a single blank line. After fixing all three files, re-run `cargo fmt --check` (expect no diff) and `cargo clippy -p lakehouse-engine --all-targets` (expect zero warnings) to confirm the gate is clean.
