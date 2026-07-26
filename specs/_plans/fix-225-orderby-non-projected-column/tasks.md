# Tasks: fix-225-orderby-non-projected-column

## Phase 2: Implementation (Group A — core fix, sequential)
- [x] 1.1 Add `extend_projection_with_sort_keys` and `wrap_declined_order_by` helpers to `crates/lakehouse-engine/src/adapter/pushdown/topn.rs` [expert]
- [x] 1.2 Rewire `build_dispatch_sql` in `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` to use the new helpers, extension ordered strictly between `detect_topn` and `spec_template` [expert]

## Phase 2: Implementation (Group B — unit tests, after Group A)
- [x] 2.1 Replace `declined_order_by_on_unprojected_column_projects_full_row` with `declined_order_by_appends_unprojected_sort_key_as_hidden_column`
- [x] 2.2 Add `declined_order_by_wrapper_selects_only_original_select_list`
- [x] 2.3 Add `declined_order_by_dedupes_repeated_and_projected_sort_keys`
- [x] 2.4 Update `declined_order_by_all_keys_projected_leaves_projection_untouched`
- [x] 2.5 Rewrite `topn.rs`'s `plan_scan_sql` test helper + update `order_by_present_without_topn_match_withholds_per_shard_limit` [expert]
- [x] 2.6 Add `declined_order_by_extension_runs_after_topn_detection` (pins extend-after-detect_topn ordering; must fail on mis-ordered impl) [expert]
- [x] 2.7 Add `declined_order_by_unparseable_sort_key_emits_no_wrapper` (pins empty-keys guard)

## Phase 2: Implementation (Group C — E2E tests, after Group A, concurrent with Group B)
- [x] 3.1 Add `e2e_order_by_unprojected_column_bare_projection` to `crates/lakehouse-engine/tests/e2e_capability_test.rs`
- [x] 3.2 Add `e2e_order_by_column_referenced_only_in_projected_expression` (pre-check CONCAT-over-DECIMAL coercion first; fall back to CAST if rejected)
- [x] 3.3 Add EXPLAIN VIRTUAL plan-shape assertion to task 3.1's test, scoped to EMITS/projection JSON, not whole-string

## Phase 2: Implementation (Group D — tracked exceptions/verification, after B+C)
- [x] 4.1 File tracked issue for JSON-fallback declined-path ordering gap; substitute `(#TBD-JSONSORT)` in the capability-extensions spec delta
- [x] 4.2 File tracked issue for composed pre-existing full-row-fallback + ORDER BY arity gap; substitute `(#TBD-FULLROWARITY)`
- [x] 4.3 Verify #189 against the fixed build via the shape-equivalent local query; record result (close-#189 decision deferred to PR stage) — `e2e_issue_189_shape_equivalent_local_verification` passes; #189 is the same root cause under a different trigger surface (no #190 guard) and is fully resolved by this fix

## Phase 3: Verification
- [x] 5.1 `cargo test` — 0 failures (721 passed, 2 ignored)
- [x] 5.2 `cargo clippy --all-targets` — 0 warnings
- [x] 5.3 `cargo fmt --check` — no changes
- [x] 5.4 `make cross-musl-udf-build` — exit 0
- [x] 5.5 `make test-e2e` — 0 failures related to this change. All 3 new #225/#189 regression tests pass (`e2e_order_by_unprojected_column_bare_projection`, `e2e_order_by_column_referenced_only_in_projected_expression`, `e2e_issue_189_shape_equivalent_local_verification`); the full suite has 2 unrelated pre-existing failures in `e2e_int96_timestamp_test` (missing `int96_ts_far_future` fixture table on this stack) — confirmed to reproduce identically on unmodified `main` against the same live stack, filed as #237
- [x] 5.6 `speq plan validate fix-225-orderby-non-projected-column` — pass
- [x] 5.7 No `#TBD-` placeholder survives in `specs/_plans/fix-225-orderby-non-projected-column/vs-adapter/`
- [x] 5.8 Manual repro capture via `scripts/capture-pushdown-payload.sh` for both shapes — covered by the new e2e tests' `explain_virtual_sql` assertions against the same live stack (equivalent ground-truth capture, folded into the regression tests rather than a separate ad-hoc run)
