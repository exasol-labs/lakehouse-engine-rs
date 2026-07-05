# Tasks: fix-aggregate-pushdown-empty-file-pruning

## Phase 2: Implementation (Group A)
- [x] 2.1 Hoist the plan-shape decision ahead of the `files.is_empty()` short-circuit in `handle_pushdown` (`crates/lakehouse-engine/src/adapter/pushdown.rs`) and dispatch to shape-specific empty-result builders: add `empty_agg_sql` (single-group, one row, per-`AggKind` literal cast to declared type; COUNT family → `0`, others → `NULL`; COUNT(DISTINCT) → `0` with no merge-UDF call) and `empty_grouped_sql` (grouped, zero rows via `WHERE 1=0`, full grouped output shape). Dispatch order: `detect_group_by_aggregates` → grouped; else `detect_aggregates(...).filter(validate_agg_col_types)` → single-group; else existing `empty_pushdown_sql` (row scan, unchanged). [expert]

## Phase 2: Implementation (Group B)
- [x] 2.2 Unit tests for the empty builders (pure SQL-string computation): `empty_files_single_group_aggregate_emits_zero_and_null_row`, `empty_files_count_distinct_emits_zero_no_merge_udf`, `empty_files_grouped_aggregate_emits_zero_rows_grouped_shape`, `empty_files_shape_matches_non_empty_plan_priority` — in `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests module).
- [x] 2.3 Restore the all-files-pruned E2E scenario removed by the #56 workaround in `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` (`WHERE id > 1000` full-prune sub-case: `count_distinct_all_files_pruned_returns_zero`), and add `sum_all_files_pruned_returns_null` and `grouped_aggregate_all_files_pruned_returns_no_rows`.

## Phase 2.5: Code Review Fixes
- [x] 2.4 Fix correctness bug found in review: `empty_result_sql`'s grouped branch skips the `validate_agg_col_types` gate the non-empty path applies (`pushdown.rs:2141`), so a grouped aggregate over a non-numeric column with all files pruned returns the grouped shape instead of falling through to the row-scan shape like the non-empty path does — reintroducing a #57-style column-count mismatch for that case. Apply the same gate, decide the HAVING-present/validation-fails sub-case consistently with the non-empty path's `Err`, correct the `empty_result_sql` doc comment's now-false blanket guarantee, and add a unit test for the previously-uncovered case (grouped aggregate over a non-numeric column, all files pruned). [expert]

## Phase 3: Verification
- [x] 3.1 Run build (`make cross-musl-udf-build`) — via `make test-e2e` (depends on it)
- [x] 3.2 Run host unit tests (`cargo test`) — 459 passed, 2 ignored
- [x] 3.3 Run E2E tests (`make test-e2e`) — 56 passed, 0 failed
- [x] 3.4 Run lint (`cargo clippy --all-targets`) — no issues
- [x] 3.5 Run format check (`cargo fmt`) — clean
