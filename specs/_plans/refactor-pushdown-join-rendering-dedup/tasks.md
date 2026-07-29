# Tasks: refactor-pushdown-join-rendering-dedup

No parallelization — every task edits `joins/rendering.rs` and/or `joins/sql_builders.rs`; run sequentially in task-number order.

## Phase 1: Baseline and close the decline-message coverage gap
- [x] 1.1 Read spec.md/decision-log context on case-folding and wrapper-deletion constraints before touching code
- [x] 1.2 On unmodified HEAD, run `cargo test -p lakehouse-engine` and confirm the four existing golden tests pass
- [x] 1.3 Add `golden_n_scan_render_decline_messages_unchanged` to `joins/sql_builders.rs`, asserting all six decline messages verbatim (must pass against unmodified HEAD)
- [x] 1.4 Confirm `ineligible_join_decline`'s existing substring assertion targets the separate seventh template

## Phase 2: Finding 3 — one decline constructor for six sites
- [x] 2.1 Add private `join_render_decline(clause: &str) -> UdfError` to `joins/sql_builders.rs` with cross-reference doc comments to `ineligible_join_decline`
- [x] 2.2 Migrate all six decline sites to call `join_render_decline`
- [x] 2.3 Verify: `cargo test -p lakehouse-engine` (goldens + 1.3's test green), `cargo clippy --all-targets`, `cargo fmt --check`

## Phase 3: Finding 4 — `column_tables` returns its three outputs
- [x] 3.1 Replace `collect_column_tables` out-param function with `pub(super) fn column_tables(expr: &Json) -> (HashSet<String>, bool, bool)`; update `walk_column_nodes` doc comment reference in `adapter/pushdown/support.rs`
- [x] 3.2 Update both call sites (`conjunct_single_side`, `build_n_scan_join_from` loop) to destructure the tuple; update `sql_builders.rs` imports
- [x] 3.3 Verify as in 2.3

## Phase 4: Finding 6 — attach-point clarity and wrapper deletion
- [x] 4.1 Replace the `resolvable.then(…).flatten()` match in `build_n_scan_join_from` with a let-chain `if`, carrying the existing guard comment verbatim
- [x] 4.2 Delete `render_join_condition`; migrate its production and test callers to `vs_expression::render_expression_safe` directly; move its rationale comment to the production call site
- [x] 4.3 Delete `render_selectlist_item_qualified`; migrate all callers/test names to `render_expression_qualified`; relocate its doc comment's design intent onto `render_expression_qualified`
- [x] 4.4 Drop both names from `sql_builders.rs` imports; fix the stale doc-comment mention in `joins/rendering.rs:97`; run the `crates/`/`specs/` grep gate per plan §Implementation Tasks 4.4
- [x] 4.5 Verify as in 2.3, plus confirm the `joins` façade baseline (9 `pub(crate)` + 5 `pub(super)`) is unchanged and `src/adapter/pushdown_surface_probe.rs` still compiles untouched

## Phase 5: Finding 5 — one side-sharding helper
- [x] 5.1 Add private `shard_side(side: &ResolvedJoinSide, tuning: &JoinScanTuning) -> Vec<Vec<FileEntry>>` to `joins/sql_builders.rs`
- [x] 5.2 Replace the sharding prefix in `build_side_fan_out_sql` and `build_broadcast_join_sql` with calls to `shard_side`
- [x] 5.3 Verify as in 2.3; `golden_broadcast_join_sql_unchanged` and `golden_n_scan_join_sql_unchanged` are the direct proof

## Phase 6: Finding 1 — one clause walk, two divergent callers [expert]
- [x] 6.1 Add missing coverage for the three divergence cases (no-short-circuit projection narrowing, first-column fallback, full-set fallback) before any merge
- [x] 6.2 Add a non-ASCII characterisation test (e.g. `ß`) pinning the divergent case-folding behavior between `collect_all_column_names` and `collect_side_column_names`
- [x] 6.3 Add `pub(super) fn referenced_clause_values(pushdown_req: &Json, visit: impl FnMut(&Json))` to `joins/rendering.rs`
- [x] 6.4 Re-express `referenced_column_projection` in terms of `referenced_clause_values`, keeping its own case folding and fallback
- [x] 6.5 Re-express `referenced_side_columns` in terms of `referenced_clause_values`, keeping its early-return short-circuit before the walk
- [x] 6.6 Verify as in 2.3, plus 6.1/6.2's new tests and both pre-existing `referenced_side_columns_*` tests green with unedited assertions

## Phase 7: Full verification
- [x] 7.1 `cargo test` (whole workspace), `cargo clippy --all-targets` zero warnings, `cargo fmt` clean
- [x] 7.2 `make cross-musl-udf-build`, bring compose stack up, run `cargo test --features exasol-e2e --test e2e_join_test -- --test-threads=1`
- [x] 7.3 Confirm net line count fell and the plan's four Requirements all hold

## Review Fixes
- [x] 8.1 In `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, restore the doc comments on `build_side_fan_out_sql` and `build_broadcast_join_sql` verbatim from `git show HEAD:crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`
- [x] 8.2 In `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, restore the deleted doc comments on `build_n_scan_join_from`, `n_scan_join_select_items`, `qualified_join_group_by`, `qualified_join_having`, and `qualified_join_order_by` verbatim from `git show HEAD:crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, changing the intra-doc link `` [`collect_column_tables`] `` to `` [`column_tables`] `` inside `build_n_scan_join_from`'s restored block
- [x] 8.3 In `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, rewrite `golden_n_scan_render_decline_messages_unchanged`'s doc comment to describe the post-refactor state (messages flow through the shared `join_render_decline` template), dropping the "today written out as six separate `UdfError::User` string literals" / "closes first" framing, keeping the final sentence about triggering each case directly against the producing function with an unrecognised node `type`
- [x] 8.4 In `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs`, restore the 9-line doc comment above `fn conjunct_single_side` verbatim from `git show HEAD:crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs`
- [x] 8.5 In `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs`, trim the inline comment above the `if !matches!(pushdown_req.get("selectList"), …)` guard in `referenced_side_columns` back to its single pre-existing line, `// Absent/empty select list ⇒ the wrapper projects every column (SELECT *).`, deleting the two added sentences about the guard running before the shared clause walk
