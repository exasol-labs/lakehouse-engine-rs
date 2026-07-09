# Tasks: fix-scalar-over-aggregate-grouped-pushdown

## Phase 2: Implementation — Detection (Group A)
- [x] 2.1 Add `ScalarOverAggregate { select_index, node }` variant to `GroupedSelectItem`; in `detect_group_by_aggregates` (`pushdown.rs:837`), classify a select item that is neither aggregate/literal/group-key but wraps ≥1 nested `function_aggregate` (every other leaf a group key, group-key-derived expr, or literal) as `ScalarOverAggregate`. [expert]
- [x] 2.2 Fold each nested aggregate into the shared `AggregatePlan` list via `parse_agg_item`, deduplicating by `AggregatePlan` equality (kind + argument) — a `COUNT(*)` used bare and inside a scalar is one `PARTIAL_*` column; record per-item mapping to rewrite nested aggregates to shared plans. Decline (→ fallback) if any nested aggregate is `DISTINCT`, non-numeric, or has an untranslatable argument. [expert]

## Phase 2: Implementation — Outer wrapper (Group B)
- [x] 2.3 Generalize `render_having_operand` (`pushdown.rs:1876`) so a `function_scalar`/arithmetic node recurses into a merge-aware renderer rewriting *every* nested `function_aggregate` to its merged `PARTIAL_*` expression (matched to `plans` by `AggregatePlan` equality), preserving scalar/arithmetic structure — instead of delegating whole subtree to `render_expression`. Also fixes scalar-over-aggregate inside HAVING. [expert]
- [x] 2.4 In `build_grouped_aggregate_scan_sql` (`pushdown.rs:1423`), render each `ScalarOverAggregate` item at its `select_index` ordinal using the 2.3 renderer, wrapped in `CAST(... AS <selectListDataTypes[select_index]>)`; interleave with `GroupKey`/`Aggregate`/`Constant` items in `selectList` order. [expert]

## Phase 2: Implementation — Fallback (Group C)
- [x] 2.5 When the grouped path declines (undecomposable item), route to a qualified single-table wrapper: `SELECT <grouped select list via vs-expression, aggregates verbatim> FROM (<build_scan_driving_sql raw sharded fan-out>) GROUP BY <keys> HAVING <…> ORDER BY <…> LIMIT <n>`. Replace the grouped path's fall-through to bare row-scan `build_scan_driving_sql`. [expert]
- [x] 2.6 Ensure the fallback carries group keys, HAVING, ORDER BY, LIMIT into the outer wrapper (per-shard scan stays LIMIT-free), and emits the shape-correct empty result when the file list is empty. [expert]

## Phase 2: Dead code removal
- [x] 2.7 Remove the grouped path fall-through to bare row-scan `build_scan_driving_sql` (`pushdown.rs` ~2291) — replaced by 2.5's qualified wrapper (the `04000` bug for grouped requests).

## Phase 3: Host unit tests (`pushdown.rs` `#[cfg(test)]`)
- [x] 3.1 `detect_group_by_aggregates` over #82's select list classifies the `ROUND(… SUM(CASE …)/COUNT(*) …)` item as `ScalarOverAggregate` and folds its inner `SUM(CASE …)`+`COUNT(*)`, deduplicated against a bare `COUNT(*)` item. (`grouped_scalar_over_aggregate_...`) [expert]
- [x] 3.2 `build_grouped_aggregate_scan_sql` for #82 emits an outer wrapper whose scalar-over-aggregate column is `ROUND(… SUM("PARTIAL_*")/SUM("PARTIAL_*") …)` merged form (no source column), cast to declared type, at correct ordinal; column count = selectList length. (`grouped_scalar_over_aggregate_renders_merged_partials`) [expert]
- [x] 3.3 Interleaving test: scalar-over-aggregate before/between/after keys and plain aggregates yields outer SELECT in selectList order, each cast from `selectListDataTypes` at its own ordinal. (`grouped_scalar_over_aggregate_preserves_selectlist_order`) [expert]
- [x] 3.4 Fallback test: grouped request whose scalar-over-aggregate wraps `COUNT(DISTINCT …)` emits the qualified single-table wrapper (`SELECT … FROM (…) GROUP BY …`) with selectList-matching column count, NOT a bare `SELECT * FROM (…)`. (`grouped_undecomposable_falls_back_to_qualified_wrapper`) [expert]

## Phase 4: E2E (`tests/e2e_scan_test.rs`, local Exasol Docker)
- [x] 4.1 Phase-5 GROUP BY E2E `test_group_by_scalar_over_aggregate_round`: #82's query runs green through the VS, matches native-table ground truth, asserts pushdown via `assert_group_by_pushed_down` (merged wrapper, no `SELECT * FROM (…)` row-scan wrapper).
- [x] 4.2 Shared-inner-aggregate E2E `test_group_by_shared_inner_aggregate_dedup`: bare `COUNT(*)` + `ROUND(…/COUNT(*)…)`, one merged partial column, correct results.

## Phase 5: Code Review
- [x] 5.1 Review all changed files (code-reviewer agent) — no blocking issues

## Phase 6: Verification
- [x] 6.1 Build (`make cross-musl-udf-build`) → exit 0
- [x] 6.2 Test host (`cargo test`) → 0 failures (457 lib + 61 vs-expression)
- [x] 6.3 Test E2E (`make test-e2e`) → 0 failures (80 E2E tests)
- [x] 6.4 Lint (`cargo clippy --all-targets`) → 0 warnings
- [x] 6.5 Format (`cargo fmt --check`) → no changes
- [x] 6.6 Verification report
