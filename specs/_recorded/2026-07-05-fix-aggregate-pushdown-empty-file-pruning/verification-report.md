# Verification Report: fix-aggregate-pushdown-empty-file-pruning

**Generated:** 2026-07-05

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Fixes #57: `handle_pushdown` now returns a shape-correct empty result (row-scan, single-group aggregate, or grouped aggregate) when Iceberg file-pruning eliminates every file. All checks green; one correctness bug found in code review (grouped branch missing the non-empty path's `validate_agg_col_types` gate) was fixed and re-verified. |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (covered by E2E — see Notes) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`, full workspace) | 461 | 459 | 2 |
| Integration/E2E (`make test-e2e`) | 56 | 56 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT COUNT(DISTINCT id) FROM <vs>.distinct_probe WHERE id > 1000` → one row, `0` | ✓ (via `count_distinct_all_files_pruned_returns_zero`) |
| `SELECT SUM(id) FROM <vs>.distinct_probe WHERE id > 1000` → one row, `NULL` | ✓ (via `sum_all_files_pruned_returns_null`) |
| `SELECT id, COUNT(*) FROM <vs>.distinct_probe WHERE id > 1000 GROUP BY id` → zero rows | ✓ (via `grouped_aggregate_all_files_pruned_returns_no_rows`) |

## Tool Evidence

### Linter

```
cargo clippy --all-targets
No issues found
```

### Formatter

```
cargo fmt -- --check
(clean, no diff)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-empty-result | Row-scan query with all files pruned returns a typed empty projection | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_file_list_returns_empty_select` | Pass |
| vs-adapter | pushdown-planning-empty-result | Single-group aggregate with all files pruned returns one shape-correct empty row | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_agg_sql_emits_zero_and_null_row_cast_to_declared_types` | Pass |
| vs-adapter | pushdown-planning-empty-result | Non-COUNT `AggKind` literals map to `NULL` | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_agg_literal_maps_non_count_kinds_to_null` | Pass |
| vs-adapter | pushdown-planning-empty-result | Single-group COUNT(DISTINCT) with all files pruned returns zero, no merge UDF | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_agg_sql_count_distinct_emits_zero_no_merge_udf` | Pass |
| vs-adapter | pushdown-planning-empty-result | Grouped aggregate with all files pruned returns zero rows in grouped shape | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_grouped_sql_emits_zero_rows_in_grouped_shape` | Pass |
| vs-adapter | pushdown-planning-empty-result | Grouped output includes constant projection columns | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_grouped_sql_includes_constant_projection_column` | Pass |
| vs-adapter | pushdown-planning-empty-result | Empty-result shape matches the plan the non-empty path would commit to | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_result_sql_dispatches_by_plan_shape` | Pass |
| vs-adapter | pushdown-planning-empty-result | Grouped aggregate over a non-numeric column with all files pruned demotes to row-scan shape | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_files_grouped_non_numeric_aggregate_demotes_to_row_scan` | Pass |
| vs-adapter | pushdown-planning-empty-result | Grouped aggregate over a non-numeric column + HAVING, all files pruned, declines | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `empty_files_grouped_non_numeric_aggregate_with_having_declines` | Pass |
| vs-adapter | pushdown-planning-empty-result | Single-group COUNT(DISTINCT), all files pruned (E2E) | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `count_distinct_all_files_pruned_returns_zero` | Pass |
| vs-adapter | pushdown-planning-empty-result | Single-group SUM, all files pruned (E2E) | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `sum_all_files_pruned_returns_null` | Pass |
| vs-adapter | pushdown-planning-empty-result | Grouped aggregate, all files pruned (E2E) | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `grouped_aggregate_all_files_pruned_returns_no_rows` | Pass |

## Notes

- Manual Testing table from `plan.md` is satisfied by the E2E tests above, which run the identical SQL against a real Exasol/Iceberg stack — no separate manual click-through was performed since it would exercise the same code path with no added signal.
- Code review (Phase 4) found one correctness bug beyond what the plan anticipated: `empty_result_sql`'s grouped branch initially skipped the `validate_agg_col_types` gate the non-empty path applies at `pushdown.rs:2141`, so a grouped aggregate over a non-numeric column with all files pruned would have returned the grouped shape instead of demoting to the row-scan shape — reintroducing a #57-style mismatch for that one case. Fixed: the grouped branch now applies the same gate and mirrors the non-empty path's HAVING-decline `Err`; `empty_result_sql`'s signature changed from `Json` to `Result<Json, UdfError>` accordingly. Two new unit tests cover the fixed case and the HAVING-decline sub-case. Full suite re-verified green after the fix (counts above reflect the post-fix state).
- Two low-severity/informational findings from code review were left as-is (not blocking): (1) the "cast unless declared type is the VARCHAR(2000000) default" rule is expressed three times across `cast_merge_items`/`empty_agg_sql`/`empty_grouped_sql` with a harmless divergence in the last — a follow-up simplification pass (`/ponytail:ponytail`) is the next step before recording; (2) `empty_grouped_sql`'s `filter_map` would silently drop a column on an out-of-range slot rather than fail loudly, which is unreachable by construction given the slots are derived from the same select-list vectors.
