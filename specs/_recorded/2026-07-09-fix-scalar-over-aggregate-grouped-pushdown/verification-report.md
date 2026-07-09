# Verification Report: fix-scalar-over-aggregate-grouped-pushdown

**Generated:** 2026-07-09

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All checks green. Issue #82's `04000` hard-fail is fixed; the scalar-over-aggregate grouped select item pushes down (merged partial/merge wrapper), with a qualified single-table wrapper fallback for undecomposable shapes. Full host + E2E suites pass; code review found no blocking issues. |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (covered by E2E ground-truth comparison) |

## Test Evidence

### Test Results

| Type | Suite | Passed | Failed | Ignored |
|------|-------|--------|--------|---------|
| Unit | `lakehouse-engine` lib | 457 | 0 | 0 |
| Unit | `vs-expression` lib | 61 | 0 | 0 |
| Integration (E2E) | `e2e_capability_test` | 8 | 0 | 0 |
| Integration (E2E) | `e2e_count_distinct_test` | 6 | 0 | 0 |
| Integration (E2E) | `e2e_join_test` | 10 | 0 | 0 |
| Integration (E2E) | `e2e_positional_deletes_test` | 11 | 0 | 0 |
| Integration (E2E) | `e2e_scan_test` | 45 | 0 | 0 |

Four new host unit tests (in `crates/lakehouse-engine/src/adapter/pushdown.rs`) and
two new E2E tests (in `crates/lakehouse-engine/tests/e2e_scan_test.rs`) are included
in the counts above and all pass.

## Tool Evidence

### Build

```
make cross-musl-udf-build  →  exit 0
(rust:1.94-bookworm container; compiled lakehouse-engine v0.24.4 release .so)
```

### Tests

```
cargo test -p lakehouse-engine --lib  →  test result: ok. 457 passed; 0 failed
make test-e2e                         →  E2E_EXIT=0 (80 E2E tests, 0 failed)
  test_group_by_scalar_over_aggregate_round ... ok
  test_group_by_shared_inner_aggregate_dedup ... ok
```

### Linter

```
cargo clippy --all-targets  →  0 warnings
```

### Formatter

```
cargo fmt --check  →  no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-grouped-agg | Single-table grouped select item that is a scalar function wrapping aggregates is pushed down | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_scalar_over_aggregate_round` | Pass |
| vs-adapter | pushdown-planning-grouped-agg | Nested aggregates are rewritten to their merged partial expressions, never rendered over source columns | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_scalar_over_aggregate_renders_merged_partials` | Pass |
| vs-adapter | pushdown-planning-grouped-agg | Inner aggregates shared across the grouped select list decompose into deduplicated partial columns | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_shared_inner_aggregate_dedup` | Pass |
| vs-adapter | pushdown-planning-grouped-agg | Scalar-over-aggregate items interleaved with keys and plain aggregates preserve select-list order | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_scalar_over_aggregate_preserves_selectlist_order` | Pass |
| vs-adapter | pushdown-planning-grouped-agg | Adapter falls back to a qualified single-table wrapper for an undecomposable grouped aggregate shape | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_undecomposable_falls_back_to_qualified_wrapper` | Pass |

Note: the detection/dedup unit test `grouped_scalar_over_aggregate_detects_and_dedups_inner_aggregates`
additionally covers the detection half of scenario 1 and the plan-list-dedup half of
scenario 3 at the unit level; the E2E tests confirm end-to-end correctness against
native ground truth.

## Notes

- **Adapter-only change.** No UDF scan-side or `crates/vs-expression` modification
  (decision-log [3]). The merged outer wrapper references only `GK_*` / `PARTIAL_*`
  columns; node-local aggregation is preserved for the decomposable path.
- **Merge renderer via substitution.** `render_scalar_over_merge` reuses
  `vs-expression::render_expression` by substituting each nested `function_aggregate`
  with a distinctive double-quoted sentinel column, rendering once, then replacing
  each sentinel with its merged `PARTIAL_*` expression — a deliberate, drift-free
  deviation from the plan's literal "mirror the arms" wording (same observable
  behavior; reviewed and judged sound, incl. token-collision safety).
- **Manual EXPLAIN VIRTUAL checks** from the plan's Manual Testing table are covered
  programmatically: the E2E tests assert pushdown shape via `assert_group_by_pushed_down`
  / `EXPLAIN VIRTUAL` inspection (merged wrapper, no `SELECT * FROM (…)` row-scan
  wrapper) and compare results to a native-table ground truth.
- **Non-blocking follow-up (out of scope):** a `MOD(...)` wrapping an aggregate would
  render the DataFusion `%` operator into Exasol-executed SQL (pre-existing for the
  HAVING catch-all, untested by #82). Flagged in the decision log for a separate issue.
- **Deferred sibling (decision-log [5]):** the no-GROUP-BY single-group
  scalar-over-aggregate (`SELECT ROUND(SUM(x)/COUNT(*),2) FROM t`) is a distinct code
  path, intentionally left for a follow-up.
