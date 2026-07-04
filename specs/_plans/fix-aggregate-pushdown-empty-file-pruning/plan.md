# Plan: fix-aggregate-pushdown-empty-file-pruning

## Summary

Fix GitHub issue #57: when Iceberg file-pruning eliminates every data file,
`handle_pushdown` unconditionally returns the raw row-scan empty shape even for
aggregate requests, so Exasol rejects the pushdown with a column-count mismatch
(`sqlCode 04000`). Make the zero-files short-circuit shape-aware so it emits the
correct empty instance of whichever plan the non-empty path would have committed
to (grouped aggregate, single-group aggregate, or row scan).

## Design

### Context

`handle_pushdown` (`crates/lakehouse-engine/src/adapter/pushdown.rs`) resolves
the file list once at line ~2081, then short-circuits at line ~2085:

```rust
if files.is_empty() {
    return Ok(empty_pushdown_sql(&proj_cols, &proj_types));
}
```

`empty_pushdown_sql` is built purely from the raw row-scan projection
(`extract_projection`), so for an aggregate query it returns the wrong column
count/shape. The aggregate-shape detection (`detect_group_by_aggregates`,
`detect_aggregates`) runs *after* this short-circuit (line ~2127+), so at the
short-circuit the code does not yet know the request's plan shape.

Both detection functions are pure over `pushdown_req` (they do not depend on the
resolved files), and every input needed to synthesize a shape-correct empty
result (`col_types`, `aggregate_exasol_types`, group-key/aggregate declared
types, the `GroupedSelectItem` classification) is already available without any
file. So the fix is a control-flow change plus small per-shape builders — no
UDF, DataFusion, or execution change (VS stays thin, per CLAUDE.md).

- **Goals** — Return a positionally-valid, semantically-correct empty result for
  every request shape (row scan, single-group aggregate incl. COUNT(DISTINCT),
  grouped aggregate) when all files are pruned; keep the response shape identical
  to what the non-empty path would commit to.
- **Non-Goals** — No change to the non-empty scan/merge SQL; no new pushdown
  capabilities; no UDF/DataFusion changes; no change to file-pruning itself.

### Decision

Hoist the request-shape decision ahead of the zero-files short-circuit and
dispatch to shape-specific empty-result builders. Reuse the existing detection
and type helpers so the empty shape can never drift from the non-empty shape.

#### Architecture

```
handle_pushdown
  ├─ extract projection / filter / limit / col_types   (unchanged)
  ├─ resolve_file_list (once)                           (unchanged)
  └─ if files.is_empty():
        decide plan shape  (grouped → single-group[gated] → row scan)
          ├─ grouped         → empty_grouped_sql   (zero rows, grouped shape)
          ├─ single-group    → empty_agg_sql       (one row, per-AggKind literal)
          └─ row scan        → empty_pushdown_sql   (existing, unchanged)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Shape-parity: derive empty shape from the same helpers the non-empty path uses (`detect_*`, `aggregate_exasol_types`, group-key/aggregate type assembly) | empty builders | Empty and non-empty column shapes can never diverge |
| Per-`AggKind` empty literal (COUNT family → `0`, others → `NULL`), cast to declared type | `empty_agg_sql` | Matches single-node SQL over zero rows; mirrors the existing zero-count NULL guard (ADR-008) |
| Zero-row grouped result (`WHERE 1=0`) with full grouped output shape | `empty_grouped_sql` | Grouped output over zero rows is empty regardless of HAVING/ORDER BY/LIMIT — no need to render them |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Grouped empty = zero rows in grouped shape whenever `detect_group_by_aggregates` succeeds (skip numeric-validate/HAVING/ORDER-BY decline paths) | Mirror the non-empty path's `Err`/native-retry decline branches exactly | A zero-row result is correct under any HAVING/ORDER BY/LIMIT and always matches `selectListDataTypes`; replicating the decline branches adds risk and code for no user-visible difference (native retry also yields empty) |
| Single-group empty keeps the `validate_agg_col_types` gate (non-numeric aggregate → row-scan empty shape) | Always emit aggregate shape for a single-group aggregate | The non-empty path demotes a non-numeric single-group aggregate to a row scan, so `selectListDataTypes` reflects the row-scan shape; emitting the aggregate shape there would reintroduce the mismatch bug |
| New feature `pushdown-planning-empty-result` owns the cross-cutting behavior | Scatter near-duplicate scenarios across `pushdown-planning`, `-grouped-agg`, `-count-distinct` | The behavior cuts across all plan shapes; one coherent home avoids duplication |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-empty-result | NEW | `vs-adapter/pushdown-planning-empty-result/spec.md` |

## Implementation Tasks

1. **[expert]** Hoist the plan-shape decision ahead of the `files.is_empty()` short-circuit in `handle_pushdown` and dispatch to shape-specific empty-result builders:
   - Add `empty_agg_sql(aggregates, aggregate_types)` — one row; per-`AggKind` empty literal (`Count`/`CountCol`/`CountDistinct` → `0`; `Sum`/`Min`/`Max`/`Avg`/`VarPop`/`VarSamp`/`StddevPop`/`StddevSamp` → `NULL`) each wrapped `CAST(<literal> AS <declared-type>)`, `FROM DUAL` (no `WHERE`).
   - Add `empty_grouped_sql(...)` — zero rows; one `CAST(NULL AS <ty>)` per grouped output column (group-key types, aggregate types, and constant projections assembled in select-list order via `GroupedSelectItem`), `FROM DUAL WHERE 1=0`.
   - Dispatch order mirrors the non-empty path: `detect_group_by_aggregates` → grouped; else `detect_aggregates(...).filter(validate_agg_col_types)` → single-group; else `empty_pushdown_sql` (unchanged).
   - Neither aggregate builder references the scan UDF or the distinct-merge UDF. [expert]
2. Unit tests for the empty builders (pure SQL-string computation) — see Scenario Coverage.
3. Restore the all-files-pruned E2E scenario removed by the #56 workaround in `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` (the `WHERE id > 1000` full-prune sub-case), and add an all-pruned SUM (single-group) and an all-pruned grouped case.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1 |
| Group B | Task 2, Task 3 |

Sequential dependencies:
- Group A → Group B (tests exercise the builders from Task 1)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none) | — | `empty_pushdown_sql` is retained as the row-scan branch; no code becomes obsolete |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Row-scan query with all files pruned returns a typed empty projection | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `empty_file_list_returns_empty_select` (existing, keep) |
| Single-group aggregate with all files pruned returns one shape-correct empty row | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `empty_files_single_group_aggregate_emits_zero_and_null_row` |
| Single-group COUNT(DISTINCT) with all files pruned returns zero | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `empty_files_count_distinct_emits_zero_no_merge_udf` |
| Grouped aggregate with all files pruned returns zero rows in grouped shape | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `empty_files_grouped_aggregate_emits_zero_rows_grouped_shape` |
| Empty-result shape matches the plan the non-empty path would commit to | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `empty_files_shape_matches_non_empty_plan_priority` |
| Single-group COUNT(DISTINCT) with all files pruned returns zero (end to end) | Integration | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `count_distinct_all_files_pruned_returns_zero` |
| Single-group SUM with all files pruned returns NULL (end to end) | Integration | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `sum_all_files_pruned_returns_null` |
| Grouped aggregate with all files pruned returns zero rows (end to end) | Integration | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `grouped_aggregate_all_files_pruned_returns_no_rows` |

Rationale: the empty-result builders are pure JSON-in/SQL-out computation, so
their column-shape and per-`AggKind` semantics are covered by unit tests (per
mission.md: unit tests only for pure computation). Integration (E2E) tests
confirm Exasol actually accepts the response and returns the correct empty
result over a real all-pruned query.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-planning-empty-result | `SELECT COUNT(DISTINCT id) FROM <vs>.distinct_probe WHERE id > 1000;` | one row, value `0` (no `sqlCode 04000`) |
| pushdown-planning-empty-result | `SELECT SUM(id) FROM <vs>.distinct_probe WHERE id > 1000;` | one row, value `NULL` |
| pushdown-planning-empty-result | `SELECT id, COUNT(*) FROM <vs>.distinct_probe WHERE id > 1000 GROUP BY id;` | zero rows |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test (host unit) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
