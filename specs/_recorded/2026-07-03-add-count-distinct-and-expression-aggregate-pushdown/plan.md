# Plan: add-count-distinct-and-expression-aggregate-pushdown

## Summary

Close the Q9b performance gap (67.27s vs Trino's 15.19s on the live 2-node `test1` cluster
over 180M-row `lineitem`) by extending single-group aggregate pushdown to cover the two
select-list shapes that currently force a full 16-column raw row-scan fallback: aggregates
over renderable scalar expression arguments (e.g. `SUM(LENGTH(L_COMMENT))`) and single-group
`COUNT(DISTINCT col)`. Tracked as GitHub issue #56.

## Design

### Context

Root cause (confirmed this session via `EXPLAIN VIRTUAL` against `test1`): `detect_aggregates`
is all-or-nothing across the whole `selectList`. `parse_agg_item` returns `None` for any item
with `distinct: true` and for any aggregate whose argument is not a bare `column` node, so the
presence of either `COUNT(DISTINCT ...)` or `SUM(LENGTH(...))` collapses the ENTIRE query to a
raw `LAKEHOUSE_SCAN(...)` that ships all 16 lineitem columns to Exasol, which then does every
aggregate itself. Q9b contains both shapes, so it never carries an `aggregates` field at all.

- **Goals** — push Q9b's aggregation (including its four `COUNT(DISTINCT)` low-cardinality
  columns and its `SUM(LENGTH(...))`) into node-local DataFusion partial aggregation, keeping
  the wire transfer to per-shard partials instead of 180M×16 raw cells.
- **Non-Goals** — GROUP BY + `COUNT(DISTINCT)` (grouped distinct); join pushdown; any bespoke
  SQL string-splitting / `CONNECT BY` rewrite; changing the one-row-per-shard partial wire
  shape; passing Arrow types across the `.so` boundary; guaranteeing high-cardinality
  `COUNT(DISTINCT)` completes (it is bounded to fail cleanly instead).

### Decision

Two independent extensions to the existing decomposable single-group aggregate library, both
reusing the established partial/merge machinery in `crates/lakehouse-engine/src/adapter/pushdown.rs`
and `crates/lakehouse-engine/src/scan/mod.rs`.

**1. Expression-argument aggregates.** The aggregate argument is rendered via the shared
`vs_expression::render_expression` (the same mechanism GROUP BY keys already use). The rendered
SQL fragment is carried on the plan as a new `AggregatePlan.arg_expr: Option<String>`; the scan
side substitutes it verbatim (no `quote_ident`), and partial/merge column types are read from the
aggregate item's declared type in `selectListDataTypes` (there is no source column to look up).

**2. Single-group COUNT(DISTINCT).** A new `AggKind::CountDistinct`. Per shard, DataFusion
computes `array_agg(DISTINCT <arg>)` (NULLs excluded); the scan UDF serializes that single Arrow
List cell to a JSON array string IN Rust (Arrow never crosses the boundary) and emits it as one
`VARCHAR` partial value. The outer wrapper merges with an ordinary scalar function call —
`<scan_schema>.LAKEHOUSE_DISTINCT_MERGE_COUNT('[' || LISTAGG("PARTIAL_cd_i", ',') || ']')` — a
third entry point in the same `.so` that parses the JSON array-of-arrays, unions into a set, and
returns the cardinality. A mandatory per-shard safety cap bounds execution.

#### Architecture

```
Exasol pushdown req
  → detect_aggregates (single-group): accepts distinct COUNT + expr args
  → AggregatePlan { kind, column?, arg_expr? }  (scan/spec.rs)
  → per shard: LAKEHOUSE_SCAN → DataFusion
        SUM/MIN/MAX/AVG/COUNT(expr)  → PARTIAL_* numeric  (as today)
        COUNT(DISTINCT arg)          → array_agg(DISTINCT arg) → JSON string, capped
  → outer wrapper merge SELECT:
        SUM/MIN/MAX over PARTIAL_*                       (as today)
        LAKEHOUSE_DISTINCT_MERGE_COUNT('['||LISTAGG(cd)||']')  (new scalar UDF)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Render arg via `render_expression` | `parse_agg_item` | Same seam GROUP BY keys use; keeps translator the single source of expression truth |
| Declared-type-driven partial/merge types | `partial_emits_items`, `cast_merge_items` | Expression args have no source column; `selectListDataTypes` is authoritative |
| Arrow List → JSON string inside UDF | scan `partial_select_items` / emit | Honors "no Arrow across `.so`"; matches the incompatible-type→VARCHAR-via-JSON convention |
| Scalar merge UDF fed by LISTAGG | `merge_select_items` | Ordinary scalar call mixed into existing merge SELECT; no bespoke SQL rewrite |
| Per-shard bounded distinct-set cap | scan distinct accumulation | "Usable engine" bounded-execution: clean error over OOM / truncated wrong count |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Scalar merge UDF fed via `LISTAGG` of per-shard JSON arrays | SET merge UDF with its own grouping protocol; bespoke SQL split/`CONNECT BY` | Interview's preferred shape; keeps one-row-per-shard wire; avoids the explicit non-goal of complex SQL rewrites; mixes into the existing merge SELECT |
| Execution-time per-shard safety cap → clean `ResourcesExhausted`-style error | Plan-time NDV-based decline to row scan | Iceberg NDV stats are not reliably available; interview names execution-time cap the safer default. Trade-off: a standalone high-cardinality `COUNT(DISTINCT)` now errors cleanly instead of falling back to a (slow) row scan — accepted per the mission's bounded-execution stance (see decision log) |
| New `arg_expr` field rather than repurposing `column` | Overload `column` to hold rendered SQL | Keeps bare-column type lookups (MIN/MAX exact type) and existing JSON round-trip intact; backward-compatible serde |
| Merge total bounded by `LISTAGG`'s 2 MB VARCHAR ceiling | Streaming SET merge for unbounded cardinality | Adequate for the target low-cardinality dimension columns (Q9b); both the per-shard cap and the LISTAGG ceiling are bounded (no OOM), satisfying the mission constraint |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-expression-aggregate | NEW | `vs-adapter/pushdown-planning-expression-aggregate/spec.md` |
| vs-adapter/pushdown-planning-count-distinct | NEW | `vs-adapter/pushdown-planning-count-distinct/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| datafusion-scan/scan-execution-partial-agg | CHANGED | `datafusion-scan/scan-execution-partial-agg/spec.md` |
| packaging/single-so-two-entry-points | CHANGED | `packaging/single-so-two-entry-points/spec.md` |

## Requirements

| Requirement | Details |
|-------------|---------|
| Safety cap thresholds | Per-shard distinct set capped at MAX_DISTINCT_ELEMENTS_PER_SHARD = 100,000 elements AND MAX_DISTINCT_BYTES_PER_SHARD = 1,048,576 (1 MiB) serialized; whichever trips first aborts with a clean bounded-resource error. 1 MiB is well under the `VARCHAR(2000000)` wire limit, leaving headroom for the array-of-arrays LISTAGG wrapping; 100,000 bounds pre-serialization memory/CPU. |
| NULL semantics | `COUNT(DISTINCT col)` never counts NULL — the per-shard local set excludes NULLs. |
| No Arrow crossing | The `array_agg(DISTINCT)` Arrow List is serialized to a JSON string inside the UDF before emit; only the string crosses the boundary. |

## Dependencies

- `vs_expression::render_expression` (existing) — renders the aggregate argument expression.
- language-container-rs multiple-entry-points-per-`.so` (0.14.0+) — hosts the third (scalar) entry point.

## Implementation Tasks

1. **Spec types (foundation)**
   1. Add `AggKind::CountDistinct` and `AggregatePlan.arg_expr: Option<String>` (serde: skip when None) to `crates/lakehouse-engine/src/scan/spec.rs`; extend the round-trip tests to cover both.

2. **Adapter plan detection & typing** (after 1)
   1. Extend `column_from_first_arg` / `parse_agg_item` in `adapter/pushdown.rs` to render a non-bare-column argument via `render_expression` into `arg_expr` for COUNT/SUM/MIN/MAX/AVG (return `None`/fall back when it does not render); keep `detect_group_by_aggregates` rejecting `distinct:true`. [expert]
   2. Extend `detect_aggregates` (single-group only) to accept `distinct:true` COUNT as `AggKind::CountDistinct`; ensure the grouped path still declines distinct. [expert]
   3. Extend `partial_emits_items`, `col_type_for`, and `validate_agg_col_types` to derive partial/merge types from the declared aggregate type (via `aggregate_exasol_types`) for expression args, and to emit a `VARCHAR(2000000)` partial column for `CountDistinct`. [expert]

3. **Merge SQL & scalar merge UDF** (after 2)
   1. Extend `merge_select_items` / `cast_merge_items` to emit the scalar-merge call for `CountDistinct` and thread the schema-qualified merge-UDF name through `build_aggregate_scan_sql` and `build_scan_driving_sql` (resolved from `SCAN_SCHEMA`, like the scan UDF). [expert]
   2. Add the `LAKEHOUSE_DISTINCT_MERGE_COUNT` scalar entry point in `crates/lakehouse-engine/src/lib.rs` plus a pure `merge_distinct_count(json) -> u64` function (parse JSON array-of-arrays → `HashSet` → cardinality; empty/NULL-safe). [expert]

4. **Scan execution** (after 1; parallel with 2/3)
   1. Extend `partial_select_items` (`scan/mod.rs`) to substitute `arg_expr` verbatim (no `quote_ident`) when present for SUM/MIN/MAX/AVG/COUNT.
   2. Implement the `CountDistinct` per-shard path: `array_agg(DISTINCT <arg>)` (NULL-excluded), collect the single Arrow List cell, serialize to a JSON array string in Rust enforcing the safety cap (clean `UdfError` on overflow, no credential in message), emit as `Value::String`. [expert]
   3. Extend `emit_null_partial_row` so an empty `CountDistinct` shard emits `[]`.

5. **Capabilities & DDL wiring** (after 3)
   1. Advertise `FN_AGG_COUNT_DISTINCT` in `adapter/capabilities.rs`.
   2. Add `CREATE OR REPLACE ... SCALAR SCRIPT ... LAKEHOUSE_DISTINCT_MERGE_COUNT(...) RETURNS DECIMAL...` DDL to the E2E harnesses (`tests/e2e_scan_test.rs`, `tests/e2e_capability_test.rs`) and `deploy/scripts/cluster-up.sh`, resolving it in the same schema as the scan UDF. [expert]

6. **Tests & benchmark** (after 4, 5)
   1. Unit tests for detection/typing/merge-SQL (Tasks 2, 3).
   2. Unit tests for scan partial rendering + the distinct-set cap (Task 4).
   3. E2E correctness tests (dedup across shards, NULL handling, empty table, high-cardinality clean error, full Q9b) against local Exasol Docker.
   4. Update the Q9b `pushdown_check` assertion in `bench/run.sh` (now expects an `aggregates` field, not a raw 16-column scan); keep `bench/trino_compare.sh`, `bench/athena_compare.sh`, `deploy/scripts/spark_queries.py` query text in sync per `bench/README.md`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group 1 | 1.1 |
| Group 2 | 2.1 → 2.2 → 2.3 (sequential, same file); 4.1, 4.2, 4.3 (scan); 3.2 (scalar UDF + pure fn) |
| Group 3 | 3.1, 5.1, 5.2 |
| Group 4 | 6.1, 6.2, 6.3, 6.4 |

Sequential dependencies:
- Group 1 → Group 2 (types must exist first)
- Group 2 → Group 3 (merge SQL depends on detection/typing; DDL depends on the scalar UDF)
- Group 3 → Group 4 (tests exercise the wired path)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none) | — | This plan is purely additive; no existing aggregate path is removed. The old `column`-only argument path remains the fast path for bare-column aggregates. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| expr-agg: SUM over a scalar expression argument is pushed down | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `sum_length_expression_argument_pushed_down` |
| expr-agg: Expression-argument partial and merge column types come from declared type | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `expression_arg_partial_and_merge_types_from_declared_type` |
| expr-agg: Aggregate over untranslatable argument falls back to row scanning | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `unrenderable_agg_arg_falls_back_to_row_scan` |
| expr-agg: Bare-column aggregates continue to decompose unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `bare_column_aggregates_unchanged_regression` |
| count-distinct: Single-group COUNT(DISTINCT) decomposed into per-shard local sets | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `count_distinct_builds_local_set_scan_spec` |
| count-distinct: Scalar merge UDF unions per-shard distinct sets into the final count | Integration | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `count_distinct_merges_across_shards_dedup_null_empty` |
| count-distinct: scalar-merge pure union/count logic (dedup, NULL, empty) | Unit | `crates/lakehouse-engine/src/lib.rs` | `merge_distinct_count_unions_dedups_and_counts` |
| count-distinct: High-cardinality COUNT(DISTINCT) fails cleanly under the safety cap | Integration | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `high_cardinality_count_distinct_fails_cleanly` |
| count-distinct: Multiple COUNT(DISTINCT) columns merge independently | Integration | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `q9b_multiple_count_distinct_and_expression_agg` |
| pushdown-planning: Adapter advertises aggregate pushdown for supported functions (adds FN_AGG_COUNT_DISTINCT) | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `capabilities_advertise_count_distinct` |
| capability-extensions: Adapter falls back for non-decomposable aggregates (grouped COUNT(DISTINCT) still falls back) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_count_distinct_falls_back_to_row_scan` |
| partial-agg: Partial aggregate over an expression argument computed from rendered expression | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `partial_sql_uses_rendered_expression_argument` |
| partial-agg: COUNT(DISTINCT) emits the shard's local distinct set as one VARCHAR partial value | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `count_distinct_partial_emits_json_array_null_excluded` |
| partial-agg: COUNT(DISTINCT) enforces a bounded per-shard safety cap | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `distinct_set_cap_returns_clean_error_no_credentials` |
| packaging: One crate exports all three entry points | Integration | `crates/lakehouse-engine/tests/two_entry_points_test.rs` | `so_exports_adapter_scan_and_distinct_merge_symbols` |
| packaging: Crate exports a scalar distinct-merge entry point in the same .so | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `distinct_merge_scalar_script_runs_from_same_so` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| expression-argument aggregates | `bash bench/run.sh` (Q9b) then inspect result | `SUM(LENGTH(L_COMMENT))` returns the correct total; `EXPLAIN VIRTUAL` for Q9b shows an `aggregates` field, not a raw 16-column `EMITS` |
| single-group COUNT(DISTINCT) | `SELECT COUNT(DISTINCT L_SHIPMODE) FROM <vs>.LINEITEM` via `exapump` on `test1` (DSN with `validateservercertificate=0`) | Correct distinct count (7 for TPC-H `L_SHIPMODE`), matching a single-node run |
| safety cap | `SELECT COUNT(DISTINCT L_ORDERKEY) FROM <vs>.LINEITEM` via `exapump` | Fails with a clean bounded-resource error naming the column and cap — no VM crash, no OOM |
| Q9b end-to-end | `bash bench/run.sh` | Q9b wall-clock materially below the 67.27s baseline (target: near Trino's ~15s), all Q1–Q9b still correct |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0; single `.so` exporting three entry points |
| Test (host unit) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures against local Exasol Docker |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
