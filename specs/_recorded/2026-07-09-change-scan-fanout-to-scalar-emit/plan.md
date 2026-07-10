# Plan: change-scan-fanout-to-scalar-emit

## Summary

Make the SCALAR-EMIT split fan-out the sole scan path — a nested `LAKEHOUSE_DISTRIBUTE_FILES`
LUA SET distributor (`GROUP BY shard_key`) does the cross-node fan-out and the
`LAKEHOUSE_SCAN` UDF becomes a SCALAR EMIT UDF that streams rows node-locally — so raw-row
scan output is no longer materialized into temp DB RAM, applied unconditionally across every
scan-driving wrapper. Closes #97.

## Design

### Context

The single-SET-UDF `GROUP BY shard_key` fan-out is load-bearing for round-robin cross-node
distribution, but for raw-row output it doubles as a MATERIALIZING operator: Exasol buffers the
UDF's emitted rows into temp DB RAM (a `tmp_subselect` temp table under `SELECT * FROM (...)`,
because a scalar-EMITS/MAP subselect can never be flattened — engine `subquery_elimination.cpp:105`,
`containsMapFunction()`), and that footprint grows with data scanned rather than staying bounded.
Measured on staging (`DBX.LINEITEM` ~210M rows, 17-col projection): one shard under `COUNT(*)`
peaked ~22 GB temp DB RAM; the full 8-shard scan could not finish (>32 GB at client timeout).

The fix separates cluster fan-out from the scan. A tiny LUA SET distributor carries only the
per-shard file-list strings (data-volume-independent) and does the `GROUP BY shard_key`
distribution; the scan runs as a SCALAR EMIT UDF over the distributed rows, and with no
top-level `GROUP BY` Exasol streams the scan output instead of buffering it.

- **Goals** — One scan path (no flag, no VS property, no mode branch). Convert EVERY
  scan-driving wrapper (raw, partial-agg merge, grouped-agg, count-distinct, top-n, broadcast
  join, N-scan join fallback) onto the nested-distributor + scalar-scan shape. Drop the
  `SELECT * FROM (...)` wrapper. Render the N-scan join fallback FROM as an `INNER JOIN … ON`
  chain with per-leg filter pushdown. Preserve result-equivalence (union/merge/grouped/top-n/join
  multiset equals single-node evaluation) for every converted path.
- **Non-Goals** — No A/B flag or planner mode branch (the spike's temporary flag does NOT ship).
  No second scan Rust entry point (`LAKEHOUSE_SCAN` itself becomes SCALAR; #94's separate
  `LAKEHOUSE_SCAN_SCALAR` is not adopted). No change to the sharding MATH (G = node_count ×
  parallelism_factor, capped 300, byte-balanced partition). No new pushdown capabilities.

### Decision

The scan `.so` keeps a single `LAKEHOUSE_SCAN` symbol; only the DDL script TYPE changes SET →
SCALAR. A new `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET script (created by DDL, NOT a Rust entry
point) does the fan-out. The shard-invariant `common` blob is spliced ONCE as the scalar scan's
first-argument literal; only the per-shard `files` list flows through the distributor.

Canonical multi-shard shape (raw):

```sql
SELECT LHVS.LAKEHOUSE_SCAN('{common}', files) EMITS (...)
FROM (
  SELECT LHVS.LAKEHOUSE_DISTRIBUTE_FILES(files)
  FROM (VALUES (0,'{f0}'),(1,'{f1}'),…) AS shards(shard_key, files)
  GROUP BY shard_key
)
[ORDER BY …] [LIMIT n]
```

Single-shard / single-file short-circuit (omit distributor + inner GROUP BY):

```sql
SELECT LHVS.LAKEHOUSE_SCAN('{common}', '{files}') EMITS (...) [ORDER BY …] [LIMIT n]
```

Aggregate / grouped / count-distinct wrappers put their merge (SUM/MIN/MAX, group-by, distinct
merge) in the OUTER ungrouped query over the scalar scan; the scalar scan fires once per shard
(the distributor emits one row per shard), so one partial row per shard is produced — identical
to the old SET path — and merge semantics are preserved.

#### Architecture

```
adapter/pushdown.rs (scan-driving SQL builders)
   build_fan_out_inner ─▶ nested distributor subquery (LUA SET, GROUP BY shard_key)
                          wrapped by outer ungrouped SCALAR LAKEHOUSE_SCAN('{common}', files)
   build_row_scan_sql / build_aggregate_scan_sql / build_grouped_aggregate_scan_sql /
   build_side_fan_out_sql / build_broadcast_join_sql / build_n_scan_join_sql
       └─ all reuse the new fan-out; SELECT * wrapper removed; ORDER BY/LIMIT direct
scan/mod.rs (run_scan)
   while ctx.next() { scan(row.files) }  — runtime built once per batch, torn down once
DDL (tests + Makefile + bench + docs)
   LAKEHOUSE_SCAN: SET SCRIPT → SCALAR SCRIPT
   + LAKEHOUSE_DISTRIBUTE_FILES: LUA SET SCRIPT (passthrough)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Separate fan-out from scan | distributor subquery vs scalar scan | Fan-out moves only file-list strings; scan streams, no materialization |
| Common-once literal | scalar scan first arg | Shard-invariant blob spliced once; only `files` varies per shard |
| Single-shard short-circuit | all wrappers | A scalar EMIT UDF over constant literals fires exactly once, no driving relation |
| Greedy-attach-by-tableName-set | N-scan join `INNER JOIN … ON` | Correct scope resolution independent of shared column names |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `LAKEHOUSE_SCAN` itself becomes SCALAR (one symbol) | #94's separate `LAKEHOUSE_SCAN_SCALAR` | One scan entry point; only DDL type changes; the compiled scan logic is shared |
| Distributor is a LUA SET script | A second Rust entry point in the `.so` | Passthrough needs no Rust; keeps the `.so` surface unchanged; created by plain DDL |
| Apply unconditionally, no flag | Keep the spike's local A/B flag | The materialization bug is universal; a mode branch is dead weight and a divergence risk |
| Drop `SELECT * FROM (...)` | Keep wrapper | The wrapper is the un-flattenable materialization boundary; nesting GROUP BY inside the distributor removes the need for it |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| parallelism/work-unit-sharding | CHANGED | `parallelism/work-unit-sharding/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/pushdown-planning-topn | CHANGED | `vs-adapter/pushdown-planning-topn/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg | CHANGED | `vs-adapter/pushdown-planning-grouped-agg/spec.md` |
| vs-adapter/pushdown-planning-join | CHANGED | `vs-adapter/pushdown-planning-join/spec.md` |
| vs-adapter/pushdown-planning-join-fallback | CHANGED | `vs-adapter/pushdown-planning-join-fallback/spec.md` |
| packaging/single-so-two-entry-points | CHANGED | `packaging/single-so-two-entry-points/spec.md` |
| packaging/e2e-harness | CHANGED | `packaging/e2e-harness/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |

## Implementation Tasks

1. Scan-side batch loop
   - [ ] 1.1 Convert `scan/mod.rs::run_scan` from a single-`ctx.next()` read to a `while ctx.next()` batch loop that scans each row's assigned file list, building the DataFusion runtime ONCE from the first row's thread config and reusing it across the batch, then tearing it down deterministically once via `run_on_runtime`/`shutdown_timeout`. Preserve `run_scan_async`/`run_on_runtime` teardown discipline. [expert]
   - [ ] 1.2 Add a scan-module test proving a multi-row batch scans every row (no row past the first dropped) and a single-row batch is byte-identical to the pre-batching output.

2. Pushdown SQL builder — fan-out primitive
   - [ ] 2.1 Add `DISTRIBUTE_FILES_UDF_NAME` constant and thread a `qualify_udf(scan_schema, …)` distributor name through the builders alongside `udf_name`/`merge_udf_name`.
   - [ ] 2.2 Rewrite `build_fan_out_inner` to emit the nested distributor subquery (`LAKEHOUSE_DISTRIBUTE_FILES(files) … FROM (VALUES …) AS shards(shard_key, files) GROUP BY shard_key`) wrapped by an outer ungrouped scalar `LAKEHOUSE_SCAN('{common}', files) EMITS (...)`, splicing `common` once as the scalar first-arg literal and flowing only `files` through the distributor. [expert]

3. Pushdown SQL builder — per-wrapper restructure (depends on 2)
   - [ ] 3.1 `build_row_scan_sql`: drop the `SELECT * FROM (...)` wrapper; attach `ORDER BY`/`LIMIT` directly to the outer ungrouped scalar select; single-shard → from-less scalar call on literals. [expert]
   - [ ] 3.2 `build_aggregate_scan_sql`: outer merge SELECT directly over the scalar scan (single-group SUM/MIN/MAX/COUNT/AVG-pair and the count-distinct scalar-merge call), no `SELECT * FROM (...)` between merge and scan; single-shard short-circuit.
   - [ ] 3.3 `build_grouped_aggregate_scan_sql`: move `GROUP BY shard_key` INTO the distributor; outer wrapper GROUP BYs the user keys over the scalar scan, preserving the select-list-order/typed-cast contract; single-shard short-circuit. [expert]
   - [ ] 3.4 `build_broadcast_join_sql` + `build_side_fan_out_sql`: fact/side fan-out uses the new distributor + scalar scan; drop the `SELECT * FROM (...)` wrapper.

4. Pushdown SQL builder — N-scan join fallback renderer (depends on 2, 3.4)
   - [ ] 4.1 Rewrite `build_n_scan_join_sql` FROM rendering from comma cross-join + flat `WHERE` to a left-to-right `INNER JOIN … ON` chain: greedy-attach each join condition by the SET of `tableName`s it touches (never by column name) to the earliest join point where all its tables are in scope; a join point with no newly-resolvable condition uses `ON 1=1`. [expert]
   - [ ] 4.2 Split `WHERE`: push each side's side-local conjuncts INTO that side's fan-out leg (already partially done via `side_local_filter`/`build_side_fan_out_sql`); keep only cross-table / OR-spanning / untagged residual conjuncts in the outer `WHERE`. [expert]

5. DDL — scripts (depends on 1, 2, 3)
   - [ ] 5.1 E2E test DDL: change `LAKEHOUSE_SCAN` from `SET SCRIPT` to `SCALAR SCRIPT` (dynamic `EMITS (...)`) and ADD a `CREATE LUA SET SCRIPT … LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000)) EMITS (files VARCHAR(2000000))` passthrough, in `e2e_scan_test.rs`, `e2e_join_test.rs`, `e2e_capability_test.rs`, `e2e_count_distinct_test.rs`, `e2e_positional_deletes_test.rs`, `tpch_loader.rs`.
   - [ ] 5.2 Deployment DDL: apply the same SET→SCALAR + new LUA distributor change to `Makefile`, `bench/run.sh`, and `docs/install.md`.

6. Test expectations (depends on 2, 3, 4)
   - [ ] 6.1 `scan_plan_shape.rs`: replace `GROUP BY shard_key) ORDER BY`, `AS shards(shard_key, files)`, no-`SELECT *`, and broadcast/`INNER JOIN … ON` assertions with the new nested-distributor + scalar-scan expectations.
   - [ ] 6.2 Update/extend `pushdown.rs` unit tests asserting the new shapes for every wrapper (raw, agg, grouped, count-distinct, top-n, broadcast, N-scan) including single-shard short-circuit and the greedy-attach `INNER JOIN … ON` FROM.
   - [ ] 6.3 Verify `two_entry_points_test.rs` still asserts the `.so` exports `__exa_udf_entry_LAKEHOUSE_SCAN` (unchanged) and that no distributor symbol is expected.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 (scan-side) |
| Group B | 2.1, 2.2 (fan-out primitive) |
| Group C | 3.1, 3.2, 3.3, 3.4 (per-wrapper restructure) |
| Group D | 4.1, 4.2 (join fallback renderer) |
| Group E | 5.1, 5.2 (DDL) |
| Group F | 6.1, 6.2, 6.3 (test expectations) |

Sequential dependencies:
- Group B → Group C (wrappers reuse the new `build_fan_out_inner`)
- Group C → Group D (join fallback reuses the restructured side fan-out)
- Groups B/C/D → Group F (test expectations assert the new shapes)
- Group A is independent of B/C/D and can run concurrently; Group E depends on A+B+C landing (DDL must match the scalar contract) but is otherwise mechanical.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| SQL fragment | `pushdown.rs` `SELECT * FROM ({inner})` wrappers in `build_row_scan_sql` and side fan-out | Replaced by outer ungrouped scalar select; the wrapper was the un-flattenable materialization boundary |
| Comma cross-join FROM | `pushdown.rs::build_n_scan_join_sql` (`{from} WHERE {conditions AND filter}`) | Replaced by the `INNER JOIN … ON` chain with greedy-attach |
| Single-`ctx.next()` guard | `scan/mod.rs::run_scan` (`let has_row = ctx.next()?; if !has_row { return }`) | Replaced by the `while ctx.next()` batch loop |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| work-unit-sharding / Scan-driving query fans out via a nested distributor over a scalar scan UDF | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `row_scan_fans_out_via_nested_distributor_over_scalar_scan` |
| work-unit-sharding / File distributor is a passthrough LUA SET script | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_distributor_passthrough_scan_returns_correct_rows` |
| work-unit-sharding / Single shard short-circuits the distributor | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `single_shard_short_circuits_distributor_fromless` |
| pushdown-planning / Pushdown resolves the file list once and builds a scan-driving query | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `pushdown_builds_scalar_scan_driving_sql` |
| pushdown-planning / Projection is pushed into the scan-driving query | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `projection_carried_in_common_literal_and_emits` |
| pushdown-planning / LIMIT is pushed into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `limit_attaches_directly_to_outer_scalar_select` |
| pushdown-planning / Aggregate wrapper SQL merges per-shard partial results | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `aggregate_merge_over_scalar_scan_no_wrapper` |
| pushdown-planning-topn / Ordered top-N over a projected column is pushed down | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `topn_order_by_limit_attaches_to_outer_scalar_select` |
| pushdown-planning-grouped-agg / Grouped scan-driving SQL fans out via a nested shard_key distributor | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_group_by_shard_key_inside_distributor` |
| pushdown-planning-grouped-agg / Grouped aggregate wrapper SQL re-groups partial results per user group key | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_grouped_aggregate_over_scalar_scan_correct` |
| pushdown-planning-join / Broadcast-eligible inner equi-join is planned as a broadcast fan-out | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `broadcast_fact_side_uses_distributor_scalar_scan` |
| pushdown-planning-join-fallback / Join above the broadcast threshold falls back to the unified unaccelerated wrapper | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `above_threshold_join_falls_back_inner_join_on` |
| pushdown-planning-join-fallback / A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `three_table_join_inner_join_on_chain` |
| pushdown-planning-join-fallback / Join conditions attach greedily by table-name set and side-local filters push into each leg | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `join_conditions_greedy_attach_and_side_local_pushdown` |
| single-so-two-entry-points / One crate exports the adapter and the scan entry points | Integration | `crates/lakehouse-engine/tests/two_entry_points_test.rs` | `so_exports_scan_symbol_and_no_distributor_symbol` |
| single-so-two-entry-points / Both scripts resolve from the same uploaded artifact | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_scalar_scan_and_adapter_resolve_same_so` |
| single-so-two-entry-points / The file distributor is a separate LUA SET script created by its own DDL | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_lua_distributor_created_without_so` |
| e2e-harness / Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_explain_virtual_shows_nested_distributor_scalar_scan` |
| e2e-harness / Harness provisions the scalar scan and the LUA distributor scripts | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_harness_provisions_scalar_scan_and_lua_distributor` |
| scan-execution / Scan loops over a batched scalar input and scans every assigned file list once | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_batched_scalar_scan_scans_every_shard_row` |
| scan-execution / Scan registers only its assigned files and returns matching rows | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_scan_registers_only_assigned_files` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/scan-execution | `make cross-musl-udf-build && make test-e2e` | E2E scan suite passes; a multi-shard raw scan returns the full row multiset (no rows dropped past the first batch row) |
| vs-adapter/pushdown-planning | `EXPLAIN VIRTUAL SELECT * FROM <vs>.<table>` on a multi-file table | Scan-driving SQL shows nested `LAKEHOUSE_DISTRIBUTE_FILES … GROUP BY shard_key` inside an outer ungrouped `LAKEHOUSE_SCAN(...)` scalar select; no `SELECT * FROM (...)` wrapper |
| parallelism/work-unit-sharding | `SELECT COUNT(*) FROM <vs>.DBX_LINEITEM` on staging (~210M rows) | Query completes; per-statement `TEMP_DB_RAM_PEAK` stays bounded (near ~85 MiB baseline) instead of growing past 22 GB / timing out |
| vs-adapter/pushdown-planning-join-fallback | `EXPLAIN VIRTUAL` of a 3-table inner join through the VS | FROM is an `INNER JOIN … ON` chain (not comma cross-join); side-local predicates appear inside each leg's fan-out, residual in the outer WHERE |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures (fails, never skips, if the stack is down) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
