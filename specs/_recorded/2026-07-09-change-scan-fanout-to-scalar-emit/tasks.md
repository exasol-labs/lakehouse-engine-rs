# Tasks: change-scan-fanout-to-scalar-emit

## Phase 2: Implementation (Group A — scan-side batch loop)
- [x] 1.1 Convert `scan/mod.rs::run_scan` to a `while ctx.next()` batch loop; build the DataFusion runtime ONCE from the first row, reuse across the batch, tear down deterministically once via `run_on_runtime`/`shutdown_timeout`. Preserve teardown discipline. [expert]
- [x] 1.2 Add scan-module test: multi-row batch scans every row (no row past first dropped); single-row batch byte-identical to pre-batching output.

## Phase 2: Implementation (Group B — fan-out primitive)
- [x] 2.1 Add `DISTRIBUTE_FILES_UDF_NAME` constant; thread `qualify_udf(scan_schema, …)` distributor name through builders alongside `udf_name`/`merge_udf_name`.
- [x] 2.2 Rewrite `build_fan_out_inner` to emit nested distributor subquery (`LAKEHOUSE_DISTRIBUTE_FILES(files) … FROM (VALUES …) AS shards(shard_key, files) GROUP BY shard_key`) wrapped by outer ungrouped scalar `LAKEHOUSE_SCAN('{common}', files) EMITS (...)`; splice `common` once as scalar first-arg literal, flow only `files` through distributor. [expert]

## Phase 2: Implementation (Group C — per-wrapper restructure, depends on B)
- [x] 3.1 `build_row_scan_sql`: drop `SELECT * FROM (...)` wrapper; attach `ORDER BY`/`LIMIT` directly to outer ungrouped scalar select; single-shard → from-less scalar call on literals. [expert]
- [x] 3.2 `build_aggregate_scan_sql`: outer merge SELECT directly over scalar scan (single-group SUM/MIN/MAX/COUNT/AVG-pair and count-distinct scalar-merge), no wrapper; single-shard short-circuit.
- [x] 3.3 `build_grouped_aggregate_scan_sql`: move `GROUP BY shard_key` INTO distributor; outer wrapper GROUP BYs user keys over scalar scan, preserving select-list-order/typed-cast contract; single-shard short-circuit. [expert]
- [x] 3.4 `build_broadcast_join_sql` + `build_side_fan_out_sql`: fact/side fan-out uses new distributor + scalar scan; drop `SELECT * FROM (...)` wrapper.

## Phase 2: Implementation (Group D — N-scan join fallback renderer, depends on C)
- [x] 4.1 Rewrite `build_n_scan_join_sql` FROM from comma cross-join + flat WHERE to left-to-right `INNER JOIN … ON` chain: greedy-attach each condition by the SET of `tableName`s it touches (never by column name) to earliest join point where all its tables are in scope; empty join point → `ON 1=1`. [expert]
- [x] 4.2 Split WHERE: push each side's side-local conjuncts INTO that side's fan-out leg; keep only cross-table / OR-spanning / untagged residual conjuncts in outer WHERE. [expert]

## Phase 2: Implementation (Group E — DDL, depends on A+B+C)
- [x] 5.1 E2E test DDL: `LAKEHOUSE_SCAN` SET SCRIPT → SCALAR SCRIPT (dynamic `EMITS`); ADD `CREATE LUA SET SCRIPT … LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000)) EMITS (files VARCHAR(2000000))` passthrough in `e2e_scan_test.rs`, `e2e_join_test.rs`, `e2e_capability_test.rs`, `e2e_count_distinct_test.rs`, `e2e_positional_deletes_test.rs`. (`tpch_loader.rs` carries no script DDL — it only seeds Iceberg data files via `common::seed`; nothing to change there.)
- [x] 5.2 Deployment DDL: same SET→SCALAR + new LUA distributor in `bench/run.sh`, `docs/install.md`. (`Makefile` has no CREATE SCRIPT DDL of its own — script creation lives only in the Rust E2E harness and `bench/run.sh`; nothing to change there.)

## Phase 2: Implementation (Group F — test expectations, depends on B/C/D)
- [x] 6.1 `scan_plan_shape.rs`: replace old `GROUP BY shard_key) ORDER BY`, `AS shards(shard_key, files)`, no-`SELECT *`, broadcast/`INNER JOIN … ON` assertions with new nested-distributor + scalar-scan expectations.
- [x] 6.2 Update/extend `pushdown.rs` unit tests for new shapes for every wrapper (raw, agg, grouped, count-distinct, top-n, broadcast, N-scan) incl. single-shard short-circuit and greedy-attach `INNER JOIN … ON`.
- [x] 6.3 Verify `two_entry_points_test.rs` still asserts `.so` exports `__exa_udf_entry_LAKEHOUSE_SCAN` (unchanged) and no distributor symbol expected.

## Phase 2b: Code review fixes
- [x] R.1 Replace 4 stale "SET UDF"/"SET SCRIPT" doc comments (scan/mod.rs, pushdown.rs) with "SCALAR EMIT UDF".
- [x] R.2 Fix stale "8 args" comment + drop `ponytail:` marker on `build_scan_driving_sql` (pushdown.rs).
- [x] R.3 Harden `clamp(1, last_join_point)` against a single-leg caller (latent panic; unreachable today).
- [ ] R.4 (DEFERRED) `ScanUdfNames` struct to bundle scan/merge/distribute names — noted, not applied (author rejected params struct; no bug; wide refactor risk).

## Phase 3: Verification
- [x] V.1 `cargo test` — 583 passed, 2 ignored, 0 failures
- [x] V.2 `cargo clippy --all-targets` — 0 issues
- [x] V.3 `cargo fmt --check` — clean
- [x] V.4 `make cross-musl-udf-build` — exit 0 (.so built in rust:1.94-bookworm)
- [x] V.5 `make test-e2e` — **80 passed, 0 failed, exit 0** (8+6+10+11+45 across the 5 binaries).
  First run failed 7/8 (query-side `EMITS` on the statically defined LUA distributor call); fixed in
  `build_fan_out_inner` and reran green. See decision-log.
