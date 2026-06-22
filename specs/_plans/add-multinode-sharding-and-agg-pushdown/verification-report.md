# Verification Report: add-multinode-sharding-and-agg-pushdown

## Verdict: PASS (host build + unit tests + lint + format all green; E2E owned by independent verifier)

All 8 plan tasks implemented, code review findings (1 BLOCKER, 2 SHOULD-FIX, cleanups) fixed,
all 14 Scenario Coverage test names present at their specified locations. The mandated
`make cross-musl-udf-build` produced a fresh glibc-2.36 `.so`; `cargo test`/`clippy`/`fmt` clean.

## Checklist results

| Step | Command | Result |
|------|---------|--------|
| Build | `make cross-musl-udf-build` | exit 0; `target/release/liblakehouse_engine.so` (165 MB, root-owned = Docker-built) fresh |
| Test | `cargo test` | `test result: ok. 75 passed; 0 failed` (lib) + `1 passed` (build_convention); 0 failures total |
| Lint | `cargo clippy --all-targets` | 0 warnings, 0 errors (exit 0) |
| Format | `cargo fmt --check` | exit 0 — no changes |
| E2E compile | `cargo test --features exasol-e2e --no-run` | compiles clean (E2E run owned by independent verifier; `make test-e2e` NOT run here) |

## Scenario Coverage audit (all 14 present)

| Scenario test | Location |
|---|---|
| create_vs_records_cluster_nodes_property | tests/e2e_scan_test.rs |
| cluster_nodes_defaults_to_one_on_connect_back_failure | src/adapter/mod.rs |
| partition_balanced_disjoint_full_coverage | src/adapter/sharding.rs |
| partition_caps_shards_at_file_count | src/adapter/sharding.rs |
| multi_shard_sql_fans_via_iproc_group_by | src/adapter/pushdown.rs |
| single_shard_sql_matches_legacy_shape | src/adapter/pushdown.rs |
| multi_shard_row_query_matches_single_shard | tests/e2e_scan_test.rs |
| reports_supported_aggregate_capabilities | src/adapter/capabilities.rs |
| aggregate_query_builds_partial_agg_spec | src/adapter/pushdown.rs |
| aggregate_wrapper_merges_partials | src/adapter/pushdown.rs |
| avg_wrapper_divides_sum_by_count_guarded | src/adapter/pushdown.rs |
| scan_emits_partial_aggregate_row | tests/e2e_scan_test.rs |
| partial_count_sum_min_max_merge_ready | tests/e2e_scan_test.rs |
| partial_avg_emits_sum_count_pair | tests/e2e_scan_test.rs |

## Tasks (1–8) — where the logic lives

1. createVirtualSchema NPROC capture — `src/adapter/mod.rs`: `resolve_cluster_nodes`, `nproc_value_to_count`; CLUSTER_NODES stored under `schemaMetadata.properties` (string); defaults to 1 on any connect-back failure.
2. File partitioning — `src/adapter/sharding.rs`: `partition_files(files, n)` (balanced, disjoint, capped at file_count, no empty shard).
3. IPROC fan-out SQL — `src/adapter/pushdown.rs`: `build_scan_driving_sql` / `build_fan_out_inner` (derived VALUES + `GROUP BY IPROC(), shard_key`); single-shard collapses to legacy `SELECT * FROM (SELECT udf(...) EMITS(...))`.
4. Aggregate detection + ScanSpec — `src/scan/spec.rs`: `AggKind`, `AggregatePlan`, `ScanSpec.aggregates`; `src/adapter/pushdown.rs`: `detect_aggregates` (fallback on GROUP BY/DISTINCT/unsupported); `src/adapter/capabilities.rs`: 7 aggregate caps added.
5. Partial-aggregate emission — `src/scan/mod.rs`: `run_partial_aggregate`, `build_partial_agg_sql`, `partial_select_items`, `emit_null_partial_row`; AVG as (sum, NULL-excluding count) pair.
6. Merge wrapper SQL — `src/adapter/pushdown.rs`: `build_aggregate_scan_sql`, `merge_select_items`; AVG = `SUM(psum)/NULLIF(SUM(pcount),0)`.
7. E2E — `tests/e2e_scan_test.rs`: CLUSTER_NODES property, per-aggregate correctness (with/without WHERE), multi-shard row-set completeness.
8. Dead code/test expectations — capabilities test updated to assert advertised aggregate caps; create-VS response asserts CLUSTER_NODES.

## Code review fixes applied

- **R.1 (BLOCKER)**: partial SUM/MIN/MAX EMITS type is now column-type-aware (`partial_emits_items` + `col_type_for`/`sum_emit_type`): MIN/MAX preserve DATE/TIMESTAMP/VARCHAR; SUM of DECIMAL widens to `DECIMAL(36,s)`; SUM of a non-numeric column falls back to row scan (`validate_agg_col_types`). Previously hardcoded DOUBLE PRECISION would crash on DATE/TIMESTAMP and lose precision on big integers.
- **R.2 (SHOULD-FIX)**: multi-shard row-scan SQL now appends the outer `LIMIT n` (was dropped, allowing up to K×N rows).
- **R.3 (cleanups)**: renamed misleading `reports_projection_filter_limit_only` → `reports_projection_filter_and_limit_capabilities`; replaced stale "Group C extension point" doc comment; removed noise-only inline section comments in `handle_pushdown` (kept WHY-bearing notes).

## Deviations from plan (with rationale)

- **AVG partial sum stays DOUBLE PRECISION even for DECIMAL columns** — AVG is inherently fractional and the final division produces a floating-point result; declaring the AVG partial sum DOUBLE avoids complicating the SUM type-widening path. Documented in code. Acceptable PoC choice consistent with the plan's AVG decomposition.
- **`multi_shard_row_query_matches_single_shard` asserts union-completeness (full id set 1..20, no gaps/dupes) rather than true cross-node placement** — the E2E Docker stack is single-node (CLUSTER_NODES defaults to 1), so true multi-node placement cannot be forced from SQL. The test asserts the correctness invariant sharding guarantees (every row returned exactly once). Honest caveat noted in the test; true cross-node validation requires a real multi-node cluster (a plan non-goal to provision).
- **CLUSTER_NODES connect-back requires an optional `CONNECTION_NAME` VS property** — the SDK connect-back API needs a ConnectionObject (credentials). When absent (as in the E2E CREATE VIRTUAL SCHEMA), the adapter safely defaults to 1. This is the spec-compliant "defaults to one when it cannot be determined" path.

## Blockers

None. The single open item by design is the live E2E run (`make test-e2e`), which is explicitly owned by the independent verifier, not this orchestration.
