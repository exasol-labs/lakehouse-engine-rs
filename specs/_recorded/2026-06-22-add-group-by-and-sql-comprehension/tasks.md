# Tasks: add-group-by-and-sql-comprehension

## Phase 0 — Dependency bump (BLOCKED: SDK 0.16.0 not yet on crates.io; latest published 0.15.1)
- [b] 0.1 Bump exasol-udf-sdk/macros to the release carrying `UdfContext::memory_limit()` once published. BLOCKED — stay on 0.14.0; 3.2/3.3 use the `0`-sentinel fallback path.

## Phase 1 — Foundation: vs-expression crate (Group A, then B)
- [x] 1.1 Create `crates/vs-expression/` workspace crate
- [x] 1.2 Move render_expression/render_junction/binary_op/quote_ident/quote_literal/sql_escape/json_scalar_to_string from adapter/predicate.rs into vs-expression/src/lib.rs [expert]
- [x] 1.3 Add arithmetic operator translation (ADD/SUB/MUL/FLOAT_DIV/NEG) [expert]
- [x] 1.4 Add CAST translation (VARCHAR/DECIMAL/DOUBLE/BOOLEAN/DATE/TIMESTAMP) [expert]
- [x] 1.5 Export render_expression / render_expression_safe / render_df_filter_safe
- [x] 1.6 Unit-test all node types
- [x] 1.7 Delete adapter/predicate.rs; replace imports with `use vs_expression::render_df_filter_safe`
- [x] 1.8 Add vs-expression to workspace members + lakehouse-engine dependency

## Phase 2 — Adapter: GROUP BY detection, sharding, scan-driving SQL (Group C, then D)
- [x] 2.1 Extend ScanSpec with `group_keys: Option<Vec<String>>`; update spec.rs round-trip tests
- [x] 2.2 Add `parallelism_factor` capture to create_virtual_schema (VS property, default 8)
- [x] 2.3 Implement shard_count(node_count, parallelism_factor, file_count); unit-test cap/clamp
- [x] 2.4 Repoint partition_files callers to pass G; one shard per file when file_count ≤ G
- [x] 2.5 Implement detect_group_by_aggregates(req) [expert]
- [x] 2.6 Add AGGREGATE_GROUP_BY_COLUMN/EXPRESSION capabilities; update tests
- [x] 2.7 Build build_grouped_aggregate_scan_sql (GROUP BY shard_key, GK_n cols, outer merge, LIMIT outer only) [expert]
- [x] 2.8 Rework row-scan fan-out to GROUP BY shard_key over G shards [expert]
- [x] 2.9 Wire detect_group_by_aggregates + shard_count into handle_pushdown
- [x] 2.10 Unit-test detect_group_by_aggregates
- [x] 2.11 Unit-test build_grouped_aggregate_scan_sql

## Phase 3 — Scan UDF: bounded runtime + grouped partial execution (Group E; E2 partially blocked)
- [x] 3.1 Implement probe_tmp_spill() -> SpillMode in scan/runtime.rs [expert]
- [x] 3.2 Implement build_runtime_env(memory_limit_bytes, spill) -> RuntimeEnv (param-driven; call site passes 0-sentinel until SDK lands) [expert]
- [x] 3.3 Wire build_runtime_env into scan session construction for all scan modes (0-sentinel path)
- [x] 3.4 Extend run_partial_aggregate to dispatch to run_grouped_partial_aggregate when group_keys present [expert]
- [x] 3.5 Implement build_grouped_partial_agg_sql(group_keys, aggregates, table, filter) [expert]
- [x] 3.6 Implement run_grouped_partial_aggregate (stream all rows, emit each group) [expert]
- [x] 3.7 Extend emit_null_partial_row/partial_emits_items/col_type_for to prepend group-key columns
- [x] 3.8 Unit-test build_runtime_env + build_grouped_partial_agg_sql

## Phase 4 — Mission and CLAUDE.md hygiene (Group F; already drafted — verify wording)
- [x] 4.1 Verify specs/mission.md memory-pool + spill + oversubscription wording
- [x] 4.2 Verify CLAUDE.md UDF parallelization & memory model section

## Phase 5 — E2E verification (Group G; after E)
- [x] 5.1 E2E test_group_by_sum_count
- [x] 5.2 E2E test_group_by_multi_key_with_filter
- [x] 5.3 E2E test_group_by_expression_key
- [x] 5.4 E2E test_group_by_avg_correctness
- [x] 5.5 E2E test_high_cardinality_group_by_spill
- [x] 5.6 E2E test_shard_key_fanout_explain
- [x] 5.N test_group_by_null_key_grouping (null key via NULLIF; seed unchanged)

## Phase 6 — Review & Verification
- [x] 6.1 Code review of changed files
- [x] 6.2 Build + unit tests + clippy + fmt
