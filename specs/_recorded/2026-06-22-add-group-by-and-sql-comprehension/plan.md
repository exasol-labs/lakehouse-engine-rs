# Plan: add-group-by-and-sql-comprehension

## Summary

Extends the shipped single-group aggregate pushdown to full GROUP BY pushdown, introduces a dedicated SQL-comprehension workspace crate (`crates/vs-expression`) that translates Exasol VS pushdown expression-JSON into DataFusion SQL fragments, replaces the misguided `GROUP BY IPROC()` node-sharding with oversubscribed work-unit sharding (`GROUP BY shard_key`, G = node_count × parallelism_factor capped at 300), and replaces the file-count cardinality guard with a metadata-sized DataFusion memory pool plus a spill-or-hardcap backstop.

## Design

### Context

v0.3.0 ships single-group aggregate pushdown (COUNT/SUM/MIN/MAX/AVG with partial/merge decomposition per ADR-008) and IPROC fan-out (ADR-007). A corrected reading of the Exasol engine internals (verified in `[redacted]`/`script-languages`) invalidates two pillars of the prior plan:

- **Parallelization (was ADR-007).** Groups drive UDF *invocations*, not OS processes; actual parallel instances on a node are a fixed VM pool sized to `NR_OF_CORES` (`[redacted]`, `[redacted]`), and groups are multiplexed onto it (`[redacted]`). `GROUP BY IPROC()` (`[redacted]`) yields one group per node → caps parallelism at node count and leaves a node's other cores idle. Group→node distribution is round-robin when group count ≤ `max_dynamic_group_count` (default 300), hash-partitioned above (`[redacted]`).
- **Memory safety (was the file-count cardinality guard).** Per-instance memory is the per-process heap (default 4096 MB) enforced via `setrlimit(RLIMIT_RSS)`; the dispatcher stalls additional concurrent VMs once usage hits 80% of the limit (`[redacted]`). The limit is available in UDF metadata (bytes) via the sibling-repo accessor `language-container-rs:add-memory-limit-metadata` (`UdfContext::memory_limit() -> u64`; `0` = unbounded/unknown).

Still valid and retained from the prior plan: the `crates/vs-expression` crate (ADR-009), group-key-as-plain-columns (ADR-010), GROUP BY partial/merge aggregate logic, broadened filter coverage, capability advertisement (GROUP BY column + expression; exclude HAVING/COUNT(DISTINCT)/joins), and the mission reframe to a usable engine.

- **Goals**
  - Push grouped aggregates (column refs and scalar expressions) to DataFusion as per-shard partial-group scans merged by Exasol.
  - Extract/generalise the expression walker into `crates/vs-expression` (arithmetic, CAST, IN, BETWEEN, LIKE, IS NULL, AND/OR), no new parser dep.
  - Parallelize by oversubscribing work units (`GROUP BY shard_key`, G = node_count × parallelism_factor capped 300) so groups spread round-robin across nodes AND multiplex onto each node's core pool.
  - Bound UDF memory by sizing the DataFusion `MemoryPool` from `ctx.memory_limit()` with a spill-or-hardcap backstop, so high-cardinality grouped queries either spill-and-complete or fail cleanly (never OOM).
- **Non-Goals**
  - HAVING, COUNT(DISTINCT), join pushdown.
  - A file-count or statistics-based cardinality guard (removed — superseded by the memory pool + spill).
  - Wiring `crates/vs-expression` into sibling strata-rs in this plan.
  - Implementing `UdfContext::memory_limit()` itself (owned by `language-container-rs:add-memory-limit-metadata`).

### Decision

#### Architecture

```
┌──────────────────────────────────────────────────────────┐
│  crates/vs-expression  (NEW standalone workspace crate)   │
│  render_expression / render_expression_safe /             │
│  render_df_filter_safe                                    │
└─────────────────────────┬────────────────────────────────┘
                          │ used by
                          ▼
┌──────────────────────────────────────────────────────────┐
│  crates/lakehouse-engine/src/adapter/                     │
│  pushdown.rs  — detect_group_by_aggregates()              │
│                 shard_count() = clamp(N×factor, 1, files, 300)
│                 build_grouped_aggregate_scan_sql()        │
│                   (GROUP BY shard_key — NOT IPROC())      │
│  capabilities.rs — GROUP BY capability strings            │
│  create_virtual_schema — NPROC capture + parallelism_factor│
└─────────────────────────┬────────────────────────────────┘
                          │ scan spec (group_keys, files)
                          ▼
┌──────────────────────────────────────────────────────────┐
│  crates/lakehouse-engine/src/scan/                        │
│  runtime.rs — build_runtime_env() sized from              │
│               ctx.memory_limit(); /tmp probe →            │
│               FairSpillPool+DiskManager | GreedyMemoryPool │
│  mod.rs     — run_grouped_partial_aggregate()             │
│               build_grouped_partial_agg_sql()             │
└──────────────────────────────────────────────────────────┘
```

Two-level grouping composes: the inner `GROUP BY shard_key` parallelizes the scan; DataFusion performs the *user* GROUP BY inside each shard invocation (emitting per-user-group partials with group-key values as plain columns, GK_0..GK_{n-1}); the outer wrapper SQL re-groups on those columns and merges the partials.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Oversubscribed `GROUP BY shard_key` fan-out | pushdown.rs grouped + row-scan paths | Spreads shard groups round-robin across nodes (G≤300) AND multiplexes onto each node's core pool; replaces `GROUP BY IPROC()` which capped parallelism at node count |
| Partial/merge decomposition | scan/mod.rs grouped path | Same correctness as ADR-008: SUM→SUM, MIN→MIN, MAX→MAX, AVG→(sum,count) pair |
| Group-key values as plain columns (GK_n) | scan + wrapper | Avoids cross-language expression-rendering mismatch; wrapper groups on column positions |
| Metadata-sized memory pool + spill backstop | scan/runtime.rs | Bounds per-instance memory under the engine's 80% stall threshold; spill lets high-cardinality grouped queries complete, hardcap fails cleanly instead of OOM |
| Standalone workspace crate | crates/vs-expression | Decouples translation from engine internals; enables strata-rs reuse |

#### Memory-safety mechanism (replaces the old file-count guard)

The scan UDF builds its `RuntimeEnv` from `ctx.memory_limit()` (bytes):
- limit > 0 → pool budget = `~0.6 × limit` (headroom below the engine's 80% stall threshold).
- limit == 0 (unknown/accessor unavailable) → conservative default budget (1024 MB).
- `/tmp` probe (tmpfs check via `/proc/mounts` + `statvfs` free space): real disk → `FairSpillPool` + `DiskManager` rooted at `/tmp` (completes at any cardinality); tmpfs/too-small → `GreedyMemoryPool` (no spill) returning clean `ResourcesExhausted`.

Layered safety: oversubscribed sharding shrinks each instance's footprint, the engine stalls concurrency at 80% of the per-process heap, the bounded pool caps in-process memory, and spill is the backstop. The `/tmp` spill is transient per-invocation scratch — never persistent state.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Oversubscribed `GROUP BY shard_key`, G = node_count × factor capped 300 | `GROUP BY IPROC()` per-node sharding (shipped ADR-007) | IPROC sharding caps parallel instances at node count; shard_key oversubscription uses each node's full core pool and stays in the round-robin distribution regime (G≤300) |
| Memory pool sized from `ctx.memory_limit()` + spill-or-hardcap | File-count cardinality guard (prior ADR-011); per-shard emitted-group cap; no guard | The guard was a heuristic with no statistical basis; a pool sized to the real per-instance limit plus spill gives correctness at any cardinality (spill) or a clean failure (hardcap), and the engine self-throttles at 80% |
| Group-key values emitted as plain columns | Re-render GROUP BY expression in wrapper | Avoids cross-language rendering mismatch (retained from ADR-010) |
| `parallelism_factor` as a VS property (default 8) | Hardcode the oversubscription factor | Lets operators tune oversubscription per cluster without code changes |
| Depend on `language-container-rs:add-memory-limit-metadata` | Reimplement the metadata read in this repo | The accessor belongs in the SDK; reimplementation would duplicate proto deserialization across repos |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `sql-comprehension/vs-expression-translator` | NEW | `specs/_plans/add-group-by-and-sql-comprehension/sql-comprehension/vs-expression-translator/spec.md` |
| `parallelism/work-unit-sharding` | NEW (renamed from `parallelism/iproc-sharding`) | `specs/_plans/add-group-by-and-sql-comprehension/parallelism/work-unit-sharding/spec.md` |
| `parallelism/iproc-sharding` | REMOVED (superseded by `work-unit-sharding`) | `specs/_plans/add-group-by-and-sql-comprehension/parallelism/iproc-sharding/spec.md` |
| `vs-adapter/pushdown-planning` | CHANGED | `specs/_plans/add-group-by-and-sql-comprehension/vs-adapter/pushdown-planning/spec.md` |
| `vs-adapter/create-virtual-schema` | CHANGED | `specs/_plans/add-group-by-and-sql-comprehension/vs-adapter/create-virtual-schema/spec.md` |
| `datafusion-scan/scan-execution` | CHANGED | `specs/_plans/add-group-by-and-sql-comprehension/datafusion-scan/scan-execution/spec.md` |
| `packaging/e2e-harness` | CHANGED | `specs/_plans/add-group-by-and-sql-comprehension/packaging/e2e-harness/spec.md` |

## Dependencies

- **`language-container-rs:add-memory-limit-metadata` (cross-repo) — LANDED, release in flight.** Adds `UdfContext::memory_limit() -> u64` (bytes; `0` = unbounded/unknown). The PR is merged; the `exasol-udf-sdk` / `exasol-udf-macros` release carrying the accessor is publishing. The remaining gate is purely the version bump (this crate currently pins `0.14.0`): task 0.1 bumps the pin to the published release once it lands. Until that bump, the memory-pool sizing task (3.2) builds against the `0`-sentinel fallback (1024 MB default budget); after the bump it uses the live `ctx.memory_limit()`. Do not hand-roll a local accessor — consume the SDK's.

## Implementation Tasks

### Phase 0 — Dependency bump (prerequisite for memory-pool sizing)

- [ ] 0.1 Once the `language-container-rs:add-memory-limit-metadata` release publishes, bump `exasol-udf-sdk` and `exasol-udf-macros` from `0.14.0` to the release version carrying `UdfContext::memory_limit()`; update `Cargo.lock`; confirm `cargo build` resolves the accessor. Unblocks task 3.2's live path. (Can run in parallel with Phase 1; the rest of Phase 3 falls back to the `0`-sentinel default until this lands.)

### Phase 1 — Foundation: vs-expression crate

- [ ] 1.1 Create `crates/vs-expression/` workspace crate (`Cargo.toml`: lib, edition 2024, deps `serde_json` + `exasol-udf-sdk` for `UdfError`)
- [ ] 1.2 Move `render_expression`, `render_junction`, `binary_op`, `quote_ident`, `quote_literal`, `sql_escape`, `json_scalar_to_string` from `adapter/predicate.rs` into `vs-expression/src/lib.rs` with identical logic [expert]
- [ ] 1.3 Add arithmetic operator translation (`function_scalar` ADD/SUB/MUL/FLOAT_DIV/NEG) [expert]
- [ ] 1.4 Add CAST translation (`function_scalar` name=CAST, `dataType` → VARCHAR/DECIMAL/DOUBLE/BOOLEAN/DATE/TIMESTAMP) [expert]
- [ ] 1.5 Export the three public entry points: `render_expression` (raising), `render_expression_safe` (None-on-failure), `render_df_filter_safe` (None + trivially-true suppression)
- [ ] 1.6 Unit-test all node types (columns, each literal, all predicates, arithmetic, CAST, IN, BETWEEN, LIKE, IS NULL, nested AND/OR)
- [ ] 1.7 Delete `adapter/predicate.rs`; replace its imports in `adapter/mod.rs` and `adapter/pushdown.rs` with `use vs_expression::render_df_filter_safe`
- [ ] 1.8 Add `vs-expression` to workspace members and as a `lakehouse-engine` dependency

### Phase 2 — Adapter: GROUP BY detection, sharding, and scan-driving SQL

- [ ] 2.1 Extend `ScanSpec` with `group_keys: Option<Vec<String>>` (serde default=None, skip_if none); update round-trip tests in `scan/spec.rs`
- [ ] 2.2 Add `parallelism_factor` capture to `create_virtual_schema` (VS property, default 8) into `adapterNotes` alongside `CLUSTER_NODES`; round-trip it at pushdown time
- [ ] 2.3 Implement `shard_count(node_count, parallelism_factor, file_count) -> usize` = `clamp(node_count × factor, 1, min(file_count, 300))` in `adapter/pushdown.rs`; unit-test the cap/clamp boundaries
- [ ] 2.4 Repoint `partition_files` callers to pass G (from `shard_count`) instead of node_count; one shard per file when file_count ≤ G; no empty shards
- [ ] 2.5 Implement `detect_group_by_aggregates(req) -> Option<(Vec<String>, Vec<AggregatePlan>)>`: checks `aggregationType == "group_by"`, renders each `groupBy` node via `render_expression` (raising), collects aggregate plans, returns None on any failure [expert]
- [ ] 2.6 Add `AGGREGATE_GROUP_BY_COLUMN` and `AGGREGATE_GROUP_BY_EXPRESSION` to `capabilities.rs`; update capability unit tests
- [ ] 2.7 Build `build_grouped_aggregate_scan_sql` in `adapter/pushdown.rs`: fan-out VALUES over G shards grouped on `shard_key` (NOT `IPROC()`), expanded EMITS (GK_0..GK_{n-1} group-key columns first, then PARTIAL_* columns), outer wrapper GROUP BY user keys + aggregate-merge; LIMIT only in the outer wrapper [expert]
- [ ] 2.8 Rework the row-scan fan-out path to also use `GROUP BY shard_key` over G shards (replacing the `GROUP BY IPROC()` form) [expert]
- [ ] 2.9 Wire `detect_group_by_aggregates` + `shard_count` into `handle_pushdown`: grouped detect succeeds → grouped scan path; else row scan / single-group aggregate
- [ ] 2.10 Unit-test `detect_group_by_aggregates` (column key, expression key, unsupported expression falls back, mixed select falls back)
- [ ] 2.11 Unit-test `build_grouped_aggregate_scan_sql` (single-key, multi-key, with filter, with AVG, GROUP BY shard_key present, no per-shard LIMIT, IPROC absent)

### Phase 3 — Scan UDF: bounded runtime + grouped partial execution

- [ ] 3.1 Implement `probe_tmp_spill() -> SpillMode` in `scan/runtime.rs`: read `/proc/mounts` for `/tmp` tmpfs detection + `statvfs` free-space check; returns Disk(path) or NoDisk [expert]
- [ ] 3.2 Implement `build_runtime_env(memory_limit_bytes: u64, spill: SpillMode) -> RuntimeEnv` in `scan/runtime.rs`: limit>0 → pool = 0.6×limit; limit==0 → 1024 MB default; Disk → `FairSpillPool` + `DiskManager` rooted at `/tmp`; NoDisk → `GreedyMemoryPool`. Source the limit from `ctx.memory_limit()` at the call site. **Blocked on `language-container-rs:add-memory-limit-metadata`** [expert]
- [ ] 3.3 Wire `build_runtime_env` into the scan session construction for all scan modes (row scan, single-group, grouped); until 3.2's SDK dep lands, exercise only the `0`-sentinel default-budget path
- [ ] 3.4 Extend `run_partial_aggregate` to dispatch to `run_grouped_partial_aggregate` when `spec.group_keys` is Some/non-empty [expert]
- [ ] 3.5 Implement `build_grouped_partial_agg_sql(group_keys, aggregates, table, filter) -> String`: `SELECT <group_key_exprs>, <partial_aggs> FROM <table> [WHERE <filter>] GROUP BY <group_key_exprs>` (no LIMIT) [expert]
- [ ] 3.6 Implement `run_grouped_partial_aggregate(ctx, session_ctx, spec)`: execute grouped partial SQL, stream all result rows (not just the first), convert each via `arrow_value_at`, emit each group row; empty result → zero rows [expert]
- [ ] 3.7 Extend `emit_null_partial_row`/`partial_emits_items`/`col_type_for` to prepend group-key columns to the EMITS layout
- [ ] 3.8 Unit-test `build_runtime_env` (limit>0 → 0.6 pool; limit==0 → default; Disk → spill pool; NoDisk → greedy pool) and `build_grouped_partial_agg_sql` (single key + COUNT, multi-key + SUM, expression key, with filter, no LIMIT)

### Phase 4 — Mission and CLAUDE.md hygiene

- [ ] 4.1 Edit `specs/mission.md`: replace the `GROUP_BY_CARDINALITY_LIMIT` Core Capability 6 and Usable-engine constraint with the memory-pool + spill + oversubscription mechanism; update Capability 3, the architecture diagram, and the glossary to work-unit sharding (done in this revision — verify wording)
- [ ] 4.2 Add the UDF parallelization & memory model section to `CLAUDE.md` with the [redacted] citations (done in this revision — verify wording)

### Phase 5 — E2E verification

- [ ] 5.1 E2E `test_group_by_sum_count`: Iceberg table with region/amount, `SELECT region, COUNT(*), SUM(amount) ... GROUP BY region`, assert per-group values; fail if stack down
- [ ] 5.2 E2E `test_group_by_multi_key_with_filter`: two GROUP BY columns + WHERE
- [ ] 5.3 E2E `test_group_by_expression_key`: `GROUP BY YEAR(order_date)`
- [ ] 5.4 E2E `test_group_by_avg_correctness`: AVG across groups with unequal row counts
- [ ] 5.5 E2E `test_high_cardinality_group_by_spill`: high distinct-group count completes (spill path) with correct per-group counts, no crash
- [ ] 5.6 E2E `test_shard_key_fanout_explain`: `EXPLAIN VIRTUAL` shows `GROUP BY shard_key` (not `IPROC()`)

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| A — vs-expression crate (self-contained) | 1.1, 1.2, 1.3, 1.4, 1.5, 1.6 |
| B — integrate crate into lakehouse-engine (after A) | 1.7, 1.8 |
| C — adapter extensions (after B) | 2.1, 2.2, 2.3, 2.5, 2.6 |
| D — sharding + scan-driving SQL (after C) | 2.4, 2.7, 2.8, 2.9, 2.10, 2.11 |
| E — scan runtime + grouped execution (after D) | 3.1, 3.4, 3.5, 3.6, 3.7, 3.8 |
| E2 — memory-pool sizing (after E; BLOCKED on cross-repo dep) | 3.2, 3.3 |
| F — docs (independent) | 4.1, 4.2 |
| G — E2E (after E and E2) | 5.1, 5.2, 5.3, 5.4, 5.5, 5.6 |

Sequential dependencies:
- A → B (crate must exist before lakehouse-engine depends on it)
- B → C (predicate.rs replaced before adapter extension)
- C → D (ScanSpec, shard_count, detect functions exist before wiring + SQL builders)
- D → E (adapter produces group_keys + shard fan-out before UDF consumes them)
- E → E2 (runtime wiring exists before pool sizing); E2's *live* `memory_limit()` path is gated on task 0.1 (the SDK version bump) — until then E2 builds against the `0`-sentinel default budget
- E + E2 → G (UDF emits + bounds memory before E2E verifies); the spill E2E (5.5) needs E2's spill pool, the rest need only E
- F independent

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Module | `crates/lakehouse-engine/src/adapter/predicate.rs` | Superseded by `crates/vs-expression` |
| Tests | `adapter/predicate.rs` tests | Moved to `vs-expression/src/lib.rs` |
| SQL builder | `GROUP BY IPROC()` fan-out form in `adapter/pushdown.rs` | Replaced by `GROUP BY shard_key` oversubscribed fan-out |
| Function/property | any file-count cardinality guard / `GROUP_BY_CARDINALITY_LIMIT` handling | Removed; superseded by the metadata-sized memory pool + spill backstop |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Bare column reference translates to quoted identifier | Unit | `crates/vs-expression/src/lib.rs` | `renders_column_as_quoted_uppercase_ident` |
| Literal nodes translate to SQL literal forms | Unit | `crates/vs-expression/src/lib.rs` | `renders_string_literal`, `renders_null_literal`, `renders_date_literal`, `renders_timestamp_literal`, `renders_bool_literal`, `renders_numeric_literal` |
| Comparison predicates translate to binary operator expressions | Unit | `crates/vs-expression/src/lib.rs` | `renders_simple_equality` |
| Logical connectives translate to AND/OR/NOT with parentheses | Unit | `crates/vs-expression/src/lib.rs` | `renders_and_predicate`, `renders_or_predicate`, `renders_not_predicate`, `renders_empty_and_as_true`, `renders_empty_or_as_false` |
| IS NULL and IS NOT NULL predicates translate correctly | Unit | `crates/vs-expression/src/lib.rs` | `renders_is_null`, `renders_is_not_null` |
| IN constant list translates to SQL IN expression | Unit | `crates/vs-expression/src/lib.rs` | `renders_in_constlist`, `renders_empty_in_as_false` |
| BETWEEN predicate translates correctly | Unit | `crates/vs-expression/src/lib.rs` | `renders_between` |
| LIKE predicate translates with optional escape character | Unit | `crates/vs-expression/src/lib.rs` | `renders_like_without_escape`, `renders_like_with_escape` |
| Arithmetic operators translate to binary SQL expressions | Unit | `crates/vs-expression/src/lib.rs` | `renders_arithmetic_add`, `renders_arithmetic_sub`, `renders_arithmetic_mul`, `renders_arithmetic_div`, `renders_arithmetic_neg` |
| CAST translates to DataFusion CAST syntax | Unit | `crates/vs-expression/src/lib.rs` | `renders_cast_varchar`, `renders_cast_decimal`, `renders_cast_double`, `renders_cast_date` |
| Unsupported node type returns error in raising mode | Unit | `crates/vs-expression/src/lib.rs` | `unsupported_node_returns_error` |
| Safe variant returns None for unsupported nodes | Unit | `crates/vs-expression/src/lib.rs` | `unsupported_node_returns_none_in_safe_mode` |
| Trivially-true filter suppressed in safe variant | Unit | `crates/vs-expression/src/lib.rs` | `true_filter_returns_none_in_safe_mode` |
| Shard count oversubscribes the cluster and is capped at the round-robin threshold | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `shard_count_oversubscribes_and_caps_at_300` |
| File list is partitioned into G balanced disjoint shards covering every file | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `partition_files_g_shards_balanced_disjoint_full_coverage` |
| Fewer files than G produces one shard per file with no empty shards | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `shard_count_clamped_to_file_count_no_empty_shards` |
| Scan-driving query fans the SET UDF across shards via GROUP BY shard_key | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `scan_driving_sql_groups_by_shard_key_not_iproc` |
| Single node with G collapsing to one preserves the single-invocation query | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `single_shard_collapses_to_single_invocation` |
| Adapter advertises GROUP BY capabilities | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_group_by_capabilities` |
| Adapter records the parallelism factor in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `create_vs_records_parallelism_factor` |
| Recorded node count and parallelism factor drive later work-unit sharding | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `adapter_notes_carry_cluster_nodes_and_parallelism_factor` |
| Grouped aggregate query is detected and translated to a grouped scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `detect_group_by_aggregates_column_key`, `detect_group_by_aggregates_expression_key`, `detect_group_by_unsupported_expression_falls_back`, `detect_group_by_mixed_select_falls_back` |
| Grouped scan spec carries group-key rendered SQL fragments | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_scan_spec_carries_group_keys` |
| Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_scan_sql_groups_by_shard_key` |
| LIMIT is NOT pushed into per-shard scan for a grouped query | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_scan_sql_has_no_per_shard_limit` |
| Grouped aggregate wrapper SQL re-groups partial results per user group key | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_aggregate_wrapper_sql_groups_by_user_key_cols` |
| Adapter falls back to row scan for unsupported grouped aggregate shape | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `detect_group_by_unsupported_expression_falls_back` |
| NULL group keys are grouped together consistently | Integration | `tests/e2e/` | `test_group_by_null_key_grouping` |
| DataFusion memory pool is sized from the per-instance memory limit | Unit | `crates/lakehouse-engine/src/scan/runtime.rs` | `build_runtime_env_sizes_pool_from_limit_fraction` |
| Default memory budget is used when the limit is unknown | Unit | `crates/lakehouse-engine/src/scan/runtime.rs` | `build_runtime_env_uses_default_budget_on_zero_limit` |
| Spill to disk is enabled when /tmp is real disk with free space | Unit | `crates/lakehouse-engine/src/scan/runtime.rs` | `build_runtime_env_uses_fair_spill_pool_when_disk` |
| Clean ResourcesExhausted error when no spill disk is available | Unit | `crates/lakehouse-engine/src/scan/runtime.rs` | `build_runtime_env_uses_greedy_pool_when_no_disk` |
| Grouped partial aggregate computes per-group partial results per shard | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `grouped_partial_agg_sql_single_key_count` |
| Grouped partial row layout matches wrapper SQL column contract | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `grouped_partial_agg_sql_layout_matches_emits` |
| LIMIT is not applied inside the grouped partial scan | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `grouped_partial_agg_sql_no_limit` |
| Group-key expressions are pushed into the DataFusion GROUP BY clause verbatim | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `grouped_partial_agg_sql_expression_key_verbatim` |
| End-to-end grouped aggregate query returns correct per-group results | Integration | `tests/e2e/` | `test_group_by_sum_count` |
| End-to-end multi-key GROUP BY with WHERE filter returns correct results | Integration | `tests/e2e/` | `test_group_by_multi_key_with_filter` |
| End-to-end GROUP BY with expression group key returns correct results | Integration | `tests/e2e/` | `test_group_by_expression_key` |
| End-to-end grouped AVG is correct across all groups | Integration | `tests/e2e/` | `test_group_by_avg_correctness` |
| High-cardinality grouped query completes via memory-pool spill | Integration | `tests/e2e/` | `test_high_cardinality_group_by_spill` |
| Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL | Integration | `tests/e2e/` | `test_shard_key_fanout_explain` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression unit tests | `cargo test -p vs-expression` | All tests pass, 0 failures |
| Adapter + scan unit tests | `cargo test -p lakehouse-engine` | All pass incl. shard_count, GROUP BY detection, runtime-env, capabilities |
| E2E grouped aggregate | `make test-e2e` (stack running) | All grouped E2E tests pass |
| E2E stack-unavailable fail | `make test-e2e` (stack stopped) | Tests fail with connection error, do not skip |
| Manual EXPLAIN VIRTUAL fan-out | Connect to Exasol, run `EXPLAIN VIRTUAL SELECT region, COUNT(*) FROM vs.sales GROUP BY region` | Output shows `GROUP BY shard_key` fan-out, not `IPROC()` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Unit tests | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| E2E | `make test-e2e` | 0 failures |
