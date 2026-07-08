# Plan: add-multinode-sharding-and-agg-pushdown

## Summary

Partition the once-resolved Iceberg data-file list across active Exasol nodes (IPROC sharding) so each node-local DataFusion runtime scans only its shard, and push single-group aggregates (COUNT/SUM/MIN/MAX/AVG) down as node-local partial aggregates that Exasol merges — the two pushdown capabilities deferred from `add-datafusion-iceberg-scan-pushdown`.

## Design

### Context

The existing pushdown layer resolves the Iceberg file list once and drives a single scan SET UDF invocation over all files on one node — the cluster's other nodes sit idle, and aggregate queries pull every raw row back into Exasol before aggregating. The resolve-once seam (adapter hands the UDF an explicit file list) was built precisely to enable cross-node sharding without touching UDF internals. This plan exploits that seam for two-level parallelism and cuts aggregate network transfer to one partial row per node.

- **Goals** — (1) Fan the scan SET UDF across `CLUSTER_NODES` Exasol nodes, each scanning a disjoint file shard. (2) Push COUNT(*)/COUNT(col)/SUM/MIN/MAX/AVG down as node-local partial aggregates merged by Exasol. (3) Stay backward-compatible: `CLUSTER_NODES = 1` reproduces today's single-invocation query byte-for-behaviour.
- **Non-Goals** — GROUP BY pushdown, HAVING, COUNT(DISTINCT), join pushdown, Databricks/Unity catalog, caching/materialization, runtime node-count refresh. The node count is captured once at `createVirtualSchema` time.

### Decision

The adapter computes file-to-shard assignment in the planning layer and expresses the cross-node fan-out as a single derived-VALUES query that Exasol distributes via IPROC. Each shard carries its own `ScanSpec` (file subset + projection/filter/limit, optionally aggregate instructions). For aggregates, the scan UDF emits one partial-result row per shard and the adapter wraps the fan-out in an outer merge aggregation.

> **Note on the sibling-project reference**: the brief expected an existing IPROC sharding pattern in the sibling project to mirror. Exploration found the sibling project does **not** shard across nodes — it uses a single-invocation cache-population UDF (`CACHE_QUERY`) with no IPROC/NPROC use. There is no pattern to copy; the IPROC fan-out below is designed from Exasol's `IPROC()`/`NPROC()` SET-UDF distribution idiom.

#### Architecture

```
createVirtualSchema
  └─ connect-back: SELECT NPROC()  ──▶  CLUSTER_NODES property (default 1)

pushdown (per query)
  ├─ resolve Iceberg file list ONCE
  ├─ read CLUSTER_NODES from VS properties
  ├─ partition files into N balanced shards  (N = min(CLUSTER_NODES, file_count))
  ├─ detect aggregate select-list → per-aggregate (kind, column) plan
  └─ build scan-driving SQL:
        ┌──────────────────────────────────────────────────────────┐
        │ SELECT <merge>                                              │
        │ FROM ( SELECT SCAN(spec_for_shard) EMITS (...)              │
        │        FROM ( VALUES (0,spec0),(1,spec1),... )              │
        │               AS shards(shard_key, spec)                    │
        │        GROUP BY IPROC(), shard_key )                        │
        └──────────────────────────────────────────────────────────┘
                       │ Exasol distributes each shard group to a node
                       ▼
   node-local DataFusion scan over shard files
      → raw rows  (non-aggregate)   OR   one partial-aggregate row (aggregate)
                       │
                       ▼
   Exasol outer merge:  union of rows  OR  SUM/MIN/MAX over partials
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Derived `VALUES` + `GROUP BY IPROC(), shard_key` fan-out | `adapter::pushdown` (sharding SQL builder) | One scan query distributes N shards to N nodes; Exasol's standard SET-UDF cross-node distribution idiom |
| Resolve-once, partition-in-planner | `adapter::pushdown` | Metadata resolved once per query (mission invariant); each UDF gets an explicit disjoint file shard |
| Balanced contiguous partition (`chunks`-style split) | new `sharding` module | Even file counts across nodes (differ by ≤1); deterministic, pure, unit-testable |
| Partial → merge aggregate decomposition | scan UDF (partial) + wrapper SQL (merge) | COUNT→SUM, SUM→SUM, MIN→MIN, MAX→MAX, AVG→SUM(sum)/SUM(count); minimizes network transfer to one row per node |
| AVG as (sum,count) pair | `ScanSpec` agg plan + scan UDF + wrapper | AVG is not directly mergeable; sum/count pair is. Wrapper divides, guarding count=0 → NULL |
| Capability gating by detection fallback | `adapter::capabilities` + `adapter::pushdown` | Advertise aggregate caps but fall back to row scan for any aggregate shape the UDF cannot compute |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Fetch `NPROC()` once at `createVirtualSchema`, store as `CLUSTER_NODES` (default 1) | Fetch per pushdown; read a static config property | Node count is stable for a VS lifetime; one connect-back avoids per-query latency; default 1 keeps the single-node path untouched when fetch fails |
| `GROUP BY IPROC(), shard_key` derived-VALUES fan-out | Per-shard separate `SELECT ... UNION ALL`; one row per file | UNION ALL of N UDF calls does not guarantee node placement and bloats SQL; IPROC grouping is the idiomatic Exasol cross-node distribution and keeps one query |
| `N = min(CLUSTER_NODES, file_count)`, no empty shards | Always N shards (some empty); one shard per file always | Empty-shard UDF invocations waste a node and an Iceberg session; capping at file count avoids them while still using every node when files allow |
| AVG decomposed to (sum, count) pair, divide in wrapper | Emit per-shard average and average the averages | Averaging averages is wrong for unequal shard sizes; sum/count pair is exactly mergeable |
| Aggregate decomposition lives split across UDF (partial) and SQL (merge) | Do the full aggregate in the UDF on one node | Splitting is what makes it scale across nodes and keeps the merge in Exasol where the partials converge |
| New `parallelism/iproc-sharding` feature | Fold sharding scenarios into `pushdown-planning` | Sharding is a distinct cross-node concern reused by both row and aggregate queries; a dedicated feature keeps the file-partition invariants (disjoint, balanced, no empty shards) first-class |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| parallelism/iproc-sharding | NEW | `parallelism/iproc-sharding/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |

## Dependencies

- `exasol-udf-sdk` connect-back (`ExaConnection::query`, `ctx.cluster_ip()`) — already a dependency; newly used in the `createVirtualSchema` path to run `SELECT NPROC()`.
- No new external crates.

## Migration

| Current | New |
|---------|-----|
| `createVirtualSchema` returns `schemaMetadata` only | Also returns a `CLUSTER_NODES` virtual-schema property |
| Pushdown drives one scan UDF call over all files | Drives one call per shard via IPROC fan-out (single-shard when `CLUSTER_NODES`=1) |
| `ScanSpec` has files/projection/filter/limit | Adds an optional `aggregates` plan (function kind + column); absent for row scans |
| Capabilities advertise projection/filter/LIMIT only | Also advertise single-group COUNT/SUM/MIN/MAX/AVG aggregate pushdown |

## Implementation Tasks

1. **Cluster-node capture in createVirtualSchema**
   - [ ] 1.1 Add a connect-back `SELECT NPROC()` call in the `createVirtualSchema` path, returning a positive integer node count.
   - [ ] 1.2 Default `CLUSTER_NODES` to 1 when connect-back or the query fails; never fail the create on this.
   - [ ] 1.3 Emit `CLUSTER_NODES` in the `createVirtualSchema` response properties; redact credentials from any connect-back error.

2. **File-list partitioning module** (`parallelism/iproc-sharding`)
   - [ ] 2.1 Add a pure `partition_files(files, n) -> Vec<Vec<String>>` producing balanced disjoint shards (counts differ by ≤1), capping shard count at `file_count`, never yielding an empty shard.
   - [ ] 2.2 Unit tests: disjointness, full coverage, balance, fewer-files-than-nodes, n=1, empty input.

3. **IPROC fan-out SQL builder** [expert]
   - [ ] 3.1 Build the derived-`VALUES` + `GROUP BY IPROC(), shard_key` scan-driving SQL that invokes the SET UDF once per shard, each carrying its shard's `ScanSpec` literal. [expert]
   - [ ] 3.2 Collapse to the existing single-invocation query shape when there is one shard (backward-compat). [expert]
   - [ ] 3.3 Unit tests asserting per-shard spec literals, IPROC grouping presence, and single-shard equivalence.

4. **Aggregate detection + ScanSpec extension** [expert]
   - [ ] 4.1 Extend `ScanSpec` with an optional `aggregates` plan (ordered list of {kind: Count|CountCol|Sum|Min|Max|Avg, column}). [expert]
   - [ ] 4.2 Detect a supported single-group aggregate select-list in the pushdown request; produce the aggregate plan, or fall back to row scanning for unsupported shapes (GROUP BY, DISTINCT, HAVING). [expert]
   - [ ] 4.3 Advertise the new aggregate capabilities in `adapter::capabilities`; update its test that currently asserts NO aggregate caps.
   - [ ] 4.4 Unit tests for detection → plan translation and fallback.

5. **Partial-aggregate emission in the scan UDF** [expert]
   - [ ] 5.1 When the spec carries an aggregate plan, run the partial aggregate in DataFusion and emit one partial-result row per shard instead of raw rows. [expert]
   - [ ] 5.2 Emit COUNT as a summable count, SUM as a summable sum, MIN/MAX as re-MIN/MAX-able extrema; empty shard → count 0, NULL sum/min/max. [expert]
   - [ ] 5.3 Emit AVG as a (partial_sum, partial_count) pair with NULL-excluding count; never a per-shard average. [expert]
   - [ ] 5.4 Unit/integration tests for each partial form, including the empty-shard row.

6. **Aggregate merge wrapper SQL** [expert]
   - [ ] 6.1 Wrap the shard fan-out in the outer merge: SUM(partial_count), SUM(partial_sum), MIN(partial_min), MAX(partial_max). [expert]
   - [ ] 6.2 AVG wrapper: `SUM(partial_sum)/SUM(partial_count)` with a count=0 → NULL guard. [expert]
   - [ ] 6.3 Unit tests asserting merge SQL shape and the AVG division/zero-guard.

7. **E2E**
   - [ ] 7.1 E2E: after `createVirtualSchema`, assert the `CLUSTER_NODES` property is stored and ≥ 1.
   - [ ] 7.2 E2E: each supported aggregate (COUNT(*), COUNT(col), SUM, MIN, MAX, AVG) over the seeded table returns the correct merged value, with and without a WHERE filter.
   - [ ] 7.3 E2E: a multi-shard row query returns the same row set as the single-shard path (sharding correctness).

8. **Dead code / docs**
   - [ ] 8.1 Remove the capabilities test assertion that aggregates are absent (now advertised); update the create-VS test expectations to include `CLUSTER_NODES`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1 (createVirtualSchema NPROC), Task 2 (partition module) |
| Group B | Task 3 (IPROC SQL), Task 4 (aggregate detection + ScanSpec) |
| Group C | Task 5 (UDF partial emit), Task 6 (merge wrapper SQL) |
| Group D | Task 7 (E2E), Task 8 (dead code) |

Sequential dependencies:
- Group A → Group B (IPROC SQL builder consumes the partition module; aggregate plan extends ScanSpec which Group A's create path also touches)
- Group B → Group C (UDF partial emit + merge wrapper consume the ScanSpec aggregate plan and the fan-out shape)
- Group C → Group D (E2E exercises the full path; dead-code cleanup follows the capability/test changes)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test assertion | `crates/lakehouse-engine/src/adapter/capabilities.rs` (`reports_projection_filter_limit_only`) | Now asserts the opposite for aggregates — aggregate caps are advertised |
| Test expectation | create-VS dispatch test(s) | Response now also carries the `CLUSTER_NODES` property |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| create-virtual-schema: Adapter records the cluster node count as a VS property | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `create_vs_records_cluster_nodes_property` |
| create-virtual-schema: Cluster node count defaults to one when it cannot be determined | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `cluster_nodes_defaults_to_one_on_connect_back_failure` |
| iproc-sharding: File list is partitioned into one shard per cluster node | Unit | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_balanced_disjoint_full_coverage` |
| iproc-sharding: Fewer files than nodes produces no empty-shard invocations | Unit | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_caps_shards_at_file_count` |
| iproc-sharding: Multi-node scan-driving query fans the SET UDF across nodes via IPROC | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `multi_shard_sql_fans_via_iproc_group_by` |
| iproc-sharding: Single cluster node preserves the existing single-invocation query | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `single_shard_sql_matches_legacy_shape` |
| iproc-sharding: union of shard outputs equals single-shard scan (row query) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `multi_shard_row_query_matches_single_shard` |
| pushdown: Adapter advertises aggregate pushdown for supported functions | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_supported_aggregate_capabilities` |
| pushdown: Aggregate query is translated into a partial-aggregate scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `aggregate_query_builds_partial_agg_spec` |
| pushdown: Aggregate wrapper SQL merges per-shard partial results | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `aggregate_wrapper_merges_partials` |
| pushdown: AVG is pushed down as a sum/count pair and divided in the wrapper | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `avg_wrapper_divides_sum_by_count_guarded` |
| scan-execution: Scan computes a node-local partial aggregate instead of raw rows | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `scan_emits_partial_aggregate_row` |
| scan-execution: Partial COUNT/SUM/MIN/MAX emitted in merge-ready form | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `partial_count_sum_min_max_merge_ready` |
| scan-execution: AVG emitted as a partial sum and partial count pair | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `partial_avg_emits_sum_count_pair` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| create-virtual-schema | `CREATE VIRTUAL SCHEMA lh USING <adapter> WITH CATALOG_URI=... TABLE_NAME=...;` then inspect VS properties | Properties include `CLUSTER_NODES` set to the cluster's `NPROC()` (≥ 1) |
| iproc-sharding | `SELECT * FROM lh.events;` on a multi-node cluster, read the query profile | Scan SET UDF invoked once per node; each scans a disjoint file shard; full row set returned |
| pushdown (aggregate) | `SELECT COUNT(*), SUM(amount), MIN(ts), MAX(ts), AVG(amount) FROM lh.events;` | One correct merged row equal to single-node evaluation |
| scan-execution (partial) | `SELECT AVG(amount) FROM lh.events WHERE region='EU';` | Correct filtered average; profile shows one partial row per node merged in Exasol |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |

## Roadmap / Deferred

| Item | Why deferred |
|------|--------------|
| GROUP BY aggregate pushdown | This slice handles single-group aggregates only; grouped partials need group-key shuffling/merge |
| COUNT(DISTINCT col) | Not mergeable from per-shard partials without exact-distinct state transfer |
| HAVING pushdown | Depends on GROUP BY pushdown |
| Join pushdown | Out of scope per mission; needs multi-table planning |
| Databricks / Unity catalog access | Separate catalog integration track |
| Caching / materialization / result reuse | Explicit mission non-goal; engine stays stateless |
| Runtime node-count refresh | `CLUSTER_NODES` is captured once at create; live cluster-resize handling is future work |
