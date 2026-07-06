# Plan: add-join-pushdown-broadcast

## Summary

Push a single two-table inner equi-join into the DataFusion scan UDF for star-schema shapes: resolve both sides once, shard the larger (fact) side, replicate the smaller (dimension) side into every shard's common spec, and join node-locally — falling back to an Exasol-executed two-scan join when the smaller side is too big or the shape is outside the broadcast contract. Implements backlog BL-001 Phase 1 (Phase 2 shuffle/partitioned join stays out of scope).

## Design

### Context

Today the adapter advertises no `JOIN*` capability, so Exasol issues one independent row-scan pushdown per table and joins the fully materialized results in its core engine. Live `EXA_USER_PROFILE_LAST_DAY` telemetry (2026-07-06, `docs/performance.md` "Bottleneck analysis") shows those core-engine `JOIN` operators costing 1.5–3.5s CPU apiece over up to 180M rows — the largest lever against Trino on the four worst-losing TPC-H join queries (Q2, Q3, Q5, NQ3). TPC-H's customer/orders/lineitem/dimension shape is exactly the small-dimension-broadcast-against-large-fact case.

- **Goals** — advertise and serve inner equi-join pushdown for the broadcast (small-side) case; execute the join in node-local DataFusion with no cross-shard exchange; never regress correctness for any inner equi-join Exasol pushes.
- **Non-Goals** — outer/full joins, non-equi conditions, multi-way (>2 table) joins in one pushdown, any large/large shuffle strategy (Phase 2), and any change to the existing large-side file-sharding/parallelism model.

### Decision

The larger side is sharded exactly as today; the smaller side's FULL file list is carried once in the shard-invariant common spec and re-scanned by every shard's DataFusion session, which joins it against that shard's fact-file subset. Both sides' file lists, logical schemas, and metadata byte sizes are resolved ONCE per query in the VS planning layer. The small side is referenced by file list (not materialized into the spec), keeping the VS thin and all execution in the UDF.

Broadcast eligibility is decided in the VS from Iceberg manifest byte sizes (no Parquet read) against a `JOIN_BROADCAST_MAX_BYTES` adapter note (default 128 MiB). Ineligible joins emit deterministic unaccelerated SQL — each table scanned through its own sharded scan-UDF fan-out subquery, joined by Exasol's core engine — so correctness never depends on Exasol's error-retry behavior. An error (native retry) is only the last resort, when even the unaccelerated fallback cannot be built.

#### Architecture

```
pushdown(join) request
  → recover BOTH Iceberg idents via TABLE_MAP
  → resolve BOTH file lists + logical schema + metadata bytes  (ONCE, VS layer)
  → smaller side ≤ JOIN_BROADCAST_MAX_BYTES ?
        yes → BROADCAST:  fact side → G shards (per-shard arg)
                          dim  side → full file list (common spec join block)
                          scan-driving SQL: GROUP BY shard_key fan-out
                            → UDF: register fact(shard)+dim(full) in one session,
                              INNER JOIN ON <condition>, stream Arrow IPC batches
        no  → UNACCELERATED: SELECT ... FROM (scan-udf fan-out t1) JOIN
                              (scan-udf fan-out t2) ON <condition>  (Exasol joins)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Reference small side by file list (not materialize) | common-spec join block | VS stays thin; all execution in UDF; avoids VARCHAR-blob bloat |
| Metadata-only side sizing | `resolve_file_list` byte sums | resolve-once; no Parquet read for the threshold decision |
| Disjoint-column-name guard | join eligibility | lets the translator render bare column names unambiguously, reusing the filter path unchanged |
| Aliased sub-SELECT per registered table | scan-execution-join | binds pushed uppercased column names to Iceberg logical names, same as the partial-agg path |
| Deterministic unaccelerated fallback SQL | ineligible join branch | correctness independent of Exasol error-retry semantics |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Reference dimension side by full file list, re-scanned per shard | Materialize dimension rows to Arrow IPC/base64 in the common spec | File-list reference keeps the VS thin, avoids a large VARCHAR blob repeated to every shard, and reuses `register_files`; the bounded small side makes N re-scans cheap |
| Ineligible → deterministic two-scan join SQL (error only last resort) | Always decline ineligible joins with an error and rely on Exasol native retry | A hard error would regress currently-working join queries if Exasol does not cleanly re-plan; the two-scan SQL reproduces today's behavior deterministically |
| Reuse `vs-expression` translator unchanged + disjoint-column guard | Extend translator to render table-qualified columns | TPC-H tables have disjoint column prefixes; the guard makes bare-name rendering safe with zero translator churn (matches user intent to reuse the translator as-is) |
| Size threshold in bytes from Iceberg metadata | Row-count threshold; reading actual rows | `file_size_in_bytes` is directly available in the manifest with no data read; bytes bound the DataFusion build-side memory the guard actually protects |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-join | NEW | `vs-adapter/pushdown-planning-join/spec.md` |
| datafusion-scan/scan-execution-join | NEW | `datafusion-scan/scan-execution-join/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |

## Dependencies

- No new crate dependencies. Reuses `crates/vs-expression`, DataFusion, iceberg-rust, `exasol-udf-sdk` 0.20.2 (`emit-arrow`).
- GitHub issue: open a tracking issue (`ghbrk gh issue create`) for BL-001 Phase 1 and reference it in the implementing commit (`Closes #<n>`) per project rules.

## Implementation Tasks

1. Capabilities
   1. Add `JOIN`, `JOIN_TYPE_INNER`, `JOIN_CONDITION_EQUI` to `CAPABILITIES` (`adapter/capabilities.rs`); update the capability unit tests to assert the three are present and the outer/all-condition/Cartesian capabilities are absent.
2. Scan-spec extension
   1. Extend `CommonScanSpec` / `ScanSpec` (`scan/spec.rs`) with an optional join block (dimension `table_root`, `files`, `logical_schema`, `join_type`, rendered `condition`); wire serde and the two-argument reconstitution merge so existing non-join specs deserialize unchanged. [expert]
3. VS-side join planning (`adapter/pushdown.rs`, `adapter/mod.rs`)
   1. Detect the join `from` clause and its two involved tables; recover both original-cased Iceberg identifiers via `TABLE_MAP`; reject (fall through) non-inner / non-equi / >2-table shapes.
   2. Resolve both sides' file lists, logical schemas, and metadata byte sizes exactly once; select the smaller side and evaluate it against `JOIN_BROADCAST_MAX_BYTES`. [expert]
   3. Render the join condition, the cross-table projection/EMITS types, and any WHERE filter via `vs-expression`; enforce the disjoint-column-name guard. [expert]
   4. Build the broadcast fan-out scan-driving SQL: fact side sharded into G work-units, dimension side's full file list in the common-spec join block. [expert]
   5. Build the unaccelerated two-scan join fallback SQL (each table its own sharded fan-out subquery, joined by Exasol); return an error only when even that cannot be built.
   6. Read the `JOIN_BROADCAST_MAX_BYTES` adapter note (with default) and thread it through `handle_pushdown`.
4. UDF-side join execution (`scan/mod.rs`)
   1. When the spec carries a join block, register both the fact (per-shard) and dimension (full) file lists as two tables in one session, each wrapped in an aliased sub-SELECT exposing Exasol-facing column names.
   2. Execute the inner equi-join with the pushed projection/filter/limit; stream joined batches via `emit_batch` (fetch → emit → drop); ensure the bounded dimension side is the hash-join build side. [expert]
   3. Route unreadable-file and deserialization errors through the existing secret-redacting `classify_scan_error` path.
5. Tests
   1. Capability advertisement test (unit + `tests/e2e_capability_test.rs`).
   2. Join detection / side-selection / threshold / SQL-shape unit tests (`tests/scan_plan_shape.rs` style).
   3. Host DataFusion join-execution tests over local Parquet (`tests/scan_join_test.rs`).
   4. E2E broadcast join correctness against local Exasol Docker (`tests/e2e_join_test.rs`).
   5. E2E above-threshold unaccelerated-fallback correctness (same file), asserting the joined result equals native execution.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 2.1 |
| Group B | 3.1, 3.6 |
| Group C | 3.2, 3.3 |
| Group D | 3.4, 3.5, 4.1, 4.2, 4.3 |
| Group E | 5.1, 5.2, 5.3, 5.4, 5.5 |

Sequential dependencies:
- Group A → Group B → Group C → Group D (planning + execution build on the extended spec)
- Group D → Group E (tests exercise the built SQL and execution paths); 5.1 depends only on 1.1

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none expected) | — | This plan is purely additive: new join branches in the adapter/scan and two new features. The `table_count != 1` decline guards in `detect_topn` and other single-table paths stay as-is (they correctly still reject joins for those non-join shapes). |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| pushdown-planning-join: Adapter advertises inner equi-join capabilities | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `advertises_inner_equi_join_capabilities` |
| pushdown-planning-join: Broadcast-eligible inner equi-join is planned as a broadcast fan-out | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `join_broadcast_fan_out_sql_shape` |
| pushdown-planning-join: Small-side selection uses Iceberg metadata and the broadcast threshold | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `join_side_selection_and_threshold` |
| pushdown-planning-join: Join above the broadcast threshold falls back to an unaccelerated two-scan join | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `join_above_threshold_unaccelerated_sql` |
| pushdown-planning-join: Join projection and EMITS span both involved tables | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `join_projection_emits_two_tables` |
| pushdown-planning-join: Join condition is rendered via the vs-expression translator | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `join_condition_rendered_via_translator` |
| pushdown-planning-join: A join outside the broadcast contract is declined safely | Unit | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `join_outside_contract_declined_safely` |
| scan-execution-join: Scan reconstitutes a join scan spec carrying two file lists | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_spec_reconstitutes_two_file_lists` |
| scan-execution-join: Scan registers both tables and executes the inner equi-join | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_executes_inner_equi` |
| scan-execution-join: Join projection, filter, and LIMIT are applied and rows streamed as Arrow IPC | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_projection_filter_limit_streamed` |
| scan-execution-join: The bounded dimension side is the hash-join build side | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_build_side_is_dimension` |
| scan-execution-join: Scan reports a clear error when an assigned join file is unreadable | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_unreadable_file_errors_without_secrets` |
| pushdown-planning (CHANGED): Adapter advertises aggregate pushdown for supported functions | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_capabilities_includes_inner_join` |
| pushdown-planning-capability-extensions (CHANGED): Arithmetic operator capabilities advertised | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `advertises_inner_equi_join_capabilities` |
| pushdown-planning-capability-extensions (CHANGED): ORDER_BY_COLUMN advertised | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `advertises_inner_equi_join_capabilities` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-join | `EXPLAIN VIRTUAL SELECT c.C_NAME, o.O_ORDERDATE FROM LH.CUSTOMER c JOIN LH.ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY WHERE o.O_ORDERDATE >= DATE '1995-01-01';` | The pushed SQL invokes the scan UDF with a join fan-out (single scan-UDF driving query), not two independent per-table scans |
| datafusion-scan/scan-execution-join | `SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM LH.CUSTOMER c JOIN LH.ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY;` | Row count and min date equal the same query run against native (non-VS) TPC-H data |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, not skips, without Exasol Docker) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
