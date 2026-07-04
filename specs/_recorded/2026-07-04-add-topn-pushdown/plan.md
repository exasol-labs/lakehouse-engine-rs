# Plan: add-topn-pushdown

## Summary

Close a live, non-join competitive loss (NQ4 `SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM lineitem ORDER BY L_EXTENDEDPRICE DESC LIMIT 20` loses to Trino 12.03s vs 4.71s on TPC-H sf=30 test1) by advertising `ORDER_BY_COLUMN` and pushing `ORDER BY <cols> LIMIT n` down as a decomposed partial/merge top-N — each shard emits its own bounded local top-N (≤ n rows) and Exasol merges them with a final `ORDER BY … LIMIT n` — instead of raw-emitting the whole table for Exasol to sort. It reuses the SHAPE of the existing aggregate partial/merge machinery, not its aggregate-specific code.

## Design

### Context

Today the adapter does NOT advertise any `ORDER_BY*` capability (a `capabilities.rs` test actively asserts `ORDER_BY*` is absent), while it DOES advertise `LIMIT`. For a `SELECT … ORDER BY sortcol LIMIT n` query, Exasol therefore cannot push the ordering, so it requests a full raw scan (all matching rows from every shard) and performs the entire sort + limit itself. For NQ4 that raw-emits ~180M rows × 2 columns across the UDF boundary when only `≤ (shard_count × 20)` rows actually need to cross. The captured NQ4 result is correct (top row `L_ORDERKEY=151324423, L_EXTENDEDPRICE=104949.00`, fully descending) — i.e. today's behavior is safe-but-slow, consistent with Exasol withholding the `LIMIT` when it cannot also push the ordering.

Root cause (read from the code, confirmed against the live cluster in the gating tasks): the optimization is unavailable purely because `ORDER_BY_COLUMN` is unadvertised — so Exasol never delegates the ordering. The downstream machinery to bound and merge is a near-exact analog of the aggregate partial/merge path already shipped (`build_scan_driving_sql` / `build_row_scan_sql` for the fan-out + outer wrapper; `ScanSpec` common/per-shard split; `build_scan_sql` in the scan UDF that already appends `WHERE`/`LIMIT`).

- **Goals** — push `ORDER BY <bare projected column(s)> LIMIT n` (single table, no GROUP BY, no aggregate, no OFFSET) down as a per-shard bounded top-N + Exasol-side merge that provably equals single-node evaluation, including descending order and NULL placement; measurably beat the 12.03s NQ4 baseline while returning identical rows.
- **Non-Goals** — no join pushdown; no change to the file-sharding / work-unit-sharding architecture; no `ORDER_BY_EXPRESSION` (expression sort keys); no `LIMIT_WITH_OFFSET` (OFFSET stays with Exasol and cannot appear in the request); no ordered-top-N over aggregate/grouped results; no window functions.

### Decision

Advertise `ORDER_BY_COLUMN`. Add an `order_by` sort-key list (column + `ascending` + `nulls_last`) to the shard-invariant scan spec. In the adapter's row-scan branch, when the request carries an `order_by` + `limit` and matches the top-N shape (single table, no aggregates/group keys/having, every sort key a bare projected column, no offset), carry the sort keys and limit into the common spec and wrap the shard fan-out in an outer `SELECT <projection> FROM (<fan-out>) ORDER BY <keys> LIMIT n`. Each shard's `build_scan_sql` then appends `ORDER BY <keys> LIMIT n` (DataFusion folds `ORDER BY … LIMIT` into a bounded TopK), emitting ≤ n rows per shard; the outer merge produces the exact top-N. The outer merge `ORDER BY` uses the identical key order/direction/NULL placement as the per-shard sort — this is the single correctness-critical invariant.

The returned SQL is **self-contained**: it carries its own outer `ORDER BY … LIMIT n`, so correctness does not depend on Exasol re-applying a backstop ordering. For every ORDER-BY-carrying shape the adapter does NOT match as a top-N, it falls back to the pre-existing plan and MUST NOT push a bare per-shard `LIMIT` ahead of a global sort — relying on Exasol to apply the `ORDER BY` it retains (the same backstop model already relied on for `LIMIT` and `HAVING`).

#### Architecture

```
getCapabilities: advertise ORDER_BY_COLUMN  (ORDER_BY_EXPRESSION / LIMIT_WITH_OFFSET stay absent)
        │
        ▼
Exasol pushes  order_by:[{expression: col, isAscending, nullsLast}], limit:{numElements:n}
        │
        ▼
adapter handle_pushdown (row-scan branch): detect_topn(order_by + limit + shape guards)
        │  matched → ScanSpec.order_by = [SortKey{col,asc,nulls_last}], ScanSpec.limit = n
        │  not matched → pre-existing plan, per-shard LIMIT withheld if order_by present
        ▼
build_row_scan_sql: per-shard  SELECT proj … ORDER BY <keys> LIMIT n   (bounded TopK per shard)
        │                       outer  SELECT proj FROM (<fan-out>) ORDER BY <keys> LIMIT n
        ▼
scan UDF build_scan_sql: appends ORDER BY <keys> LIMIT n after WHERE  → DataFusion TopK, ≤n rows emitted
        ▼
Exasol merges ≤ (shard_count × n) rows → final ORDER BY … LIMIT n
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Capability advertisement gated on a backing bounded-sort path | `capabilities.rs` + adapter top-N detection | Never advertise `ORDER_BY_COLUMN` without the per-shard+merge path that honors it — same discipline used for arithmetic operators and GROUP BY tuple |
| Reuse the partial/merge SHAPE (per-shard bound → Exasol merge) | `build_row_scan_sql` fan-out + outer wrapper | The top-N is a new partial/merge variant, not a new architecture; mirrors the aggregate path's fan-out-then-merge structure |
| Sort keys carried in the shard-invariant common spec | `ScanSpec` / `CommonScanSpec` `order_by` field | Every shard must run the IDENTICAL bounded sort; putting keys in the common blob makes "same sort for every shard" a structural guarantee, like the shared LIMIT |
| Identical order/direction/NULL placement per-shard and in merge | adapter SQL builder + scan `build_scan_sql` | The distributed top-N is exact only if the two sorts agree on ranking; a mismatch silently diverges from single-node results |
| Self-contained outer ORDER BY (no dependence on Exasol backstop) | adapter outer wrapper SQL | Correctness of the matched top-N does not rest on whether Exasol re-sorts; the returned SQL fully specifies the final ordering |
| Correctness-safe decline for unmatched shapes | adapter row-scan / aggregate branches | An ORDER BY the adapter cannot bound falls back to the pre-existing plan (no per-shard limit ahead of a global sort), never a wrongly-truncated or misordered result |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Advertise `ORDER_BY_COLUMN` only; keep `ORDER_BY_EXPRESSION` and `LIMIT_WITH_OFFSET` absent | Advertise expression ordering and/or offset too | Column sort keys cover the target query (NQ4) and the common top-N shape; expression/offset ordering add rendering + bounded-sort-with-skip complexity with no evidence of need. Keeping them unadvertised means Exasol never pushes a shape the adapter has no path for |
| Require every sort key to be a bare column that is ALSO in the projection | Emit unprojected sort keys as extra trailing EMITS columns dropped by the outer SELECT | The projected-key restriction lets the outer merge sort on already-emitted columns with zero extra machinery and covers NQ4; unprojected sort keys are a clean future extension, deferred to keep the MVP provably simple. Unmatched keys decline safely |
| Self-contained outer `ORDER BY … LIMIT n` in the returned SQL | Rely on Exasol re-applying the pushed ORDER BY as a top-level backstop | Not depending on the backstop for the matched path removes an entire class of "does Exasol re-sort?" risk; the outer wrapper already exists for the fan-out, so adding `ORDER BY … LIMIT` to it is cheap |
| Multi-column sort keys handled by the same list-rendering path | Single-column MVP only, decline multi-key | Rendering a comma-separated key list (each with direction + NULL placement) is the same code for one or many keys, so multi-key falls out for free; a key that is not a bare projected column still declines. If any multi-key edge cost appears in implementation, drop to single-key and decline multi-key (still an acceptable v1) |
| Gate the code change on live verification of today's ORDER BY + LIMIT behavior and Exasol's order_by request shape | Assume from the code that today is safe-but-slow and code straight to it | The whole plan-shape (pure optimization vs optimization + latent per-shard-limit bug) turns on whether Exasol pushes a bare `LIMIT` today for an ORDER BY query, and the exact `order_by` JSON field names / NULL semantics must match what the live cluster emits — both are verified before coding, mirroring the sibling plan's live root-cause gate |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-topn | NEW | `vs-adapter/pushdown-planning-topn/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |

The NQ4 re-run against test1 is manual benchmark verification (see § Verification), not a spec feature — matching the repo's convention that the bench harness is not a spec feature.

## Dependencies

- Access to the Exasol test1 cluster + Glue/S3 TPC-H sf=30 `lineitem` for the gating live-capture tasks (A1/A2) and the NQ4 re-run (C1); credentials in `bench/.env`. Requires a built `.so` staged to BucketFS. Host-only unit/integration tests do not.
- The staged SLC on test1 must match the crate's `exasol-udf-sdk` version (fingerprint check) before any live capture or NQ4 re-run — the sibling plan hit a 0.19.1/0.20.1 mismatch; run `make install-slc` if the fingerprint is stale.

## Implementation Tasks

### Group A — Gating live investigation (must complete before Group B code)

- [ ] A1 Live-verify today's `ORDER BY + LIMIT` behavior: run `EXPLAIN VIRTUAL` for the NQ4 shape against test1 and capture whether Exasol pushes a `limit` element (and any `order_by`) when `ORDER_BY_COLUMN` is UNadvertised. Confirm whether the current code would ever push a bare per-shard `LIMIT` for an ORDER BY query (read `extract_limit` + `build_row_scan_sql`; both already read). Record in the decision log whether this plan is pure optimization or also fixes a latent per-shard-limit truncation bug. Gates A2/B3.
- [ ] A2 Capture the exact `order_by` request shape: temporarily advertise `ORDER_BY_COLUMN` on a scratch `.so` (or read the Exasol VS protocol docs if a scratch deploy is not warranted) and record, from live `EXPLAIN VIRTUAL` for the NQ4 shape, the exact JSON field names and semantics Exasol emits for each sort key — the sort-key expression node shape, the ascending flag name (e.g. `isAscending`), and the NULL-placement flag name/meaning (e.g. `nullsLast`) — plus whether Exasol still re-applies a top-level `ORDER BY`/`LIMIT` backstop when it pushes them. Record in the decision log; these field names pin B3/B4.
- [ ] A3 Confirm DataFusion folds `ORDER BY <col> LIMIT n` into a bounded TopK (not a full global `SortExec`) on the single-shard raw-scan path, so the per-shard sort is memory-bounded; note the exact `build_scan_sql` insertion point (after `WHERE`, before/with `LIMIT`) and confirm the existing raw-scan plan-shape guard (`scan-execution`: "no needless global SortExec") is reconciled with an intentional bounded TopK when `order_by` is set.

### Group B — Ordered top-N pushdown (code)

- [ ] B1 Add an `order_by` sort-key list to `ScanSpec` / `CommonScanSpec` (`crates/lakehouse-engine/src/scan/spec.rs`): a `Vec<SortKey>` (or `Option`), where `SortKey { column: String, ascending: bool, nulls_last: bool }`, `#[serde(default, skip_serializing_if = ...)]` so legacy specs (no `order_by`) still deserialize and the common-blob wire shape is unchanged when absent. Add round-trip serde coverage.
- [ ] B2 Advertise `ORDER_BY_COLUMN` in `crates/lakehouse-engine/src/adapter/capabilities.rs`; FLIP the existing `ORDER_BY*`-must-be-absent assertion to assert `ORDER_BY_COLUMN` present AND `ORDER_BY_EXPRESSION` + `LIMIT_WITH_OFFSET` + all join capabilities absent.
- [ ] B3 Detect the ordered-top-N shape and build its SQL in `crates/lakehouse-engine/src/adapter/pushdown.rs`: parse `order_by` (using the field names verified in A2) into `SortKey`s; match the shape (single involved table, no aggregates/group keys/having, `limit` present with no offset, every sort key a bare column present in the projection); on match set `ScanSpec.order_by` + `limit` and make `build_row_scan_sql` wrap the fan-out in an outer `ORDER BY <keys> LIMIT n`; on non-match, ensure NO bare per-shard `LIMIT` is emitted when an `order_by` is present (withhold it). Add plan-shape unit/integration tests asserting the outer + per-shard ORDER BY appear on match and are absent (with the limit withheld) on the decline shapes. [expert]
- [ ] B4 Render the per-shard `ORDER BY` in the scan UDF (`build_scan_sql`, `crates/lakehouse-engine/src/scan/mod.rs`): append `ORDER BY <key [ASC|DESC] [NULLS FIRST|NULLS LAST]>, …` after the `WHERE` and before/with the existing `LIMIT`, with direction and NULL placement matching the adapter's outer merge EXACTLY. Verify DataFusion's NULL-ordering default and render an explicit `NULLS FIRST`/`LAST` so per-shard and merge agree regardless of defaults. Add unit tests over a local Parquet file asserting a bounded, correctly-ordered result including a DESC + NULL-placement case. [expert]
- [ ] B5 End-to-end integration test (`crates/lakehouse-engine/tests/e2e_scan_test.rs`): an ordered-top-N query over a MinIO-backed Iceberg table asserting the merged pushdown result (rows AND order) equals single-node `ORDER BY … LIMIT n`, plus a decline case (ORDER BY over an unprojected column, or ORDER BY with no LIMIT) that falls back correctly.

### Group C — Benchmark verification

- [ ] C1 Re-run NQ4 (`./bench/run.sh`, test1, staged `.so`) after Group B lands: confirm (a) correctness — identical 20 rows/values to the captured raw-scan baseline (top row `L_ORDERKEY=151324423, L_EXTENDEDPRICE=104949.00`, full list in `bench/reports/bench-report-20260704-122600.txt`) and (b) a measured speedup from the 12.03s baseline. Add an NQ4 `pushdown_check` asserting the scan spec carries `order_by` + `LIMIT` (not a bare 2-column raw scan). Record the elapsed and the Trino delta (Trino baseline 4.71s already captured) in the decision log.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (gating live capture) | A1 then A2; A3 concurrent with A1/A2 (host-only) |
| Group B (code) | B1 and B2 concurrent after A2; then B3; then B4; then B5 |
| Group C (bench) | C1 after Group B |

Sequential dependencies:
- A1 → A2 → B3 (the `order_by` field names verified in A2 pin the B3 parser; A1 decides pure-opt vs bugfix framing)
- B1, B2 → B3 → B4 → B5 (B3 needs the spec field and the capability; B4 renders what B3 plans; B5 exercises the full path)
- Group B → C1 (NQ4 re-run needs the built `.so` with the top-N path)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test assertion | `crates/lakehouse-engine/src/adapter/capabilities.rs` (`has_order_by` must-be-absent check) | Replaced by the inverted assertion in B2 (`ORDER_BY_COLUMN` present, `ORDER_BY_EXPRESSION`/`LIMIT_WITH_OFFSET`/join absent) — the old "no ORDER_BY at all" invariant no longer holds |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| topn: Ordered top-N over a projected column is pushed down as per-shard bounded sort plus Exasol merge | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `ordered_topn_pushes_down_matches_single_node` |
| topn: Ordered top-N over a projected column is pushed down (plan shape) | Integration | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `ordered_topn_emits_per_shard_and_outer_order_by` |
| topn: Per-shard row limit is emitted only alongside the matching per-shard sort | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `order_by_present_without_topn_match_withholds_per_shard_limit` |
| topn: Ordered top-N preserves descending and NULL ordering | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `ordered_scan_sql_preserves_desc_and_null_placement` |
| topn: Unsupported ordered-query shapes decline the ordered-top-N path | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `unsupported_order_by_shape_declines_topn` |
| cap-ext: ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `capabilities_advertise_order_by_column_only` |
| cap-ext: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `unbounded_order_by_falls_back_correctness_safe` |
| scan-exec: Scan emits a bounded local top-N when the spec carries an order-by | Integration | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `order_by_spec_emits_bounded_topk_not_global_sort` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Ordered top-N pushdown (NQ4) | `./bench/run.sh` then inspect the `PUSHDOWN: NQ4` block and `TIMING lakehouse NQ4` in `bench/reports/<ts>.txt` | `OK    pushed: order_by` and `OK    pushed: LIMIT` (no 2-column raw `LAKEHOUSE_SCAN` fallback); NQ4 top row `L_ORDERKEY=151324423, L_EXTENDEDPRICE=104949.00`; elapsed materially below the 12.03s baseline |
| ORDER_BY_COLUMN advertised | `EXPLAIN VIRTUAL SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM <vs>.LINEITEM ORDER BY L_EXTENDEDPRICE DESC LIMIT 20;` on test1 | The generated pushdown SQL carries a per-shard and an outer `ORDER BY … LIMIT 20` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (host) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
