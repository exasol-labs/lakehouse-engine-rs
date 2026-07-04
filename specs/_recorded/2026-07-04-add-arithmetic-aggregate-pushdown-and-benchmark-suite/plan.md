# Plan: add-arithmetic-aggregate-pushdown-and-benchmark-suite

## Summary

Close a real, non-join competitive-benchmark gap by making `SUM(col_a OP col_b)` (two-column binary arithmetic under SUM) push down as a decomposed partial/merge aggregate instead of raw-emitting both columns, and expand the manually-invoked bench suite with five new TPC-H-shaped queries (NQ1-NQ5) wired identically across all four dialect scripts. A gated parallelism-factor sweep is included as an evidence-first validation task whose outcome may legitimately be a no-op.

## Design

### Context

`docs/performance.md` shows losses vs Trino. Live EXPLAIN VIRTUAL + PROFILE against test1 (2-node r8i.2xlarge, sf=30) attributed them:

- **Join-shaped Q2/Q3/Q5 and high-cardinality Q7 are structural / already-correct** — out of scope (no join pushdown per mission.md; Exasol's own join execution is correct per PROFILE).
- **`SUM(l_extendedprice * l_discount)` (TPC-H Q6 shape, no join) falls back to raw-emitting `[L_EXTENDEDPRICE, L_DISCOUNT]`** — the same expensive path as the join queries, for a query that should be fully pushable (live: 5.96s).

Root cause (confirmed by reading the code, not assumed):

1. `crates/lakehouse-engine/src/adapter/capabilities.rs` advertises `FN_MOD` but **no** arithmetic binary-operator capabilities (`FN_ADD`/`FN_SUB`/`FN_MULT`/`FN_FLOAT_DIV`). Without them Exasol never pushes an aggregate whose argument is an arithmetic expression tree down as a `function_aggregate` — it requests the raw operand columns and computes the SUM itself.
2. The downstream machinery already exists: `arg_column_or_expr` (pushdown.rs:999) renders *any* translatable expression argument via `render_expression`, and `SUM(expr)` decomposition + declared-type widening (`col_type_for`/`sum_emit_type`) already handle expression arguments (built for #56). So this is a small gap inside already-designed machinery, not a new subsystem.
3. **Naming risk**: the `crates/vs-expression` translator matches the multiplication node `name` as `"MUL"` (lib.rs:345), but Exasol's capability/name vocabulary uses `MULT`. Arithmetic ops were only ever unit-tested with hand-crafted `"MUL"` JSON and never exercised live (they were unadvertised). If Exasol emits `MULT`, the translator declines → row-scan fallback even after advertising. The exact live `name` strings must be captured and reconciled.

- **Goals** — make `SUM(col OP col)` for `OP ∈ {*, +, -, /}` between two numeric columns decompose into the existing partial/merge plan; correct type derivation for a DECIMAL product; grow the bench suite to cover untested engine areas; gather evidence on parallelism oversubscription.
- **Non-Goals** — no join pushdown; no change to file-sharding/work-unit-sharding architecture; no generalization to arbitrary/N-ary expression trees or to COUNT/AVG/MIN/MAX arithmetic arguments (SUM only, unless the existing machinery makes more trivially free); no operational benchmark run / `docs/performance.md` edit (that is the follow-up step).

### Decision

Advertise the four arithmetic binary-operator capabilities, reconcile the translator's operator-name matching to what Exasol actually emits (verified live), and confirm the existing expression-argument SUM decomposition + declared-type widening handles a two-column DECIMAL product without overflow. Everything downstream of capability advertisement already exists.

#### Architecture

```
getCapabilities: advertise FN_ADD/FN_SUB/FN_MULT/FN_FLOAT_DIV
        │
        ▼
Exasol pushes  SUM( function_scalar(MULT, [col_a, col_b]) )  in the pushdown request
        │
        ▼
pushdown.rs arg_column_or_expr → render_expression  ──►  "(L_EXTENDEDPRICE * L_DISCOUNT)"
        │                                   (vs-expression translator; name reconciled to MULT)
        ▼
AggKind::Sum + arg_expr set → partial EMITS type from declared selectListDataTypes
        │                     (col_type_for → sum_emit_type widens DECIMAL(p,s)→DECIMAL(36,s))
        ▼
per-shard partial SUM over the rendered expr  ─►  outer wrapper merges  ─►  final result
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Capability advertisement gated on backing translator arm | `capabilities.rs` + `vs-expression` | Coherence: never advertise an operator the translator would decline (same discipline as `AGGREGATE_GROUP_BY_TUPLE`) |
| Reuse expression-argument partial/merge machinery | `pushdown.rs` `arg_column_or_expr` / `col_type_for` / `sum_emit_type` | The two-column product is just another renderable argument; no new decomposition path |
| Declared-type-driven partial column sizing | `pushdown.rs` `col_type_for` | A DECIMAL product has no single source column; the SUM's declared `selectListDataTypes` type is the sole authoritative source, widened to `DECIMAL(36,s)` to avoid mid-merge overflow |
| Correctness-safe fallback on untranslatable node | `render_expression` `.ok()` → decline pushdown | An unrecognized operator/operand degrades to row scan (slower, still correct), never a wrong answer |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Advertise arithmetic operator capabilities globally (enables arithmetic in filters/select-lists too, not only SUM args) | A hypothetical "arithmetic only inside SUM" advertisement | The Exasol capability model is coarse — there is no position-scoped capability. The translator already handles arithmetic in every position and untranslatable nodes fall back safely, so the incidental broadening is safe and a net win |
| Scope the code fix to SUM only | Extend to COUNT/AVG/MIN/MAX arithmetic args | Matches the required scope; other aggregates already route through the same `arg_column_or_expr` seam, so they may come along for free — note if so, but do not require it |
| Item 3 (parallelism sweep) is evidence-gated, no-op acceptable | Hardcode `BENCH_PARALLELISM_FACTOR`/`resolve_parallelism_factor` default change now | The diagnostic flagged the 10-30% improvement as UNVERIFIED (plausible only if emit-ack-latency-bound; zero gain if CPU-decode-bound). Changing a default without the sweep would be speculative |
| Item 5 (bench queries) is bench-script-only, not a spec feature | A `packaging` spec delta for the bench harness | The bench harness is explicitly "NOT a spec feature" (sweep.sh header; README: separate from CI `make test-e2e`). Following the repo's own convention |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-expression-aggregate | CHANGED | `vs-adapter/pushdown-planning-expression-aggregate/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |

Items 3 (parallelism sweep) and 5 (new bench queries) are bench-harness / validation work with no spec feature (see Consequences); they appear only as implementation tasks and manual-verification rows.

## Dependencies

- Access to the Exasol test1 cluster + Glue/S3 TPC-H sf=30 data for live root-cause capture (task 4.1) and the parallelism sweep (tasks 3.1/3.2). Both require a built `.so` uploaded to BucketFS (`bench/.env` `remote` mode). Host-only unit/integration tests do not.

## Implementation Tasks

### Group A — Two-column arithmetic aggregate pushdown (item 4)

1.1 Live root-cause capture: run `EXPLAIN VIRTUAL` for the NQ1 shape (`SUM(L_EXTENDEDPRICE * L_DISCOUNT) ... WHERE ...`) against test1; record the exact `function_scalar` `name` Exasol emits for `*`, `+`, `-`, `/`, and the SUM's declared `selectListDataTypes` result type (precision/scale of the DECIMAL product). Write findings into the decision log. This gates 1.2 and 1.3.

1.2 Advertise the arithmetic binary-operator capabilities (`FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`) in `crates/lakehouse-engine/src/adapter/capabilities.rs`; extend the `reports_audited_capability_set` / `reports_supported_*` tests to assert their presence and that no join capability appears.

1.3 Reconcile the `crates/vs-expression` arithmetic operator-name matching (lib.rs `function_scalar` arm) with the verified live names from 1.1 (accept the real Exasol name, e.g. `MULT`; keep back-compat for existing `MUL` tests only if the live name differs); add unit tests rendering `(L_EXTENDEDPRICE * L_DISCOUNT)` from a two-column node and asserting the advertised operator set and the rendered set stay in lockstep. [expert]

1.4 Verify and, if needed, adjust the expression-argument SUM type derivation in `crates/lakehouse-engine/src/adapter/pushdown.rs` for a two-column DECIMAL product: partial column sized from the declared SUM `selectListDataTypes` type via `col_type_for`, widened to `DECIMAL(36,s)` via `sum_emit_type`, staying within Exasol's `DECIMAL(p<=36,s<=36)` limits and not overflowing per-shard partial sums; add plan-shape unit/integration coverage asserting the `aggregates` field (with `arg_expr`) appears — NOT a raw 2-column row-scan fallback — and that the merge CAST type is correct. [expert]

1.5 End-to-end integration test: NQ1-shape query over a MinIO-backed Iceberg table asserting the merged pushdown result equals single-node evaluation, plus the untranslatable-argument path still falls back to row scan.

### Group B — Five new benchmark queries (item 5)

2.1 Add NQ1-NQ5 to `bench/run.sh` (Exasol dialect) as `run_query` entries following the Q1-Q9b TIMING convention; add a `pushdown_check` for NQ1 asserting the `aggregates` + `arg_expr` fields appear (post Group A, mirroring the existing Q9b check) and a `pushdown_check` for NQ2 asserting `LIKE` + `IN` reach the scan spec.

2.2 Add NQ1-NQ5 to `bench/trino_compare.sh` and `bench/athena_compare.sh` in Presto/Trino dialect (Athena shares the Presto dialect), matching the existing `run_timed` + `TIMING <engine> <name> <seconds>` convention.

2.3 Add NQ1-NQ5 to `deploy/scripts/spark_queries.py` in Spark SQL dialect, matching the existing `QUERIES` list convention.

2.4 Update the `bench/README.md` "keep all four in sync" note to cover NQ1-NQ5; run `./bench/run.sh selftest` (offline string-logic self-check, no DB) to confirm the scripts parse.

### Group C — Parallelism-factor sweep (item 3, evidence-gated, no-op acceptable)

3.1 Add a `BENCH_PARALLELISM_FACTOR` sweep to `bench/sweep.sh` (or a sibling driver) with config rows for factor ∈ {8, 16, 24}, measured against Q2/Q3/Q5 (the raw-emit-heavy join queries the change would help) and Q9b (non-join wide-projection regression check). Run against test1, capturing elapsed per config into `bench/reports/`.

3.2 Analyze the sweep evidence and record the finding in the decision log **either way**. IF (and only if) the evidence shows a real, repeatable improvement without a Q9b regression, apply the chosen change (`bench/.env` default, `resolve_parallelism_factor` default in `adapter/mod.rs`, or both) and add a spec delta to `parallelism/work-unit-sharding`. IF the evidence is flat/negative, change nothing and record it as a validated no-op.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (code) | 1.2 and 1.3 concurrent after 1.1; then 1.4; then 1.5 |
| Group B (bench wiring) | 2.2 and 2.3 concurrent; 2.1 any time; 2.4 after 2.1-2.3 |
| Group C (sweep) | 3.1 then 3.2 |

Sequential dependencies:
- 1.1 → 1.2, 1.3 → 1.4 → 1.5 (within Group A)
- Group A and Group B run in parallel, EXCEPT 2.1's NQ1 `pushdown_check` (asserting the `aggregates` field) can only pass once Group A has landed — wire it in Group B, verify it after Group A.
- Group C is independent of A and B (it targets join queries unaffected by item 4); it needs a built `.so` on test1 and should sweep against the final binary.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none expected) | — | This plan extends existing machinery; the only likely edit is correcting the `MUL`→`MULT` operator-name match, which replaces rather than removes. Remove any now-dead `"MUL"`-only match arm if the live name proves to be exclusively `MULT`. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| expr-agg: SUM over a two-column binary-arithmetic argument is pushed down | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `sum_two_column_product_pushes_down_matches_single_node` |
| expr-agg: SUM over a two-column binary-arithmetic argument is pushed down (plan shape) | Integration | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `sum_two_column_product_emits_aggregates_not_raw_scan` |
| expr-agg: A DECIMAL two-column product widens its partial column from the declared SUM result type | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `decimal_product_sum_partial_widens_to_decimal_36` |
| cap-ext: Arithmetic operator scalar-function capabilities are advertised | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `capabilities_advertise_arithmetic_operators` |
| cap-ext: An advertised arithmetic expression Exasol cannot decompose remains correctness-safe | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `unrenderable_arithmetic_arg_falls_back_to_row_scan` |
| scalar-ops: Arithmetic operators translate to binary SQL expressions (name reconciled) | Unit | `crates/vs-expression/src/lib.rs` | `arithmetic_two_column_mult_renders_with_live_name` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Arithmetic aggregate pushdown (NQ1) | `./bench/run.sh` then inspect the `PUSHDOWN: NQ1` block in `bench/reports/<ts>.txt` | `OK    pushed: aggregates` and `OK    pushed: arg_expr` (no 2-column raw `LAKEHOUSE_SCAN` fallback); NQ1 revenue = 3,695,273,210 |
| New bench queries wired in all 4 scripts | `./bench/run.sh selftest` | Self-check passes (scripts parse, query names resolve) |
| Trino/Athena/Spark NQ1-NQ5 dialects | `grep -c 'NQ[1-5]\|nq[1-5]' bench/trino_compare.sh bench/athena_compare.sh deploy/scripts/spark_queries.py` | 5 per script |
| Parallelism sweep (item 3) | `./bench/sweep.sh bench/reports/pf-sweep-$(date +%s).txt` (test1, staged `.so`) | Per-factor elapsed rows for Q2/Q3/Q5/Q9b; decision recorded in decision log |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (host) | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
