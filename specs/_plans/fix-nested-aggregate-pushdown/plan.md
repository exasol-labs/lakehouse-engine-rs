# Plan: fix-nested-aggregate-pushdown

## Summary

Fix issue #52: the nested-aggregate query `SELECT COUNT(*) FROM (SELECT L_ORDERKEY, COUNT(*) AS cnt FROM TPCH.LINEITEM GROUP BY L_ORDERKEY) t` fails fast (~1.2s, at pushdown-SQL-generation time) with `F-UDF-CL-RUST-9001: ... DataFusion SQL error: Schema error: No field named "NULL"`, while Athena/Trino/Spark all handle the identical query. The exact defect mechanism and the exact `pushdownRequest` JSON Exasol sends for this shape are currently UNKNOWN, so this plan front-loads a bounded diagnostic spike that captures the real request against the local Exasol Docker E2E stack, and only then decomposes a behavioral fix (correct pushdown OR clean fallback to row-scan — never crash) plus a Q7-shape regression test.

## Design

### Context

Issue #52's stated hypothesis — that the adapter substitutes a literal `NULL` where a field reference is expected when composing an outer `COUNT(*)` over an already-pushed-down inner `GROUP BY` — was investigated and **does not match any code in this repo** (see decision-log entry [1] for the full trace). The genuine `"NULL"` SQL-literal producers (`crates/vs-expression/src/lib.rs`, `empty_pushdown_sql` at `adapter/pushdown.rs:2543`) do not feed an aggregate's function argument, the `COUNT(*)` arms render no NULL placeholder, and the adapter has **no code path that recursively reads a nested `from`/sub-select** — `adapter/mod.rs` always reads table identity from flat `involvedTables[0].name` and `selectList`/`groupBy`/`filter` from the top level of `pushdownRequest`. `capabilities.rs` advertises no subquery capability.

Because the error is a `DataFusion SQL error` surfaced from **inside** the UDF's DataFusion session (i.e. from `build_scan_sql` / `build_dataframe` in `crates/lakehouse-engine/src/scan/mod.rs`, which builds the per-shard SELECT from a `ScanSpec`), whatever request Exasol actually sends causes the adapter to build a `ScanSpec` whose rendered per-shard SQL references a phantom `"NULL"` identifier absent from the source table's schema. The real request structure must be captured before any fix is written, so the fix targets the actual mechanism rather than a guessed one.

- **Goals** — (1) Capture the real `pushdownRequest` JSON Exasol sends for the Q7 nested-aggregate shape against the local Docker E2E stack; (2) make the query behave correctly — either a correctly composed pushdown OR a safe fallback to non-pushed row-scan execution — and NEVER crash/error at planning time; (3) lock in a Q7-shape E2E regression test so it never regresses silently.
- **Non-Goals** — Building general multi-level nested-aggregate / subquery pushdown composition as a new capability (mission lists "complex query rewrites" under Out of Scope); live-cluster (AWS Glue) re-verification (treated as generic Exasol optimizer/pushdown behavior reproducible on local Docker — flagged for user override at review, see decision-log [3]); changing shard-count math, memory sizing, or the existing grouped/single-group aggregate decomposition.

### Decision

Spike-first, then a behavioral fix whose concrete shape is determined by the spike's captured data. The plan deliberately does NOT pre-commit to a specific code edit (e.g. "add sub-select walking to `adapter/mod.rs`") because the correct code layer is unknown until the captured request is in hand. Once captured, the fix is one of two families the plan must support without re-planning:

1. **Correct handling** — if the captured request is a well-formed shape the adapter mis-parses into a phantom-column ScanSpec, correct the parsing/detection so the emitted SQL only references real source columns (or falls through cleanly).
2. **Safe fallback** — if the composed shape cannot be soundly pushed down, ensure the adapter's aggregate/grouped-aggregate detection returns `None` for it (or an equivalent guard rejects it) so the existing row-scan fallback engages and Exasol computes the outer aggregate on returned rows.

Either way the acceptance criterion is behavioral (spec `pushdown-planning` § "Composed pushdown request never renders a scan spec that references a non-source column"): the query returns the correct `COUNT(*)` or falls back, and never surfaces a DataFusion schema error.

#### Architecture

```
Q7 nested-aggregate SQL
  → Exasol optimizer → pushdownRequest JSON  ← [SPIKE captures THIS exact shape]
  → adapter/mod.rs::handle_pushdown_request
       reads involvedTables[0].name, selectList, groupBy, filter (all top-level today)
  → detect_aggregates / detect_group_by_aggregates  ← [FIX guards or corrects HERE (layer TBD by spike)]
  → ScanSpec → scan-driving SQL
  → scan/mod.rs::build_scan_sql/build_dataframe (DataFusion)  ← [where "No field named NULL" is raised today]
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Diagnostic spike gates the fix | Task 1 output → Task 2/3 scope | Root cause genuinely unknown; the captured request shape determines which layer to patch and which fix family applies |
| Behavioral acceptance, not prescribed edit | Task 3 acceptance criteria | Implementer only knows the concrete fix after the spike; spec asserts observable behavior (correct result OR clean fallback, never crash) |
| Fail-closed to row-scan fallback | detect_* returning `None` | Repo already has a row-scan fallback for unsupported shapes; extending it is lower-risk than growing subquery-pushdown surface area (out of scope) |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Repro-first spike before writing any fix | Skip root-causing; add blanket defensive fallback for "any unrecognized shape" | Blanket fallback risks masking the real defect without knowing whether the current fallback-detection layer is even the right patch point; spike is a bounded cost that de-risks the fix (interview Q1) |
| Local Exasol Docker + Iceberg/MinIO E2E as the repro/verification environment | Live-cluster (AWS Glue) re-verification | Treated as generic Exasol pushdown-composition behavior, not Glue-specific; matches repo's standard E2E convention (must fail, not skip). Flagged for user override at review (interview Q2) |
| Fix scoped to correct-pushdown-OR-safe-fallback for this shape | Build general nested/subquery aggregate pushdown | Mission explicitly lists "complex query rewrites" as Out of Scope; goal is correct/safe behavior for the shape, not a new capability |
| Reuse the seeded `events` table for the regression test | Load a TPC-H LINEITEM fixture to mirror `bench/run.sh` exactly | The `events` table already supports `GROUP BY id` / `GROUP BY MOD(id,4)`; the nested-aggregate SQL *shape* (outer COUNT(*) over inner grouped sub-select) is what regressed, and it reproduces identically over `events` at zero fixture cost |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning | CHANGED | `specs/_plans/fix-nested-aggregate-pushdown/vs-adapter/pushdown-planning/spec.md` |
| packaging/e2e-harness | CHANGED | `specs/_plans/fix-nested-aggregate-pushdown/packaging/e2e-harness/spec.md` |

## Implementation Tasks

1. **Diagnostic spike — capture the real `pushdownRequest` JSON for the Q7 nested-aggregate shape.** [expert]
   Add TEMPORARY diagnostic logging in `crates/lakehouse-engine/src/adapter/mod.rs::handle_pushdown_request` (the entry at ~line 290) that emits the raw incoming pushdown-request JSON and the final generated scan-driving SQL via `udf_log!`/`ctx` output. Bring up the local Exasol Docker + MinIO + Iceberg REST stack (the existing E2E harness `common::stack` bootstrap), install the SLC and `.so` (via `make cross-musl-udf-build` then the harness upload path — NEVER host `cargo build --release`), create the VS over the seeded `events` table, then run the Q7-shape query `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM {vs}.events GROUP BY id) t`. Capture the UDF's emitted request+SQL using the `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<host>:<port>'` redirect to an `nc -l` listener reachable from the container (per repo CLAUDE.md "Live debugging"). **Output of this task, recorded in decision-log entry [4]:** the exact JSON shape of the request (does Exasol compose outer+inner into one request with a nested `from`/sub-select? what are `involvedTables[0].name`, `selectList`, `groupBy`, `aggregationType`, `filter`?), the exact scan-driving SQL the adapter generated, and the precise substring that produced the `"NULL"` identifier. This output DETERMINES the scope of Tasks 2–3. Remove the temporary logging before the fix lands (or gate it behind the existing `LAKEHOUSE_UDF_DEBUG_LEVEL` if it proves generally useful — decide in the write-up).

2. **Root-cause write-up + fix-family selection.**
   From Task 1's captured data, write the concrete root cause into the plan's decision-log (entry [4]) and select which fix family applies: (a) correct the adapter's request parsing/aggregate-detection so the emitted SQL references only real source columns, or (b) tighten the detection guard so this shape returns `None` and the existing row-scan fallback engages. Identify the exact function(s) and line(s) to change (candidates from the investigation: `detect_aggregates` ~659, `detect_group_by_aggregates` ~762, the involved-table/selectList reads in `adapter/mod.rs` ~454–465, or the ScanSpec→SQL rendering in `scan/mod.rs::build_scan_sql`). This task produces the concrete edit spec that Task 3 implements.

3. **Implement the behavioral fix.** [expert]
   Apply the fix selected in Task 2 at the layer the spike identified. Acceptance is BEHAVIORAL, not a mandated implementation: after the change, the Q7-shape query MUST either (i) return the correct outer `COUNT(*)` via a correctly composed pushdown, or (ii) cleanly fall back to a row-scan ScanSpec (no aggregates field) so Exasol computes the outer aggregate — and MUST NOT emit any scan-driving SQL that references a column absent from the involved table's resolved logical schema, and MUST NOT surface a DataFusion `Schema error: No field named ...`. Preserve all existing single-group and grouped-aggregate pushdown behavior (the existing `pushdown.rs`/`scan/mod.rs` unit tests and GROUP BY E2E tests must stay green). Satisfies `pushdown-planning` § "Composed pushdown request never renders a scan spec that references a non-source column".

4. **Host unit test for the composed-request guard.**
   Add a unit test in `crates/lakehouse-engine/src/adapter/pushdown.rs` (debug profile, host `cargo test`) that feeds the captured composed `pushdownRequest` JSON (from Task 1) to the detection/parsing path and asserts the fix's invariant directly: either the produced `ScanSpec` references only real source columns / falls back to row-scan (no aggregates), OR — if fix family (a) — that the rendered SQL contains no phantom `NULL` identifier. This is the pure-parsing regression guard that runs without Docker.

5. **E2E regression test for the Q7 nested-aggregate shape.**
   Add a `#[test]` in `crates/lakehouse-engine/tests/e2e_scan_test.rs` (feature `exasol-e2e`) that runs `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM {vs}.events GROUP BY id) t` against the VS over the seeded `events` table and asserts the single returned value equals the number of distinct `id` values (20 for the seeded table), and that the query succeeds (no `Schema error`/pushdown-generation error). The test MUST fail (not skip) if the Docker stack or MinIO is unavailable, and the DSN MUST include `validateservercertificate=0`. Satisfies `e2e-harness` § "End-to-end nested aggregate over a grouped sub-select returns the correct outer count".

6. **Verification gate.**
   Run `cargo test` (host, debug), `cargo clippy --all-targets`, `cargo fmt`, then `make cross-musl-udf-build` and `make test-e2e` (rebuilds the `.so` only if the crate changed, then runs the full E2E suite including the new Q7 regression test). Confirm the failing query now passes and no existing aggregate/GROUP BY test regressed. The implementing commit references `Closes #52`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1 (diagnostic spike) |
| Group B | Task 2 (root-cause write-up + fix selection) |
| Group C | Task 3 (implement fix) |
| Group D | Task 4 (host unit test), Task 5 (E2E regression test) |
| Group E | Task 6 (verification gate) |

Sequential dependencies:
- Group A → Group B (fix family cannot be chosen until the request shape is captured)
- Group B → Group C (the concrete edit is defined by the root-cause write-up)
- Group C → Group D (tests assert the implemented invariant; Task 4/5 may be authored in parallel once the fix lands)
- Group D → Group E

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Temporary logging | `crates/lakehouse-engine/src/adapter/mod.rs::handle_pushdown_request` | The spike's raw-request/SQL diagnostic logging (Task 1) MUST be removed before the fix lands, unless deliberately promoted behind the existing `LAKEHOUSE_UDF_DEBUG_LEVEL` gate (decided in Task 2 write-up) |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Composed pushdown request never renders a scan spec that references a non-source column | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `composed_nested_aggregate_request_does_not_reference_phantom_column` |
| Composed pushdown request never renders a scan spec that references a non-source column | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_nested_aggregate_over_grouped_subselect_returns_correct_count` |
| End-to-end nested aggregate over a grouped sub-select returns the correct outer count | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_nested_aggregate_over_grouped_subselect_returns_correct_count` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning | Against the local Docker VS: `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM MY_LAKEHOUSE.EVENTS GROUP BY id) t` | Returns a single row with value `20` (distinct `id` count); no `F-UDF-CL-RUST-9001` / `Schema error: No field named` error |
| packaging/e2e-harness | `make test-e2e` | The new `e2e_nested_aggregate_over_grouped_subselect_returns_correct_count` test passes; all pre-existing aggregate/GROUP BY E2E tests stay green |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (host unit) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures (incl. new Q7 regression test) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
