# Verification Report: add-group-by-and-sql-comprehension

## Bottom Line

**PASS** — all implementation tasks complete except the explicitly-blocked Phase 0 dep bump. Unit suite 138/138 green, clippy clean, fmt clean. E2E suite (`make test-e2e`, SLC `.so` built in `rust:1.92-bookworm`) **22/22 green** against the live Exasol + Iceberg REST + MinIO stack.

> E2E note: Exasol's optimizer does not push aggregation down for the 20-row test table (it pushes filters and aggregates the small result natively), so the partial-aggregate path is exercised by unit tests rather than E2E; correctness E2E tests pass either way. The shard fan-out (`GROUP BY shard_key`) is observed E2E via a 2-data-file seed (G = min(8, 2, 300) = 2). On production-scale tables the cost model favours pushing the aggregation. The `test_group_by_expression_key`/`avg` expected values follow Exasol's half-away-from-zero `CAST(... AS DECIMAL)` rounding.

## Status by Phase

| Phase | Status | Notes |
|-------|--------|-------|
| 0 — SDK dep bump | **DEFERRED → next plan** | `exasol-udf-sdk`/`exasol-udf-macros` `0.16.0` (carrying `UdfContext::memory_limit()`) **published to crates.io on 2026-06-22** (after this plan's implementation). This crate still pins `0.14.0`; `build_runtime_env` runs the `0`-sentinel default-budget path (1024 MB). The bump (and wiring the live `ctx.memory_limit()`) crosses `0.15.0`'s breaking "remove dead public API" change, so it is moved to the follow-up pushdown-capabilities plan rather than retrofitted here. |
| 1 — vs-expression crate | DONE | New `crates/vs-expression`; moved + extended predicate walker (arithmetic, CAST); `predicate.rs` deleted. 35 tests. |
| 2 — adapter (GROUP BY detect, shard_count, scan SQL) | DONE | `GROUP BY shard_key` fan-out replaces `GROUP BY IPROC()`. |
| 3 — scan UDF (bounded runtime + grouped partial) | DONE | `runtime.rs` (spill probe + memory pool), grouped partial-agg streaming exec. |
| 4 — mission/CLAUDE.md hygiene | DONE | Cardinality-guard wording replaced by memory-pool/spill/oversubscription. |
| 5 — E2E tests | DONE (compile) | 6 GROUP BY tests + null-key test; run under `make test-e2e`. |

## Automated Checks (host)

| Check | Command | Result |
|-------|---------|--------|
| Unit tests | `cargo test -p lakehouse-engine -p vs-expression` | 138 passed, 0 failed |
| E2E compile | `cargo test -p lakehouse-engine --features exasol-e2e --no-run` | exit 0 |
| Lint | `cargo clippy -p lakehouse-engine -p vs-expression --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | clean |

> `cargo build --release` is intentionally NOT run on host (would write a host-glibc `.so`). The SLC-matching `.so` build (`make cross-musl-udf-build`) and `make test-e2e` run in the pipeline test step.

## Code Review

One correctness gap fixed: grouped aggregate path now runs `validate_agg_col_types` (SUM over VARCHAR/DATE falls back cleanly instead of an opaque UDF error). Flaky null-group position assertion loosened. Guardrail comment cleanups applied. Deliberately deferred (out of scope, recorded): `GroupedScanConfig` parameter-object refactor; `u32→usize` consistency.

## Learnings & Shortcomings (feed the follow-up pushdown-capabilities plan)

This plan shipped GROUP BY pushdown that is **correct** and **unit-proven**, but live E2E investigation surfaced gaps that belong to a dedicated next plan rather than this one.

### Learnings

1. **Exasol aggregation pushdown is cost-based, not capability-gated.** With the GROUP BY capabilities advertised, Exasol still sends a plain `{"type":"select"}` request (no `aggregationType`/`groupBy`) for the 20-row test table and aggregates natively — verified via `EXPLAIN VIRTUAL` (`exapump`). Filters *are* pushed on the same table, so this is aggregation-specific cost optimization, not a missing capability. Consequence: the partial/merge node-local aggregation path (the feature's performance value) is only exercised on **production-scale** tables; small-table E2E can only assert correctness. Any future "aggregate pushdown actually fires" E2E needs a large, multi-file seed.
2. **The shard fan-out is independent of aggregate pushdown.** `GROUP BY shard_key` is emitted by the row-scan path whenever `G = shard_count(nodes, factor, files) > 1`. With a single data file `G = 1` (single invocation, no fan-out by design). Observing the fan-out E2E only needs ≥2 data files — it does not require Exasol to push the aggregation.
3. **`exasol-udf-sdk 0.16.0` is now on crates.io** — the `ctx.memory_limit()` accessor is available; the memory pool can be sized from the real per-instance limit instead of the 1024 MB sentinel.
4. **E2E harness container discovery** must use the Compose **service label** (`com.docker.compose.service=exasol`), not a hardcoded project-prefixed name — the stack may run under any Compose project name.

### Shortcomings / open scope (→ next plan: pushdown capabilities)

- **JOIN pushdown is NOT implemented** (still listed Out-of-Scope in `mission.md`). Product direction: **joins should be pushed down.** This requires verifying the Exasol VS join request shape against the capability list (`capabilities_list.md`: `JOIN`, `JOIN_TYPE_*`, `JOIN_CONDITION_EQUI`) and extending the single-table scan-spec model to a multi-table one so the DataFusion UDF can register both Iceberg tables and execute the join (file-sharding strategy across a join is an open design question).
- **Capability coverage audit pending.** Verify our advertised capabilities against the authoritative `capabilities_list.md` and the VS API doc; advertise everything the DataFusion UDF can execute (candidates: `ORDER_BY_COLUMN`/`ORDER_BY_EXPRESSION`, `LIMIT_WITH_OFFSET`, additional `FN_*` scalar functions in `vs-expression`, any missing `FN_PRED_*`). Guiding principle confirmed: **advertise only what the UDF can execute; anything not advertised, Exasol post-processes natively** — so unsupported shapes stay correct, just not pushed.
- **Live `ctx.memory_limit()` wiring deferred** — bump `exasol-udf-sdk`/`exasol-udf-macros` `0.14.0 → 0.16.0` (crosses `0.15.0`'s breaking API removal; verify compilation), then replace the `0`-sentinel at the `build_runtime_env` call site.
- **Aggregate-pushdown E2E coverage** — add a large/multi-file seed so the partial/merge path is exercised E2E (not just unit-tested), if the next plan wants to prove the optimizer pushes it.
