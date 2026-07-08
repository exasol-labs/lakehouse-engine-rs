# Plan: change-engine-throughput

## Summary

Aggressively optimize the embedded DataFusion-in-UDF scan engine for end-to-end query
throughput by making per-instance thread/partition budgeting selectable (AUTO-derived or
FIXED), guaranteeing a lean repartition-free raw-scan pipeline with Parquet row-group/page
pruning, and adding opt-in phase telemetry — then driving a benchmark sweep that measures,
with facts not assumptions, what actually moves throughput toward the target.

## Optimization Goal

> EDIT THIS SECTION — the numeric target and success criteria are owned by the operator and
> may be adjusted before implementation begins.

**Throughput target: ≥ 1 GB/s end-to-end; aim for 1–2 GB/s; go further if the data supports
it.** (Current live-cluster baseline ≈ 0.2 GB/s across a far VPC. 1 GB/s is the minimum bar;
1–2 GB/s is the expected achievable range; anything beyond is a bonus.) The benchmark establishes
the baseline first, then each lever is measured against it. The IMPORT FROM PARQUET ceiling
(Task 9) sets the hard upper bound for the UDF path.

**Success criteria:**

1. **Linear scaling** — end-to-end throughput rises roughly linearly as UDF instances per node
   (work-unit shards multiplexed onto the core pool) increase, until a node's cores or the
   object-storage path saturate.
2. **No CPU oversubscription** — `(udf_instances_per_node × df_threads_per_udf) ≤ NR_OF_CORES`
   under AUTO mode; the node is not driven past its core count.
3. **Stable bounded memory** — per-instance RSS stays under the engine's per-instance limit
   across the sweep; no OOM, no VM crash; high-cardinality grouped queries either spill or return
   a clean `ResourcesExhausted`.
4. **Config-driven tuning without recompilation** — every throughput lever (threading mode,
   threads/partitions, decode batch size, emit batch threshold, parallelism factor) is a VS
   property / scan-spec field; reaching a tuning point never requires rebuilding the `.so`.
5. **Minimal latency** — startup overhead per UDF invocation stays a small fraction of scan
   wall-clock; telemetry attributes startup vs object-storage import vs send-back so latency
   regressions are visible.
6. **Low startup overhead** — UDF boot cost (DataFusion session creation, S3 client init,
   Iceberg file registration) is measured and minimized. If startup is a material fraction of
   total query wall-clock (> ~10% of the scan body), it becomes a first-class lever: defer
   heavyweight init, pre-build session config, or parallelize file registration. The telemetry
   startup phase (Task 4.2 / 6.3) quantifies this before any optimization is committed.

**Open empirical questions (answered by the sweep, NOT assumed):**

- Is the current default of **1 thread / 1 partition per instance a bottleneck**? It has NEVER
  been tested above 1. The sweep measures `df_threads_per_udf` and `df_target_partitions` > 1.
- How much of the 0.2 GB/s is **S3 travel time across the far VPC**? The phase telemetry's
  object-storage-import timing isolates it. A separate future plan moves S3 into the VPC; the
  decode-emit overlap buffer is gated on that measurement (see Design § Consequences).
- **How much wall-clock does UDF startup consume?** Every shard invocation pays boot cost
  (Tokio runtime init, DataFusion session, S3 object-store registration, Iceberg file-list
  deserialization). With many shards and small file assignments, startup could dominate. The
  telemetry startup phase (Task 6.3) measures it; if it exceeds ~10% of scan body time it is
  promoted to an optimization lever in the decision log and addressed in a follow-up plan.

## Design

### Context

Live-cluster throughput is ~0.2 GB/s, 5× short of the 1 GB/s target. The engine already streams
batches incrementally, sizes its memory pool, and shards work across the cluster — but the
per-instance thread budget defaults to 1 and has never been measured higher, the raw-scan
physical plan has not been audited for needless repartition stages, and there is no instrument
to attribute where the wall-clock goes. We must add the missing tuning levers and the measurement
surface, then let benchmark facts decide which levers to turn — without committing speculative
buffering work that the data may not justify.

- **Goals** — (1) a selectable AUTO/FIXED per-instance thread/partition budget that auto-derives a
  non-oversubscribing value; (2) a guaranteed lean, repartition-free raw-scan pipeline with Parquet
  row-group/page pruning enabled; (3) opt-in, production-silent phase telemetry reusing the lc-rs
  debug surface; (4) a benchmark sweep harness that measures the levers and isolates S3 travel cost.
- **Non-Goals** — changing work-unit sharding or its file_size balance weight (Q4: keep it); building
  a committed decode-emit overlap buffer (Q1: conditional, measure-first); moving S3 into the VPC
  (a separate future plan); any caching, materialization, or join pushdown (mission out-of-scope).

### Decision

Keep the scan UDF **mode-agnostic** — it consumes only integer `df_threads_per_udf` /
`df_target_partitions` / `df_batch_size` fields. All AUTO-vs-FIXED logic lives in the adapter at
`createVirtualSchema` time and is recorded in `adapterNotes`, preserving the thin-VS / stateless-UDF
boundary. Telemetry layers on the archived per-process checkpoint infrastructure rather than a new
mechanism. The decode-emit buffer is described but NOT built; it is gated on telemetry evidence.

#### Architecture

```
createVirtualSchema (adapter)                          per-shard scan UDF (DataFusion)
┌────────────────────────────────────┐                ┌─────────────────────────────────────┐
│ DATAFUSION_THREADING_MODE AUTO|FIXED│                │ session_config_for_spec:            │
│  AUTO: threads = max(1,             │  adapterNotes  │  target_partitions = df_target_...  │
│    floor(NR_OF_CORES /              │ ─────────────▶ │  batch_size       = df_batch_size   │
│    udf_instances_per_node))         │  → ScanSpec    │  parquet pruning  = ENABLED         │
│  partitions = threads (lockstep)    │  integers only │  runtime kind     = f(threads)      │
│  FIXED: supplied values verbatim    │                │ plan: Parquet→Filter→Project→       │
└────────────────────────────────────┘                │       CoalesceBatches→emit_batch    │
                                                       │ telemetry (gated): startup |        │
                                                       │   obj-store import | send-back      │
                                                       └─────────────────────────────────────┘
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Mode resolution at the thin VS, integers to the UDF | adapter `createVirtualSchema` → ScanSpec | Keeps the UDF stateless/mode-agnostic; one seam for all CPU budgeting |
| Reuse archived checkpoint infra for telemetry | `scan/diagnostics.rs` (from `archive/udf-diagnostics-checkpoints`) | Per-PID isolation, monotonic seq, RSS sampling already proven under concurrent shard VMs |
| Config-gated, default-OFF observability | `LAKEHOUSE_UDF_DEBUG_LEVEL` / `ctx.debug_level()` | Zero production overhead; final benchmarks run with it OFF |
| Measure-first before speculative buffering | telemetry → sweep → conditional buffer | Q1: only add the decode-emit buffer if phase timings prove a gain |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| AUTO/FIXED mode property, AUTO derives `floor(cores/instances)` | Always auto; always manual; bake a fixed thread count | Operator can pin for repeatable benchmarks (FIXED) or let the engine avoid oversubscription (AUTO); honors Q3 "auto but overridable" |
| Partitions held in lockstep with threads in AUTO | Independent partition tuning | A partition count above the thread budget cannot run in parallel; lockstep avoids wasted partitions. FIXED still allows independent values for the sweep |
| AUTO uses `udf_instances_per_node = parallelism_factor` (un-clamped by file count) | Use `ceil(min(G,300)/node_count)` or the file-count-clamped per-node share | The file list is unknown at `createVirtualSchema`, so neither the 300-cap nor the `≤ file_count` clamp can be applied yet. Before those clamps `G/node_count = parallelism_factor` exactly. Both clamps only *reduce* the per-node instance count (which would *raise* the thread budget), so using the un-clamped `parallelism_factor` is the conservative floor on threads / ceiling on instances — the derived budget never oversubscribes even at the maximal configured fan-out. Caveat: when `parallelism_factor > NR_OF_CORES` the `max(1, …)` floor forces 1 thread/instance and `instances × threads = parallelism_factor > cores`; oversubscription is then handled by the engine multiplexing surplus instances onto the core pool, so the invariant is asserted in tests only for the realistic `cores ≥ instances` regime |
| Telemetry built on archived diagnostics.rs, phase timings added | New telemetry module from scratch | Reuses proven concurrency-safe, per-PID, crash-durable infra (Q2); only adds `Instant` phase boundaries |
| Decode-emit overlap buffer is CONDITIONAL, not committed | Build a bounded `DF_MAX_BUFFERED_BATCHES` queue now | Q1: tune knobs first; the S3-in-VPC plan + telemetry must first show read/emit do not already overlap and that decoupling pays. Spec scenario is framed conditionally |
| Keep work-unit sharding & file_size weight unchanged | Re-balance by row count or file count | Q4: explicitly keep file_size byte-balanced bin-pack; no spec-delta for sharding |
| Parquet row-group/page pruning as an engine guarantee (spec), pruning-effectiveness as a measurement (task) | Verify only by benchmark; or only by spec | The config flags are a correctness-adjacent engine guarantee (spec scenario); whether they cut bytes-read enough is a fact for the sweep (task) |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `datafusion-scan/scan-execution-threading` | CHANGED | `datafusion-scan/scan-execution-threading/spec.md` |
| `datafusion-scan/scan-execution-telemetry` | NEW | `datafusion-scan/scan-execution-telemetry/spec.md` |
| `datafusion-scan/scan-execution` | CHANGED | `datafusion-scan/scan-execution/spec.md` |
| `datafusion-scan/scan-execution-memory-and-credentials` | CHANGED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |
| `vs-adapter/create-virtual-schema-adapter-notes` | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes/spec.md` |

## Dependencies

- Archived branch `archive/udf-diagnostics-checkpoints` — source of `scan/diagnostics.rs` checkpoint
  infrastructure the telemetry feature is based on.
- `bench/run.sh` + `bench/.env` harness (docker self-contained or remote AWS Glue + live cluster).
- `debug-cluster.md` (repo root, do-not-stage) jumphost for read-only live-cluster probing.
- lc-rs 0.19.1 SLC debug surface (`udf_log!`, `ctx.debug_level()`, `LAKEHOUSE_UDF_DEBUG_LEVEL`).

## Implementation Tasks

> Tasks 1–4 are ENGINE FEATURES (covered by spec deltas). Tasks 5–8 are BENCHMARK / HARNESS / SWEEP /
> MEASUREMENT work — these are NOT engine features and intentionally have NO spec scenarios.

### 1. Threading mode (AUTO / FIXED)

- [ ] 1.1 Add a `DATAFUSION_THREADING_MODE` VS/connection property (`AUTO` | `FIXED`, default `AUTO`, case-insensitive) parsed in the adapter and recorded in `adapterNotes`.
- [ ] 1.2 Implement AUTO derivation: `df_threads_per_udf = max(1, floor(NR_OF_CORES / udf_instances_per_node))` where `udf_instances_per_node` is the per-node share of the shard count `G = node_count × parallelism_factor` (capped 300); set `df_target_partitions` in lockstep. Fall back to 1 when `NR_OF_CORES` is 0. [expert]
- [ ] 1.3 Preserve FIXED mode: supplied `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF` used verbatim, each defaulting to `max(NR_OF_CORES, 1)`.
- [ ] 1.4 Round-trip the resolved integer fields into every per-shard ScanSpec (UDF stays mode-agnostic — no UDF code change beyond consuming existing fields).
- [ ] 1.5 Unit tests for AUTO derivation arithmetic (incl. oversubscription invariant `instances × threads ≤ cores`), FIXED passthrough, default-AUTO, and `NR_OF_CORES=0` fallback.

### 2. Repartition-free raw-scan pipeline

- [ ] 2.1 Inspect the raw-scan physical plan (`EXPLAIN`/`displayable(plan)`) and assert the pipeline is `ParquetExec → FilterExec → ProjectionExec → CoalesceBatchesExec` with no `RepartitionExec`, `CoalescePartitionsExec`, global `SortExec`, or global aggregate when `df_target_partitions == 1`. [expert]
- [ ] 2.2 If a needless stage is present, adjust session config / plan construction to elide it without changing the result set. [expert]
- [ ] 2.3 Add a unit/integration test asserting the physical-plan shape (string-match on the displayable plan) for the single-partition raw-scan path.

### 3. Parquet row-group & page pruning

- [ ] 3.1 Enable Parquet predicate pushdown + row-group statistics pruning + page-index pruning on the session/Parquet scan options (distinct from Iceberg file pruning).
- [ ] 3.2 Add a test asserting the pruning flags are set and that result rows are identical with pruning on vs off.

### 4. On-demand phase telemetry

- [ ] 4.1 Restore `scan/diagnostics.rs` checkpoint infrastructure from `archive/udf-diagnostics-checkpoints` (per-PID file, monotonic seq, RSS sampling) into the working tree, deactivated by default.
- [ ] 4.2 Add three monotonic-clock phase accumulators — startup, object-storage import (time awaiting each stream batch), send-back/emit (time inside emit/flush) — wired at the existing checkpoint sites in `scan/mod.rs` + `scan/emit.rs`. [expert]
- [ ] 4.3 Gate all telemetry emission on the debug level (`ctx.debug_level()` / `LAKEHOUSE_UDF_DEBUG_LEVEL`, default `info`); emit nothing at the default level. Make every telemetry write best-effort (never fails the scan).
- [ ] 4.4 Emit one per-VM-tagged telemetry record at completion reporting the three phase durations; assert they sum to scan-body wall-clock within tolerance.
- [ ] 4.5 Unit tests: silent at default level; three phases reported and distinct when enabled; telemetry-write failure does not fail the scan.

### 5. Synthetic micro-benchmarks (TASK ONLY — no spec)

- [ ] 5.1 Synthetic emit-only benchmark: feed pre-built RecordBatches of BIGINT / DOUBLE / TIMESTAMP / DECIMAL / VARCHAR and production-shaped schemas through `emit_batch`; measure rows/sec and GB/sec of the emit path in isolation. [expert]
- [ ] 5.2 Scan-only benchmark: Iceberg → DataFusion stream with NO emit (drain the stream); isolates object-storage import + decode throughput from send-back. [expert]
- [ ] 5.3 Wire both behind the bench harness / a cargo bench binary; record GB/sec, CPU util, RSS.

### 6. End-to-end benchmark & baseline (TASK ONLY — no spec)

- [ ] 6.1 Establish the current end-to-end throughput baseline via `make bench` (remote AWS Glue + live cluster) with telemetry OFF; capture GB/sec, rows/sec, latency, CPU, memory, network.
- [ ] 6.2 Re-run with telemetry ON (debug level) to capture the startup / object-storage-import / send-back phase split; quantify S3 travel time across the far VPC. [expert]
- [ ] 6.3 **Startup cost analysis** — from the telemetry output, extract the startup phase duration per VM across all shard invocations: report min/median/max and startup as a % of total per-VM wall-clock. Shard fan-out amplifies startup (G shards × startup_ms = total startup tax). If startup exceeds ~10% of scan body on any shard size, record it as an optimization lever in the decision log. [expert]
- [ ] 6.4 Sweep shard count G with telemetry ON to observe how startup fraction changes as per-shard file assignment shrinks (more shards = less data per shard = startup becomes larger fraction). Identify the shard count below which startup dominates scan time — this is the floor for meaningful sharding at the current startup cost.

### 7. Parameter sweep (TASK ONLY — no spec)

- [ ] 7.1 Build a sweep driver over the matrix {UDF instances per node × `df_threads_per_udf` × `df_target_partitions` × `df_batch_size` (decode) × emit batch threshold}, configured via VS properties / `bench/.env` (no recompilation per point). Treat decode `batch_size` and the SDK emit-flush threshold (4,000,000 bytes) as DISTINCT axes. [expert]
- [ ] 7.2 Run the sweep against the live cluster; for each point record rows/sec, GB/sec, CPU util, RSS, network, latency. Telemetry OFF for the timed points, ON for a diagnostic subset.
- [ ] 7.3 Empirically answer Q3: is 1 thread / 1 partition a bottleneck? Report the throughput vs threads/partitions curve with facts. [expert]
- [ ] 7.4 Verify the success criteria (linear scaling, no oversubscription, stable memory, startup fraction < 10% of scan body) hold across the sweep; write the findings report under `bench/reports/`. If startup is the binding constraint, add a "startup optimization" row to the decision log with the measured cost and proposed reduction (e.g. lazy S3-client init, pre-built session config, parallel file registration).

### 9. IMPORT FROM PARQUET goal benchmark (TASK ONLY — no spec)

Establish Exasol's native MPP Parquet IMPORT as the **goal throughput ceiling**: an IMPORT of the
same files the VS reads, parallelized by the Exasol engine, with no UDF subprocess on the data
path. The difference between IMPORT throughput and the VS throughput is the cost of the UDF layer
(Iceberg metadata, DataFusion, SLC round-trips).

> Pattern from the sibling project's `tests/exasol_import_test.rs` — the query shape is:
> ```sql
> IMPORT INTO (<col> <type>, …)
> FROM PARQUET AT 's3://<bucket>/;Endpoint=<endpoint>'
> USER '<access_key>' IDENTIFIED BY '<secret_key>'
> FILE '<path1>' FILE '<path2>' …
> ```
> For STS sessions add `SESSION_TOKEN '<token>'` after `IDENTIFIED BY`. The AT URL must be the `s3://`
> form (not the `http://` Endpoint override) for the S3 driver to recognize the credential clauses on
> the real cluster.

- [ ] 9.1 Resolve the Parquet file paths for the largest TPC-H table (`lineitem`, scale factor from
  `bench/.env`) via the Iceberg catalog (Glue REST) — the same file list the VS planning layer
  would produce (predicate-free, all columns). Use the AWS CLI / Glue API, or an equivalent
  catalog-listing tool if available; record the file count and total uncompressed bytes for the
  report.
- [ ] 9.2 Build the full `IMPORT INTO (...) FROM PARQUET AT '…' USER '…' IDENTIFIED BY '…' FILE '…' …`
  SQL for `lineitem`, using the column schema from the Glue catalog (type mapping follows the Exasol
  IMPORT FROM PARQUET driver: numerics → `DECIMAL`, doubles → `DOUBLE PRECISION`, varchars →
  `VARCHAR(2000000)`, dates → `DATE`, timestamps → `TIMESTAMP`). Wire into `bench/run.sh` as
  `BENCH_TARGET=remote` harness or a standalone one-shot script.
- [ ] 9.3 Run the IMPORT benchmark against the live cluster: record total rows, wall-clock, GB/sec
  (uncompressed Parquet bytes / wall-clock), CPU utilization (if observable via `c4`), and Exasol
  `PROFILE` summary if `BENCH_PROFILE=1`. Run 3× and take the median.
- [ ] 9.4 Run the VS `SELECT * FROM tpch.lineitem` benchmark (from Task 6 / 7 baseline) on the same
  cluster session immediately after, for a fair apples-to-apples comparison.
- [ ] 9.5 Record the **IMPORT ceiling** and **VS gap** in `bench/reports/` as the reference row in the
  final benchmark report: `IMPORT GB/sec`, `VS GB/sec (baseline)`, `gap factor`. The IMPORT
  result is the optimization target ceiling, not an expectation to exceed.

### 8. Conditional decode-emit overlap buffer (TASK ONLY — gated; no committed spec work)

- [ ] 8.1 GATE: only proceed if the phase telemetry (6.2 / 7.2) shows object-storage import and send-back do NOT already overlap and that decoupling them would yield a measured gain. Record the gate decision in the decision log.
- [ ] 8.2 If and only if the gate passes: prototype a bounded `DF_MAX_BUFFERED_BATCHES` producer/consumer overlap; re-measure against the baseline before committing. (Spec scenario for this is framed conditionally in `scan-execution`; author the committed scenario only after the gate passes.) [expert]

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (engine features, independent) | Task 1, Task 2, Task 3, Task 4 |
| Group B (micro-benchmarks) | Task 5 |
| Group C (e2e baseline + sweep) | Task 6, Task 7 |
| Group D (conditional) | Task 8 |
| Group E (goal benchmark) | Task 9 |

Sequential dependencies:
- Group A → Group C (the sweep tunes the levers Task 1/2/3 expose; telemetry from Task 4 feeds 6.2/7.2)
- Group B can run in parallel with Group A (synthetic benches need only `emit_batch` / the scan stream)
- Group C → Group D (Task 8 is gated on Task 6.2 / 7.2 telemetry evidence)
- Group E (Task 9) runs in parallel with Group C — both need the live cluster but are independent queries; run Task 9.3–9.4 in the same session for a fair comparison

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none expected) | — | Telemetry restores archived code rather than removing; threading-mode and pruning changes extend existing seams. Re-confirm at implementation time. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| threading: AUTO derives non-oversubscribing budget | Unit | `crates/lakehouse-engine/src/adapter/…/threading.rs` (tests) | `auto_mode_derives_non_oversubscribing_threads` |
| threading: AUTO falls back to 1 when cores unknown | Unit | same | `auto_mode_falls_back_to_one_when_cores_zero` |
| threading: FIXED uses supplied values verbatim | Unit | same | `fixed_mode_uses_supplied_values` |
| threading: mode defaults to AUTO when absent | Unit | same | `threading_mode_defaults_to_auto` |
| threading: explicit target partitions applied | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (tests) | `session_config_uses_spec_target_partitions` |
| threading: single-thread runtime when threads==1 | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (tests) | `runtime_is_current_thread_when_one` |
| threading: multi-thread runtime when threads>1 | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (tests) | `runtime_is_multi_thread_when_threads_exceeds_one` |
| telemetry: silent at default level | Integration | `crates/lakehouse-engine/tests/scan_telemetry.rs` | `telemetry_silent_at_default_level` |
| telemetry: three phases reported when enabled | Integration | `crates/lakehouse-engine/tests/scan_telemetry.rs` | `telemetry_reports_three_phases_when_enabled` |
| telemetry: import vs emit attributed separately | Integration | `crates/lakehouse-engine/tests/scan_telemetry.rs` | `telemetry_attributes_import_separately_from_emit` |
| telemetry: write failure never fails the scan | Integration | `crates/lakehouse-engine/tests/scan_telemetry.rs` | `telemetry_failure_never_fails_scan` |
| scan: raw plan carries no needless repartition/coalesce | Integration | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `raw_scan_plan_has_no_repartition_stage` |
| memory: Parquet row-group & page pruning enabled | Integration | `crates/lakehouse-engine/tests/scan_parquet_pruning.rs` | `scan_enables_rowgroup_and_page_pruning` |
| adapter-notes: records threading mode | Integration | `crates/lakehouse-engine/tests/adapter_notes.rs` | `records_datafusion_threading_mode` |
| adapter-notes: target partitions per mode | Integration | `crates/lakehouse-engine/tests/adapter_notes.rs` | `records_target_partitions_per_mode` |
| adapter-notes: threads-per-udf per mode | Integration | `crates/lakehouse-engine/tests/adapter_notes.rs` | `records_threads_per_udf_per_mode` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| threading mode (AUTO) | Create VS without `DATAFUSION_THREADING_MODE`, then `SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME='TPCH'` | `adapterNotes` carries `DATAFUSION_THREADING_MODE: AUTO` and `threads × instances ≤ NR_OF_CORES` |
| threading mode (FIXED) | Create VS with `DATAFUSION_THREADING_MODE=FIXED DATAFUSION_THREADS_PER_UDF=4` | `adapterNotes` shows `threads_per_udf=4` verbatim |
| repartition-free plan | `make bench` (docker) then inspect `EXPLAIN VIRTUAL` / scan plan in the report | Raw-scan plan shows no `RepartitionExec`/`CoalescePartitionsExec` |
| telemetry | Run a scan with `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS=<jumphost:port>` + `LAKEHOUSE_UDF_DEBUG_LEVEL=debug` | Three phase-timing lines (startup / obj-store import / send-back) per VM; nothing at default `info` |
| benchmark sweep | `make bench` with sweep `.env` matrix | `bench/reports/<ts>.txt` with GB/sec per (instances × threads × partitions × batch × emit-batch) point |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 (`.so` built in `rust:1.92-bookworm`) |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures (fails, not skips, if Docker stack down) |
| Benchmark | `make bench` | Report written to `bench/reports/`; GB/sec recorded |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
