# Decision Log: change-engine-throughput

Date: 2026-06-27

## Interview

**Q1 — What is the primary lever to reach 1 GB/s?**
**A:** Tune knobs first and then add additional buffer. In the next plan we will create an S3
within the VPC; measure how much S3 travel time affects this, and only add the buffer if it
really adds value. The idea sounds very interesting. → The decode/emit producer-consumer overlap
buffer is NOT committed work: measure first (the telemetry's object-storage-import timing reveals
S3 travel cost), add the bounded buffer only if benchmark evidence shows a gain. A separate future
plan moves S3 into the VPC.

**Q2 — What telemetry surface should be used?**
**A:** Reuse lc-rs — there is also an archive/branch with work we moved out. The code should be
deactivated in production; only measure when explicitly told. → Reuse the lc-rs 0.19.0 debug
surface (`udf_log!` + `ctx.debug_level()` + `LAKEHOUSE_UDF_DEBUG_LEVEL`, default `info`), base the
telemetry on the archived `archive/udf-diagnostics-checkpoints:.../scan/diagnostics.rs`
(checkpoint infra, 521 lines) rather than designing from scratch. Telemetry default OFF/silent in
production; required phase timings: (a) UDF/DataFusion startup, (b) object-storage import/read
time, (c) send-back/emit time to Exasol.

**Q3 — Per-UDF thread count: auto-derive or manual?**
**A:** Auto-derive, but overridable. Add a parameter to trigger auto or fixed thread count. We
never tested DataFusion with more than one thread — are we sure 1 thread isn't a bottleneck?
NEVER ASSUME; only facts count; we must get faster. → Spec a `DATAFUSION_THREADING_MODE` (AUTO vs
FIXED). AUTO derives `threads_per_udf = max(1, floor(cores / udf_instances_per_node))` with
`target_partitions` in lockstep; FIXED keeps explicit overrides (current behavior). Do NOT assume
1 thread/partition is optimal — add benchmark tasks that empirically sweep > 1 and report whether
single-thread is a bottleneck.

**Q4 — Work-unit sharding balance weight?**
**A:** Keep file_size weight. → Do NOT change `parallelism/work-unit-sharding`; the greedy
descending file_size byte-balanced bin-pack stays. No spec-delta for sharding/work-balancing.

## Design Decisions

### [1] AUTO/FIXED threading mode resolved at the thin VS, integers to the UDF

- **Decision:** A `DATAFUSION_THREADING_MODE` VS property (default AUTO) selects how thread/partition
  budgets are computed at `createVirtualSchema` time. The scan UDF stays mode-agnostic and consumes
  only the resolved integer `df_threads_per_udf` / `df_target_partitions` fields. AUTO derives
  `max(1, floor(NR_OF_CORES / udf_instances_per_node))` with partitions held in lockstep.
- **Alternatives:** (a) always-auto — rejected, operator needs to pin values for repeatable
  benchmarks; (b) always-manual — rejected, oversubscription is easy to hit by hand; (c) bake a
  fixed thread count — rejected, no recompilation-free tuning.
- **Rationale:** Honors Q3 ("auto but overridable"), keeps the thin-VS / stateless-UDF boundary
  (CLAUDE.md architecture), and the lockstep prevents partitions exceeding the runnable thread budget.
- **Promotes to ADR:** yes

### [2] Telemetry built on the archived checkpoint infrastructure, default OFF

- **Decision:** Restore `scan/diagnostics.rs` from `archive/udf-diagnostics-checkpoints` (per-PID file
  isolation, monotonic seq, RSS sampling) and add three monotonic-clock phase accumulators (startup /
  object-storage import / send-back). Gate all emission on the lc-rs debug level; emit nothing at the
  production default `info`; every telemetry write is best-effort and never fails a scan.
- **Alternatives:** A fresh telemetry module — rejected; the archived infra is already proven
  concurrency-safe under multiple shard VMs and crash-durable, and Q2 explicitly says reuse it.
- **Rationale:** Zero production overhead, final benchmarks run with it OFF, and the object-storage
  import phase is the lever that later isolates S3 travel cost (feeds the Q1 buffer gate).
- **Promotes to ADR:** yes

### [3] Decode-emit overlap buffer is conditional/measure-first, not committed

- **Decision:** Do NOT build a bounded `DF_MAX_BUFFERED_BATCHES` producer/consumer buffer now. The
  `scan-execution` spec describes it only as a conditional capability gated on phase-telemetry
  evidence; the committed scenario is authored only after the gate (Task 8.1) passes.
- **Alternatives:** Commit the buffer immediately — rejected per Q1 (tune knobs first; the S3-in-VPC
  plan + telemetry must first show read and emit don't already overlap and that decoupling pays).
- **Rationale:** Avoids speculative work the data may not justify; keeps the streaming discipline
  (fetch-one/emit/drop) intact until proven otherwise.
- **Promotes to ADR:** yes

### [4] Scope split — engine features get specs; benchmarks/harness/sweeps are tasks only

- **Decision:** Spec deltas cover only engine feature changes (threading mode, telemetry capability,
  repartition-free pipeline, Parquet pruning guarantee, adapter-notes recording). The E2E/benchmark
  harness, synthetic emit/scan benches, parameter sweeps, baseline measurement, and the empirical
  >1-thread test live in plan.md's task list (Tasks 5–8) with NO spec scenarios.
- **Alternatives:** Spec the benchmark behavior too — rejected; benchmarks measure the engine, they
  are not engine capabilities, and specs should describe behavior under test, not the test rig.
- **Rationale:** Keeps the spec library a description of product behavior; measurement tooling evolves
  faster and is not a deliverable behavior contract.
- **Promotes to ADR:** yes

### [5] Parquet row-group/page pruning is an engine guarantee (spec); pruning effectiveness is a measurement (task)

- **Decision:** Enabling Parquet predicate pushdown + row-group + page-index pruning is a spec scenario
  in `scan-execution-memory-and-credentials` (a configuration guarantee composing with Iceberg file
  pruning). Whether it cuts bytes-read enough to move throughput is measured in the sweep (task).
- **Alternatives:** Verify only by benchmark (no spec) — rejected, the config flags are a stable
  behavioral guarantee worth pinning; spec the byte savings — rejected, that's data-dependent.
- **Rationale:** Separates the deterministic config guarantee from the data-dependent measurement.
- **Promotes to ADR:** no

### [6] Keep work-unit sharding and its file_size balance weight unchanged

- **Decision:** No spec-delta touches `parallelism/work-unit-sharding`; the greedy descending
  file_size byte-balanced bin-pack and `G = node_count × parallelism_factor` (cap 300) stay as-is.
- **Alternatives:** Re-balance by row count / file count — rejected per Q4 ("keep file_size weight").
- **Rationale:** Direct interview decision; sharding is not on the throughput-bottleneck critical path
  identified for this plan.
- **Promotes to ADR:** no

## Empirical Decisions (from live-cluster measurement)

### [7] Task 8 decode-emit overlap buffer — GATE FAILED, not built

- **Decision:** Do NOT build the bounded `DF_MAX_BUFFERED_BATCHES` producer/consumer buffer.
- **Evidence:** Live telemetry (connect-back, single COUNT shard) measured startup ≈110ms,
  object-storage import ≈650ms, emit ≈2ms. The scan is overwhelmingly import-bound; the emit phase
  is negligible. Overlapping a ~2ms emit with a ~650ms import yields essentially zero wall-clock gain.
- **Rationale:** The conditional buffer was spec'd as measure-first; the measurement says the
  precondition (emit and import both significant and serialized) does not hold. A buffer helps an
  emit-bound workload; the far-VPC scan is read-bound. Building it would add concurrency complexity for
  no measured benefit. Reconsider only if a future workload (wide `SELECT *` with huge emit over
  in-VPC S3) shows emit and import both material.
- **Promotes to ADR:** no

### [8] AUTO threading default is safe but NOT the fastest for I/O-bound remote scans

- **Decision:** Keep the spec'd `AUTO` default (`threads = max(1, floor(NR_OF_CORES / parallelism_factor))`)
  as the general safety default, but document that the measured-optimal config for the far-VPC
  remote-scan workload is `FIXED` with `threads = target_partitions = NR_OF_CORES`. The bench harness
  and recommended production config for remote scans use `FIXED`.
- **Evidence (threading sweep, NR_OF_CORES=4, PARALLELISM_FACTOR=8, lineitem ~1.7 GB):**
  1/1 (= what AUTO derives here) → Q4 12.45s; 2/2 → 10.52s; **4/4 → 8.94s (best)**; 8/8 → 10.02s.
  Single-thread-per-instance is **+39%** on the full scan.
- **Rationale:** The scan is I/O-bound (S3 across the VPC), so threads overlap S3 read latency rather
  than competing for CPU — more threads per instance help up to ≈NR_OF_CORES even though
  `instances × threads` exceeds the VS-reported core count. AUTO's anti-oversubscription premise is
  correct for CPU/memory-bound work but counterproductive when read-bound. The optimal thread count is
  workload-dependent, so hardcoding oversubscription as the default would be wrong for other
  deployments. Shipping the safe AUTO default + an explicit FIXED lever + a documented recommendation
  is the principled outcome; a future "I/O-aware AUTO" (oversubscribe when read-bound) is the follow-up.
- **Promotes to ADR:** yes

### [9] The throughput bottleneck is far-VPC S3 read cost, not the UDF engine

- **Decision:** The engine-side levers in this plan (Parquet `pushdown_filters`, lean repartition-free
  plan, row-group/page pruning, optimal threads, projection/partial-agg pushdown) are delivered and
  help ~30-40%, but the ~5× gap from 0.19 GB/s to the 1 GB/s target is dominated by S3 read latency
  across the VPC, confirmed three ways: (a) telemetry import≈650ms ≫ emit≈2ms; (b) threads help only
  by overlapping S3 waits; (c) native Exasol `IMPORT FROM PARQUET` full-read of the same lineitem files
  hits the same ~0.17 GB/s ceiling (10.07s) — and the VS path is competitive/faster via pushdown.
- **Rationale:** Validates the operator's planned next step (move S3 into the VPC) as the highest-value
  throughput lever, and confirms the UDF layer is not the limiter. Fact-based, measured on the live
  cluster.
- **Promotes to ADR:** yes

## Review Findings

**Ponytail simplification review (net: ~−170 lines possible, deferred to operator):**
- `scan/diagnostics.rs` restored ~170 lines of checkpoint + panic-hook infra (`debug_checkpoint`,
  `install_panic_hook`, `debug_set_rows`, `current_rss_bytes`, etc.) with **zero production callers** —
  the live telemetry path uses only `PhaseTimers` + the `telemetry_*` fns + `append_record`. Left in
  place because Task 4.1 explicitly restored it "deactivated by default" and the telemetry spec frames
  it as the reusable base; flagged as trimmable-to-telemetry-only if the dormant per-PID checkpoint
  trail is not wanted. Not deleted unilaterally (spec-sanctioned dormant code).
- Rest of the diff is lean: threading-mode plumbing minimal; `emit_one_batch` preserves the
  fetch/emit/drop discipline; test-seam `pub fn`s justified; the `run_on_runtime` flaky-test fix correct.
