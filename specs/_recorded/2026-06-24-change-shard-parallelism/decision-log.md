# Decision Log: change-shard-parallelism

Date: 2026-06-24

Supersedes and extends `change-sharding-byte-balanced-and-cores-aware` (the
byte-balanced sharding + cores-aware default-factor work) with a new DataFusion
CPU-oversubscription concern. The plan name was shortened from
`change-sharding-byte-balanced-and-cores-aware` to `change-shard-parallelism`.

## Interview

**Q:** How should a file whose `file_size_in_bytes` is 0 be weighted in the byte-balanced split?
**A:** Treat it as 1 byte — assign it to the smallest shard by weight; never skip it (skipping would drop the file from the scan and produce wrong results).

**Q:** The cores-aware default parallelism factor should be `NR_OF_CORES ×` what?
**A:** × 2. Two-times oversubscription per node absorbs stragglers (a node keeps cores busy when one shard runs long) without incurring excessive session-startup cost.

**Q:** Is the Tokio runtime the source of the per-node thread oversubscription?
**A:** No — Tokio is already `new_current_thread()` (one OS thread, correct). The actual gap is DataFusion's `target_partitions`, which `SessionConfig::new()` defaults to the host core count, so each UDF instance spawns core-count partitions and a node running `NR_OF_CORES` concurrent instances is oversubscribed by `instances × cores`.

**Q:** What knobs should be exposed for the per-instance CPU bound, and what are their defaults?
**A:** Two independent VS properties — `DATAFUSION_TARGET_PARTITIONS` (→ DataFusion `target_partitions`) and `DATAFUSION_THREADS_PER_UDF` (→ Tokio runtime kind / worker threads). Both default to 1. Both are configurable end-to-end (VS property → adapterNotes → ScanSpec → scan UDF). With the defaults each UDF instance uses exactly one core.

**Q:** Should the scan UDF auto-derive `target_partitions` from the core count?
**A:** No. The recommended scale-up formula `df_target_partitions = max(1, floor(NR_OF_CORES / parallelism_factor))` is documented as guidance only; defaults stay 1 and the operator overrides via the property.

## Design Decisions

### [1] Byte-balanced sharding via longest-processing-time-first greedy assignment

- **Decision:** Replace the count-balanced `partition_files` with `partition_files_by_bytes`, which sorts files by size descending and greedily assigns each to the currently-lightest shard (LPT heuristic). Files of reported size 0 are weighted as 1 byte.
- **Alternatives:** (a) Strict prefix-sum equal-byte split preserving file order — rejected: a single large file can badly skew shards. (b) Full dynamic-programming optimum partition — rejected: overkill for a balancing heuristic, and not worth the complexity. (c) Keep count-balancing — rejected: equal file count ≠ equal scan work, so the slowest shard dominates wall-clock.
- **Rationale:** LPT is O(n log n), deterministic, and near-optimal for minimising the heaviest shard's byte load, which is the straggler that bounds query latency. File sizes are already available on `FileScanTask` at zero extra metadata cost.
- **Promotes to ADR:** yes

### [2] Hardware-aware default parallelism factor (`max(NR_OF_CORES × 2, 8)`)

- **Decision:** Capture `NR_OF_CORES` via `SELECT PARAM_VALUE('NR_OF_CORES')` during `createVirtualSchema` and use `NR_OF_CORES × 2` as the default `PARALLELISM_FACTOR`, floored at 8. An explicitly supplied `PARALLELISM_FACTOR` property always wins.
- **Alternatives:** Keep the static constant 8 — rejected: it is a magic number unrelated to the node's actual core pool, so on large nodes it under-subscribes and on tiny nodes it could over-subscribe. Multipliers of ×1 or ×4 — rejected per interview (×2 balances straggler absorption against session-startup cost).
- **Rationale:** Per-node parallelism is bounded by a fixed VM pool sized to `NR_OF_CORES` (verified against the Exasol engine internals). Sizing G to the real core count makes the default sensible for the actual hardware. The floor at 8 preserves prior behaviour when `NR_OF_CORES` is unavailable (→0) or yields a product below 8 (e.g. single-core dev VMs).
- **Promotes to ADR:** yes

### [3] Reuse the existing NPROC connect-back session for the core-count query

- **Decision:** Fetch `NR_OF_CORES` in the same read-only connect-back session already opened for `SELECT NPROC()` (one session, two queries), defaulting `NR_OF_CORES` to 0 on any failure.
- **Alternatives:** A separate connect-back call dedicated to `NR_OF_CORES` — rejected: an extra login round-trip for a value that is only meaningful when `NPROC()` already succeeded.
- **Rationale:** Minimises connect-back overhead and keeps the failure semantics aligned with `CLUSTER_NODES` (both come from the same session; if the session fails, both fall back to safe defaults).
- **Promotes to ADR:** no

### [4] Scope boundary — no change to the 300 cap, fan-out shape, or scan ABI

- **Decision:** Leave `shard_count` (the `node_count × parallelism_factor` formula, the 300 cap, and the clamp to `[1, file_count]`) and the `GROUP BY shard_key` fan-out SQL builders unchanged. Keep the scan UDF ABI a single JSON VARCHAR argument; only add two new optional integer fields to `ScanSpec`.
- **Alternatives:** Lift or tune the 300 cap alongside this work — rejected: 300 is a fixed Exasol `max_dynamic_group_count` default the engine cannot raise; out of scope. Restructure the spec argument into typed columns — rejected: unnecessary, breaks back-compat.
- **Rationale:** Keeps the change contained to the resolve→partition seam, the create-VS connect-back, and the additive ScanSpec fields, minimising blast radius and review surface.
- **Promotes to ADR:** no

### [5] Per-instance CPU bound via two orthogonal VS properties, both defaulting to 1

- **Decision:** Expose `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` as VS properties, store them as `DF_TARGET_PARTITIONS` / `DF_THREADS_PER_UDF` adapterNotes, round-trip them into two new `ScanSpec` fields (`df_target_partitions`, `df_threads_per_udf`), and consume them in the scan UDF: `SessionConfig::with_target_partitions(n)` and the Tokio runtime kind respectively. Both default to 1 (one core per UDF instance).
- **Alternatives:** (a) A single combined parallelism knob — rejected: DataFusion partition count (logical work splitting) and Tokio worker-thread count (OS threads) are orthogonal levers and operators may want to tune each. (b) Leave `target_partitions` at DataFusion's default (host core count) — rejected: that is the oversubscription bug (`instances × cores` ≈ `NR_OF_CORES²` threads on a node). (c) Auto-derive `target_partitions` from cores in code — rejected: hides cross-layer coupling and removes operator control (see [7]).
- **Rationale:** Default 1 + 1 makes each UDF instance use exactly one core, so cluster-level shard fan-out is the single, predictable source of parallelism and a node is never oversubscribed by default. Each property remains a clean operator override.
- **Promotes to ADR:** yes

### [6] Tokio runtime kind chosen from the spec at `run_scan`; ScanSpec fields optional with default 1

- **Decision:** In `run_scan`, deserialize the `ScanSpec` before constructing the Tokio runtime, then build it conditionally: `threads <= 1 → new_current_thread()`, `threads > 1 → new_multi_thread().worker_threads(threads)`. Make both new `ScanSpec` fields `#[serde(default)]` to 1 so pre-existing serialized specs deserialize unchanged.
- **Alternatives:** Always `new_current_thread()` (cannot honour >1) or always `new_multi_thread()` (spawns a pool even for the 1-thread default — wasteful and itself oversubscribing) — both rejected. Make the fields required — rejected: breaks back-compat with already-serialized specs.
- **Rationale:** The conditional honours the configured value exactly while keeping the common default cheap. Optional-with-default-1 keeps the spec ABI backward-compatible (purely additive).
- **Promotes to ADR:** no

### [7] Recommended `target_partitions` formula is documented guidance, not enforced

- **Decision:** Document `df_target_partitions = max(1, floor(NR_OF_CORES / parallelism_factor))` in the create-virtual-schema spec as the recommended scale-up value, but never compute it automatically in code; the runtime default stays 1.
- **Alternatives:** Auto-apply the formula when the property is absent — rejected: it couples the scan-side partition count to the adapter-side parallelism factor implicitly, surprises operators, and removes a deliberate control. With the default `parallelism_factor = NR_OF_CORES × 2` the formula resolves to 1 anyway, so the documented default and the enforced default agree.
- **Rationale:** Keeps the two layers decoupled and the behaviour explicit; the operator opts into oversubscription consciously.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
