# Decision Log: change-bounded-remote-scans

Date: 2026-06-26

## Interview

No clarifying interview was conducted. The plan was authored from a completed
live-cluster spike whose findings the orchestrator supplied in full. The spike
measured the four root causes on the live AWS Glue cluster (3 nodes × 8 cores,
~64 GiB RAM, ~49 GiB DB MemSize → ~15 GiB/node free, `/tmp` = tmpfs) and the SDK
0.18.0 API surface was confirmed from docs.rs. No requirement was ambiguous.

## Design Decisions

### [1] Adopt Arrow-IPC emit (`emit_batch`) on the raw-row scan path

- **Decision:** Emit each `RecordBatch` via the SDK's `EmitBatch` API (`emit-arrow` feature), which serializes to Arrow IPC bytes internally, removing the per-batch `Vec<Value>` conversion on the raw-row path.
- **Alternatives:** Keep row-by-row `Vec<Value>` emit but only shrink batches; emit raw Arrow types across the boundary.
- **Rationale:** Row-by-row holds the `RecordBatch` and its `Vec<Value>` copy simultaneously at peak — exactly the double-materialization the spike measured for the ~60M-row join. IPC emit removes the second copy while still honouring the Arrow-TypeId ABI rule: only IPC bytes cross the `.so` boundary, never typed Arrow objects.
- **Promotes to ADR:** yes

### [2] Surface `ResourcesExhausted` as a distinct clean error, not a storage error

- **Decision:** Classify a DataFusion `ResourcesExhausted` condition before redaction and surface it as a memory-exhaustion error, distinct from the "assigned data could not be read" wrapping.
- **Alternatives:** Let it fall through the existing `redact_storage_error` path.
- **Rationale:** The current path reclassifies a genuine memory bound as a misleading storage-read failure. The operator needs the true cause to right-size cores/parallelism. This is the bounded backstop the mission's "usable engine" constraint requires when `/tmp` cannot spill.
- **Promotes to ADR:** yes

### [3] Bound Parquet decode working set via `batch_size`

- **Decision:** Set DataFusion `batch_size` in `session_config_for_spec` from a spec field (conservative default, clamped ≥1), applied on both scan paths.
- **Alternatives:** Rely on the memory pool alone.
- **Rationale:** DataFusion's `GreedyMemoryPool`/`FairSpillPool` bound aggregation/sort/join but NOT the Parquet→Arrow decode/scan buffers (spike root cause 2). `batch_size` is the lever that bounds that out-of-pool working set.
- **Promotes to ADR:** yes

### [4] Do not change spill / `probe_tmp_spill`

- **Decision:** Leave `probe_tmp_spill` returning `NoDisk` for the live tmpfs `/tmp`; the bounded clean error is the backstop, not spill.
- **Alternatives:** Make `/tmp` spill work on the live cluster.
- **Rationale:** Live `/tmp` is RAM-backed tmpfs; spilling there is a RAM trap. The existing probe is already correct; no change.
- **Promotes to ADR:** no

### [5] Reuse existing `BENCH_*` env for remote bench parallelism knobs

- **Decision:** The remote bench builds `VS_EXTRA_PROPS` from `BENCH_NR_OF_CORES`/`BENCH_PARALLELISM_FACTOR` with the same defaults the docker path uses, factored into a shared helper.
- **Alternatives:** New remote-only environment variables.
- **Rationale:** The docker path already reads these vars; one knob set across both targets prevents drift and avoids a second naming scheme.
- **Promotes to ADR:** no

### [6] SDK/macros bump scoped as mechanical prerequisite, not a spec'd behavior

- **Decision:** The `0.16.0 → 0.18.0` SDK + macros bump and `emit-arrow` feature add are Cargo.toml edits sequenced first, not captured as spec scenarios.
- **Alternatives:** Spec the version bump.
- **Rationale:** No external behavior change of its own; it only unblocks `emit_batch`. Spec scenarios describe behavior, not dependency pins.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
