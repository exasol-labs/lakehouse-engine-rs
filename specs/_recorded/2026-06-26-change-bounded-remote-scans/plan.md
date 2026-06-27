# Plan: change-bounded-remote-scans

## Summary

Ship the fixes a completed live-cluster spike identified for the AWS Glue OOM crashes (`F-UDF-CL-RUST-9001 VM crashed`): adopt the SDK's Arrow-IPC `emit_batch` path to kill emit double-materialization, surface `ResourcesExhausted` as a clean bounded error instead of a VM crash, bound the Parquet decode working set via `batch_size`, and wire `NR_OF_CORES`/`PARALLELISM_FACTOR` through the remote bench harness — so grouped aggregates and full-row joins run bounded on the live 3-node cluster.

## Design

### Context

The live AWS Glue cluster (3 nodes × 8 cores, ~15 GiB/node free after the DB engine) crashes the UDF VM for grouped aggregates over full `lineitem` and for joins emitting ~60M rows. The spike measured four root causes; this plan ships the behavioral fixes plus the operational version bumps that unblock them.

- **Goals** — non-crashing, bounded execution on the live cluster for high-cardinality grouped aggregates and large-result scans; shrink peak per-instance footprint enough that 8 instances × per-instance pool fits within ~15 GiB/node; produce a clean, accurate error if a bound is hit rather than an OOM crash; let the remote bench drive the same cores/parallelism knobs the docker bench already drives.
- **Non-Goals** — spill-to-`/tmp` as a backstop (the live `/tmp` is tmpfs/RAM, so `probe_tmp_spill` correctly returns `NoDisk` and we do NOT change it); pool-sizing math (already spec'd and implemented correctly); join pushdown or query rewrites (out of mission scope); the re-bench run itself (observational, post-merge).

### Decision

Four independent behavioral changes plus two mechanical version bumps.

#### Architecture

```
scan UDF run()
  ├─ session_config_for_spec ── + batch_size  ← bounds Parquet decode working set
  ├─ raw-row path ── emit_batch(&RecordBatch) ── Arrow IPC bytes ── ctx (no Vec<Value> intermediate)
  └─ error path ── redact_storage_error ── recognizes ResourcesExhausted ── clean memory-exhaustion error

bench/run.sh
  └─ remote target ── VS_EXTRA_PROPS now carries NR_OF_CORES + PARALLELISM_FACTOR (was "")
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Arrow-IPC emit (`EmitBatch`/`emit_batch`) | `scan/emit.rs` raw-row path | Eliminates the simultaneous `RecordBatch` + `Vec<Value>` double-materialization; only IPC bytes cross the `.so` boundary, preserving the Arrow-TypeId-ABI safety rule |
| Error classification before redaction | `scan/emit.rs` `redact_storage_error` | A `ResourcesExhausted` condition must be reported as memory exhaustion, not masked as a storage-read failure |
| Config-driven decode bound | `scan/mod.rs` `session_config_for_spec` | `batch_size` caps the per-batch decode/scan working set that the memory pool does not account for |
| Env-to-VS-property passthrough | `bench/run.sh` remote target | Remote cluster must exercise the cores/parallelism knobs the docker path already wires |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Use SDK `emit_batch` (Arrow IPC) on the raw-row path | Keep row-by-row `Vec<Value>` emit; shrink batches only | Row-by-row holds two full copies of every batch at peak; IPC emit removes the `Vec<Value>` copy entirely while staying ABI-safe (bytes, not Arrow types) |
| Surface `ResourcesExhausted` as a distinct clean error | Let it fall through `redact_storage_error` as "data could not be read" | The current redaction reclassifies it into a misleading storage error; the operator needs the true cause to right-size cores/parallelism |
| Bound decode via `batch_size` (spec-sourced, conservative default) | Rely on the memory pool alone | DataFusion's pool bounds aggregation/sort/join but NOT Parquet→Arrow decode buffers; `batch_size` is the lever that bounds that working set |
| Wire remote bench knobs from existing `BENCH_*` env, matching docker defaults | New separate remote-only vars | The docker path already reads `BENCH_NR_OF_CORES`/`BENCH_PARALLELISM_FACTOR`; reuse keeps one knob set across targets |
| Do NOT change spill / `probe_tmp_spill` | Make `/tmp` spill work on the live cluster | Live `/tmp` is tmpfs (RAM-backed); spilling there is a RAM trap, so a bounded clean error is the correct backstop |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |
| datafusion-scan/scan-execution-memory-and-credentials | CHANGED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |
| packaging/cloud-e2e-harness | CHANGED | `packaging/cloud-e2e-harness/spec.md` |

## Dependencies

- `exasol-udf-sdk` `0.16.0` → `0.18.0` with the `emit-arrow` feature added (alongside existing `connect-back`); `exasol-udf-macros` `0.16.0` → `0.18.0` in lockstep. Both pinned in the root `Cargo.toml` and `crates/lakehouse-engine/Cargo.toml`.
- `arrow` already carries the `ipc` feature in `crates/lakehouse-engine/Cargo.toml` — no change needed.

## Migration

| Current | New |
|---------|-----|
| `emit_stream` converts each batch to `Vec<Value>` and emits row-by-row | `emit_stream` emits each `RecordBatch` via `emit_batch` (Arrow IPC); no `Vec<Value>` intermediate on the raw-row path |
| `redact_storage_error` wraps every scan error as "assigned data could not be read" | `ResourcesExhausted` is classified first and surfaced as a memory-exhaustion error; other errors keep the storage-redaction path |
| `session_config_for_spec` sets only `target_partitions` | also sets `batch_size` (spec-sourced, conservative default, clamped ≥1) |
| `bench/run.sh` remote: `VS_EXTRA_PROPS=""` | remote builds `VS_EXTRA_PROPS` with `NR_OF_CORES` + `PARALLELISM_FACTOR` from `BENCH_*` env, matching docker defaults |

## Implementation Tasks

1. **SDK + feature bumps (mechanical, do first — unblocks emit_batch)**
   1. 1.1 Bump `exasol-udf-sdk` to `0.18.0` and add the `emit-arrow` feature in the root `Cargo.toml` and `crates/lakehouse-engine/Cargo.toml`; bump `exasol-udf-macros` to `0.18.0`. Run `cargo update -p exasol-udf-sdk -p exasol-udf-macros` and confirm the workspace builds (`cargo test --no-run`).
   2. 1.2 Confirm the exact `EmitBatch` trait import path and `emit_batch(&RecordBatch)` signature against the 0.18.0 source (docs.rs hides `emit-arrow`-gated items); record it for task 2.

2. **emit_batch adoption — `scan/scan-execution`**
   1. 2.1 Replace the raw-row `batch_to_rows` + row-by-row emit loop in `scan/emit.rs::emit_stream` with `emit_batch(&batch)` per batch, fetching/emitting/dropping one batch at a time; remove the `Vec<Value>` intermediate on this path. [expert]
   2. 2.2 Update the `emit_stream` unit test (`emits_batch_by_batch_without_materializing`) to assert batch-at-a-time IPC emit and the no-`Vec<Value>` invariant; remove or repoint any now-dead `batch_to_rows` usage on the raw-row path (keep it if the partial-aggregate path still needs row conversion).

3. **ResourcesExhausted surfacing — `scan/scan-execution`**
   1. 3.1 In `scan/emit.rs::redact_storage_error` (and/or its call sites in `scan/mod.rs`), classify a DataFusion `ResourcesExhausted` condition and surface a clean memory-exhaustion error distinct from the "assigned data could not be read" wrapping, keeping credential redaction. [expert]
   2. 3.2 Add a unit test asserting a `ResourcesExhausted`-shaped error is surfaced as a memory-exhaustion error (not a storage error) and carries no credential values.

4. **batch_size decode bound — `scan/scan-execution-memory-and-credentials`**
   1. 4.1 Add a `df_batch_size` field to `ScanSpec` (`scan/spec.rs`) following the `df_target_partitions` round-trip + backward-compat-default pattern.
   2. 4.2 Set `batch_size` in `session_config_for_spec` (`scan/mod.rs`) from the spec value (conservative default when absent, clamped ≥1); ensure it applies on both the raw-row and partial-aggregate paths.
   3. 4.3 Add unit tests: `df_batch_size` survives JSON round-trip and defaults on legacy specs; `session_config_for_spec` applies the configured batch size and clamps a sub-1 value to 1.

5. **Remote bench wiring — `packaging/cloud-e2e-harness`**
   1. 5.1 In `bench/run.sh`, replace the remote target's `VS_EXTRA_PROPS=""` with a block carrying `NR_OF_CORES` + `PARALLELISM_FACTOR` from `BENCH_NR_OF_CORES`/`BENCH_PARALLELISM_FACTOR`, reusing the docker path's defaults (factor the shared `printf` into a helper so docker and remote cannot drift).

6. **Tracking + docs (mechanical)**
   1. 6.1 Open the GitHub issue for this work (`ghbrk gh issue create`) per project rules and reference it in the implementing commit (`Closes #<n>`).
   2. 6.2 Update `CLAUDE.md` SDK-version and emit-buffering notes if the `emit_batch` adoption changes the documented emit guidance (Emit buffering section).

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (after Group 0) | Task 3 (ResourcesExhausted), Task 4 (batch_size), Task 5 (remote bench) |
| Group 0 (prerequisite) | Task 1 (SDK bump) |
| Group B (after Group A) | Task 2 (emit_batch — depends on 1.2 import confirmation) |

Sequential dependencies:
- Group 0 → all (the SDK + `emit-arrow` bump must land before `emit_batch` compiles).
- Task 2 depends on Task 1.2 (confirmed `EmitBatch` import).
- Task 3, Task 4, Task 5 are mutually independent and touch disjoint code (`redact_storage_error`, `session_config_for_spec`/`spec.rs`, `bench/run.sh`).

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function (conditional) | `scan/convert.rs::batch_to_rows` | Becomes dead on the raw-row scan path once `emit_batch` lands; remove ONLY if no other path (partial-aggregate row conversion) still uses it — otherwise keep |
| Test assertion | `scan/emit.rs` `emits_batch_by_batch_without_materializing` (row-by-row assertions) | Replaced by the IPC batch-emit assertions in task 2.2 |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Arrow batches are emitted incrementally as Arrow IPC and never double-materialized | Unit | `crates/lakehouse-engine/src/scan/emit.rs` | `emits_batch_by_batch_without_materializing` (updated for IPC emit) |
| Arrow batches are emitted incrementally as Arrow IPC and never double-materialized | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `scan_registers_assigned_files_and_returns_rows` (existing raw-row scan still returns correct rows via the new emit path) |
| Scan surfaces a clean memory-exhaustion error instead of crashing the VM | Unit | `crates/lakehouse-engine/src/scan/emit.rs` | `resources_exhausted_surfaces_as_memory_error_not_storage_error` (new) |
| Scan surfaces a clean memory-exhaustion error instead of crashing the VM | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_high_cardinality_group_by_spill` (existing high-cardinality grouped scan completes or errors cleanly, no VM crash) |
| Scan bounds the Parquet decode working set via a configured batch size | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `df_batch_size_round_trips_and_defaults` (new) |
| Scan bounds the Parquet decode working set via a configured batch size | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `session_config_applies_batch_size_and_clamps_floor` (new) |
| Remote bench wires NR_OF_CORES and PARALLELISM_FACTOR into the virtual schema | Manual | `bench/run.sh` (remote target) | see Manual Testing — verified by inspecting the generated `CREATE VIRTUAL SCHEMA` SQL |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-execution (emit_batch + ResourcesExhausted) | `make test-e2e` (local Exasol Docker), then live re-run of the grouped-aggregate-over-full-lineitem and ~60M-row join queries via `BENCH_TARGET=remote bench/run.sh` | Queries complete with correct results, or return a clean memory-exhaustion error; no `F-UDF-CL-RUST-9001 VM crashed` |
| scan-execution-memory-and-credentials (batch_size) | `make test-e2e` | Scans complete; per-instance peak footprint reduced (no node OOM-kill at 8 instances/node) |
| cloud-e2e-harness (remote bench wiring) | `BENCH_TARGET=remote BENCH_NR_OF_CORES=8 BENCH_PARALLELISM_FACTOR=8 bench/run.sh` (or dry-run echo of the generated SQL) | The emitted `CREATE VIRTUAL SCHEMA` SQL contains `NR_OF_CORES = '8'` and `PARALLELISM_FACTOR = '8'` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF .so) | `make cross-musl-udf-build` | Exit 0 |
| Test (host unit) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
