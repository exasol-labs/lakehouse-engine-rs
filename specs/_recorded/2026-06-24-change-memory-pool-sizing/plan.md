# Plan: change-memory-pool-sizing

## Summary

Make the DataFusion scan UDF size its memory pool from the *net* per-instance budget — the per-instance RSS limit minus a fixed container/binary overhead — instead of treating the whole limit as available, and expose both the pool fraction and the overhead as VS properties (`MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`) that round-trip through `adapterNotes` into the scan spec.

## Design

### Context

`build_runtime_env` currently sizes the DataFusion memory pool to `0.6 × ctx.memory_limit()`. But `ctx.memory_limit()` is the per-process `RLIMIT_RSS` cap, and RSS counts *everything* resident in the process — the Rust SLC binary, shared libraries, allocator arenas, and stacks — before DataFusion's allocator takes over. The Rust SLC container consumes roughly 150 MB of that budget at startup. Treating the full limit as DataFusion-available systematically over-allocates the pool, pushing each instance closer to the engine's 80% concurrency-stall threshold and increasing OOM risk on dense nodes (3 nodes × 60 cores × ~150 MB ≈ 27 GB cluster-wide fixed cost is invisible to the current formula).

- **Goals** — Subtract a configurable container overhead from the per-instance limit before applying the pool fraction; make both the fraction and the overhead VS-configurable with safe defaults; keep the existing unknown-limit fallback path unchanged.
- **Non-Goals** — No node-level RAM discovery (no engine API exists); no change to the spill probe, the credential passthrough, the threading/partition configuration, or the shard-count math; no change to the 80% engine handbrake itself.

### Decision

The new pool formula, applied inside `build_runtime_env` only when `memory_limit_bytes > 0`:

```
net    = saturating_sub(memory_limit_bytes, overhead_bytes)
budget = max( (net as f64 * fraction) as u64, MIN_POOL_FLOOR_BYTES )
```

When `memory_limit_bytes == 0` the existing `DEFAULT_BUDGET_BYTES` (1 GiB) fallback is used unchanged, ignoring fraction and overhead.

Both inputs are VS properties resolved at `createVirtualSchema`, recorded in `adapterNotes`, round-tripped at pushdown, and carried in each per-shard `ScanSpec`. This mirrors the existing `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF` plumbing exactly.

**Defaults and constants:**

| Name | Default | Rationale |
|------|---------|-----------|
| `MEMORY_POOL_FRACTION` (VS prop) → `memory_pool_fraction` (ScanSpec f64) | `0.6` | Unchanged from today; keeps the net pool below the 80% handbrake. |
| `INSTANCE_OVERHEAD_MB` (VS prop) → `instance_overhead_mb` (ScanSpec u64) | `200` | Margin over the ~150 MB empirical estimate. RSS overhead (shared libs + allocator arenas + stack) is hard to bound and varies; under-sizing risks OOM, which is strictly worse than a slightly smaller pool. 200 MB leaves a comfortable cushion while costing only ~50 MB of pool versus the raw estimate. |
| `MIN_POOL_FLOOR_BYTES` (scan const) | `256 MiB` | Degenerate guard: if `overhead_bytes ≥ limit` (e.g. a tiny limit, or a mis-set overhead), `net` collapses toward zero and `fraction × net` would be near-zero. The floor guarantees a usable pool so the session context still builds and a scan can run. |

**Handbrake verification** (limit = 4096 MB, overhead = 200 MB, fraction = 0.6):
`net = 3896 MB; budget = 0.6 × 3896 ≈ 2338 MB`. The engine stalls at `0.8 × 4096 = 3277 MB`. `2338 < 3277` — the new formula stays well within the handbrake, with *more* headroom than the old `0.6 × 4096 = 2458 MB` because the subtraction shrinks the pool further. Confirmed: subtracting overhead can only lower the budget, so the handbrake invariant is preserved for any non-negative overhead.

#### Architecture

```
createVirtualSchema
  resolve_memory_pool_fraction(props)  ──┐
  resolve_instance_overhead_mb(props)  ──┤
                                         ▼
                     build_adapter_notes(... MEMORY_POOL_FRACTION, INSTANCE_OVERHEAD_MB)
                                         │  (stringified JSON, persisted by Exasol)
                                         ▼
pushdown  adapter_note(req, MEMORY_POOL_FRACTION / INSTANCE_OVERHEAD_MB)
                                         │
                                         ▼
          handle_pushdown(... memory_pool_fraction, instance_overhead_mb)
                                         │
                                         ▼
          ScanSpec { memory_pool_fraction: f64, instance_overhead_mb: u64, ... }  (serde defaults)
                                         │  JSON over Value::String
                                         ▼
scan UDF  build_session_context(spec, ctx.memory_limit())
            → build_runtime_env(memory_limit_bytes, fraction, overhead_bytes, spill)
                net = limit.saturating_sub(overhead); budget = max(net·fraction, FLOOR)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| VS prop → adapterNotes → ScanSpec round-trip | `adapter/mod.rs`, `adapter/pushdown.rs`, `scan/spec.rs` | Reuse the exact `df_target_partitions` plumbing; no new transport channel. |
| `#[serde(default = "fn")]` field | `scan/spec.rs` | Pre-existing scan specs deserialize to the documented defaults — backward compatible. |
| `saturating_sub` + `max(floor)` guard | `scan/runtime.rs::build_runtime_env` | Never produce a zero/negative or panicking budget on degenerate inputs. |
| Resolve-once, validate-and-default | `resolve_memory_pool_fraction` / `resolve_instance_overhead_mb` | Mirror `resolve_parallelism_factor`: parse, validate range, fall back to default. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Subtract overhead *before* fraction | Apply a smaller fixed fraction (e.g. 0.5) | Overhead is roughly constant in absolute bytes, not proportional to the limit; subtracting a constant models reality and stays correct as limits scale. |
| Default overhead 200 MB | 150 MB (raw estimate); 256 MB | 150 MB is the floor of the estimate with no margin; 200 MB adds a cushion for allocator/stack variance without materially shrinking the pool. |
| Floor the final pool at 256 MiB | Clamp `net` to a floor; error out when overhead ≥ limit | Flooring the final budget is the simplest single guard that guarantees a usable pool in every degenerate case; erroring would needlessly fail scans on small-limit nodes. |
| Carry fraction+overhead in ScanSpec | Read them from UDF metadata in the scan | The fraction/overhead are operator policy set at the VS, not engine-reported; the limit itself is the only value read at runtime via `ctx.memory_limit()`. |
| Keep the 1 GiB unknown-limit fallback | Apply the floor there too | The fallback already encodes a conservative known-good budget; fraction/overhead are meaningless without a real limit. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-memory-and-credentials | CHANGED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |

## Implementation Tasks

1. **ScanSpec fields**
   1.1 Add `memory_pool_fraction: f64` (serde default `0.6`) and `instance_overhead_mb: u64` (serde default `200`) to `ScanSpec` in `crates/lakehouse-engine/src/scan/spec.rs`, with default fns.
   1.2 Extend the round-trip unit test to assert both fields survive serialize→deserialize and that a legacy payload lacking them deserializes to `0.6` / `200`.

2. **Scan runtime formula** [expert]
   2.1 Change `build_runtime_env` in `crates/lakehouse-engine/src/scan/runtime.rs` to take `fraction: f64` and `overhead_bytes: u64`; compute `net = memory_limit_bytes.saturating_sub(overhead_bytes)`, `budget = max((net as f64 * fraction) as u64, MIN_POOL_FLOOR_BYTES)` for the positive-limit branch; leave the `== 0` fallback returning `DEFAULT_BUDGET_BYTES` unchanged. [expert]
   2.2 Add `MIN_POOL_FLOOR_BYTES` const (256 MiB) with a doc comment; update `MEMORY_FRACTION` doc to note it is now the *default*, not a hardcoded value.
   2.3 Update `build_session_context` in `crates/lakehouse-engine/src/scan/mod.rs` to pass `spec.memory_pool_fraction` and `spec.instance_overhead_mb * 1024 * 1024` into `build_runtime_env`.
   2.4 Update/extend the `runtime.rs` unit tests: positive-limit budget = `fraction × (limit − overhead)`; overhead ≥ limit clamps to the floor; zero-limit still uses `DEFAULT_BUDGET_BYTES`. [expert]
   2.5 Update the `scan/mod.rs` seam tests (`session_context_sizes_pool_from_ctx_limit`, zero-limit test) to the new formula.

3. **Adapter property resolution + adapterNotes**
   3.1 In `crates/lakehouse-engine/src/adapter/mod.rs`, add consts `PROP_MEMORY_POOL_FRACTION` / `NOTE_MEMORY_POOL_FRACTION` / `DEFAULT_MEMORY_POOL_FRACTION` (0.6) and `PROP_INSTANCE_OVERHEAD_MB` / `NOTE_INSTANCE_OVERHEAD_MB` / `DEFAULT_INSTANCE_OVERHEAD_MB` (200).
   3.2 Add `resolve_memory_pool_fraction(props)` (parse f64, accept `0 < x ≤ 1.0`, else default) and `resolve_instance_overhead_mb(props)` (parse u64, accept `≥ 0`, else default), mirroring `resolve_df_target_partitions`.
   3.3 Extend `build_adapter_notes` signature + body to record both new keys; update both call sites (`handle_create_virtual_schema`, all in-file tests that call `build_adapter_notes`).
   3.4 Call the two resolvers in `handle_create_virtual_schema` and thread their results into `build_adapter_notes`.

4. **Pushdown round-trip into ScanSpec**
   4.1 In `handle_pushdown_request` (`adapter/mod.rs`), read `NOTE_MEMORY_POOL_FRACTION` / `NOTE_INSTANCE_OVERHEAD_MB` from `adapter_note(...)` with the same default/validate pattern; pass into `handle_pushdown`.
   4.2 Extend `handle_pushdown` signature in `crates/lakehouse-engine/src/adapter/pushdown.rs` with `memory_pool_fraction: f64`, `instance_overhead_mb: u64`; set both fields on every `ScanSpec` template (grouped-agg, single-group/row paths).
   4.3 Update all `ScanSpec { ... }` literals in `pushdown.rs` tests (and any other test modules) to set the two new fields.

5. **Tests**
   5.1 Add adapter unit tests: `resolve_memory_pool_fraction` defaults/validates (absent, empty, 0, >1.0, valid); `resolve_instance_overhead_mb` defaults/validates; both round-trip through `build_adapter_notes` + `adapter_note`.
   5.2 Add an integration/E2E assertion (extend `e2e_scan_test.rs` or the existing memory seam) that a scan spec with explicit fraction/overhead sizes the pool to `fraction × (limit − overhead)` end to end.

6. **Verification & cleanup**
   6.1 Run `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
   6.2 Run `make test-e2e` against the local Exasol Docker container.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1 (ScanSpec fields), 3 (adapter resolution) |
| Group B | 2 (scan runtime formula), 4 (pushdown round-trip) |
| Group C | 5 (tests) |
| Group D | 6 (verification) |

Sequential dependencies:
- Group A → Group B (task 2.3 needs the ScanSpec fields from 1.1; task 4.2 needs them too)
- Group A → Group B (task 4.1 needs the adapterNotes keys from 3.1)
- Group B → Group C (tests assert the new formula and round-trip)
- Group C → Group D

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none) | — | The change is additive: `MEMORY_FRACTION` and `DEFAULT_BUDGET_BYTES` remain (now serving as defaults / fallback). No symbols become obsolete. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Scan sizes its memory pool from the reported per-instance limit | Unit | `crates/lakehouse-engine/src/scan/runtime.rs` | `build_runtime_env_sizes_pool_from_net_budget` |
| Scan sizes its memory pool from the reported per-instance limit (seam) | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `session_context_sizes_pool_from_ctx_limit` |
| Scan falls back to the default budget when no memory limit is reported | Unit | `crates/lakehouse-engine/src/scan/runtime.rs` | `build_runtime_env_uses_default_budget_on_zero_limit` |
| Scan clamps the memory pool to a minimum floor when overhead exceeds the limit | Unit | `crates/lakehouse-engine/src/scan/runtime.rs` | `build_runtime_env_clamps_to_floor_when_overhead_exceeds_limit` |
| Adapter records the memory-pool fraction in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `create_vs_records_memory_pool_fraction` |
| Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `create_vs_records_instance_overhead_mb` |
| Recorded memory budget controls round-trip into the scan spec | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `memory_budget_round_trips_into_scan_spec` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/create-virtual-schema | Create a VS with `MEMORY_POOL_FRACTION='0.5'` and `INSTANCE_OVERHEAD_MB='256'`, then `SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME='<vs>';` (via `exapump`) | `adapterNotes` JSON contains `"MEMORY_POOL_FRACTION":"0.5"` and `"INSTANCE_OVERHEAD_MB":"256"` |
| datafusion-scan/scan-execution-memory-and-credentials | `make test-e2e` then run a scan query through the VS against the seeded Iceberg table | Query returns correct rows; no OOM/`ResourcesExhausted` at the default budget |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, not skips, if no DB) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
