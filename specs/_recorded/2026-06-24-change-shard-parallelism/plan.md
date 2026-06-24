# Plan: change-shard-parallelism

## Summary

Two parallelism concerns merged into one change (superset of the validated
`change-sharding-byte-balanced-and-cores-aware` plan):

1. **Cluster-level work balance** — make work-unit sharding byte-balanced (split
   files by cumulative `file_size_in_bytes` rather than by count) and make the
   default `PARALLELISM_FACTOR` hardware-aware (`max(NR_OF_CORES × 2, 8)`) by
   capturing `NR_OF_CORES` over the same `createVirtualSchema` connect-back that
   already fetches `NPROC()`.
2. **Per-instance CPU bound** — stop each scan UDF instance from spawning
   CPU-core-count DataFusion threads. Expose two new VS properties,
   `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` (both default
   `1`), store them in `adapterNotes`, round-trip them into `ScanSpec`, and have
   the scan UDF set DataFusion's `target_partitions` and choose its Tokio runtime
   kind from those values. With the defaults each UDF instance uses exactly one
   core, so the cluster-level shard fan-out provides parallelism without
   per-node oversubscription (`instances × cores`).

## Design

### Context

The current sharding splits files into G equal-count groups. Equal *count* does not
mean equal *work*: one shard of three 1 GB files does far more I/O than a shard of
three 1 KB files, so the slowest shard (straggler) dominates wall-clock time. Iceberg
already reports `FileScanTask.file_size_in_bytes`, so a byte-balanced split is
available at zero extra metadata cost.

Separately, the default `PARALLELISM_FACTOR` is the magic constant 8. The Exasol
engine architect confirmed that actual per-node parallelism is bounded by a fixed
per-node VM pool sized to `NR_OF_CORES`. Oversubscribing to `NR_OF_CORES × 2` sizes
G to the real hardware (straggler absorption without excessive session-startup cost)
instead of a guess; the user can still override `PARALLELISM_FACTOR` explicitly.

A third gap surfaces inside the scan UDF itself. Tokio is already
`new_current_thread()` (one OS thread — correct). But DataFusion's
`SessionConfig::new()` defaults `target_partitions` to the host core count, and the
UDF never overrides it. Because Exasol multiplexes up to `NR_OF_CORES` concurrent UDF
instances onto a node, the effective thread count is `instances × target_partitions`
≈ `NR_OF_CORES²` (e.g. 32 × 32 = 1024 on a 32-core node) — massive oversubscription
that thrashes the node. The fix is to make per-instance CPU width explicit and
configurable: set DataFusion `target_partitions` from the spec (default 1) and choose
the Tokio runtime kind from the spec (default current-thread). Cluster-level
parallelism (the shard fan-out) is then the single source of parallelism, and the
node is never oversubscribed by default.

- **Goals** — (1) shards differ by cumulative bytes, not file count, so per-shard
  scan work is balanced; (2) the default parallelism factor reflects per-node core
  count; (3) `PARALLELISM_FACTOR` remains fully user-overridable; (4) per-instance
  DataFusion partition count and Tokio worker-thread count are explicit, default to
  1, and are user-overridable via two VS properties carried through `adapterNotes`
  into `ScanSpec`.
- **Non-Goals** — no change to the 300 cap, the clamp-to-`[1, file_count]` rule, the
  `GROUP BY shard_key` fan-out shape, the resolve-once seam, or the scan UDF ABI
  (the JSON spec stays one VARCHAR arg; only two new optional integer fields are
  added). No per-file cost model beyond raw byte size (no row-count or
  predicate-selectivity weighting). No code that auto-derives `target_partitions`
  from cores — the recommended formula `max(1, floor(NR_OF_CORES / parallelism_factor))`
  is documented guidance only, never enforced.

### Decision

#### Architecture

```
createVirtualSchema:
  resolve_cluster_nodes(ctx) ──one connect-back session──▶ SELECT NPROC()
                                                          └▶ SELECT PARAM_VALUE('NR_OF_CORES')
       │                                                         │
       ▼                                                         ▼
  CLUSTER_NODES                                            NR_OF_CORES
       └──────────────┬────────────────────────────────────────┘
                      ▼
        resolve_parallelism_factor(props, nr_of_cores)
          = PARALLELISM_FACTOR prop, else max(NR_OF_CORES × 2, 8)
        resolve_df_target_partitions(props) = prop (≥1), else 1
        resolve_df_threads_per_udf(props)   = prop (≥1), else 1
                      ▼
        build_adapter_notes → {CLUSTER_NODES, PARALLELISM_FACTOR, NR_OF_CORES,
                               DF_TARGET_PARTITIONS, DF_THREADS_PER_UDF}  (persisted)

pushdown:
  read adapterNotes → cluster_nodes, parallelism_factor,
                      df_target_partitions, df_threads_per_udf
  resolve_file_list ──▶ plan_files_from_table → Vec<(path, size_bytes)>
                                   │
                                   ▼
        shard_count(nodes, factor, file_count)            (UNCHANGED: 300 cap, clamp)
                                   │
                                   ▼
        partition_files_by_bytes(files_with_sizes, G)     (NEW: byte-balanced)
                                   │
                                   ▼
        ScanSpec { …, df_target_partitions, df_threads_per_udf }  (NEW fields)
                                   │
                                   ▼
        build_*_scan_sql(shards: Vec<Vec<String>>, …)     (UNCHANGED downstream)

scan UDF (run_scan):
  parse ScanSpec FIRST  ──▶ df_threads_per_udf == 1 ? new_current_thread()
                                                    : new_multi_thread().worker_threads(n)
  build_session_context ──▶ SessionConfig.with_target_partitions(df_target_partitions)
```

The byte partitioner consumes `(path, size)` pairs and returns `Vec<Vec<String>>` —
the same shard shape every downstream SQL builder already expects, so that change is
contained to the file-resolution → partition seam. The two new ScanSpec fields are
optional integers defaulting to 1, so the JSON spec stays backward-compatible and the
ABI is unchanged.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Longest-processing-time-first (LPT) greedy bin-balancing | `partition_files_by_bytes` | Standard makespan-minimisation heuristic: sort files by size descending, assign each to the currently-lightest shard. Cheap, deterministic, near-optimal for balancing total bytes. |
| Single connect-back session, two queries | `resolve_cluster_nodes` | Reuse the one read-only session already opened for `NPROC()`; avoids a second login round-trip. |
| Floor-at-8 default | `resolve_parallelism_factor` | `NR_OF_CORES × 2` could be 0 on a dev VM that fails to report cores; flooring at 8 preserves the previous sensible behaviour. |
| adapterNotes round-trip | `NR_OF_CORES`, `DF_TARGET_PARTITIONS`, `DF_THREADS_PER_UDF` notes | Same persisted channel as `CLUSTER_NODES`/`PARALLELISM_FACTOR`; Exasol drops returned `properties`, so notes are the only durable seam. |
| Optional-with-default-1 spec fields | `ScanSpec.df_target_partitions`, `ScanSpec.df_threads_per_udf` | `#[serde(default = …)]` keeps old specs valid and means "1 core per instance" unless explicitly raised. |
| Parse spec before building the runtime | `run_scan` | The Tokio runtime kind depends on `df_threads_per_udf`, so the spec must be deserialized before the runtime is constructed (a small reorder of `run_scan`). |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| LPT greedy byte balancing | Strict prefix-sum equal-byte split (preserve file order); full DP optimum | LPT is O(n log n), deterministic, and near-optimal; a prefix-sum split can be badly skewed by one large file; DP is overkill for a balancing heuristic. |
| Treat 0-size file as 1 byte | Skip 0-size files; treat as average size | Skipping would drop a file from the scan (correctness bug). 1 byte keeps it in the lightest shard with negligible weight. (Interview decision.) |
| `NR_OF_CORES × 2` default | × 1 (exactly core count); × 4 | × 2 gives straggler absorption — a node keeps cores busy when one shard runs long — without the session-startup overhead of higher multipliers. (Interview decision.) |
| Default floored at 8 | Default to `NR_OF_CORES × 2` with no floor | A dev/single-core VM or a failed `NR_OF_CORES` lookup (→0) would otherwise collapse the factor to 0/2; the floor preserves prior behaviour and keeps single-node tests meaningful. |
| Fetch `NR_OF_CORES` in the existing session | A separate connect-back call | One session, two queries: no extra login, and the value is only meaningful when `NPROC()` already succeeded. |
| Two separate properties (`DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`), both default 1 | A single combined "df_parallelism" knob; auto-deriving from cores | DataFusion partition count (logical work splitting) and Tokio worker threads (OS threads) are orthogonal levers; exposing both lets the operator tune each. Default 1+1 = exactly one core per instance, which is the safe Exasol-aligned baseline. (Interview decision.) |
| Tokio runtime kind chosen from spec at `run_scan` | Always `new_current_thread()`; always `new_multi_thread()` | Always current-thread cannot honour `>1`; always multi-thread spawns a pool even for the 1-thread default (wasteful and oversubscribing). Conditional honours the spec exactly. |
| `target_partitions` set, never auto-derived in code | Auto-set `target_partitions = floor(cores/factor)` | Auto-derivation hides a coupling between two layers and removes operator control; the formula is documented as guidance and left to the operator to apply via the property. |
| Scope boundary — no change to the 300 cap, fan-out shape, or scan ABI | Lift/tune the 300 cap; restructure the spec arg | 300 is a fixed Exasol `max_dynamic_group_count` default; out of scope. The ABI stays one JSON VARCHAR with two new optional integer fields. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| parallelism/work-unit-sharding | CHANGED | `specs/_plans/change-shard-parallelism/parallelism/work-unit-sharding/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `specs/_plans/change-shard-parallelism/vs-adapter/create-virtual-schema/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `specs/_plans/change-shard-parallelism/datafusion-scan/scan-execution/spec.md` |

## Implementation Tasks

1. **T1 — Thread file sizes through file resolution.** Change `plan_files_from_table` (`crates/lakehouse-engine/src/adapter/pushdown.rs`) to return `Vec<(String, u64)>` by mapping each `FileScanTask` to `(t.data_file_path, t.file_size_in_bytes)`. Propagate the type change up through `resolve_file_list` (both signed and unsigned paths return `(Vec<(String,u64)>, StorageProps)`) and `handle_pushdown` (the `files` binding, the `files.is_empty()` guard, and the `files.len()` passed to `shard_count`). Downstream of partitioning, shards stay `Vec<Vec<String>>` — only the resolve→partition seam changes.

2. **T2 — Byte-balanced partitioner.** Add `partition_files_by_bytes(files: Vec<(String, u64)>, n: usize) -> Vec<Vec<String>>` in `crates/lakehouse-engine/src/adapter/sharding.rs`: clamp `n` to `[1, files.len()]`; sort files by size descending (treating size 0 as 1); greedily assign each to the shard with the smallest running byte total (a min-by-weight pick); return `Vec<Vec<String>>`. Empty input → empty `Vec`. Replace the `partition_files` call site in `handle_pushdown` with `partition_files_by_bytes`. Keep or remove the old `partition_files` per T5. Add unit tests in `sharding.rs`. [expert]

3. **T3 — Capture `NR_OF_CORES` in the create-VS connect-back.** In `crates/lakehouse-engine/src/adapter/mod.rs`, extend `resolve_cluster_nodes` to also run `SELECT PARAM_VALUE('NR_OF_CORES')` in the same `session`, parse the returned value to `u32` (0 on NULL/parse-failure/missing session — reuse/extend a value-to-int helper analogous to `nproc_value_to_count`, accounting for `PARAM_VALUE` returning a VARCHAR), and return both counts (e.g. change the return type to `(u32, u32)` or add a small struct). Add `const NOTE_NR_OF_CORES: &str = "NR_OF_CORES";`. Thread `nr_of_cores` into `build_adapter_notes` and write the `NR_OF_CORES` note. Update `handle_create_virtual_schema` call sites.

4. **T4 — Cores-aware default factor.** Change `resolve_parallelism_factor` in `mod.rs` to accept `nr_of_cores: u32` and compute the default as `max((nr_of_cores as usize) * 2, 8)` when the `PARALLELISM_FACTOR` property is absent/invalid; an explicit valid property still wins. Update the `handle_create_virtual_schema` call site to pass the resolved `nr_of_cores`. (The pushdown-time read of `PARALLELISM_FACTOR` from adapterNotes in `handle_pushdown_request` already round-trips the stored value and needs no formula change.)

5. **T5 — Update sharding/notes tests and remove dead code.** Update all `partition_files` call sites in the `pushdown.rs` unit tests (lines ~1771, ~2333, and the `tests` mod ~2930–3150) to construct `(path, size)` inputs and call `partition_files_by_bytes` (or build shards directly), asserting byte-balance where appropriate. Update `mod.rs` tests (`adapter_notes_*`, `resolve_cluster_nodes_*`, ~lines 398–540) for the new `resolve_cluster_nodes` return shape, the `NR_OF_CORES` note, and the new default-factor formula (including the floor-at-8 and `NR_OF_CORES × 2` cases). If `partition_files` has no remaining callers after T2, remove it and its tests. Run `cargo test` to green.

6. **T6 — VS adapter: DataFusion thread properties.** In `crates/lakehouse-engine/src/adapter/mod.rs` add `const PROP_DF_TARGET_PARTITIONS: &str = "DATAFUSION_TARGET_PARTITIONS";` and `const PROP_DF_THREADS_PER_UDF: &str = "DATAFUSION_THREADS_PER_UDF";`, plus `const NOTE_DF_TARGET_PARTITIONS: &str = "DF_TARGET_PARTITIONS";` and `const NOTE_DF_THREADS_PER_UDF: &str = "DF_THREADS_PER_UDF";`, and `const DEFAULT_DF_TARGET_PARTITIONS: usize = 1;` / `const DEFAULT_DF_THREADS_PER_UDF: usize = 1;`. Add `resolve_df_target_partitions(props)` and `resolve_df_threads_per_udf(props)` (each: parse the property, accept positive integers, default 1 when absent/empty/zero/invalid) modelled on `resolve_parallelism_factor`. Thread both values into `build_adapter_notes` and write the two notes alongside the existing ones. At pushdown time in `handle_pushdown_request`, read both notes (`adapter_note(... ).and_then(parse::<usize>).filter(>=1).unwrap_or(1)`) and pass them into `handle_pushdown`. Add the two parameters to `handle_pushdown` (`crates/lakehouse-engine/src/adapter/pushdown.rs`) and set both new `ScanSpec` fields in the two `ScanSpec` template construction sites (~lines 1175 and 1209). Add `df_target_partitions: usize` and `df_threads_per_udf: usize` to `ScanSpec` (`crates/lakehouse-engine/src/scan/spec.rs`) with `#[serde(default = "default_df_threads")]`-style defaults of 1 so old specs deserialize.

7. **T7 — Scan UDF: consume thread config from ScanSpec.** In `run_scan` (`crates/lakehouse-engine/src/scan/mod.rs`), restructure so `ScanSpec::from_json` is parsed before the Tokio runtime is built (it already is — confirm ordering), then build the runtime conditionally: `if spec.df_threads_per_udf <= 1 { Builder::new_current_thread() } else { Builder::new_multi_thread().worker_threads(spec.df_threads_per_udf) }`, both `.enable_all().build()`. In `build_session_context`, change `SessionConfig::new().with_information_schema(false)` to also call `.with_target_partitions(spec.df_target_partitions.max(1))`. Add unit tests in `scan/mod.rs` (or a small testable helper) covering the conditional runtime selection (1 → current-thread, >1 → multi-thread worker count) and the session-config target-partitions path. [expert]

8. **T8 — Threading tests and ScanSpec round-trip.** Add `mod.rs` tests for the two new adapterNotes keys and their default-1 resolution (absent/zero/invalid → 1, explicit positive → that value). Add `scan/spec.rs` tests for the ScanSpec round-trip with the new fields and for missing-field deserialization defaulting both to 1 (back-compat with pre-existing serialized specs). Confirm the full host `cargo test` and the E2E suite stay green.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (sharding path) | T1, T2 |
| Group B (adapter notes / factor) | T3, T4 |
| Group C (DataFusion threading) | T6, T7 |
| Group D (cleanup / tests) | T5, T8 |

Sequential dependencies:
- T1 → T2 (the partitioner consumes the `(path, size)` pairs T1 produces).
- T3 → T4 (the default-factor formula needs the `nr_of_cores` value T3 resolves).
- T6 → T7 (the scan UDF reads the `ScanSpec` fields that T6 adds and populates).
- Group A, Group B, and Group C are independent of each other and may proceed concurrently.
- (Group A + Group B) → T5 (T5 updates the sharding/notes tests both groups change).
- (Group C) → T8 (T8 covers the threading notes + ScanSpec round-trip T6/T7 add).
- T5 and T8 are independent of each other and may proceed concurrently once their respective groups land.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/adapter/sharding.rs::partition_files` | Replaced by `partition_files_by_bytes`. Remove only if no callers remain after T2; otherwise keep until they are migrated. |
| Tests | `crates/lakehouse-engine/src/adapter/sharding.rs` count-balance tests for `partition_files` | Remove together with the function if it is deleted; otherwise retain. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| File list is partitioned into G byte-balanced disjoint shards covering every file | Unit | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_balances_cumulative_size` |
| File list is partitioned into G byte-balanced disjoint shards covering every file (disjoint + full coverage) | Unit | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_disjoint_full_coverage` |
| File list is partitioned into G byte-balanced disjoint shards covering every file (0-size = 1 byte, never skipped) | Unit | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_zero_size_treated_as_one_never_skipped` |
| File list is partitioned into G byte-balanced disjoint shards covering every file (G ≥ file_count → one per shard) | Unit | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_one_file_per_shard_when_g_exceeds_count` |
| Adapter records the per-node core count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `adapter_notes_records_nr_of_cores` |
| Adapter records the per-node core count in the virtual-schema adapterNotes (defaults to 0 on failure) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `nr_of_cores_defaults_to_zero_when_unavailable` |
| Adapter records the parallelism factor in the virtual-schema adapterNotes (default = NR_OF_CORES × 2) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `default_parallelism_factor_is_cores_times_two` |
| Adapter records the parallelism factor in the virtual-schema adapterNotes (floor at 8) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `default_parallelism_factor_floors_at_eight` |
| Adapter records the parallelism factor in the virtual-schema adapterNotes (explicit prop overrides) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `explicit_parallelism_factor_overrides_default` |
| Adapter records the DataFusion target partition count in the virtual-schema adapterNotes (default 1) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_defaults_to_one` |
| Adapter records the DataFusion target partition count in the virtual-schema adapterNotes (explicit prop) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_uses_supplied_value` |
| Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes (default 1) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_threads_per_udf_defaults_to_one` |
| Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes (explicit prop) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_threads_per_udf_uses_supplied_value` |
| Scan applies the explicitly-configured DataFusion target partition count | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `session_config_uses_spec_target_partitions` |
| Scan builds a single-threaded Tokio runtime when threads-per-UDF is 1 | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `runtime_is_current_thread_when_threads_is_one` |
| Scan builds a multi-threaded Tokio runtime when threads-per-UDF exceeds 1 | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `runtime_is_multi_thread_when_threads_exceeds_one` |
| ScanSpec round-trips the new threading fields and defaults both to 1 when absent | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `scan_spec_threading_fields_round_trip_and_default_to_one` |
| Byte-balanced sharding + default 1-thread config produce correct row content end-to-end (regression) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | existing scan E2E tests (single-node, G→1) pass unchanged |

Note: the sharding scenario is pure computation over a file list with no I/O, so it is covered by unit tests per the mission's unit-test exception. The adapterNotes and ScanSpec scenarios are exercised by `NoopCtx`-based and serde round-trip unit tests in `mod.rs` / `scan/spec.rs` (connect-back cannot be opened under `NoopCtx`, so the 0/floor/default-1 paths are unit-testable). The Tokio-runtime and session-config scenarios are covered by extracting the runtime-kind decision and the target-partitions setting into small, directly testable helpers. The E2E path is a regression guard only — the defaults (G→1, 1 partition, current-thread) must leave row content unchanged.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| parallelism/work-unit-sharding | `cargo test -p lakehouse-engine sharding` | All `partition_by_bytes_*` tests pass; shards balanced by cumulative bytes, every file present exactly once. |
| vs-adapter/create-virtual-schema | `cargo test -p lakehouse-engine adapter_notes` | adapterNotes JSON string contains `NR_OF_CORES`, `DF_TARGET_PARTITIONS`, `DF_THREADS_PER_UDF`; default factor = `max(NR_OF_CORES×2, 8)`; both DataFusion counts default to 1. |
| datafusion-scan/scan-execution | `cargo test -p lakehouse-engine scan::` | ScanSpec round-trip carries `df_target_partitions`/`df_threads_per_udf`, both default to 1 when absent; runtime-kind helper picks current-thread for 1 and multi-thread for >1; session config sets target partitions from the spec. |
| All (end-to-end) | `make test-e2e` | Existing scan E2E suite green against the local Exasol Docker container; row content unchanged under byte-balanced sharding with the default 1-partition / current-thread config. |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 (`.so` builds in `rust:1.92-bookworm`). |
| Test (host) | `cargo test` | 0 failures. |
| Test (E2E) | `make test-e2e` | 0 failures against local Exasol Docker container. |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings. |
| Format | `cargo fmt` | No changes. |
