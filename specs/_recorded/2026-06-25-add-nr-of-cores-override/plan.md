# Plan: add-nr-of-cores-override

## Summary

Adds an `NR_OF_CORES` VS property that lets operators override the auto-detected per-node core count, and changes the DataFusion target-partition and threads-per-UDF defaults from a hard-coded `1` to `max(nr_of_cores, 1)` so scans auto-parallelize to the node's core count when it is known.

## Design

### Context

The adapter currently auto-detects `NR_OF_CORES` via `SELECT PARAM_VALUE('NR_OF_CORES')` over a connect-back session at `createVirtualSchema` time.
Two problems exist:

1. The connect-back path may be unavailable in some deployments (no `CONNECTION_NAME`), leaving `NR_OF_CORES: 0` even when the operator knows the core count.
2. Even when `NR_OF_CORES` is correctly detected, `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` still default to `1` (a hard-coded constant), meaning DataFusion runs single-threaded regardless of available cores — undermining the intent of per-instance parallelism.

- **Goals** — allow operators to supply the core count explicitly; make the DataFusion threading defaults hardware-aware when the core count is known; remain fully backward-compatible when the core count is unknown (0 → defaults stay 1, identical to today's behavior).
- **Non-Goals** — changing `PARALLELISM_FACTOR` resolution logic; capping the cores-driven default; changing any scan-side behavior (the scan UDF already reads `target_partitions` and `threads_per_udf` from the scan spec verbatim).

### Decision

#### Architecture

```
createVirtualSchema request
  ├─ NR_OF_CORES VS property present and ≥1?
  │    Yes → use it as nr_of_cores; skip connect-back core detection
  │    No  → run SELECT PARAM_VALUE('NR_OF_CORES') over connect-back (unchanged)
  │          → 0 on failure (unchanged)
  │
  └─ resolve_df_target_partitions(props, nr_of_cores)
     resolve_df_threads_per_udf(props, nr_of_cores)
       explicit property ≥1 → use it
       else                 → max(nr_of_cores, 1)   ← was: hard-coded 1
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Property-overrides-auto-detect | `resolve_cluster_nodes` (or split helper) | Consistent with how `PARALLELISM_FACTOR` overrides a derived default |
| Cores-driven default with floor | `resolve_df_target_partitions`, `resolve_df_threads_per_udf` | `max(nr_of_cores, 1)` ensures minimum of 1 when cores unknown (0) |
| Explicit-wins-over-default | both `resolve_df_*` functions | Existing explicit-override semantics preserved |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Property overrides connect-back, does NOT combine them | average or sum | Simplest mental model; operator sets it once and knows exactly what value is used |
| Absent/empty/non-positive property is ignored (falls back to auto-detect) | treat any value as override | Avoids a misconfigured empty string silently setting cores to 0 |
| Default `max(nr_of_cores, 1)` for both DataFusion threading settings | separate per-setting defaults, cap at some fraction | Symmetric, matches the natural "use all available cores in this UDF instance" intent; operator can still override either independently |
| No additional cap on cores-driven default | cap at `parallelism_factor` floor or similar | The operator or engine's 80% throttle manages oversubscription; this layer just reflects detected hardware |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/create-virtual-schema-adapter-notes | CHANGED | `specs/_plans/add-nr-of-cores-override/vs-adapter/create-virtual-schema-adapter-notes/spec.md` |

## Dependencies

None — purely internal adapter logic. No external crate changes required.

## Migration

The behavior change is intentional and backward-compatible by design:

| Condition | Before | After |
|-----------|--------|-------|
| `NR_OF_CORES` unknown (0) | `DF_TARGET_PARTITIONS: 1`, `DF_THREADS_PER_UDF: 1` | `DF_TARGET_PARTITIONS: 1`, `DF_THREADS_PER_UDF: 1` (identical) |
| `NR_OF_CORES` detected (e.g. 8) | `DF_TARGET_PARTITIONS: 1`, `DF_THREADS_PER_UDF: 1` | `DF_TARGET_PARTITIONS: 8`, `DF_THREADS_PER_UDF: 8` (auto-parallelize) |
| `NR_OF_CORES` overridden via property | not supported | uses supplied value |
| Explicit `DATAFUSION_TARGET_PARTITIONS` set | uses supplied value | uses supplied value (unchanged) |

Operators who want to preserve single-threaded behavior on a cluster with known cores must now explicitly set `DATAFUSION_TARGET_PARTITIONS=1` and `DATAFUSION_THREADS_PER_UDF=1`.

## Implementation Tasks

- [ ] 1.1 Add `PROP_NR_OF_CORES = "NR_OF_CORES"` constant in `crates/lakehouse-engine/src/adapter/mod.rs`
- [ ] 1.2 In `resolve_cluster_nodes` (or a split-out helper), check `PROP_NR_OF_CORES` property first: parse to `u32` ≥ 1, use it as `nr_of_cores` and skip the connect-back `SELECT PARAM_VALUE('NR_OF_CORES')` call; otherwise fall through to auto-detect as today [expert]
- [ ] 1.3 Change `resolve_df_target_partitions` signature to accept `nr_of_cores: u32`; replace `unwrap_or(DEFAULT_DF_TARGET_PARTITIONS)` with `unwrap_or_else(|| (nr_of_cores as usize).max(1))`
- [ ] 1.4 Change `resolve_df_threads_per_udf` signature to accept `nr_of_cores: u32`; replace `unwrap_or(DEFAULT_DF_THREADS_PER_UDF)` with `unwrap_or_else(|| (nr_of_cores as usize).max(1))`
- [ ] 1.5 Update call sites of `resolve_df_target_partitions` and `resolve_df_threads_per_udf` in `handle_create_virtual_schema` (~line 164-165) to pass `nr_of_cores`
- [ ] 2.1 Write unit test: `NR_OF_CORES` property ≥ 1 is used as override (returns that value; connect-back `PARAM_VALUE` query not issued)
- [ ] 2.2 Write unit test: `NR_OF_CORES` property absent/empty/zero/negative falls back to connect-back auto-detect path
- [ ] 2.3 Write unit test: `resolve_df_target_partitions` — explicit `DATAFUSION_TARGET_PARTITIONS` wins over cores-driven default
- [ ] 2.4 Write unit test: `resolve_df_target_partitions` — absent property with `nr_of_cores=8` defaults to `8`
- [ ] 2.5 Write unit test: `resolve_df_target_partitions` — absent property with `nr_of_cores=0` defaults to `1`
- [ ] 2.6 Write unit test: `resolve_df_threads_per_udf` — explicit `DATAFUSION_THREADS_PER_UDF` wins over cores-driven default
- [ ] 2.7 Write unit test: `resolve_df_threads_per_udf` — absent property with `nr_of_cores=8` defaults to `8`
- [ ] 2.8 Write unit test: `resolve_df_threads_per_udf` — absent property with `nr_of_cores=0` defaults to `1`
- [ ] 3.1 Bump crate `lakehouse-engine` version `0.10.0` → `0.11.0` in `crates/lakehouse-engine/Cargo.toml`
- [ ] 3.2 Update `Cargo.lock` (run `cargo check` or `cargo build`) to record the new version

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (implementation) | 1.1, 1.2, 1.3, 1.4 |
| Group B (call-site + tests) | 1.5, 2.1–2.8 (after Group A) |
| Group C (version bump) | 3.1, 3.2 (independent) |

Sequential dependencies:
- Group A → Group B (1.5 and tests need updated function signatures)
- Group C is fully independent of A and B

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Constant | `DEFAULT_DF_TARGET_PARTITIONS` in `mod.rs` | Replaced by `max(nr_of_cores, 1)` default — remove if no longer referenced after task 1.3 |
| Constant | `DEFAULT_DF_THREADS_PER_UDF` in `mod.rs` | Replaced by `max(nr_of_cores, 1)` default — remove if no longer referenced after task 1.4 |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| NR_OF_CORES VS property overrides the connect-back auto-detected core count | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `nr_of_cores_property_overrides_connect_back` |
| NR_OF_CORES VS property is ignored when absent, empty, or not a positive integer | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `nr_of_cores_property_falls_back_to_auto_detect` |
| Adapter records the DataFusion target partition count (cores-driven default) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_defaults_to_nr_of_cores` |
| Adapter records the DataFusion target partition count (explicit wins) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_explicit_wins` |
| Adapter records the DataFusion target partition count (unknown cores → 1) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_unknown_cores_defaults_to_1` |
| Adapter records the DataFusion threads-per-UDF count (cores-driven default) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_threads_per_udf_defaults_to_nr_of_cores` |
| Adapter records the DataFusion threads-per-UDF count (explicit wins) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_threads_per_udf_explicit_wins` |
| Adapter records the DataFusion threads-per-UDF count (unknown cores → 1) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_threads_per_udf_unknown_cores_defaults_to_1` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| NR_OF_CORES override | Create VS with `NR_OF_CORES=4` property; query `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` | `adapterNotes` JSON contains `"NR_OF_CORES":4`, `"DF_TARGET_PARTITIONS":4`, `"DF_THREADS_PER_UDF":4` |
| Auto-detect path unchanged | Create VS without `NR_OF_CORES` property (connect-back available) | `adapterNotes` JSON contains auto-detected core count in `NR_OF_CORES`, same value in `DF_TARGET_PARTITIONS` and `DF_THREADS_PER_UDF` |
| Backward compat (cores unknown) | Create VS without `NR_OF_CORES` and without `CONNECTION_NAME` | `adapterNotes` JSON contains `"NR_OF_CORES":0`, `"DF_TARGET_PARTITIONS":1`, `"DF_THREADS_PER_UDF":1` |
| Explicit override still wins | Create VS with `NR_OF_CORES=8`, `DATAFUSION_TARGET_PARTITIONS=2` | `adapterNotes` JSON contains `"DF_TARGET_PARTITIONS":2` (explicit wins over cores-driven default of 8) |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF) | `make cross-musl-udf-build` | Exit 0 |
| Test (unit) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
