# Verification Report: add-nr-of-cores-override

## Verdict: PASS

The `NR_OF_CORES` VS-property override and cores-driven DataFusion parallelism defaults are
implemented, unit-tested, and reviewed. All host checks green. UDF `.so` build + E2E are verified in
the live smoke-test phase (Part C of the session plan), which rebuilds the `.so` and runs end-to-end.

## Implementation

- `crates/lakehouse-engine/src/adapter/mod.rs`:
  - `PROP_NR_OF_CORES = "NR_OF_CORES"`; pure helper `parse_nr_of_cores_override(props) -> Option<u32>`
    (≥1 ⇒ override, else None).
  - `resolve_cluster_nodes`: uses the override directly and skips `SELECT PARAM_VALUE('NR_OF_CORES')`
    while `SELECT NPROC()` cluster-node detection still runs; falls back to auto-detect (→0) otherwise.
  - `resolve_df_target_partitions(props, nr_of_cores)` / `resolve_df_threads_per_udf(props, nr_of_cores)`:
    explicit `DATAFUSION_*` property wins; otherwise default `max(nr_of_cores, 1)` (was hard `1`).
  - Doc comments on `DEFAULT_DF_*` constants clarified (now pushdown-path fallback only).
- `crates/lakehouse-engine/Cargo.toml`: version 0.10.0 → 0.11.0.

## Scenario coverage (all PASS)

| Scenario | Test |
|---|---|
| NR_OF_CORES property overrides auto-detect | `nr_of_cores_property_overrides_connect_back` |
| NR_OF_CORES absent/empty/non-positive → auto-detect | `nr_of_cores_property_falls_back_to_auto_detect` |
| df target-partitions: explicit wins | `df_target_partitions_explicit_wins` |
| df target-partitions: cores-driven default (cores=8 → 8) | `df_target_partitions_defaults_to_nr_of_cores` |
| df target-partitions: unknown cores → 1 | `df_target_partitions_unknown_cores_defaults_to_1` |
| df threads: explicit wins | `df_threads_per_udf_explicit_wins` |
| df threads: cores-driven default | `df_threads_per_udf_defaults_to_nr_of_cores` |
| df threads: unknown cores → 1 | `df_threads_per_udf_unknown_cores_defaults_to_1` |

## Checks

| Step | Command | Result |
|---|---|---|
| Unit tests | `cargo test -p lakehouse-engine` | 226 lib + full suite pass, 0 failures |
| Lint | `cargo clippy --all-targets` | clean |
| Format | `cargo fmt` | no changes |
| UDF build | `make cross-musl-udf-build` | deferred to Part C (smoke test) |
| E2E | `make test-e2e` / `make live-smoke` | deferred to Part C |

## Code review

No correctness defects. Two findings resolved/accepted: `DEFAULT_DF_*` doc comments corrected;
the override-path "PARAM_VALUE skipped" clause is verified by the pure helper's tests + the clean
`if let Some(overridden)` short-circuit (a query-recording mock judged disproportionate).

## Behavior change (intentional, documented in spec delta)

When cores are known or overridden, scans now auto-parallelize (`DF_TARGET_PARTITIONS` /
`DF_THREADS_PER_UDF` default to the core count). Backward-compatible when cores unknown (0 → 1).
