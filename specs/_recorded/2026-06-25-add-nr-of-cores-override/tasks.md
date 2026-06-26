# Tasks: add-nr-of-cores-override

## Phase 2: Implementation (Group A — adapter/mod.rs)
- [x] 1.1 Add `PROP_NR_OF_CORES = "NR_OF_CORES"` constant
- [x] 1.2 `resolve_cluster_nodes`: property override of connect-back core count [expert]
- [x] 1.3 `resolve_df_target_partitions(props, nr_of_cores)` → default `max(nr_of_cores,1)`
- [x] 1.4 `resolve_df_threads_per_udf(props, nr_of_cores)` → default `max(nr_of_cores,1)`

## Phase 2: Implementation (Group B — call-site + tests)
- [x] 1.5 Update call sites in `handle_create_virtual_schema` to pass `nr_of_cores`
- [x] 2.1 Test: NR_OF_CORES property ≥1 overrides (no PARAM_VALUE query)
- [x] 2.2 Test: NR_OF_CORES absent/empty/zero/negative → auto-detect fallback
- [x] 2.3 Test: df_target_partitions explicit wins
- [x] 2.4 Test: df_target_partitions absent, cores=8 → 8
- [x] 2.5 Test: df_target_partitions absent, cores=0 → 1
- [x] 2.6 Test: df_threads_per_udf explicit wins
- [x] 2.7 Test: df_threads_per_udf absent, cores=8 → 8
- [x] 2.8 Test: df_threads_per_udf absent, cores=0 → 1

## Phase 2: Implementation (Group C — version)
- [x] 3.1 Bump crate version 0.10.0 → 0.11.0
- [x] 3.2 Update Cargo.lock (cargo check)

## Phase 3: Verification
- [x] V.1 cargo test (host) — 0 failures
- [x] V.2 cargo clippy --all-targets — 0 warnings
- [x] V.3 cargo fmt — no changes
