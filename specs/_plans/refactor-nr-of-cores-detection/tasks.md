# Tasks: refactor-nr-of-cores-detection

## PR Lifecycle
- [x] resolved
- [ ] implemented
- [ ] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Adapter core-count resolution and its observations)
- [x] 1.1 Check for an existing GitHub issue covering this refactor; create one with `gh issue create` if none exists, and reference it in the implementing commit as `Closes #<n>`.
- [x] 1.2 In `adapter/mod.rs`, delete `PROP_NR_OF_CORES`, `NOTE_NR_OF_CORES`, `parse_nr_of_cores_override`, `available_parallelism_or_0`. Replace `resolve_nr_of_cores(props: &Json) -> u32` with a parameterless `resolve_nr_of_cores() -> u32` calling `std::thread::available_parallelism()` and delegating to a new pure `core_count_or_default(io::Result<NonZeroUsize>) -> u32` (Err -> 1). Update the call site in `handle_create_virtual_schema`.
- [x] 1.3 Delete the `NOTE_NR_OF_CORES` insert and the `nr_of_cores: u32` parameter from `build_adapter_notes`. Update its doc comment.
- [x] 1.4 Update the `nr_of_cores == 0` framing in doc comments of `resolve_parallelism_factor`, `resolve_df_threading`, `auto_threads_per_udf`, `resolve_df_fixed_count`, `PROP_PARALLELISM_FACTOR`, `DEFAULT_PARALLELISM_FACTOR` to describe the floor as the ordinary formula, with no reference to `NR_OF_CORES` or `0`.
- [x] 1.5 [expert] Delete the `if nr_of_cores == 0 { return DEFAULT_S3_MAX_CONNECTIONS; }` early return in `resolve_s3_max_connections` and rewrite its doc-comment bullet. Keep the `nr_of_cores: u32` parameter, the AUTO formula, and the `DEFAULT_S3_MAX_CONNECTIONS` import (still used by `handle_pushdown_request`).
- [x] 1.6 In `scan/spec.rs`, delete only the AUTO-derivation clause from the `DEFAULT_S3_MAX_CONNECTIONS` doc comment, keeping and naming both surviving roles (serde default, pushdown-side fallback at `adapter/mod.rs:404`).
- [x] 1.7 Reword `scan/diagnostics.rs:48` (`do NOT rely on NR_OF_CORES=1`) and `scan/mod.rs:109` (`the NR_OF_CORES-bound VM pool`) to name the budget/pool without the property name.
- [x] 2.1 In `adapter/adapter_tests.rs`, drop the second positional argument from all 17 `build_adapter_notes(...)` call sites.
- [x] 2.2 Delete `adapter_notes_records_nr_of_cores`, `nr_of_cores_property_overrides_auto_detect`, `nr_of_cores_property_falls_back_to_auto_detect`.
- [x] 2.3 Rewrite `nr_of_cores_from_available_parallelism_when_unavailable` as `core_count_from_available_parallelism_is_not_recorded`, asserting a positive host-sourced count and no `NR_OF_CORES` key from `build_adapter_notes`. Add `NR_OF_CORES` absence to `adapter_notes_omit_cluster_nodes`.
- [x] 2.4 Add `core_count_defaults_to_one_when_detection_fails` (calls `core_count_or_default` with `Err`, asserts `1`).
- [x] 2.5 Add `nr_of_cores_property_is_ignored`: two property objects differing only by a literal `"NR_OF_CORES": "999"` key, assert every resolver returns identical values for both.
- [x] 2.6 Change the `0` core-count argument to `1` in four tests and rename three: `df_target_partitions_unknown_cores_defaults_to_1` -> `df_target_partitions_one_core_defaults_to_1`; `df_threads_per_udf_unknown_cores_defaults_to_1` -> `df_threads_per_udf_one_core_defaults_to_1`; `auto_mode_falls_back_to_one_when_cores_zero` -> `auto_mode_yields_one_thread_on_one_core`; `resolve_s3_max_connections_fixed_value_wins` keeps its name, changes only its `0` argument.
- [x] 2.7 [expert] Rewrite `resolve_s3_max_connections_auto_zero_cores_defaults` as `resolve_s3_max_connections_auto_one_core_yields_four`, asserting `resolve_s3_max_connections(&absent, 1, 1) == 4` and `resolve_s3_max_connections(&absent, 1, 8) == 4`.
- [x] 2.8 Seed `"NR_OF_CORES": "8"` into `refresh_rebuilds_table_map_preserves_notes`'s adapterNotes, assert the key survives the rebuild carrying its original value.
- [x] 3.1 In `tests/e2e_scan_test.rs`, flip the `NR_OF_CORES` assertion in `create_vs_omits_cluster_nodes_from_adapter_notes` from present to absent, mirroring the `CLUSTER_NODES` assertion. Update the doc comment.
- [x] 3.2 [expert] Add `adapter_detects_container_cpu_quota` to the same file: create a probe VS with `parallelism_factor(1)`, read the container's `/sys/fs/cgroup/cpu.max` via `docker exec`, assert `DF_THREADS_PER_UDF` equals the container-read quota, and first assert the quota is strictly less than the test host's own `available_parallelism()`.
- [x] 3.3 If task 3.2 fails because the adapter VM reports the host core count rather than the container quota, STOP and escalate. Do not weaken the assertion.
- [x] 3.4 Make the E2E CI job satisfy task 3.2's precondition: print the runner's `nproc` where `make test-e2e` runs, lower `LH_EXASOL_CPUS` for that job if it does not stay below `nproc`, and correct the vCPU comment.

## Phase 2: Implementation (Group B: Bench harness and operator documentation)
- [ ] 4.1 In `bench/run.sh`, drop the `nr_of_cores` parameter and the `NR_OF_CORES` line from `build_vs_extra_props`. Update both call sites and the comment.
- [ ] 4.2 Update the five `build_vs_extra_props` calls in the `bench/run.sh selftest` block and the two `case` patterns that assert the property string. Run `bash bench/run.sh selftest` and confirm exit 0.
- [ ] 4.3 Remove the `NR_OF_CORES` property and `BENCH_NR_OF_CORES` variable from `bench/batch_size_sweep.sh`, `bench/emit_s3conn_sweep.sh`, `bench/batch_size_aggcheck.sh`.
- [ ] 4.4 Remove `BENCH_NR_OF_CORES` from `bench/.env.example` and `deploy/scripts/secrets.sh`.
- [ ] 5.1 Delete the `NR_OF_CORES` row from `docs/tuning.md`'s property table. Restate the `PARALLELISM_FACTOR`, `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_TARGET_PARTITIONS` defaults against the detected core count. Rewrite the quick recommendation and the unknown-core bullet.
- [ ] 5.2 Rewrite the Parallelism section of `bench/README.md`: core count is auto-detected, `LH_EXASOL_CPUS` controls it in docker mode, remote mode has no equivalent lever.
- [ ] 5.3 Remove `BENCH_NR_OF_CORES` from the variable list in `docs/benchmark.md`.

## Phase 5: Verification
- [ ] V.1 Build: `make cross-udf-build` -> exit 0
- [ ] V.2 Test: `cargo test` -> 0 failures
- [ ] V.3 Test (E2E): `make test-e2e` -> 0 failures
- [ ] V.4 Bench self-check: `bash bench/run.sh selftest` -> exit 0
- [ ] V.5 Lint: `cargo clippy --all-targets` -> 0 errors, 0 warnings
- [ ] V.6 Format: `cargo fmt` -> no changes
- [ ] V.7 Residual reference sweep: `git grep -n "NR_OF_CORES" -- . ':!specs/_plans' ':!specs/_decision'` -> only the four Exasol-parameter mentions
- [ ] V.8 Manual multi-node gate (human-executed pre-merge, recorded as not-run in verification-report.md until performed)
