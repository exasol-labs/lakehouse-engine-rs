# Verification Report: refactor-nr-of-cores-detection

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `NR_OF_CORES` property, adapterNotes note, and `0`-unknown sentinel removed; core count auto-detected via `std::thread::available_parallelism()`. The plan's own E2E verification gate caught a real defect during implementation (CPU-quota constraints are invisible to the UDF sandbox); the plan was revised to verify via CPU affinity (`cpuset`) instead, with the residual CFS-quota blind spot tracked as issue #421, not silently accepted. Adapter detection code shipped unchanged from the original design. |
| Code review | 10 findings — 10 fixed (8 standard, 2 expert) |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-udf-build` via `make test-e2e`'s dependency; `.so` current) |
| Tests | ✓ (1667 passed, 0 failed) |
| Lint | ✓ (`cargo clippy --all-targets --all-features`, 0 diagnostics) |
| Format | ✓ (`cargo fmt --all --check`, clean) |
| Scenario Coverage | ✓ (every scenario mapped to an existing, passing test; one stale name fixed) |
| Manual Tests | ~ (rows 1-6 functionally covered by automated E2E/unit suites; row 7 not run, optional; row 8 multi-node gate not run — human-only, no multi-node cluster available) |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed | Ignored |
|------|-----|--------|--------|---------|
| Unit + Integration (`cargo test`) | 1667 | 1667 | 0 | 2 |
| E2E (`make test-e2e`, 15 binaries, `LH_EXASOL_CPUSET=0-1`) | 342 | 342 | 0 | 0 |

### Key scenario tests (spot-named)

- `core_count_from_available_parallelism_is_not_recorded`, `core_count_defaults_to_one_when_detection_fails`, `core_count_uses_the_detected_count_when_detection_succeeds` — resolver correctness, including the `Ok` arm a mutation check proved the original coverage missed
- `nr_of_cores_property_is_ignored` — a still-supplied `NR_OF_CORES` property changes no resolved budget
- `resolve_s3_max_connections_auto_one_core_yields_one_threads_share` — deliberate 16→4 budget change at a floor core count, pinned by exact value (not an inequality) after code review
- `create_vs_omits_cluster_nodes_from_adapter_notes` — adapterNotes carry neither `CLUSTER_NODES` nor `NR_OF_CORES`
- `adapter_detects_container_cpuset` (renamed from `adapter_detects_container_cpu_quota`) — **the test that caught the real defect.** First implementation asserted CFS-quota (`cpu.max`) detection and failed live (`left: 4, right: 2`) because the Exasol UDF sandbox mounts no cgroupfs. Investigated live (not assumed): `available_parallelism()` correctly reads cgroup v2 quota when visible; CPU affinity (`cpuset`) does propagate into the sandbox. Revised to assert against `cpuset.cpus.effective`; passed 3x live against the running container (range form `0-1`, comma form `0,2`, restored `0-1`)

## Tool Evidence

### Linter

```
cargo clippy --all-targets --all-features
    Finished `dev` profile [unoptimized + debuginfo] target(s)
0 warnings, 0 errors
```

### Formatter

```
cargo fmt --all --check
(no output — clean)
```

### Residual reference sweeps

```
git grep -n "NR_OF_CORES" -- . ':!specs/_plans' ':!specs/_decision'
```
Only: the 4 accepted Exasol-parameter terminology mentions (`CLAUDE.md`, `docs/architecture.md`, `specs/mission.md`, `specs/udf-context.md`), intentional test assertions in `adapter_tests.rs`/`e2e_scan_test.rs`, and not-yet-recorded permanent `specs/*.md` (recorded by `/speq:record`). Zero production-code hits.

```
git grep -n "LH_EXASOL_CPUS\b" -- . ':!specs/_plans' ':!specs/_decision'
```
Zero hits — fully renamed to `LH_EXASOL_CPUSET` across `docker-compose.yml`, `.github/workflows/ci.yml`, `bench/`, `docs/`.

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | create-virtual-schema-adapter-notes | createVirtualSchema adapterNotes omit the cluster node count | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes | Adapter derives the per-node core count from available_parallelism() on every request | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `core_count_from_available_parallelism_is_not_recorded`, `nr_of_cores_property_is_ignored` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes | Adapter uses a core count of 1 when available_parallelism() cannot report one | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `core_count_defaults_to_one_when_detection_fails` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes | The adapter VM detects the Docker container's CPU affinity set | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `adapter_detects_container_cpuset` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the parallelism factor | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `default_parallelism_factor_floors_at_eight` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the DataFusion target partition count | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `df_target_partitions_one_core_defaults_to_1` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the DataFusion threads-per-UDF count | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `df_threads_per_udf_one_core_defaults_to_1` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the memory-pool fraction | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `memory_budget_params_round_trip_through_adapter_notes` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the instance-overhead megabytes | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolve_instance_overhead_mb_defaults_and_validates` | Pass |
| vs-adapter | create-virtual-schema | Records the Exasol-name to Iceberg-identifier map in adapterNotes | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `table_map_merges_with_existing_notes` | Pass |
| vs-adapter | refresh-and-set-properties | Refresh rebuilds the table map and preserves other adapter notes | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `refresh_rebuilds_table_map_preserves_notes` | Pass |
| datafusion-scan | scan-execution-threading | AUTO mode derives a per-instance thread budget that does not oversubscribe a node | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `auto_mode_derives_non_oversubscribing_threads` | Pass |
| datafusion-scan | scan-execution-threading | AUTO mode yields a single thread on a one-core node | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `auto_mode_yields_one_thread_on_one_core` | Pass |
| datafusion-scan | scan-execution-threading | FIXED mode uses the operator-supplied thread and partition values verbatim | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `fixed_mode_uses_supplied_values` | Pass |
| datafusion-scan | scan-execution-connection-concurrency | AUTO derivation sizes the per-instance budget from node capacity | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolve_s3_max_connections_auto_scales_with_cores` | Pass |
| datafusion-scan | scan-execution-connection-concurrency | AUTO derivation yields the single-core budget when the core count cannot be detected | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolve_s3_max_connections_auto_one_core_yields_one_threads_share` | Pass |
| e2e-harness | cloud-e2e-harness | Remote bench wires PARALLELISM_FACTOR into the virtual schema | `bench/run.sh` offline self-check | `bash bench/run.sh selftest` | Pass |
| e2e-harness | cloud-e2e-harness | Remote bench selects its catalog backend from the bench environment | `bench/run.sh` offline self-check | `bash bench/run.sh selftest` | Pass |

## Notes

**The plan's own verification gate worked as designed.** Task 3.2's Docker E2E test was written specifically because CLAUDE.md's Verification discipline requires a live check over documentation or memory for exactly this kind of host-detection claim. It caught that `std::thread::available_parallelism()`, while itself correct, cannot see a CPU constraint expressed as a CFS bandwidth quota (Docker `cpus:`, an un-pinned Kubernetes `limits.cpu`) from inside the Exasol UDF sandbox, because the sandbox mounts no cgroupfs. This was verified live with throwaway probe UDFs and `docker update --cpuset-cpus`, not assumed. The plan was revised (not the adapter code, which ships exactly as originally designed) to verify via CPU affinity (`cpuset`), which does propagate into the sandbox, and the residual CFS-quota blind spot is tracked as **issue #421** — a named, scoped limitation, not a silent gap.

**Manual Testing rows 1, 2, and 3** (adapterNotes shape, cpuset affinity detection, `NR_OF_CORES=99` ignored) are functionally exercised by the automated E2E and unit suites above but were not independently re-run through the `exapump` CLI specifically (not installed in this environment). **Row 7** (`BENCH_TARGET=docker make bench`) was not run — an optional operational sanity check, not a scenario-coverage gate. **Row 8**, the multi-node pre-merge gate, is explicitly human-only per the plan text (CI runs no multi-node cluster) and is recorded here as **not run**, per the plan's own instruction, until a human executes it against a real multi-node Exasol cluster before this PR leaves draft.

**Decision-log entry `[9]`** (promoted to ADR) records the full investigation: the falsified hypothesis (adapter needs cgroup-aware code), the confirmed mechanism (cpuset visibility vs. CFS-quota invisibility inside the UDF sandbox), and the rejected alternatives (UDF metadata — contradicts this plan's stated Non-Goal; cgroup v1 support — no evidence of production need). This is a general fact about what's observable inside the UDF sandbox that will bind future host-detection work in this repo, not just this plan.
