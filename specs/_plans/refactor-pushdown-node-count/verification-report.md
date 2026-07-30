# Verification Report: refactor-pushdown-node-count

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All automated checks green (unit, lint, format, build, E2E). Two plan-mandated manual gates (four-node staging check, legacy-`.so` REFRESH check) could NOT be run in this sandbox — no four-node cluster and no pre-refactor `.so` artifact are available here. Plan.md states both **must run before the PR leaves draft** — see Notes. |
| Code review | 5 findings — 5 fixed (4 standard, 1 expert) |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ⚠ (3/5 via automated-equivalent; 2/5 not run — see Notes) |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | All 16 plan-listed scenarios have a passing unit and/or integration test (see Scenario Coverage below) |
| Integration | All plan-listed integration scenarios covered by the E2E suite |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`, 40 suites) | 903 | 903 | 0 |
| Integration/E2E (`make test-e2e`, 8 binaries) | 191 | 191 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `ADAPTER_NOTES` carries no `CLUSTER_NODES`, still carries `NR_OF_CORES`/`PARALLELISM_FACTOR`/`TABLE_MAP` | ✓ (via automated E2E equivalent: `create_vs_omits_cluster_nodes_from_adapter_notes`) |
| Legacy-`.so` REFRESH drops an inherited `CLUSTER_NODES` key on a pre-refactor schema | ✗ NOT RUN — requires a pre-refactor `.so` build, unavailable in this sandbox |
| `EXPLAIN VIRTUAL` generated SQL still carries the `LAKEHOUSE_DISTRIBUTE_FILES`/`shards(shard_key, files)` fan-out shape | ✓ (via automated E2E equivalent: `pushdown_shards_from_handshake_node_count_without_note`) |
| Four-node staging: `NPROC()` = 4 and shard row count = `min(4×PARALLELISM_FACTOR, 300, file_count)` | ✗ NOT RUN — requires a four-node Exasol cluster, unavailable in this sandbox (only a single-node Docker Exasol is available here) |
| `COUNT(*)` unchanged by the fan-out change | ✓ (via automated E2E equivalent: full E2E suite's correctness assertions, incl. `multi_shard_row_query_matches_single_shard`) |

## Tool Evidence

### Linter

```
cargo clippy --all-targets --workspace
Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.82s
0 warnings, 0 errors
```

### Formatter

```
cargo fmt --check
(no output — no diff)
```

### Build

```
make cross-musl-udf-build
→ target/release/liblakehouse_engine.so produced (165.9 MB), exit 0
```

### E2E

```
make test-e2e (8 test binaries, --test-threads=1)
8/8 suites: test result: ok. 0 failed
Totals: 60 + 16 + 7 + 15 + 6 + 16 + 11 + 60 = 191 passed, 0 failed
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning | Pushdown reads the cluster node count from the UDF handshake | `adapter/mod.rs` | `cluster_nodes_from_context_passes_through_reported_node_count` | Pass |
| vs-adapter | pushdown-planning | Pushdown reads the cluster node count from the UDF handshake | `tests/e2e_scan_test.rs` | `pushdown_shards_from_handshake_node_count_without_note` | Pass |
| vs-adapter | pushdown-planning | Pushdown node count falls back to one when the handshake reports none | `adapter/mod.rs` | `cluster_nodes_from_context_defaults_to_one_when_node_count_zero` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes | createVirtualSchema adapterNotes omit the cluster node count | `adapter/mod.rs` | `adapter_notes_omit_cluster_nodes`, `refresh_notes_drop_inherited_cluster_nodes`, `build_adapter_notes_merges_existing` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes | createVirtualSchema adapterNotes omit the cluster node count | `tests/e2e_scan_test.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the per-node core count | `adapter/mod.rs` | `adapter_notes_records_nr_of_cores` | Pass |
| parallelism | work-unit-sharding | Recorded parallelism factor drives later work-unit sharding | `adapter/mod.rs` | `adapter_notes_carry_parallelism_factor` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the parallelism factor | `adapter/mod.rs` | `create_vs_records_parallelism_factor` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the DataFusion target partition count | `adapter/mod.rs` | `df_target_partitions_uses_supplied_value` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the DataFusion threads-per-UDF count | `adapter/mod.rs` | `df_threads_per_udf_uses_supplied_value` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the memory-pool fraction | `adapter/mod.rs` | `memory_budget_params_round_trip_through_adapter_notes` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the instance-overhead megabytes | `adapter/mod.rs` | `memory_budget_params_round_trip_through_adapter_notes` | Pass |
| parallelism | work-unit-sharding | Shard count oversubscribes the cluster and is capped at the round-robin threshold | `adapter/pushdown/support.rs` | `shard_count_oversubscribes_and_caps_at_300`, `shard_count_clamped_to_file_count_no_empty_shards` | Pass |
| parallelism | work-unit-sharding | Shard count oversubscribes the cluster and is capped at the round-robin threshold | `tests/e2e_scan_test.rs` | `multi_shard_row_query_matches_single_shard` | Pass |
| vs-adapter | create-virtual-schema | Create virtual schema records the Exasol-name to Iceberg-identifier map | `adapter/mod.rs` | `create_vs_records_table_map_in_adapter_notes`, `table_map_merges_with_existing_notes` | Pass |
| vs-adapter | refresh-and-set-properties | Refresh rebuilds the table map and preserves other adapter notes | `adapter/mod.rs` | `refresh_rebuilds_table_map_preserves_notes`, `refresh_notes_drop_inherited_cluster_nodes` | Pass |

## Notes

- **Two plan-mandated manual gates are unautomatable in this sandbox** — plan.md § Manual Testing states both must run "before the PR leaves draft":
  1. **Four-node staging gate** — distinguishes a correctly-read live handshake node count from the `0 => 1` floor (a single-node container reports `1` under either path, so it cannot prove the refactor works on a real multi-node cluster). Needs a four-node Exasol staging cluster.
  2. **Legacy-key REFRESH check** — proves a pre-refactor persisted `CLUSTER_NODES` note is actually dropped on `REFRESH`. Needs a pre-refactor `.so` build; this repo/sandbox only has the post-refactor artifact.

  A human with access to the right infrastructure must run both before this ships. Recommend flagging this explicitly on the PR rather than treating "PASS" here as full sign-off.

- **One review finding intentionally left without an automated test** (documented in plan.md § Verification, added during the code-review fix pass): the "persisted note never wins over the live handshake value" shape is guarded by two `cargo clippy -D warnings` lint failures (an unused parameter / unused function) that were verified by injecting the regression — not by a test. A third regression shape (note silently falls back when the parameter is somehow absent) is caught only by the two manual gates above, not by any automated check.

- Task 4.1 (expert review fix) deleted the originally-planned test `pushdown_ignores_persisted_cluster_nodes_note` after determining it was assertion-free (a tautology derived from the same two literals on both sides) — see decision documented in plan.md § Verification and specs/_plans/refactor-pushdown-node-count/review-findings.md.

- GitHub issue [#287](https://github.com/exasol-labs/lakehouse-engine-rs/issues/287) tracks deleting the `NOTE_CLUSTER_NODES` tombstone constant once every deployed virtual schema has refreshed on this version or later.
