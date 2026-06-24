# Verification Report: change-shard-parallelism

**Generated:** 2026-06-24

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Byte-balanced sharding, cores-aware default parallelism factor, and per-instance DataFusion CPU bound all implemented; full host + E2E suites green. |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Host (unit + crate) | 223 | 223 | 0 |
| E2E — capability | 7 | 7 | 0 |
| E2E — scan | 22 | 22 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine sharding` — all `partition_by_bytes_*` pass, balanced by cumulative bytes | ✓ |
| `cargo test -p lakehouse-engine adapter_notes` — notes carry NR_OF_CORES, DF_TARGET_PARTITIONS, DF_THREADS_PER_UDF; default factor max(cores×2,8) | ✓ |
| `cargo test -p lakehouse-engine scan::` — ScanSpec round-trips threading fields, runtime-kind + target-partitions helpers | ✓ |
| `make test-e2e` — row content unchanged under byte-balanced sharding with default 1-partition / current-thread config | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: No issues found
```

### Formatter

```
cargo fmt --check: clean (no changes)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| parallelism | work-unit-sharding | Byte-balanced shards (cumulative size) | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_balances_cumulative_size` | Pass |
| parallelism | work-unit-sharding | Disjoint + full coverage | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_disjoint_full_coverage` | Pass |
| parallelism | work-unit-sharding | 0-size = 1 byte, never skipped | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_zero_size_treated_as_one_never_skipped` | Pass |
| parallelism | work-unit-sharding | G ≥ file_count → one per shard | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_one_file_per_shard_when_g_exceeds_count` | Pass |
| vs-adapter | create-virtual-schema | Records NR_OF_CORES note | `crates/lakehouse-engine/src/adapter/mod.rs` | `adapter_notes_records_nr_of_cores` | Pass |
| vs-adapter | create-virtual-schema | NR_OF_CORES defaults to 0 on failure | `crates/lakehouse-engine/src/adapter/mod.rs` | `nr_of_cores_defaults_to_zero_when_unavailable` | Pass |
| vs-adapter | create-virtual-schema | Default factor = cores × 2 | `crates/lakehouse-engine/src/adapter/mod.rs` | `default_parallelism_factor_is_cores_times_two` | Pass |
| vs-adapter | create-virtual-schema | Default factor floors at 8 | `crates/lakehouse-engine/src/adapter/mod.rs` | `default_parallelism_factor_floors_at_eight` | Pass |
| vs-adapter | create-virtual-schema | Explicit prop overrides default | `crates/lakehouse-engine/src/adapter/mod.rs` | `explicit_parallelism_factor_overrides_default` | Pass |
| vs-adapter | create-virtual-schema | DF target partitions default 1 | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_defaults_to_one` | Pass |
| vs-adapter | create-virtual-schema | DF target partitions explicit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_uses_supplied_value` | Pass |
| vs-adapter | create-virtual-schema | DF threads-per-UDF default 1 | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_threads_per_udf_defaults_to_one` | Pass |
| vs-adapter | create-virtual-schema | DF threads-per-UDF explicit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_threads_per_udf_uses_supplied_value` | Pass |
| datafusion-scan | scan-execution | Session config uses spec target partitions | `crates/lakehouse-engine/src/scan/mod.rs` | `session_config_uses_spec_target_partitions` | Pass |
| datafusion-scan | scan-execution | Current-thread runtime when threads = 1 | `crates/lakehouse-engine/src/scan/mod.rs` | `runtime_is_current_thread_when_threads_is_one` | Pass |
| datafusion-scan | scan-execution | Multi-thread runtime when threads > 1 | `crates/lakehouse-engine/src/scan/mod.rs` | `runtime_is_multi_thread_when_threads_exceeds_one` | Pass |
| datafusion-scan | scan-execution | ScanSpec threading fields round-trip + default 1 | `crates/lakehouse-engine/src/scan/spec.rs` | `scan_spec_threading_fields_round_trip_and_default_to_one` | Pass |
| All | end-to-end | Byte-balanced sharding + default config → correct rows (regression) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | scan E2E suite (22 tests) | Pass |

## Notes

- Code review found one correctness must-fix: the byte-size field was initially read as `FileScanTask.length` (scan-range length); corrected to `FileScanTask.file_size_in_bytes` (true file size) per the plan. Three nice-to-fixes also applied: tightened helper visibility (`pub` → private) in `scan/mod.rs`, tightened the balance assertion to exact equality, and added a `rt-multi-thread` tokio feature to the lib dependency (required at release build for the multi-thread runtime path; host tests passed via dev-dep feature unification but the release `.so` build initially failed without it).
- `vs-expression` crate unchanged (stays at 0.2.0); `lakehouse-engine` bumped 0.6.0 → 0.7.0.
- The `target_partitions` scale-up formula `max(1, floor(NR_OF_CORES / parallelism_factor))` is documented guidance only; defaults remain 1, never auto-derived.
