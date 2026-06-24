# Verification Report: change-memory-pool-sizing

**Generated:** 2026-06-24

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Net-budget memory-pool sizing (`fraction × (limit − overhead)`, floored at 256 MiB) shipped; `MEMORY_POOL_FRACTION` / `INSTANCE_OVERHEAD_MB` round-trip VS-prop → adapterNotes → ScanSpec → pool. All host + E2E suites green. |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build`, .so rebuilt at 0.8.0) |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (covered by E2E) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (host lib) | 175 | 175 | 0 |
| E2E capability | 7 | 7 | 0 |
| E2E scan | 22 | 22 | 0 |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: No issues found
```

### Formatter

```
cargo fmt --check: clean (exit 0)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-execution-memory-and-credentials | Sizes pool from reported per-instance limit (net budget) | `src/scan/runtime.rs` | `build_runtime_env_sizes_pool_from_net_budget` | Pass |
| datafusion-scan | scan-execution-memory-and-credentials | Sizes pool from limit (seam) | `src/scan/mod.rs` | `session_context_sizes_pool_from_ctx_limit` | Pass |
| datafusion-scan | scan-execution-memory-and-credentials | Falls back to default budget when no limit reported | `src/scan/runtime.rs` | `build_runtime_env_uses_default_budget_on_zero_limit` | Pass |
| datafusion-scan | scan-execution-memory-and-credentials | Clamps to floor when overhead exceeds limit | `src/scan/runtime.rs` | `build_runtime_env_clamps_to_floor_when_overhead_exceeds_limit` | Pass |
| vs-adapter | create-virtual-schema | Records memory-pool fraction in adapterNotes | `src/adapter/mod.rs` | `resolve_memory_pool_fraction_defaults_and_validates` / `memory_budget_params_round_trip_through_adapter_notes` | Pass |
| vs-adapter | create-virtual-schema | Records instance-overhead MB in adapterNotes | `src/adapter/mod.rs` | `resolve_instance_overhead_mb_defaults_and_validates` / `memory_budget_params_round_trip_through_adapter_notes` | Pass |
| datafusion-scan | scan-execution-memory-and-credentials | Recorded memory budget round-trips into scan spec | `src/scan/mod.rs` | `memory_budget_round_trips_into_scan_spec` | Pass |

## Notes

- Scenario `memory_budget_round_trips_into_scan_spec` was placed as a host seam test in `src/scan/mod.rs` (where `build_session_context` is reachable) rather than `tests/e2e_scan_test.rs` — the plan explicitly permitted "extend e2e_scan_test.rs OR the existing memory seam". The host seam proves values flow from the spec (explicit 0.5 / 256 MiB) rather than hardcoded constants.
- Code review: 3 findings fixed (missing `// ponytail:` rationale on `build_adapter_notes`; cross-module coupling in `default_memory_pool_fraction` → removed now-vestigial `MEMORY_FRACTION` const and inlined `0.6`; magic `1` → `DEFAULT_DF_*` consts in the pushdown adapterNotes read). 3 pre-existing/minor findings skipped with rationale (redundant runtime test assertion already covered at seam; pre-existing what-comments in `probe_tmp_spill`; per-query `parse_adapter_notes` re-parse — negligible, not per-row).
- Version bumped `lakehouse-engine` 0.7.0 → 0.8.0 (additive, backward-compatible feature; per-feature bump convention).
