# Tasks: change-memory-pool-sizing

## Phase 2: Implementation (Group A+B — core formula + spec)  [agent: expert, files: scan/spec.rs, scan/runtime.rs, scan/mod.rs]
- [x] 1.1 Add `memory_pool_fraction: f64` (serde default 0.6) + `instance_overhead_mb: u64` (serde default 200) to `ScanSpec` with default fns
- [x] 1.2 Extend ScanSpec round-trip test: both fields survive serialize→deserialize; legacy payload → 0.6 / 200
- [x] 2.1 `build_runtime_env(fraction, overhead_bytes)`: net = saturating_sub; budget = max(net·fraction, MIN_POOL_FLOOR_BYTES); zero-limit fallback unchanged [expert]
- [x] 2.2 Add `MIN_POOL_FLOOR_BYTES` (256 MiB) const + doc; update `MEMORY_FRACTION` doc to "default"
- [x] 2.3 `build_session_context` passes `spec.memory_pool_fraction` + `spec.instance_overhead_mb * 1024 * 1024`
- [x] 2.4 runtime.rs tests: positive-limit budget; overhead≥limit→floor; zero-limit→default [expert]
- [x] 2.5 scan/mod.rs seam tests updated to new formula
- [x] 5.2 Add `memory_budget_round_trips_into_scan_spec` seam test (explicit fraction/overhead → pool = fraction×(limit−overhead))

## Phase 2: Implementation (Group A+B — adapter plumbing)  [agent: standard, files: adapter/mod.rs, adapter/pushdown.rs]
- [x] 3.1 Add consts PROP/NOTE/DEFAULT for MEMORY_POOL_FRACTION (0.6) and INSTANCE_OVERHEAD_MB (200)
- [x] 3.2 `resolve_memory_pool_fraction` (0<x≤1.0) + `resolve_instance_overhead_mb` (≥0)
- [x] 3.3 Extend `build_adapter_notes` signature + body; update all call sites (prod + tests)
- [x] 3.4 Call resolvers in `handle_create_virtual_schema`, thread into build_adapter_notes
- [x] 4.1 `handle_pushdown_request` reads both notes (default/validate); passes into handle_pushdown
- [x] 4.2 `handle_pushdown` signature + set both fields on every ScanSpec template
- [x] 4.3 Update all ScanSpec literals in pushdown.rs tests with the two new fields
- [x] 5.1 Adapter unit tests: resolvers default/validate; round-trip through build_adapter_notes

## Phase 4: Review
- [x] 4.0 code-reviewer over changed files (3 findings fixed; 3 skipped with rationale)

## Phase 5: Verification
- [x] 6.1 cargo test (175 passed), cargo clippy --all-targets (0 issues), cargo fmt (clean)
- [x] 6.2 make test-e2e (29 passed: 7 capability + 22 scan, 0 failed)
