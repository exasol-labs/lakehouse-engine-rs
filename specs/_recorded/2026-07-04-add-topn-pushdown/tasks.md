# Tasks: add-topn-pushdown

## Group A — Gating live investigation
- [x] A1 Live-verify today's ORDER BY + LIMIT behavior against test1; confirm pure-optimization vs latent per-shard-limit bug. Record in decision-log.
- [x] A2 Capture exact `order_by` request shape (field names, ascending flag, nulls placement) against test1. Record in decision-log.
- [x] A3 Confirm DataFusion folds ORDER BY+LIMIT into bounded TopK on the raw-scan path; note build_scan_sql insertion point.

## Group B — Ordered top-N pushdown (code)
- [x] B1 Add `order_by` sort-key list to ScanSpec/CommonScanSpec (scan/spec.rs); serde round-trip coverage.
- [x] B2 Advertise ORDER_BY_COLUMN in capabilities.rs; flip the must-be-absent assertion.
- [x] B3 Detect ordered-top-N shape + build SQL in pushdown.rs; withhold per-shard LIMIT on non-match. [expert]
- [x] B4 Render per-shard ORDER BY in scan/mod.rs build_scan_sql with matching direction/NULL placement. [expert]
- [x] B3b Fix latent gap B4 flagged: `detect_topn` must decline a sort key whose Arrow type needs the JSON-fallback VARCHAR cast (List/Struct/Decimal256/etc.) — per-shard DataFusion would sort on the native value while Exasol's merge sorts on the emitted JSON string, which can silently disagree. Not hit by NQ4 (DECIMAL, not fallback), but must be closed before this ships. `types::mapping::needs_json_fallback(&DataType)` and `arrow_type_from_tag` are both `pub`; `logical_schema: Vec<LogicalField>` (with each field's Arrow type tag) is already available in `pushdown.rs`'s `handle_pushdown`. Thread it into `detect_topn` and decline (return `None`) when a sort key's resolved Arrow type needs the fallback. [expert]
- [x] B5 E2E integration test: ordered-top-N over MinIO-backed Iceberg matches single-node eval; decline case falls back correctly. NOTE: full-file E2E run surfaced a pre-existing-test regression caused by B2 (`ORDER_BY_COLUMN` advertisement) unrelated to the two new tests — see decision-log.md.
- [x] B6 CRITICAL correctness fix found by B5's live E2E run: advertising `ORDER_BY_COLUMN` (B2) means Exasol NO LONGER reapplies its own backstop ORDER BY/LIMIT once it has delegated ordering to the VS — this was previously an implicit safety net the whole plan's "decline falls back safely" design (and the pre-existing grouped-aggregate path) silently depended on. Confirmed two live regressions: (1) `build_grouped_aggregate_scan_sql` never renders ORDER BY, so `test_high_cardinality_group_by_spill`/`test_high_cardinality_multi_key_group_by_spill` now return unsorted results; (2) `detect_topn`'s decline path (e.g. ORDER BY over an unprojected column) now returns fully unsorted, unlimited rows instead of Exasol reapplying the ordering. FIX: whenever the pushdown request carries `order_by` — matched top-N or not — every SQL-returning path (row-scan fallback, grouped-aggregate merge) must render its OWN explicit final `ORDER BY <rendered keys> [LIMIT n]` wrapping whatever SQL it would otherwise return (reuse `render_order_by_clause`). The matched top-N path already does this optimally (no change). The decline/fallback paths need an explicit global sort+limit wrapper added around the full (unbounded) result — this reproduces today's pre-B2 behavior exactly, just moving the responsibility from Exasol's implicit backstop into the VS's own returned SQL, since that backstop no longer exists once the capability is advertised. Re-run the two previously-failing tests plus the FULL `make test-e2e` suite to confirm. [expert]

## Group C — Benchmark verification
- [x] C1 Re-run NQ4 against test1; confirm correctness + speedup vs 12.03s baseline; add pushdown_check for order_by/LIMIT. Record in decision-log.

## Phase 4: Code Review
- [x] 4.1 Review all changed files (code-reviewer agent) — 1 LOW nit (stale ponytail comment arg count), fixed inline

## Phase 5: Verification
- [x] 5.1 Build: make cross-musl-udf-build (current, confirmed during B6/C1)
- [x] 5.2 Test: cargo test --workspace — all suites ok, 0 failed
- [x] 5.3 Test E2E: make test-e2e — 53 passed, 0 failed (confirmed during B6)
- [x] 5.4 Lint: cargo clippy --workspace --all-targets — no issues
- [x] 5.5 Format: cargo fmt --check — clean
- [x] 5.6 Scenario coverage audit — all 8 plan scenarios covered (see verification-report.md)

## Phase 6: Verification Report
- [x] 6.1 Generate verification-report.md
