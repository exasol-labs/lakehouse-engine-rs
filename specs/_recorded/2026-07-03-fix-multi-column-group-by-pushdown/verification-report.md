# Verification Report: fix-multi-column-group-by-pushdown

**Generated:** 2026-07-03

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Multi-column GROUP BY is pushed down as partial aggregation (AGGREGATE_GROUP_BY_TUPLE advertised, N-key path verified). All host tests, lint, format, and the full E2E suite pass against the live Exasol + MinIO + Iceberg REST stack. |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ (.so built at 0.21.0) |
| Tests (`cargo test --workspace`) | ✓ |
| Lint (`cargo clippy --all-targets --features exasol-e2e`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (EXPLAIN VIRTUAL probes against live DB) |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed | Ignored |
|------|-----|--------|--------|---------|
| Unit (lakehouse-engine lib) | 351 | 351 | 0 | 0 |
| Unit (vs-expression) | 53 | 53 | 0 | 0 |
| E2E (`e2e_scan_test`, `--features exasol-e2e`) | 36 | 36 | 0 | 0 |
| E2E (`e2e_capability_test`) | 7 | 7 | 0 | 0 |

`make test-e2e` exit code: 0.

## Tool Evidence

### Linter

```
cargo clippy --all-targets --features exasol-e2e → 0 warnings, 0 errors
```

### Formatter

```
cargo fmt --check → clean (no diffs)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning | Adapter advertises aggregate pushdown incl. TUPLE, backed by the multi-key path | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_group_by_capabilities`, `reports_audited_capability_set`, `reports_supported_aggregate_capabilities` | Pass |
| vs-adapter | pushdown-planning | Multi-column GROUP BY pushed as partial aggregation (not raw scan) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_multi_key_with_filter` (EXPLAIN-verified) | Pass |
| vs-adapter | pushdown-planning-grouped-agg | Every element of a multi-key tuple may be an expression; one untranslatable element forces fallback | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `detect_group_by_all_expression_multi_key` | Pass |
| vs-adapter | pushdown-planning-grouped-agg | Each group key resolves its own declared type by select index | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `group_key_types_multi_key_mixed_types` | Pass |
| vs-adapter | pushdown-planning-grouped-agg | Multi-key SQL build places HAVING+LIMIT only in outer wrapper | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_wrapper_multi_key_having_and_limit_outer_only` | Pass |
| packaging | e2e-harness-grouped-order | Interleaved multi-key GROUP BY with aggregate between keys, EXPLAIN-verified | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_interleaved_multi_key` | Pass |
| packaging | e2e-harness-grouped-order | Expression-valued (advertised-fn) multi-key tuple GROUP BY, mixed types, EXPLAIN-verified | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_expr_multi_key_tuple` | Pass |
| packaging | e2e-harness-grouped-order | Multi-key GROUP BY with HAVING and LIMIT in outer wrapper | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_multi_key_having_limit` | Pass |
| datafusion-scan | scan-execution-grouped-agg | High-cardinality multi-key grouped scan completes under bounded pool | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_high_cardinality_multi_key_group_by_spill` | Pass |

## Notes / Findings (reflected in the spec deltas)

1. **Pushdown-occurred evidence is the scan-spec `group_keys` field, not `GROUP BY shard_key`.**
   The `shard_key` fan-out is emitted only when the assigned file list spans more than one shard;
   a WHERE filter that prunes to a single file pushes down grouped aggregation with
   `GROUP BY "GK_0", "GK_1"` and no `shard_key` fan-out. The E2E helper `assert_group_by_pushed_down`
   and the affected spec scenarios were corrected accordingly.

2. **GROUP BY keys built from unadvertised scalar ops (arithmetic `/`/`*`, `CAST`) are not pushed
   down.** They are rendered by the VS expression translator but the capabilities are not advertised
   (future scope), so Exasol does not send them as pushed group keys and falls back to a raw scan.
   Verified via live EXPLAIN VIRTUAL probes: `MOD(id,4), MOD(id,5)` and `MOD(id,4), UPPER(name)`
   push down; `MOD(id,4), CAST(score/50.0 AS DECIMAL(4,0))` falls back. Expression-tuple test and
   spec examples now use advertised scalar functions.

3. **No production code changes to the N-key path were required** — `detect_group_by_aggregates`,
   `group_key_exasol_types`, and `build_grouped_aggregate_scan_sql` were already N-key-generic and
   verified correct end-to-end. The change is the capability advertisement plus test coverage.
