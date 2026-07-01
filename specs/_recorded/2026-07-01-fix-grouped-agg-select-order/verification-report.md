# Verification Report: fix-grouped-agg-select-order

**Generated:** 2026-07-01

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Grouped-aggregate outer-SELECT now follows the user's `selectList` order for any interleaving of keys and aggregates (issue #33). All host + E2E tests green. A HAVING-over-aggregates bug discovered during E2E (HAVING silently dropped → wrong results) was fixed in the same change. |

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
| Unit (`cargo test --lib`) | 325 | 325 | 0 |
| Integration/E2E — `e2e_scan_test` (`--features exasol-e2e`, live stack) | 32 | 32 | 0 |
| Integration/E2E — `e2e_capability_test` | 7 | 7 | 0 |

13 new unit tests (10 ordering/classification + 3 HAVING-over-merge) and 4 new E2E tests.

## Tool Evidence

### Linter

```
cargo clippy --all-targets --features exasol-e2e → 0 warnings, 0 errors
```

### Formatter

```
cargo fmt --check → clean (no changes)
```

## Scenario Coverage

| Feature | Scenario | Test Location | Test Name | Passes |
|---------|----------|---------------|-----------|--------|
| vs-adapter/pushdown-planning-grouped-agg | Grouped aggregate detected & translated | `adapter/pushdown.rs` | `detect_group_by_aggregates_preserves_select_list_order` | Pass |
| vs-adapter/pushdown-planning-grouped-agg | Outer wrapper re-groups partials per user key | `adapter/pushdown.rs` | `grouped_wrapper_outer_select_follows_select_list_order` | Pass |
| vs-adapter/pushdown-planning-grouped-agg | Outer SELECT preserves selectList order (interleaved) | `adapter/pushdown.rs` | `grouped_wrapper_agg_before_key_ordering`, `grouped_wrapper_interleaved_multi_key_ordering`, `grouped_wrapper_expr_key_after_agg_ordering` | Pass |
| vs-adapter/pushdown-planning-grouped-agg | Group-key type resolved by index (no VARCHAR drift) | `adapter/pushdown.rs` | `group_key_type_resolved_by_index_not_string_match` | Pass |
| vs-adapter/pushdown-planning-grouped-agg | HAVING over aggregates renders against merge decomposition (regression from E2E) | `adapter/pushdown.rs` | `render_having_over_merge` unit tests | Pass |
| packaging/e2e-harness | Aggregate before single group key (#33 repro) | `tests/e2e_scan_test.rs` | `test_group_by_agg_before_key` | Pass |
| packaging/e2e-harness | Interleaved multi-key GROUP BY | `tests/e2e_scan_test.rs` | `test_group_by_interleaved_multi_key` | Pass |
| packaging/e2e-harness | Expression group key after aggregate (DECIMAL, not VARCHAR) | `tests/e2e_scan_test.rs` | `test_group_by_expr_key_after_agg` | Pass |
| packaging/e2e-harness | Aggregate-first GROUP BY with HAVING | `tests/e2e_scan_test.rs` | `test_group_by_agg_first_with_having` | Pass |

## Manual Tests

| Test | Result |
|------|--------|
| `SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)` (via Exasol Docker + MinIO, `.so` 0.17.1) — no "Data type mismatch in column number 1"; per-group sums match key-first form | ✓ (covered by `test_group_by_agg_before_key`) |
| `make test-e2e` — all four new grouped-order cases pass; suite fails (not skips) if stack down | ✓ (MAKE_EXIT=0) |

## Notes

- **Scope expansion (in-scope):** the plan assumed HAVING rendering already worked. E2E revealed that `vs-expression`'s renderer has no `function_aggregate` case, so a HAVING containing an aggregate (`SUM(score) > 250`) was silently dropped and Exasol did not re-apply the delegated HAVING → all groups returned. Fixed by rendering HAVING against the merge decomposition (`SUM("PARTIAL_sum_0") > 250`), fail-closed (decline pushdown → native execution) when unrenderable. No capability change; stayed within `pushdown.rs`.
- Version bumped `0.17.0` → `0.17.1` (patch, bug fix).
