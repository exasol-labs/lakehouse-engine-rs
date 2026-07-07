# Verification Report: fix-join-decline-hard-fail

**Generated:** 2026-07-07

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | 3+ table inner-join pushdown now falls back to a correct N-scan unaccelerated wrapper instead of hard-failing with `F-UDF-CL-RUST-9001` (issue #76). All host and E2E suites green, no regression to the existing two-table broadcast/two-scan path. |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (subsumed by live E2E — see Notes) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`, full workspace) | 550 | 550 | 2 |
| Integration/E2E (`make test-e2e`, live Exasol Docker) | 76 | 76 | 0 |

Per-suite E2E breakdown (`make test-e2e`, `EXIT_CODE=0`):

| Suite | Passed | Failed |
|-------|--------|--------|
| e2e_capability_test | 8 | 0 |
| e2e_count_distinct_test | 6 | 0 |
| e2e_join_test | 8 | 0 |
| e2e_positional_deletes_test | 11 | 0 |
| e2e_scan_test | 43 | 0 |

`e2e_join_test` includes both new tests for this fix (`e2e_three_table_join_result_correct`,
`e2e_four_table_join_result_correct`) alongside all pre-existing two-table join tests
(broadcast, above-threshold fallback, aggregate-over-join) — unchanged and still green,
confirming no regression to the frozen two-table path.

## Tool Evidence

### Linter

```
cargo clippy --all-targets
No issues found
```

### Formatter

```
cargo fmt --check
(no output — clean)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-join | A join outside the broadcast contract is declined safely (CHANGED) | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `join_outside_contract_declined_safely` | Pass |
| vs-adapter | pushdown-planning-join | A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — detection, 3-table | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `three_table_inner_join_is_multitable` | Pass |
| vs-adapter | pushdown-planning-join | A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — detection, 4-table | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `four_table_inner_join_is_multitable` | Pass |
| vs-adapter | pushdown-planning-join | A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — SQL shape, Q1 | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `build_n_scan_join_sql_for_q1_shape_supplier_nation_region` | Pass |
| vs-adapter | pushdown-planning-join | A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — SQL shape, NQ3 (N=4) | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `build_n_scan_join_sql_for_nq3_shape_part_partsupp_supplier_nation` | Pass |
| vs-adapter | pushdown-planning-join | A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — shared column names | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `build_n_scan_join_sql_renders_qualified_when_three_tables_share_column_name` | Pass |
| vs-adapter | pushdown-planning-join | A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — runtime, 3 tables | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_three_table_join_result_correct` | Pass |
| vs-adapter | pushdown-planning-join | A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — runtime, 4 tables | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_four_table_join_result_correct` | Pass |

Existing scenarios (broadcast, threshold, projection/EMITS, condition rendering, shared-column
two-scan, aggregate-over-join, capabilities) were not modified and their pre-existing tests remain
green, confirmed by the full-suite run above.

## Notes

- **Manual Testing subsumed by E2E.** The plan's Manual Testing table (`EXPLAIN VIRTUAL` shape
  check + a live `COUNT(*)` join query) is exactly what `e2e_three_table_join_result_correct` and
  `e2e_four_table_join_result_correct` already assert programmatically against the live Exasol
  Docker container: pushed-SQL shape (via `has_n_scan_wrapper`) and correct results. Re-running
  the same queries by hand through `exapump interactive` would duplicate this evidence without
  adding coverage, so it was not done separately.
- **Design note carried forward, not actioned in this plan:** the implementing agent found that
  `IneligibleJoinReason::TooManyTables`'s loop-based producer (`pushdown.rs:3701`) is now dead code
  (superseded by the earlier `join_tree_is_multi_table` check in the same function), while its
  other producer — the `involvedTables.len() != 2` mismatch case (`pushdown.rs:3738`) — remains
  genuinely reachable and is still covered by `involved_table_count_mismatch_is_ineligible`. The
  variant itself is not dead, so it and `join_outside_contract_declined_safely`'s existing
  `TooManyTables` facet were left as-is per the plan's task 5.3 removal criterion ("only if fully
  unreachable"). The single dead branch at `pushdown.rs:3701` is a small, low-risk cleanup
  candidate for a future pass — flagging it here rather than expanding this fix's scope.
- **E2E fixture naming:** task 6.1's seed extension reuses the existing `dim_customer`/
  `fact_orders` star-schema naming (adding `fact_lineitem`/`dim_supplier`) rather than literal
  TPC-H table names (`PART`/`PARTSUPP`/`SUPPLIER`/`NATION`), per the plan's explicit "pick
  whichever shape is cheapest" latitude for task 6.1. The 3-table and 4-table join shapes and
  cardinalities still mirror Q2 and NQ3 respectively, which is what the scenario requires.
