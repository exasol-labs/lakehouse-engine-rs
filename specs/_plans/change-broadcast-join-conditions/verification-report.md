# Verification Report: change-broadcast-join-conditions

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Composite keys, expression keys, and `ON` filters take the broadcast path. `ON` filters plan as `WHERE` conjuncts on both join paths. The E2E suite passes 420 of 420. Three host tests that this change does not touch fail in this environment, and they fail the same way on unmodified `origin/main`. |
| Code review | 4 findings — 4 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ (3 pre-existing environment failures in untouched code, see Notes) |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit and host integration (`cargo llvm-cov --workspace`, lines) | 91.71% workspace; 100.00% `joins/condition_filters.rs`; 98.09% `joins/mod.rs` |
| E2E | Not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`, lib targets) | 2082 | 2079 | 0 |
| Integration, host (`tests/it`, `scan_footer_refetch_observable`) | 125 | 125 | 2 |
| E2E (`make e2e-up test-e2e`) | 420 | 420 | 0 |

The three unit failures are `catalog_http::tests::a_connection_that_never_completes_ends_at_the_connect_timeout` (`lakehouse-catalog`), `iceberg_write::tests::a_commit_that_keeps_failing_stops_after_at_least_20_retries_stating_the_last_outcome` (`lakehouse-catalog-write`), and `export::iceberg_sink::tests::abort_keeps_the_data_files_while_the_commit_outcome_is_unknown` (`lakehouse-engine`). The two ignored host integration tests were already ignored before this change.

### Manual Tests

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL` of `MY_LAKEHOUSE_JOIN_LOW` join `ON o.O_CUSTKEY = c.C_CUSTKEY AND c.C_NAME = 'customer-02'` | ✓ Fan-out legs `LHS_T0` and `LHS_T1`; the `DIM_CUSTOMER` leg's filter is `("C_NAME" = 'customer-02')`; the outer `ON` is `(("LHS_T0"."O_CUSTKEY" = "LHS_T1"."C_CUSTKEY"))` |
| `EXPLAIN VIRTUAL` of `MY_LAKEHOUSE_JOIN` join on two keys plus `c.C_NAME <> 'customer-01'` | ✓ One scan UDF with a `"join":{` block, condition `(O_CUSTKEY = C_CUSTKEY) AND (O_ORDERKEY = C_CUSTKEY)`, common filter `("LHS_T1"."C_NAME" <> 'customer-01')` |
| `EXPLAIN VIRTUAL` of `MY_LAKEHOUSE_JOIN` join `ON o.O_CUSTKEY = c.C_CUSTKEY AND o.O_ORDERKEY > c.C_CUSTKEY` | ✓ A `"join":{` block, no `LHS_T0`/`LHS_T1` fan-out aliases; the comparison is in the common filter |
| The composite-key query over `MY_LAKEHOUSE_JOIN` and over the native copies in `JOIN_ORACLE` | ✓ Identical rows (4 rows, orders 2 to 5) |

## Tool Evidence

### Linter

```
make lint → exit 0, 0 warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.87s
```

### Formatter

```
make fmt-check → exit 0
cargo fmt --all -- --check
```

### Build

```
make cross-udf-build → exit 0
  LAKEHOUSE_SCAN: ABI version 12, fingerprint 0.33.0:rustc_1.98.1__48a229cea_2026-09-01_ — OK
✓ 4 UDF(s) validated in 'target/release/liblakehouse_engine.so'
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| pushdown-joins | pushdown-planning-join-condition-filters | The filters of an inner join's condition are planned as WHERE conjuncts | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_join_on_filters_plan_as_where_conjuncts` | Pass |
| pushdown-joins | pushdown-planning-join-condition-filters | An ON filter on one leg of a join of three or more legs reaches that leg's scan | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_three_table_join_on_filters_reach_their_leg_scans` | Pass |
| pushdown-joins | pushdown-planning-join-condition-filters | A join equality that reads several legs on one side stays in the condition | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_three_table_join_equality_over_two_legs_stays_in_on` | Pass |
| pushdown-joins | pushdown-planning-join-condition-filters | The filters of a join on an outer join's null-supplying side stay in its condition | `crates/lakehouse-engine/src/adapter/pushdown/joins/condition_filters_tests.rs` | `outer_joins_and_null_supplying_subtrees_keep_their_conditions` | Pass |
| pushdown-joins | pushdown-planning-join | A two-leg join on a composite key is planned as a broadcast fan-out | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_composite_key_join_takes_the_broadcast_path_and_matches_native` | Pass |
| pushdown-joins | pushdown-planning-join | A join key that compares expressions is planned as a broadcast fan-out | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_expression_key_join_takes_the_broadcast_path_and_matches_native` | Pass |
| pushdown-joins | pushdown-planning-join | A two-leg join with filters in its condition is planned as a broadcast fan-out | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_join_with_on_filters_takes_the_broadcast_path_and_matches_native` | Pass |
| pushdown-joins | pushdown-planning-join | A bare LIMIT over a broadcast-eligible join is served by the broadcast path with a per-shard post-join cap | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_bare_limit_stays_broadcast_and_truncates` | Pass |
| pushdown-joins | pushdown-planning-join-fallback | A join outside the broadcast contract is declined safely | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_aggregating_join_takes_the_broadcast_path_and_matches_native` | Pass |
| scan-aggregation | scan-execution-join | The fact scan skips row groups that no dimension join key matches | `crates/lakehouse-e2e/tests/e2e_local/join.rs`, `crates/lakehouse-engine/tests/it/scan_join_test.rs` | `e2e_composite_key_broadcast_join_over_a_clustered_fact_matches_seed`, `composite_key_join_prunes_fact_row_groups_outside_each_key_columns_bounds` | Pass |

Unit tests in `condition_filters_tests.rs` cover the join-equality and join-key tests as one case table, the move in a nested three-leg tree, the merge after an existing filter, several equalities kept as one conjunction, a self-join occurrence alias, and a condition left whole. `on_filters_prune_each_delta_join_leg_and_keep_the_broadcast_plan` (`joins_tests.rs`) covers the planner wiring. `expression_key_join_returns_the_inner_join_rows_from_one_hash_table` (`scan_join_test.rs`) shows that an expression key keeps `CollectLeft` and returns the inner join's rows.

## Notes

- The three failing unit tests connect to a localhost fake catalog and fail with `operation timed out`. They fail on unmodified `origin/main` in this environment on every run, inside and outside the sandbox. This change does not touch `lakehouse-catalog`, `lakehouse-catalog-write`, or the export sink.
- The plan's checklist runs `cargo test --workspace --lib`, which skips `tests/it`. The host integration tests were run with `cargo test --workspace`, and the composite-key and expression-key host scan tests live there.
- With a composite key, the fact scan in the host test skipped at least every row group outside some key column's bounds and read the group whose key columns each hold a dimension key with no matching tuple, as the scan scenario allows.
- The plan-comparison E2E tests normalize the order of each shard list's files, because a pushdown assigns files to shards in no fixed order.
- Open question (#TBD), unchanged from the plan: whether to open an apache/datafusion issue for one `IN` list per key column.
