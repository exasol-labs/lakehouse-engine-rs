# Plan: change-broadcast-join-conditions

## Summary

A two-leg inner join on a composite key, on a key that compares expressions, or with filters in its `ON` clause takes the broadcast path, and every filter written in `ON` is planned as a WHERE conjunct (#664). Such a filter then prunes its leg's files and reaches its leg's scan on both join paths.

## Context

- Today a join is broadcast only when its whole condition is one `predicate_equal` node. Exasol sends a composite key and an `ON` filter as one `predicate_and` condition, so both take the N-scan.
- A filter written in `ON` never reaches a leg's file pruning or scan filter, on either path, so its table is read in full. Exasol keeps such a filter in the join condition and does not move it to `filter`.
- For inner joins, a conjunct in `ON` and the same conjunct in `WHERE` return the same rows. The adapter advertises only `JOIN_TYPE_INNER`.
- Exasol pushes a join only when its condition holds an equality whose two sides read tables of different sides of that join, expressions included (`o.k = c.k + 1`, or `a.k + b.k = c.k` in a three-table join). DataFusion hash-joins a two-table equality of that shape, and the broadcast hands DataFusion the condition as SQL.
- With several keys, DataFusion 55.2 pushes the dimension key set to the fact scan as one `struct(...) IN (...)` filter, which Parquet statistics cannot evaluate. Only each key column's bounds skip row groups. The spike measured 9 of 16 row groups read with two keys and 3 of 16 with one. No apache/datafusion issue covers this.
- A spike on Exasol 2026.2 with outer joins advertised showed that Exasol sends a join on an outer join's null-supplying side as its own request, and that moving an `ON` conjunct of an outer join to `WHERE` changes the rows. Outer joins are planned in #663.
- The architecture delta (`architecture.md`) adds one constraint: join filters are moved before join planning, by one planner module.
- Branch `spike/composite-key-broadcast` (ce65475c) holds a prototype of the key check and the move, without the outer-join guard and with a plain-column key test.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| Pushdown Planning, Join Condition Filters | NEW | `pushdown-joins/pushdown-planning-join-condition-filters/spec.md` |
| Pushdown Planning, Broadcast Inner Equi-Join | CHANGED | `pushdown-joins/pushdown-planning-join/spec.md` |
| Pushdown Planning, Unified Unaccelerated Join Fallback | CHANGED | `pushdown-joins/pushdown-planning-join-fallback/spec.md` |
| DataFusion Scan Execution, Broadcast Join | CHANGED | `scan-aggregation/scan-execution-join/spec.md` |

## Impact

- Two-leg inner joins on a composite key, on an expression key, or with filters in `ON` move from the N-scan to the broadcast path when the small side is at or below `JOIN_BROADCAST_MAX_BYTES`. Results are unchanged.
- A filter in `ON` now prunes files and filters its leg's scan on both paths, so a query no longer needs that filter moved to `WHERE` by hand.
- A composite-key broadcast skips fewer fact row groups than a single-key broadcast, and a key whose fact side is an expression may skip fewer still. Both still read no more than the N-scan.
- No CONNECTION field, property, catalog kind, install step, or advertised capability changes. No breaking change.

## Requirements

| Requirement | Details |
|-------------|---------|
| One owner, three separate rules | A new module under `crates/lakehouse-engine/src/adapter/pushdown/joins/` owns: the stays-in-condition test (a join equality), the move (tree walk, path flag, merge into `filter`), and the join-key test. The move asks only the stays test, and `plan_broadcast_join` asks only the join-key test, replacing the top-level `predicate_equal` check in `joins/mod.rs`. Changing which conjuncts stay edits one function and its case table. |
| The move runs before join planning | `plan_join` plans the moved request: side filters, emission, the empty result, broadcast, and the N-scan all read it. The rewrite goes through `PushdownRequest` (architecture constraint on request rewrites). |
| Outer-join guard | The tree walk carries a flag for "inside an outer join's null-supplying side", set on the right child of `left_outer`, the left child of `right_outer`, and both children of `full_outer`. |
| Leg sets | Each test reads the set of legs each side of `=` binds to, through the leg binding in `joins/attribution.rs`. A join equality needs two non-empty, disjoint sets; a join key needs two different single legs. `plan_join` runs `ensure_columns_declared` before the move, so the binding never sees an undeclared column. |
| Docs | `docs/capabilities.md`: the broadcast paragraph lists composite keys, expression keys, and `ON` filters planned as `WHERE`, and states the weaker row-group skipping of composite and fact-side expression keys. `docs/capability-matrix.md`: the `JOIN_CONDITION_EQUI` note says "Composite and expression keys; `ON` filters act as `WHERE`." |

## Dependencies

- Open question (#TBD): whether to open an apache/datafusion issue asking for one `IN` list per key column, so a composite key set prunes on statistics.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function body | `crates/lakehouse-engine/src/adapter/pushdown/joins/mod.rs` `plan_broadcast_join`, the `is_equi` top-level `predicate_equal` check | Replaced by the join-key test of the new module |
| Test comment | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs` `a_non_equi_conjunct_stays_in_the_one_join_condition` | Its doc comment says the condition takes the fallback. Its assertion on `detect_join` stays true; the comment is rewritten |
| Test case | `crates/lakehouse-e2e/tests/e2e_local/join.rs` `e2e_aggregating_join_takes_the_broadcast_path_and_matches_native` | The `ON … AND o.O_ORDERKEY > c.C_CUSTKEY` query moves from the `n_scan` list to the `broadcast` list, the value the spec names as changing |
| Test case | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs`, the case commented "A condition beyond the one equality is never broadcast" | Its N-scan expectation becomes false. It expects `BROADCAST_FULL_ROW`, and its comment goes |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The filters of an inner join's condition are planned as WHERE conjuncts | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_join_on_filters_plan_as_where_conjuncts` (case table: for each `ON k AND p`, the `EXPLAIN VIRTUAL` SQL equals that of `ON k WHERE p` on `MY_LAKEHOUSE_JOIN` and `MY_LAKEHOUSE_JOIN_LOW`, and rows equal the native copy; cases include a self-join filter on one occurrence) |
| An ON filter on one leg of a join of three or more legs reaches that leg's scan | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_three_table_join_on_filters_reach_their_leg_scans` |
| A join equality that reads several legs on one side stays in the condition | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_three_table_join_equality_over_two_legs_stays_in_on` (asserts from `EXPLAIN VIRTUAL` that `a.k + b.k = c.k` is in the chain's `ON` and `c.r = 'x'` is in leg `c`'s scan filter, then compares rows with the native copy) |
| The filters of a join on an outer join's null-supplying side stay in its condition | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/condition_filters_tests.rs` | `outer_joins_and_null_supplying_subtrees_keep_their_conditions` (case table: `left_outer`, `right_outer`, `full_outer`, inner under each null-supplying side, inner on a preserved side, inner above a right outer join) |
| A two-leg join on a composite key is planned as a broadcast fan-out | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_composite_key_join_takes_the_broadcast_path_and_matches_native` (case table: two keys, keys reversed, three keys, self-join with a `DATE` key, a dimension with duplicate composite keys, aggregate, `GROUP BY`, `ORDER BY … LIMIT`; each on both virtual schemas) |
| A join key that compares expressions is planned as a broadcast fan-out | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_expression_key_join_takes_the_broadcast_path_and_matches_native` (expression on the dimension side, on the fact side, alone and beside a filter) |
| A two-leg join with filters in its condition is planned as a broadcast fan-out | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_join_with_on_filters_takes_the_broadcast_path_and_matches_native` (case table from the issue's spike table, plus a declined `ON` filter that takes the N-scan) |
| A bare LIMIT over a broadcast-eligible join is served by the broadcast path with a per-shard post-join cap | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_bare_limit_stays_broadcast_and_truncates` (gains a composite-key row) |
| A join outside the broadcast contract is declined safely | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_aggregating_join_takes_the_broadcast_path_and_matches_native` (the `ON` non-equi query moves to the broadcast list; the `SECOND(...)` query keeps the N-scan) |
| The fact scan skips row groups that no dimension join key matches | Integration | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_composite_key_broadcast_join_over_a_clustered_fact_matches_seed` (the clustered fixture gains a second key column on both tables; asserts the broadcast block and the seeded rows) |

Unit tests of the new module's pure logic, in `condition_filters_tests.rs`, back the E2E scenarios above and carry no `/// Scenario:` line: the stays test and the join-key test as one case table each (bare columns, an expression side, a same-leg equality, a side reading two legs, a side reading no column, a self-join's two occurrences), the move in a nested three-leg tree, the merge with an existing `filter`, and a condition with no join equality left whole.

### Manual Testing

Against the local stack (`make e2e-up`, then `make test-e2e` once to create the join virtual schemas).

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Pushdown Planning, Join Condition Filters | `exapump sql -p local "EXPLAIN VIRTUAL SELECT o.O_ORDERKEY FROM MY_LAKEHOUSE_JOIN_LOW.FACT_ORDERS o JOIN MY_LAKEHOUSE_JOIN_LOW.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY AND c.C_NAME = 'customer-02'"` | Two fan-out legs (`LHS_T0`, `LHS_T1`); the `DIM_CUSTOMER` leg's scan spec carries the `C_NAME = 'customer-02'` filter, and the outer `INNER JOIN … ON` holds only the key equality |
| Pushdown Planning, Broadcast Inner Equi-Join | `exapump sql -p local "EXPLAIN VIRTUAL SELECT o.O_ORDERKEY, c.C_NAME FROM MY_LAKEHOUSE_JOIN.FACT_ORDERS o JOIN MY_LAKEHOUSE_JOIN.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY AND o.O_ORDERKEY = c.C_CUSTKEY AND c.C_NAME <> 'customer-01'"` | One scan UDF with a `"join":{` block whose condition holds both key equalities, and the `C_NAME` conjunct in the common spec's filter |
| Pushdown Planning, Unified Unaccelerated Join Fallback | `exapump sql -p local "EXPLAIN VIRTUAL SELECT o.O_ORDERKEY FROM MY_LAKEHOUSE_JOIN.FACT_ORDERS o JOIN MY_LAKEHOUSE_JOIN.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY AND o.O_ORDERKEY > c.C_CUSTKEY"` | A `"join":{` block, no `LHS_T0`/`LHS_T1` fan-out legs |
| DataFusion Scan Execution, Broadcast Join | Run the composite-key query above without `EXPLAIN VIRTUAL`, then the same query over the native copies from `copy_join_tables_natively` | Identical row multisets |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace --lib` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Lint | `make lint` | 0 errors/warnings |
| Format | `make fmt-check` | No changes |
