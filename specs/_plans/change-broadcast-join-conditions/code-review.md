# Code Review Findings: change-broadcast-join-conditions

## Summary
- Files reviewed: 13
- Total findings: 4

Evidence gathered: `cargo test --workspace --lib joins` passed (143 tests). The two new host scan tests passed. `make lint` and `make fmt-check` logs from the implementer were clean. Of the new E2E tests, only `e2e_join_on_filters_plan_as_where_conjuncts` has a recorded run (`target/speq-e2e-new.log`). The other new E2E tests still need task 10's run.

## Findings

### crates/lakehouse-engine/tests/it/scan_join_test.rs

#### [WEAK_ASSERTION] The composite-key pruning test cannot detect a missing second-column bound
- Location: `composite_key_join_prunes_fact_row_groups_outside_each_key_columns_bounds`, lines 981 and 994
- Issue: The scenario requires the fact scan to skip "every row group that lies outside the bounds of any one key column". With `dim_keys = [(3, 30), (5, 50), (4, 999)]`, `O_REGION` bounds are `[30, 999]`, so every row group outside them (groups 0 to 2) also lies outside the `C_CUSTKEY` bounds `[3, 5]`. The assertion `pruned >= CLUSTERED_ROW_GROUPS - 3` therefore still passes if DataFusion pushes only the first key's bounds. A temporary run with `dim_keys = [(3, 30), (5, 50), (7, 50)]` returned `pruned = 61`. That count is reachable only when the `O_REGION` bounds `[30, 50]` skip groups 6 and 7, which lie inside the `C_CUSTKEY` bounds `[3, 7]`. So the stronger assertion holds today.
- Fix: In `composite_key_join_prunes_fact_row_groups_outside_each_key_columns_bounds`, set `let dim_keys = [(3, 30), (5, 50), (7, 50)];` and rename `outside_custkey_bounds` to `outside_some_key_bounds`, keeping the value `CLUSTERED_ROW_GROUPS as usize - 3`. Groups 3, 4, and 5 are the only ones inside both key columns' bounds. Change its assertion message to say that every row group outside the `C_CUSTKEY` bounds [3, 7] or the `C_REGION` bounds [30, 50] must be pruned. Replace the comment above `dim_keys` with: "Groups 6 and 7 lie inside the C_CUSTKEY bounds but outside the C_REGION bounds, so only the second key's bounds skip them. Group 4 lies inside both bounds, and no dimension tuple matches it." Keep `assert_eq!(rows, clustered_rows([3, 5]))`.

#### [STALE_DOC] `execute_join_plan` doc omits its fourth return value
- Location: `execute_join_plan`, its doc comment
- Issue: The doc names three values ("The rows, the pruned fact row groups, and the hash join's partition mode"), but the function returns a 4-tuple whose last element is the rendered physical plan.
- Fix: Change the doc comment of `execute_join_plan` to "The rows, the pruned fact row groups, the hash join's partition mode, and the rendered physical plan of `spec`'s plan."

### crates/lakehouse-engine/src/adapter/pushdown/joins/condition_filters_tests.rs

#### [STALE_DOC] `equality_cases` doc lists the wrong tuple fields
- Location: line 79
- Issue: The doc reads "(case, conjunct, join equality, join key)", but each tuple is `(case, legs, conjunct, join equality, join key)`.
- Fix: Change the doc comment of `equality_cases` to "The equality cases both tests share: (case, legs, conjunct, join equality, join key)."

### crates/lakehouse-e2e/tests/e2e_local/join.rs

#### [MISSING_SCENARIO_ASSERTION] The three-leg ON filter is never shown to prune as a WHERE conjunct
- Location: `e2e_three_table_join_on_filters_reach_their_leg_scans`
- Issue: The scenario "An ON filter on one leg of a join of three or more legs reaches that leg's scan" requires each conjunct to "prune that leg's files as a WHERE conjunct does". The test checks only that each filter appears in a scan filter and that the rows match. The ON-versus-WHERE plan comparison in `e2e_join_on_filters_plan_as_where_conjuncts` covers only two-leg joins, so no test covers file pruning for a moved conjunct in a three-leg join.
- Fix: In `e2e_three_table_join_on_filters_reach_their_leg_scans`, add a `where_query` constant: `"SELECT c.C_NAME, l.L_LINENUMBER, l.L_QUANTITY FROM {T}.DIM_CUSTOMER c JOIN {T}.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY JOIN {T}.FACT_LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY WHERE c.C_NAME = 'customer-02' AND l.L_LINENUMBER = 1"`. After the existing `pushed` assignment, assert `shard_order_free(&pushed) == shard_order_free(&isolated_pushdown_statement(&mut conn, &where_query.replace("{T}", VS_NAME)))` with the message "the three-leg ON filters must plan, and prune, as the same WHERE filters". The test already carries its `/// Scenario:` line, so add none.
