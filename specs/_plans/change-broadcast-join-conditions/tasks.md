# Tasks: change-broadcast-join-conditions

- [x] 1. New module `joins/condition_filters.rs`: the join-equality test, the join-key test, and the move with the outer-join path flag, unit-tested in `condition_filters_tests.rs` (Join Condition Filters; outer-join scenario)
- [x] 2. `plan_join` plans the moved request after `ensure_columns_declared`; `plan_broadcast_join` asks the join-key test instead of the top-level `predicate_equal` check (Broadcast Inner Equi-Join; Dead Code Removal row 1)
- [x] 3. Rewrite the doc comment of `a_non_equi_conjunct_stays_in_the_one_join_condition` and update unit and golden tests whose expectation the move changes (Dead Code Removal row 2)
- [x] 4. Host scan test: composite-key and expression-key broadcast joins return the inner join's rows, keep `CollectLeft`, and skip the row groups per-column bounds give (DataFusion Scan Execution, Broadcast Join)
- [x] 5. E2E: `e2e_join_on_filters_plan_as_where_conjuncts`, `e2e_three_table_join_on_filters_reach_their_leg_scans`, `e2e_three_table_join_equality_over_two_legs_stays_in_on` (Join Condition Filters)
- [x] 6. E2E: `e2e_composite_key_join_takes_the_broadcast_path_and_matches_native`, `e2e_expression_key_join_takes_the_broadcast_path_and_matches_native`, `e2e_join_with_on_filters_takes_the_broadcast_path_and_matches_native`, and a composite-key row in `e2e_broadcast_join_bare_limit_stays_broadcast_and_truncates` (Broadcast Inner Equi-Join)
- [x] 7. E2E: move the `ON … AND o.O_ORDERKEY > c.C_CUSTKEY` query to the broadcast list in `e2e_aggregating_join_takes_the_broadcast_path_and_matches_native`, and expect `BROADCAST_FULL_ROW` in the `capability_exclusion.rs` case (Unified Unaccelerated Join Fallback; Dead Code Removal rows 3, 4)
- [x] 8. E2E: `e2e_composite_key_broadcast_join_over_a_clustered_fact_matches_seed`, with a second key column on the clustered fixture (DataFusion Scan Execution, Broadcast Join)
- [x] 9. Docs: `docs/capabilities.md` broadcast paragraph and `docs/capability-matrix.md` `JOIN_CONDITION_EQUI` note
- [x] 10. Verification: build, unit, E2E, lint, format, scenario coverage, manual checks
