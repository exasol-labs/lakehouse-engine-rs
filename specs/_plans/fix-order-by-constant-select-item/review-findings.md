# Code Review Findings: fix-order-by-constant-select-item

## Summary
- Files reviewed: 7
- Total findings: 6 (standard: 6, expert: 0)

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [OUTDATED_COMMENT] The `PushdownRequest` invariant contradicts the `FALSE` key that `parse` inserts
- Location: lines 890-891
- Issue: The doc comment states "Its `orderBy` holds no constant key". The next sentence states that an `orderBy` of constants only with a non-zero offset holds one `FALSE` key, which is a constant key. `drop_constant_sort_keys` inserts that `literal_bool` key, so the first sentence does not match the code. Plan task 2.2 specified the qualified wording "no constant key that Exasol pushed". The stated reason also covers only a bare number, while the rule drops every literal type.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, replace the doc paragraph on `PushdownRequest` that starts "Its `orderBy` holds no constant key" with these two lines: "Its `orderBy` holds no constant key that Exasol pushed, because a constant does not change the order and Exasol reads a bare number there as a select-list position. An `orderBy` of constants only with a non-zero offset holds one `FALSE` key instead."

### crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs

#### [MISSING_BOUNDARY_TEST] The OFFSET-window test passes against the unfixed adapter
- Location: `constant_only_order_by_keeps_its_offset_window`, lines 205-228
- Issue: tasks.md records that this test passed on main. Every row uses the constant `1`, which main renders as select-list position 1 (a valid position) or maps to the projected column `_LH_PROJ_1`, so both return 2 rows. The test asserts only the row count and the join route. It never checks the THEN step of `An ORDER BY of constants only keeps its OFFSET window`, which requires the adapter to order by a constant that Exasol does not read as a select-list position. Removing the `FALSE` substitution from `drop_constant_sort_keys`, while still keeping constant keys, would pass this test.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs, in `constant_only_order_by_keeps_its_offset_window`, (a) add the `5` variant of each of the three queries (`SELECT id, 5 FROM {t} {WINDOW}`, `SELECT id, 5, COUNT(*) FROM {t} GROUP BY id {WINDOW}`, and `join_from("e.ID, 5", WINDOW)`), each expecting `Rows::DistinctSeeded(2)`, because main fails the grouped and join shapes with `Too large value 5` (SQL state 04000); (b) for every row-scan, grouped, and join query in the test, assert that `explain_virtual_sql(&mut conn, &query)` contains `ORDER BY FALSE`, with the query and the pushed SQL in the failure message. Run the test against the local stack and confirm it passes.

#### [SHRINKABLE] The join-route check re-implements `assert_two_scan_join_wrapper`
- Location: `constant_only_order_by_keeps_its_offset_window`, lines 221-226
- Issue: The test calls `explain_virtual_sql` and asserts `has_two_scan_wrapper(&pushed) && !has_broadcast_join_block(&pushed)`. `assert_two_scan_join_wrapper` in crates/lakehouse-e2e/tests/e2e_local/fixtures.rs (line 230) performs this exact check, and join.rs in this change already calls it. specs/testing.md § Test code requires a new test to start from the existing helpers.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs, replace the `let pushed = explain_virtual_sql(&mut conn, &join);` line and the `assert!(has_two_scan_wrapper(...) && !has_broadcast_join_block(...), ...)` block in `constant_only_order_by_keeps_its_offset_window` with `assert_two_scan_join_wrapper(&mut conn, &join);`, and add `assert_two_scan_join_wrapper` to the `crate::fixtures` import. If the `ORDER BY FALSE` assertion from the finding above needs the pushed SQL of the join, keep one `explain_virtual_sql` call for that assertion only.

#### [SHRINKABLE] `first_column_ids` is a one-line wrapper with one caller
- Location: lines 20-22
- Issue: `first_column_ids` only returns `int_column(&conn.query_columns(query)[0])`, and `assert_rows` is its only caller. The extra name adds an indirection without hiding any logic.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs, delete `first_column_ids` and change the first line of `assert_rows` to `let ids = int_column(&conn.query_columns(query)[0]);`.

#### [INFORMATION_LEAKAGE] The events-to-labels join query is written twice in two test files
- Location: order_by_constant.rs `labels_table` and `join_from`, lines 51-61; crates/lakehouse-e2e/tests/e2e_local/join.rs `e2e_join_fallback_constant_sort_key_returns_the_native_order`, lines 508-513
- Issue: Both files build `SELECT … FROM <events> e JOIN <labels> l ON e.ID = l.ID …` independently. The only difference is the virtual schema name (`LAKEHOUSE_VS` and `VS_NAME_LOW`). specs/testing.md § Test code requires that the change that writes a second copy of a helper extract it, and that a helper a second test file needs move into the area's support module.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/fixtures.rs, add `pub fn events_labels_join(vs_name: &str, select: &str, tail: &str) -> String` that returns `format!("SELECT {select} FROM {} e JOIN {} l ON e.ID = l.ID {tail}", vs_table(vs_name, E2E_TABLE), vs_table(vs_name, E2E_TABLE_2))`, with a one-line doc comment, and add `E2E_TABLE_2` to its `crate::common::seed` import. In crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs, delete `labels_table` and `join_from`, replace each `join_from(select, tail)` call with `events_labels_join(LAKEHOUSE_VS, select, tail)`, and drop the imports that become unused (`E2E_TABLE_2`, `vs_table`). In crates/lakehouse-e2e/tests/e2e_local/join.rs, build the query in `e2e_join_fallback_constant_sort_key_returns_the_native_order` as `events_labels_join(VS_NAME_LOW, &format!("e.ID, {constant}"), "ORDER BY 2 DESC, 1 LIMIT 3")` and remove `E2E_TABLE` and `E2E_TABLE_2` from the seed import if no other code in join.rs uses them.

### crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs

#### [INFORMATION_LEAKAGE] `parsed_order_by` spells the `limit` node as inline JSON
- Location: `parsed_order_by`, lines 2828-2831
- Issue: The helper sets the request's window with `.field("limit", serde_json::json!({"numElements": 3, "offset": offset}))`. specs/testing.md § Test code requires tests to build request JSON through the support builders, so a field added to the `limit` node later changes one builder instead of every test. The request builder in crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs has `limit(num_elements)` (line 1697) but no variant that sets an offset.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs, add `pub(crate) fn limit_with_offset(self, num_elements: u64, offset: u64) -> Self` beside `limit`, which calls `self.field("limit", serde_json::json!({"numElements": num_elements, "offset": offset}))`. In crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs, replace the `.field("limit", …)` call in `parsed_order_by` with `.limit_with_offset(3, offset)`. Run `cargo test --workspace --lib parse_drops_constant_sort_keys`.

## Expert fixes
[none]
