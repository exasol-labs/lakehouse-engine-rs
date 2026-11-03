# Tasks: change-broadcast-join-shared-column-names

## Phase 2: Implementation (Group A: Wire format and scan)
- [x] 1.1 Widen `ProjectionItem::Column` and `SortKey.column` to a column reference with optional relation alias; add `JoinSpec` fact and dimension aliases; define `relation_alias(leg)`; add `scan/spec_tests.rs::output_names_of_distinct_pairs_differ`
- [x] 1.2 Convert the scan's join execution (`build_join_sql`, `combined_type`, `combined_upper_fields`, `render_join_sort_target`, column-type closure) to (relation alias, column) pairs; reject an empty join projection
- [x] 1.3 Move scan fixtures to qualified items and aliases; add `build_join_sql_resolves_a_shared_column_name_per_relation` and `build_join_sql_rejects_an_empty_projection`

## Phase 2: Implementation (Group B: Broadcast planning)
- [x] 2.1 Replace the column-type list with a (relation alias, column) universe in `support.rs` and `binary_filter.rs` [expert]
- [x] 2.2 Qualify the broadcast path through `JoinLegs::qualify`; drop the guard and `strip_table_alias`; record the dimension leg; decline output names over 128 characters [expert]
- [x] 2.3 Delete `disjoint_schema_guard` and `join_col_types`; move goldens and inline strings; add the unit tests

## Phase 2: Implementation (Group C: E2E and docs)
- [x] 3.1 Add `e2e_broadcast_join_over_shared_column_names_matches_native`, `score` seed column, scenario tags, harness doc comment
- [x] 3.2 Run the three self-join E2E tests on `VS_NAME` and `VS_NAME_LOW`
- [x] 3.3 Update `docs/capabilities.md`
- [x] 3.4 Keep the N-scan half of the binary-filter join test on a low-threshold schema; rewrite the `direct_storage.rs` comment

## Phase 3: Verification
- [x] 4.1 Build `.so`: `make cross-udf-build`
- [x] 4.2 Unit tests: `cargo test --workspace`
- [x] 4.3 E2E: `make e2e-up test-e2e`
- [x] 4.4 Lint and format: `make fmt-check lint`
- [x] 4.5 Manual testing steps from the plan

## Phase 4: Review Fixes
- [x] 4.1 In `joins/mod.rs`, correct the `plan_join` doc comment and the comment above `let is_equi`: an unattributable column fails the request through `unattributable_decline`
- [x] 4.2 In `joins/attribution.rs`, document on `JoinLegs::qualify` and `qualify_every_column` that they take a raw request tree once
- [x] 4.3 In `scan/spec.rs`, add a doc comment to `ColumnRef::qualified`
- [x] 4.4 In `tests/it/scan_join_test.rs`, replace the `write_orders` doc comment that cites the removed disjoint-column guarantee
- [x] 4.5 In `tests/it/scan_positional_deletes.rs`, delete the `write_keyed_parquet` doc comment requiring disjoint column names
- [x] 4.6 In `e2e_local/scan.rs`, fix the inverted 1-byte threshold comment
