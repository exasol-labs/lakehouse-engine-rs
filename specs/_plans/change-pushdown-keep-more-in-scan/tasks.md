# Tasks: change-pushdown-keep-more-in-scan

## Phase 2: Implementation (Group A: date-part types)
- [x] 1.1 Measure date-part result types on Exasol 8.29.13 and 2025.1.16, correct the spec delta where it differs
- [x] 1.2 `scalar_functions.rs` and `ScalarFunction::result_type`: fixed `DECIMAL(4,0)` for YEAR, `DECIMAL(2,0)` for MONTH, DAY, HOUR, MINUTE, WEEK
- [x] 1.3 E2E rows in `SPLIT_AGGREGATE_CASES` and the scenario line in `e2e_split_aggregate_columns_match_native_oracle`
- [x] 1.4 `docs/capabilities.md` § Aggregation

## Phase 2: Implementation (Group B: WHERE split)
- [x] 2.1 `ScanTranslator::split_where` renders per top-level conjunct, new `ScanWhere` variant, shared per-conjunct screen [expert]
- [x] 2.2 `build_dispatch_sql` routes the new variant to the qualified single-table fallback with a narrowed emission
- [x] 2.3 `render_broadcast_join_emitting` returns `Ok(None)` for the new variant
- [x] 2.4 Update tests that encode the whole-WHERE decline
- [x] 2.5 Golden `group_by_fallback_rendering_filter.sql`, generated before 2.1
- [x] 2.6 E2E `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` and scenario line moves
- [x] 2.7 `docs/capabilities.md` § Filtering and § Text and session settings

## Phase 2: Implementation (Group D: #461 guard)
- [x] 3.1 `datafusion_truncates_an_overprecise_timestamp_literal` in `raw_scan_tests.rs`

## Phase 3: Verification
- [x] 5.1 `make cross-udf-build`
- [x] 5.2 `cargo test --workspace`
- [x] 5.3 `make e2e-up test-e2e` (Exasol 2025)
- [x] 5.4 E2E on Exasol 8.29.13
- [x] 5.5 `make fmt-check lint`
- [x] 5.6 Manual verification commands of the plan

## Phase 4: Review Fixes
- [x] 4.1 `scan_typing_tests.rs`: delete the `/// Scenario: A date-part function takes its fixed native type` line above `a_date_part_function_takes_its_fixed_native_type`, keep the test body
- [x] 4.2 `support.rs`: `flatten_conjuncts(filter: &Json) -> Vec<&Json>` with a private accumulating helper and a doc stating the return value; update `screen_conjuncts`, `partition_conjuncts` (joins/rendering.rs) and the leg-equality scan (joins/broadcast_policy.rs)
- [x] 4.3 `support.rs`: add `PushdownRequest::with_filter`; `mod.rs`: `fallback` closure takes `emitted: &Emitted`, the `ScanWhere::Split` arm calls `fallback` with the narrowed emission
- [x] 4.4 `pushdown_tests.rs`: rename `pruning_input_unchanged_when_df_render_declines` to `a_split_where_keeps_each_declined_conjunct_as_exasol_sent_it` and reword its assertion message
- [x] 4.5 `capability.rs`: `fan_out_emits` returns `Option<&str>`; `where_split_mismatch` reports a plan without a fan-out EMITS clause and compares `emits_id` only when one exists
- [x] 4.6 `capability.rs`: `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` drops the `row_scan` precondition and asserts the native copy's `LIKE '1%'` ids are `[1, 5, 7]`
- [x] 4.7 `raw_scan_tests.rs`: build the row's epoch millis with `chrono` in `datafusion_truncates_an_overprecise_timestamp_literal`, drop the comment
- [x] 4.8 `scan_typing.rs` `ScanTyping::render_conjunct` refuses implicit text; `support.rs` `ScanTranslator::conjunct` used by `screen_conjuncts`; `ScanWhere::Declined` doc; E2E `Oracle::Error` case `C_VARCHAR AND C_DECIMAL_A LIKE '1%' AND ID > 1` in `DECLINED_CONJUNCT_CASES` [expert]
