# Code Review Findings: change-exasol-type-model

## Summary
- Files reviewed: 49
- Total findings: 20 (standard: 19, expert: 1)
- `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --all-features -- -D warnings` pass on the working tree.
- Judged implementer deviations: 1, 2, 3, 4, 5, 7 (the added test), 8, and 10 are accepted. Deviation 6 is accepted, but the branch its deleted test covered stays behind (scalar_over_agg.rs finding). Deviation 9 is a finding (single_group_agg_tests.rs). The clippy suppression of deviation 7 is the expert finding.

## Standard fixes

### crates/vs-expression/src/exasol_type.rs

#### [CONTEXTLESS_ERROR] A DECIMAL precision above 255 reports the u8 range instead of Exasol's limit
- Location: lines 295-306, `decimal_from_json`
- Issue: `decimal_from_json` bounds `precision` and `scale` to `u8::MIN..=u8::MAX`. For `{"type": "decimal", "precision": 300, "scale": 2}` the error states "`precision` must be an integer from 0 to 255", which is not Exasol's constraint. Exasol accepts a precision from 1 to 36 and a scale from 0 to the precision. Only a precision from 37 to 255 reaches the `Decimal::new` message that states this.
- Fix: In crates/vs-expression/src/exasol_type.rs, in `decimal_from_json`, read the precision with `bounded_field(dt, "precision", 1, MAX_DECIMAL_PRECISION)?` and the scale with `bounded_field(dt, "scale", 0, MAX_DECIMAL_PRECISION)?`. Keep the `Decimal::new` call and its message, which still refuses a scale above the precision. Run `cargo test --workspace --lib exasol_type_from_json`.

#### [MISSING_DOC_COMMENT] Three public items carry no doc comment
- Location: lines 47, 51, 116
- Issue: `Decimal::precision`, `Decimal::scale`, and `ExasolType::timestamp` are public and undocumented. `ExasolType::timestamp` had a doc comment on `main`, and the move dropped it. The signature does not tell a reader that `timestamp` checks no range, while `decimal` beside it panics outside Exasol's domain.
- Fix: In crates/vs-expression/src/exasol_type.rs, add a one-line doc comment to `Decimal::precision` (`/// The total digit count, from 1 to 36.`), to `Decimal::scale` (`/// The fractional digit count, at most the precision.`), and to `ExasolType::timestamp` (``/// `TIMESTAMP(precision)` without a range check; precision 3 renders as the bare `TIMESTAMP`.``).

### crates/vs-expression/src/lib.rs

#### [OUTDATED_COMMENT] The `render_cast_target` doc claims a decline the DataFusion dialect does not make
- Location: lines 311-314
- Issue: The doc states that "The DataFusion dialect declines a target whose DataFusion type would compute other values than Exasol's." `datafusion_cast_target` renders every `CHAR(n)`, sized `VARCHAR(n)`, and ASCII target as a bare `VARCHAR`, which computes other values: it applies no length check, no blank padding, and no ASCII check. `docs/semantic-differences.md` § CAST to text documents that deviation, and the plan's Out of scope moves its fix to (#587).
- Fix: In crates/vs-expression/src/lib.rs, replace the doc comment of `render_cast_target` with these two lines: ``/// The Exasol dialect renders the target's `Display`. The DataFusion dialect declines a target it`` and ``/// cannot represent, and renders CHAR or VARCHAR as unsized VARCHAR without Exasol's checks (#587).``

### crates/lakehouse-engine/src/types/exasol_type.rs

#### [SHALLOW_MODULE] The engine's re-export covers only `ExasolType`, so seven imports bypass it
- Location: line 3
- Issue: The module gives the engine one path to the shared type (decision-log [1]), but it re-exports only `ExasolType`. Seven imports reach `vs_expression::exasol_type` directly for the other items of the same module: `adapter/mod.rs:56`, `adapter/pushdown/format/delta_schema.rs:5`, `adapter/pushdown/grouped_agg.rs:7`, `adapter/pushdown/scalar_over_agg.rs:11`, `adapter/pushdown/support.rs:16`, `adapter/pushdown/test_support_tests.rs:19`, and `types/mapping.rs:9`. `types/mapping.rs` imports from both paths, on lines 7 and 9.
- Fix: In crates/lakehouse-engine/src/types/exasol_type.rs, replace the re-export with `pub use vs_expression::exasol_type::{Decimal, ExasolType, MAX_DECIMAL_PRECISION, MAX_VARCHAR_SIZE, exasol_type_from_json, exasol_type_to_json};`. Change the seven imports listed above to import from `crate::types::exasol_type` (in `types/mapping.rs`, from `super::exasol_type`), and keep the `Decimal as ExasolDecimal` alias in `delta_schema.rs`. Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`.

### crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs

#### [INFORMATION_COUPLING] The key-type fallback spells the scan's key type as a literal beside the constant that names it
- Location: line 223 (`group_key_exasol_types`), line 425 (`GROUP_KEY_EMIT_TYPE`)
- Issue: A key without a projected type or a `groupBy` `dataType` falls back to `ExasolType::WIDEST_VARCHAR`. The function's doc calls that fallback "the type the scan emits every key in", which `GROUP_KEY_EMIT_TYPE` names. `build_grouped_aggregate_scan_sql` leaves a key uncast only when its type equals `GROUP_KEY_EMIT_TYPE`. When (#587) changes the emit type, the fallback keeps the old literal, and every such key gains a cast to `VARCHAR(2000000)`.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs, in `group_key_exasol_types`, replace `.map_or(Ok(ExasolType::WIDEST_VARCHAR), exasol_type_from_json)` with `.map_or(Ok(GROUP_KEY_EMIT_TYPE), exasol_type_from_json)`.

### crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs

#### [UNREACHABLE_CODE] `cast_merge_items` keeps an uncast branch that only test fixtures reach
- Location: lines 327-345, `cast_merge_items`
- Issue: Both production callers pass one declared type per plan. `fold_aggregate_plan` pushes a type for every plan it adds, and `single_group_plan_types` sizes its list to the plans. The `None => expr` arm and the doc sentence "An aggregate without a declared type merges uncast" serve only three fixtures in `support_tests.rs` that pass `&[]` (lines 628, 802, 834). Task 3.5 of the plan states that SUM, COUNT, AVG, and statistic merges are cast, and `specs/testing.md` § Testing strategy allows no production code that exists only for a test. The test that covered this branch was deleted (deviation 6), so the branch is now also untested.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs, in `cast_merge_items`, add `debug_assert_eq!(aggregates.len(), aggregate_types.len());`, zip `aggregate_types` into the iterator in place of `aggregate_types.get(i)`, delete the `None => expr` arm, and delete the sentence "An aggregate without a declared type merges uncast." from its doc comment. In crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs, pass `&[ExasolType::DoublePrecision, ExasolType::decimal(18, 0)]` at line 628, `&vec![ExasolType::DoublePrecision; agg_plans.len()]` in `build_agg_sql` at line 802, and `&[ExasolType::decimal(18, 0)]` at line 834. Run `cargo test --workspace --lib aggregate_` and `cargo test --workspace --lib merge_`, and confirm that every assertion passes unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/single_group_agg_tests.rs

#### [OBSOLETE_TEST] A test named for the removed absent-list default
- Location: lines 200-214, `single_group_scalar_over_aggregate_defaults_declared_type_when_absent`
- Issue: The test proved the `VARCHAR(2000000)` default for a request without `selectListDataTypes`, which `select_list_types` now refuses. It now declares `VARCHAR(2000000)` through the builder and asserts that the item carries that type, and its `expect` message still reads "must decompose without declared types". `single_group_scalar_over_aggregate_preserves_selectlist_order_and_item_types` already asserts that a scalar-over-aggregate item carries its declared type.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/single_group_agg_tests.rs, delete `single_group_scalar_over_aggregate_defaults_declared_type_when_absent` with its `#[test]` attribute, and delete any import that only it uses.

#### [REDUNDANT_COMMENT] A unit test repeats the scenario tag its E2E test carries
- Location: line 1106, above `an_aggregate_whose_declared_type_the_scan_cannot_emit_declines`
- Issue: The test carries `/// Scenario: An INTERVAL, GEOMETRY, or HASHTYPE select-list item, ...`. `specs/testing.md` § Coverage rule lets a unit test carry the line only when it is the scenario's one proof. The plan's Scenario Coverage maps this scenario to the E2E test `non_emittable_select_items_match_native`, which carries the line. The plan names no unit test for this tag.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/single_group_agg_tests.rs, delete the `/// Scenario:` line above `an_aggregate_whose_declared_type_the_scan_cannot_emit_declines`.

### crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs

#### [REDUNDANT_COMMENT] Three unit tests repeat scenario tags their E2E tests carry
- Location: line 2215 (`min_over_char_expression_declares_char_partial_and_an_uncast_merge`), line 2924 (`a_group_key_the_scan_cannot_emit_declines`), line 2942 (`an_aggregate_whose_declared_type_the_scan_cannot_emit_declines`)
- Issue: The first test carries "The merge casts a merged value only when merging can change its type", which the E2E test `merge_casts_only_values_whose_type_merging_changes` proves and tags. The other two carry "An INTERVAL, GEOMETRY, or HASHTYPE select-list item, ...", which `non_emittable_select_items_match_native` proves and tags. `specs/testing.md` § Coverage rule lets a unit test carry the line only when it is the scenario's one proof, and the plan names none of these three for a tag.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs, delete the `/// Scenario:` lines at lines 2215, 2924, and 2942. Keep the "A VARCHAR-declared type is unaffected" and "A pushdown request with a missing or invalid Exasol type fails the query" tags, which the plan assigns.

#### [SHRINKABLE] Two builder chains are left unformatted because their inline JSON overflows the line width
- Location: lines 456-459 (`detect_group_by_mixed_select_falls_back`), lines 468-473 (`composed_nested_aggregate_request_does_not_reference_phantom_column`)
- Issue: The chained builder calls start in column 0. rustfmt skips a statement it cannot fit into the line width, so `make fmt-check` passes while the chain stays unindented. The overflow comes from inline JSON literals, which `specs/testing.md` § Test code replaces with support builders.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs, bind the inline `YEAR` and `groupBy` JSON literals of both chains to `let` bindings above the chain, write `.field("type", "select")` in place of `.field("type", serde_json::json!("select"))`, then run `cargo fmt --all` and confirm that no line of either chain starts in column 0.

### crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs

#### [REDUNDANT_COMMENT] Two unit tests repeat scenario tags their E2E tests carry
- Location: line 4618 (`cast_to_declared_type_casts_a_genuine_varchar_2000000`), line 4816 (`a_select_item_the_scan_cannot_emit_widens_the_projection`)
- Issue: The first carries "The merge casts a merged value only when merging can change its type", which `merge_casts_only_values_whose_type_merging_changes` proves and tags. The second carries "An INTERVAL, GEOMETRY, or HASHTYPE select-list item, ...", which `non_emittable_select_items_match_native` proves and tags. `specs/testing.md` § Coverage rule lets a unit test carry the line only when it is the scenario's one proof.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs, delete the `/// Scenario:` lines at lines 4618 and 4816.

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs

#### [REDUNDANT_COMMENT] A unit test repeats the scenario tag its E2E test carries
- Location: line 3595, above `wrapper_casts_a_null_literal_to_its_declared_type`
- Issue: The test carries "A NULL select item in an Exasol-rendered wrapper keeps its declared type". The plan's Scenario Coverage maps that scenario to the E2E test `typed_null_select_item_keeps_its_declared_type`, which carries the line, and task 3.6 assigns no tag to this unit test. `specs/testing.md` § Coverage rule lets a unit test carry the line only when it is the scenario's one proof.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs, delete the `/// Scenario:` line at line 3595.

#### [SHRINKABLE] Two builder chains are left unformatted because their inline JSON overflows the line width
- Location: lines 2481-2496, lines 2529-2537
- Issue: The chained builder calls start in column 0, because rustfmt skips a statement it cannot fit into the line width. The overflow comes from the inline `having`, `orderBy`, and `SUM(CASE ...)` JSON literals inside the chain.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs, bind the `having` JSON, the `orderBy` JSON, and the `SUM(CASE ...)` aggregate (built with `agg_item_expr("SUM", <case node>, false)`) to `let` bindings above each chain, then run `cargo fmt --all` and confirm that no line of either chain starts in column 0.

### crates/lakehouse-engine/src/adapter/capabilities_tests.rs

#### [SHRINKABLE] A builder chain is left unformatted because its inline aggregate JSON overflows the line width
- Location: lines 40-44, `reports_group_by_capabilities`
- Issue: The chain starts its calls in column 0, because rustfmt skips a statement it cannot fit into the line width. The overflow comes from an inline `function_aggregate` COUNT literal that the support helper `agg_item` builds.
- Fix: In crates/lakehouse-engine/src/adapter/capabilities_tests.rs, import `agg_item` from `crate::adapter::pushdown::test_support`, replace the inline COUNT literal with `agg_item("COUNT", None, false)`, then run `cargo fmt --all` and confirm that no line of the chain starts in column 0.

### crates/lakehouse-engine/src/adapter/pushdown/refused_columns_tests.rs

#### [SHRINKABLE] Three builder chains are left unformatted because their inline JSON overflows the line width
- Location: lines 43-45, 121-122, 142-143
- Issue: The chained calls start in column 0, because rustfmt skips a statement it cannot fit into the line width. The overflow comes from inline filter and COUNT literals.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/refused_columns_tests.rs, replace the inline COUNT literal at line 143 with `agg_item("COUNT", None, false)`, bind each inline `predicate_is_not_null` filter to a `let` above its chain, write `.field("type", "select")` in place of `.field("type", serde_json::json!("select"))`, then run `cargo fmt --all` and confirm that no line of the three chains starts in column 0.

### crates/lakehouse-engine/src/adapter/pushdown/topn_tests.rs

#### [SHRINKABLE] Two builder chains are left unformatted because their inline JSON overflows the line width
- Location: lines 311-321 (`order_by_present_without_topn_match_withholds_per_shard_limit`), lines 705-715 (`row_scan_decline_order_by_no_limit_wraps_outer_order_by`)
- Issue: The chained calls start in column 0, because rustfmt skips a statement it cannot fit into the line width. Both chains repeat the `LINEITEM` columns and the `L_EXTENDEDPRICE` descending `orderBy` literal that `nq4_request` in `test_support_tests.rs` already builds, and the first spells `.limit(20)` as a raw `limit` field.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/topn_tests.rs, bind the `orderBy` JSON literal to a `let` above each chain, replace `.field("limit", serde_json::json!({"numElements": 20}))` with `.limit(20)`, write `.field("type", "select")` in place of `.field("type", serde_json::json!("select"))`, then run `cargo fmt --all` and confirm that no line of either chain starts in column 0.

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [SHRINKABLE] Two fixtures patch column notes by hand instead of through the support helper
- Location: lines 377-378 (`unity_kind_pushdown_routes_to_the_unity_catalog_loader`), lines 1450-1454 (`role_join_pushdown_request`)
- Issue: Both fixtures index into `involvedTables[...]["columns"][0]["adapterNotes"]` after `build()`. The support module already offers `with_notes(request, table, notes)`, which sets a column's note by table and column name. `specs/testing.md` § Test code asks a new test to start from the support helpers.
- Fix: In crates/lakehouse-engine/src/adapter/adapter_tests.rs, replace the assignment at lines 377-378 with `let req = with_notes(req, "ORDERS", &[("ID".to_string(), <the same note string>.to_string())]);`, and replace the loop at lines 1450-1454 with two `with_notes` calls, one for `CUSTOMER` and `C_CUSTKEY` and one for `ORDERS` and `O_CUSTKEY`, carrying the same note strings. Import `with_notes` from `crate::adapter::pushdown::test_support`, and run `cargo test --workspace --lib adapter::tests::`.

### crates/lakehouse-e2e/tests/e2e_local/capability.rs

#### [INFORMATION_COUPLING] Two tests read a native copy that an earlier helper call left behind
- Location: line 3126 (`cast_to_a_non_emittable_target_matches_native`), line 3234 (`strict_type_check_accepts_every_request_shape`); crates/lakehouse-e2e/tests/common/e2e_harness.rs lines 1004-1011
- Issue: Both tests query `NATIVE_COPY` after `assert_filters_match_native_copy` or `assert_queries_match_native_copy` returns. They rely on the table those assertion helpers create as a side effect, which neither helper's doc promises. The first test also depends on the column set (`C_VARCHAR, C_TS`) that the earlier call chose. `NATIVE_COPY` was made `pub` only for these reads.
- Fix: In crates/lakehouse-e2e/tests/common/e2e_harness.rs, make `create_native_copy` `pub`. In crates/lakehouse-e2e/tests/e2e_local/capability.rs, call `create_native_copy(&mut conn, &table, "C_VARCHAR")` immediately before the native HASHTYPE query in `cast_to_a_non_emittable_target_matches_native`, and `create_native_copy(&mut conn, &table, TYPED_COLUMNS)` immediately before the native `RANDOM()` count in `strict_type_check_accepts_every_request_shape`. Keep both queries on `NATIVE_COPY`.

### crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs

#### [DUPLICATE_TEST] The bare-TIMESTAMP filter test compares the same filters with native twice
- Location: lines 314-320 and 321, `cast_to_bare_timestamp_filters_at_milliseconds_like_native`
- Issue: `assert_filters_match_native_copy` already compares the `ID` list of each filter with the native copy. The following `counts` block compares `COUNT(*)` of the same filters with a second native copy, which equal `ID` lists already imply, and recreates the copy. The loop after it compares each count with the literal list `[2, 2, 2]`.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs, delete the `counts` binding and its `assert_queries_match_native_copy` call. Declare `const SEEDED_PAIR_ROWS: i64 = 2;` with the doc comment `/// Each filter matches one seeded pair of rows.`, change `for (filter, want) in filters.iter().zip([2, 2, 2])` to `for filter in &filters`, and compare the count with `SEEDED_PAIR_ROWS`. Remove the `assert_queries_match_native_copy` import if no other test of the file uses it.

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [TOO_MANY_ARGUMENTS] The parsed select-list types travel beside the request JSON through 15 functions
- Location: `select_list_types` and `declared_select_type` (lines 1058-1101); crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs line 822
- Issue: The change adds `select_types: &[ExasolType]` next to `pushdown_req: &Json` in 15 functions: `build_dispatch_sql`, `classify_request_shape`, `detect_group_by_aggregates`, `detect_aggregates`, `group_key_exasol_types`, `project_columns`, `empty_result_sql`, `EmptyRelation::pushdown`, `plan_join`, `render_broadcast_join`, `outer_wrapper_clauses`, `n_scan_join_select_items`, `build_n_scan_join_sql`, `build_qualified_single_table_fallback_sql`, and `qualified_single_table_fallback_pushdown`. Every caller derives both values from one request, but no type ties them together. A caller can pass a list on which the count check never ran. For that reason `declared_select_type` returns `Option`, and every consumer carries a decline arm for an index the count check rules out. The added parameter pushed `build_qualified_single_table_fallback_sql` past clippy's argument limit, and the change silenced the lint with a new `#[allow(clippy::too_many_arguments)]`.
- Fix:
  1. In crates/lakehouse-engine/src/adapter/pushdown/support.rs, add `pub struct PushdownRequest { json: Json, select_types: Vec<ExasolType> }`. Its doc comment states that it pairs the `pushdownRequest` JSON with the select-list types Exasol declared for it, and that `parse` is its only constructor, so every holder has passed the strict type and count check. Turn `select_list_types` into `pub fn parse(json: Json) -> Result<Self, UdfError>` with the same body and errors. Add the accessors `json(&self) -> &Json`, `select_types(&self) -> &[ExasolType]`, `select_type(&self, select_index: usize) -> Option<ExasolType>` (which replaces the free function `declared_select_type`), and `with_json(&self, json: Json) -> Self`, which keeps the types for the qualified copy that `render_broadcast_join` builds.
  2. Re-export `PushdownRequest` from `adapter::pushdown` next to `detect_aggregates`, because `crates/lakehouse-engine/tests/it/scan_plan_shape.rs` calls `detect_aggregates`.
  3. In `handle_pushdown` (adapter/pushdown/mod.rs), build one `PushdownRequest` from the cloned `pushdownRequest` in place of the `select_list_types` call.
  4. In each of the 15 functions listed above, replace the parameter pair `pushdown_req: &Json, select_types: &[ExasolType]` with `pushdown: &PushdownRequest`, and read the JSON through `pushdown.json()`. `single_group_plan_types`, `constant_projection_sql`, and `empty_select_list_typed_sql` take only the types and keep their slice parameter. In `render_broadcast_join`, pass `&pushdown.with_json(qualified_req)` to `project_columns`.
  5. Delete `#[allow(clippy::too_many_arguments)]` from `build_qualified_single_table_fallback_sql` (crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs line 822).
  6. In crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs, make `PushdownRequestBuilder::build_pushdown` return a `PushdownRequest` built through `PushdownRequest::parse`, panicking with the parse error. Change the `select_types(request)` helper to return `PushdownRequest::parse(pd(request))`. Update every test call site. A test that pins an absent, invalid, or miscounted list calls `PushdownRequest::parse` on the JSON from `build()`. In `select_list_data_types_check_answers_per_request_shape`, compare `parse(...).map(|request| request.select_types().to_vec())` with the expected outcome.
  7. Keep every assertion and every golden file unchanged (`specs/testing.md` § Regression guards). Run `cargo test --workspace` and `make fmt-check lint`.
