# Tasks: change-exasol-type-model

## Phase 2: Implementation (Group A: E2E reproductions and docs)
- [x] 1.1 `capability.rs`: turn `e2e_both_dialects_unrenderable_predicate_errors_without_rows` into `cast_to_a_non_emittable_target_matches_native`
- [x] 1.2 `capability.rs`: add `non_emittable_select_items_match_native` and the `assert_queries_match_native_copy` harness helper
- [x] 1.3 `capability.rs`: add `typed_null_select_item_keeps_its_declared_type`
- [x] 1.4 `capability.rs`: add `strict_type_check_accepts_every_request_shape`
- [x] 1.5 `timestamp_precision.rs`: bare TIMESTAMP EMITS at precision 3, `cast_to_bare_timestamp_filters_at_milliseconds_like_native`, `cast_to_timestamp_with_local_time_zone_matches_native`, `specs/testing.md` oracle amendment, scenario tags
- [x] 1.6 `scan.rs`: add `merge_casts_only_values_whose_type_merging_changes`
- [x] 1.7 `docs/capability-matrix.md`: CAST note
- [x] 1.8 `docs/install.md`: Exasol 8.29.x prerequisite
- [x] 1.9 `docs/semantic-differences.md`: `## CAST to text` section

## Phase 2: Implementation (Group B: Shared type model and CAST rendering)
- [x] 2.1 Move `ExasolType`, `exasol_type_from_json`, `exasol_type_to_json` into `crates/vs-expression/src/exasol_type.rs`
- [x] 2.2 Model the type per the issue comment's decisions 2, 6, and 7 [expert]
- [x] 2.3 `vs-expression/src/lib.rs`: `render_cast_target` through `exasol_type_from_json` in both dialects

## Phase 2: Implementation (Group C: Adapter strict input, routing, and merge casts)
- [x] 3.1 Request builder in `test_support_tests.rs`, move every request fixture to it
- [x] 3.2 Parse `selectListDataTypes` once per request with the lenient parser [expert]
- [x] 3.3 Make the moved parser strict, with every caller [expert]
- [x] 3.4 Route items the scan cannot compute through `is_udf_emittable()`
- [x] 3.5 Merge casts: `cast_to_declared_type(expr, ExasolType)` and uncast MIN/MAX [expert]
- [x] 3.6 `joins/sql_builders.rs`: typed NULL literal item in the wrapper

## Phase 3: Verification
- [x] 4.1 `cargo test --workspace`
- [x] 4.2 `make fmt-check lint`
- [x] 4.3 `make cross-udf-build`
- [x] 4.4 `make e2e-up test-e2e` (Exasol 2025.1.16)
- [x] 4.5 `EXASOL_IMAGE=exasol/docker-db:8.29.13 make e2e-up test-e2e` (Exasol 8.29.13)
- [x] 4.6 Manual testing table of the plan

## Phase 4: Review Fixes
- [x] 4.7 `vs-expression/src/exasol_type.rs`: bound `decimal_from_json` precision to 1..=`MAX_DECIMAL_PRECISION` and scale to 0..=`MAX_DECIMAL_PRECISION` through `bounded_field`
- [x] 4.8 `vs-expression/src/exasol_type.rs`: doc comments on `Decimal::precision`, `Decimal::scale`, and `ExasolType::timestamp`
- [x] 4.9 `vs-expression/src/lib.rs`: replace the `render_cast_target` doc comment with the two-line text naming (#587)
- [x] 4.10 `types/exasol_type.rs`: re-export every item of `vs_expression::exasol_type`, move the seven direct imports to the engine path
- [x] 4.11 `grouped_agg.rs`: `group_key_exasol_types` falls back to `GROUP_KEY_EMIT_TYPE`
- [x] 4.12 `scalar_over_agg.rs`: `cast_merge_items` zips the declared types, drops the uncast arm and its doc sentence; `support_tests.rs` fixtures pass declared types
- [x] 4.13 `single_group_agg_tests.rs`: delete `single_group_scalar_over_aggregate_defaults_declared_type_when_absent`
- [x] 4.14 `single_group_agg_tests.rs`: delete the `/// Scenario:` line above `an_aggregate_whose_declared_type_the_scan_cannot_emit_declines`
- [x] 4.15 `grouped_agg_tests.rs`: delete the three redundant `/// Scenario:` lines
- [x] 4.16 `grouped_agg_tests.rs`: bind the inline JSON of the two unformatted builder chains to `let`s, `.field("type", "select")`, `cargo fmt --all`
- [x] 4.17 `support_tests.rs`: delete the two redundant `/// Scenario:` lines
- [x] 4.18 `joins/sql_builders_tests.rs`: delete the `/// Scenario:` line above `wrapper_casts_a_null_literal_to_its_declared_type`
- [x] 4.19 `joins/sql_builders_tests.rs`: bind the `having`, `orderBy`, and `SUM(CASE ...)` JSON of the two unformatted chains to `let`s, `cargo fmt --all`
- [x] 4.20 `capabilities_tests.rs`: replace the inline COUNT literal with `agg_item("COUNT", None, false)`, `cargo fmt --all`
- [x] 4.21 `refused_columns_tests.rs`: `agg_item` for COUNT, bind the filters to `let`s, `.field("type", "select")`, `cargo fmt --all`
- [x] 4.22 `topn_tests.rs`: bind the `orderBy` JSON to `let`s, `.limit(20)`, `.field("type", "select")`, `cargo fmt --all`
- [x] 4.23 `adapter_tests.rs`: patch column notes through `with_notes` in the two fixtures
- [x] 4.24 `e2e_local/capability.rs`: make `create_native_copy` `pub` and call it before the two `NATIVE_COPY` reads
- [x] 4.25 `e2e_local/timestamp_precision.rs`: drop the duplicate `counts` comparison, compare with `SEEDED_PAIR_ROWS`
- [x] 4.26 `support.rs`: `PushdownRequest` pairs the request JSON with its parsed select-list types through the 15 functions, drop the `too_many_arguments` allow [expert]
