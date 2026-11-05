# Verification Report: change-exasol-type-model

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Unit tests, lint, format, the `.so` build, and the local E2E suite on Exasol 8.29.13 and 2025.1.16 pass, and every manual test that the local stack can run matches its expected output. |
| Code review | 20 findings — 20 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | 2125 | 2123 | 2 |
| E2E, local suite on Exasol 2025.1.16 (`make e2e-up test-e2e`) | 355 | 355 | 0 |
| E2E, local suite on Exasol 8.29.13 (`EXASOL_IMAGE=exasol/docker-db:8.29.13 make e2e-up test-e2e`) | 355 | 355 | 0 |

The 2 ignored unit tests predate this plan. The Unity Catalog suite was not run: `.speq/implement-pr-hook.md` limits the run to the local E2E suite unless the plan changes a feature that only a remote suite exercises, and this plan changes no Unity Catalog behavior. CI runs it on the PR.

### Manual Tests

The manual tests ran on Exasol 2025.1.16 against `MY_LAKEHOUSE` and `DIRECT_LAKEHOUSE`. The local E2E suite covers 8.29.13; the manual queries were not repeated there.

| Test | Result |
|------|--------|
| `SELECT ID, CAST(C_TS AS TIMESTAMP WITH LOCAL TIME ZONE)` | ✓ 12 rows, first `1,2024-01-01T00:00:00.100`, no dialect error |
| `EXPLAIN VIRTUAL ... WHERE CAST(C_VARCHAR AS GEOMETRY) IS NULL` | ✓ outer `WHERE (CAST("LHS_T0"."C_VARCHAR" AS GEOMETRY(0)) IS NULL)` |
| `SELECT ID, C_TS - C_TS` | ✓ 12 rows of `+000000000 00:00:00.000000000`, no `Data type mismatch` |
| `SELECT ID, CAST(NULL AS GEOMETRY)` | ✓ 12 rows, NULL in the second column |
| `SELECT *` and `SELECT RANDOM()` | ✓ 12 rows each, no strict-input error |
| `EXPLAIN VIRTUAL SELECT ID, C_QTY + 1` | ✓ the scan computes the item, `EMITS (..., "_LH_PROJ_1" DECIMAL(21,0))` |
| `EXPLAIN VIRTUAL SELECT MIN(C_TS), MAX(C_TS), SUM(C_QTY)` | ✓ `MIN("PARTIAL_min_0"), MAX("PARTIAL_max_1"), CAST(SUM("PARTIAL_sum_2") AS DECIMAL(31,0))` |
| `EXPLAIN VIRTUAL SELECT MIN(C_QTY + 1), SUM(C_QTY + 1)` | ✓ uncast MIN, `CAST(SUM(...) AS DECIMAL(32,0))` |
| `EXPLAIN VIRTUAL SELECT MIN(C_QTY), ROUND(SUM(C_QTY), 2)` | ✓ uncast MIN, `CAST(ROUND(SUM(...), 2) AS DECIMAL(31,0))` |
| `EXPLAIN VIRTUAL SELECT C_VARCHAR, MIN(CAST(C_VARCHAR AS CHAR(20))) ... GROUP BY C_VARCHAR` | ✓ `EMITS ("GK_0" VARCHAR(2000000), "PARTIAL_min_0" CHAR(20))`, merge `MIN("PARTIAL_min_0")` uncast |
| `EXPLAIN VIRTUAL ... WHERE C_DATE LIKE '2024%'` | ✓ scan filter `(CAST("C_DATE" AS VARCHAR) LIKE '2024%')` |
| `SELECT ID, LOWER(C_DATE)` | ✓ `2024-01-01`, `2024-01-02`, ... |
| `SELECT C_TS - C_TS, COUNT(*) ... WHERE ID < 0 GROUP BY C_TS - C_TS` | ✓ 0 rows, no error |
| `DIRECT_LAKEHOUSE.ALL_TYPES` decimal column types | ✓ `DECIMAL(18,0)`, `DECIMAL(36,6)`, `VARCHAR(2000000) UTF8` for 38,10 and 256-bit 50,2 |
| `MY_LAKEHOUSE.TYPED_DISTINCT_PROBE` column types | ✓ unchanged, `C_TS` is `TIMESTAMP(6)` |
| Delta Schema Type Mapping (`UNITY_DELTA_E2E_VS`) | not run: needs the Unity Catalog stack, which the project hook excludes |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.88s
```

### Formatter

```
cargo fmt --all -- --check
(no output, exit 0)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| sql-comprehension | vs-expression-translator-cast | CAST renders the mapped target type per dialect | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `cast_to_a_non_emittable_target_matches_native` | Pass |
| sql-comprehension | vs-expression-translator-cast | CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs` | Pass |
| sql-comprehension | vs-expression-translator-cast | The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `non_emittable_select_items_match_native` | Pass |
| sql-comprehension | vs-expression-translator-cast | A CAST to TIMESTAMP without a precision compares at three fractional digits | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `cast_to_bare_timestamp_filters_at_milliseconds_like_native` | Pass |
| sql-comprehension | vs-expression-translator-cast | A CAST to TIMESTAMP WITH LOCAL TIME ZONE keeps its declared precision | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `cast_to_timestamp_with_local_time_zone_matches_native` | Pass |
| pushdown | pushdown-declined-filter-self-apply | A predicate unrenderable under both dialects returns a clean error | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `single_table_wrapper_errors_when_declined_predicate_renders_in_neither_dialect`, `n_scan_wrapper_errors_when_declined_predicate_renders_in_neither_dialect` | Pass |
| pushdown-capabilities | pushdown-planning-capability-extensions | An INTERVAL, GEOMETRY, or HASHTYPE select-list item, or a TIMESTAMP WITH LOCAL TIME ZONE aggregate or GROUP BY key, is computed by Exasol over the sharded scan | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `non_emittable_select_items_match_native` | Pass |
| pushdown | pushdown-planning-literal-projection | A NULL select item in an Exasol-rendered wrapper keeps its declared type | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `typed_null_select_item_keeps_its_declared_type` | Pass |
| pushdown | pushdown-planning | A pushdown request with a missing or invalid Exasol type fails the query | `crates/vs-expression/src/exasol_type_tests.rs`, `crates/vs-expression/src/lib_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` | `exasol_type_from_json_refuses_an_incomplete_or_out_of_range_type`, `cast_to_an_incomplete_target_is_refused_in_both_dialects`, `select_list_data_types_check_answers_per_request_shape`, `involved_column_with_an_unparsable_type_fails`, `unprojected_group_key_with_an_invalid_cast_type_fails` | Pass |
| pushdown | pushdown-planning | Every request of a supported Exasol version passes the strict type check | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `strict_type_check_accepts_every_request_shape` | Pass |
| pushdown | pushdown-planning | Projected CAST expression preserves the declared TIMESTAMP fractional-seconds precision in its EMITS type | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_digit`, `cast_to_timestamp_with_local_time_zone_matches_native` | Pass |
| pushdown-aggregates | pushdown-planning-single-group-agg | The merge casts a merged value only when merging can change its type | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `merge_casts_only_values_whose_type_merging_changes` | Pass |
| pushdown-aggregates | pushdown-planning-expression-aggregate | Expression-argument partial and merge column types come from the declared aggregate type | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `merge_casts_only_values_whose_type_merging_changes` | Pass |
| pushdown-aggregates | pushdown-planning-single-group-agg-scalar-over-aggregate | Scalar-over-aggregate and plain aggregate items interleave in select-list order with per-item declared types | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `merge_casts_only_values_whose_type_merging_changes` | Pass |
| pushdown-types | pushdown-planning-char-type-declaration | A MIN or MAX over a CHAR-typed expression declares a CHAR partial column | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `merge_casts_only_values_whose_type_merging_changes` | Pass |
| pushdown-types | pushdown-planning-char-type-declaration | A VARCHAR-declared type is unaffected | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs`, `crates/vs-expression/src/exasol_type_tests.rs` | `group_key_exasol_types_resolves_varchar_key_unchanged`, `varchar_declared_group_key_is_left_unpadded`, `display_renders_every_spelling_the_producers_emit_byte_for_byte` | Pass |
| pushdown-types | pushdown-planning-like-type-coercion | LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `like_guard_date_subject_wraps_cast` | Pass |
| pushdown-types | pushdown-planning-like-type-coercion | A select-list LIKE over a DATE column projects the CAST-to-VARCHAR form | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `selectlist_like_over_date_projects_cast_expr` | Pass |
| pushdown-types | pushdown-planning-string-fn-type-coercion | A string-position DATE column argument is wrapped in an explicit CAST to VARCHAR | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `string_fn_guard_casts_date_argument_to_varchar` | Pass |
| file-planning | pushdown-planning-empty-result | Empty-result shape matches the plan the non-empty path would commit to | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `non_emittable_select_items_match_native` | Pass |
| scan-types | type-mapping | In-range Decimal128 maps to a precise Exasol DECIMAL | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `all_types_directories_declare_and_return_their_mapped_values` | Pass |
| scan-types | type-mapping | Out-of-range Decimal128 falls back to VARCHAR via JSON | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `all_types_directories_declare_and_return_their_mapped_values` | Pass |
| scan-types | type-mapping | Incompatible Arrow types are serialized to JSON VARCHAR | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `incompatible_types_map_to_varchar_json` | Pass |
| scan-types | type-mapping | A Parquet-sourced column maps through the Arrow-input direction | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `all_types_directories_declare_and_return_their_mapped_values` | Pass |
| delta | delta-type-mapping | Every Delta type Exasol represents natively maps to its own Arrow tag | `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `unity_delta_varied_types_return_their_expected_exasol_types_and_values` | Not run locally (Unity suite, CI) |
| delta | delta-type-mapping | A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `decimal_outside_exasol_domain_maps_to_utf8` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | A decimal initial-default outside Exasol's catalog-decimal domain is not encoded as a numeric default | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `iceberg_declaration_omits_default_for_decimal_outside_exasol_domain` | Pass |
| vs-adapter | create-virtual-schema-declaration-details | A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision | `crates/vs-expression/src/exasol_type_tests.rs` | `exasol_type_to_json_round_trips_every_variant` | Pass |
| pushdown | pushdown-planning-selectlist-expressions | Scalar select-list expression is pushed into the scan-driving query | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `selectlist_predicate_node_projects_as_expr` | Pass |

The Delta native-type scenario changes wording only. Its unit counterpart `every_natively_representable_delta_type_maps_to_its_own_arrow_tag` passes, and CI runs the Unity Catalog suite on the PR.

## Notes

- `cast_to_a_non_emittable_target_matches_native` asserts the error prefix `data exception - Invalid hash format: `, the text native Exasol returns over the WebSocket, instead of the plan's `Invalid hash format: `.
- The strict parser also refuses a `characterSet` other than UTF8 or ASCII and a non-boolean `withLocalTimeZone`, which Exasol never sends.
- `PushdownRequest` in `adapter/pushdown/support.rs` holds the request JSON and its once-parsed select-list types (decision-log [3]). The pushdown surface probe tests (`pushdown_surface_probe_tests.rs`, `tests/it/pushdown_public_surface.rs`) do not list it.
- A scalar-over-aggregate item of a type the scan cannot emit is not declined, because no `EMITS` column carries it and Exasol computes it in the merge.
