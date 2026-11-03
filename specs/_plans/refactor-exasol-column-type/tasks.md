# Tasks: refactor-exasol-column-type

## Phase 2: Implementation (Group A: Exasol type and edges)
- [x] 1.1 Add `types/exasol_type.rs` with `ExasolType`, `Display` SQL renderer, family predicates, and pinned byte-exact tests
- [x] 1.2 Make every `types/mapping.rs` input edge return `ExasolType`; `exasol_type_to_json(&ExasolType)` as the one JSON renderer; delete `exasol_type_to_arrow`; switch `build_listing_virtual_tables` and `render_join_sort_target`; update `mapping_tests.rs`

## Phase 2: Implementation (Group B: Pushdown typing)
- [x] 2.1 Carry `ExasolType` in every pushdown producer and carrier; render via `Display` at every `EMITS`/`CAST` site; remove stage-1 `to_string()` calls [expert]
- [x] 2.2 Replace each string consumer with a variant match; delete `classify_exa_type`, `ExaTypeClass`, `parse_decimal_args`, `is_numeric_exasol_type`; pin per-variant answers [expert]
- [x] 3.1 `ColumnTypes` value `ColumnType { exa_type, binary }`; one constructor; resolver builds the map once and returns it; `ResolvedJoinSide` carries it [expert]
- [x] 3.2 Switch every reader to `&ColumnTypes`; delete `BinaryColumns`, `leg_binary_columns`, `extract_projection`, `involved_table_columns`; shared test helper [expert]
- [x] 4.1 Add `ProjectedColumn`; replace parallel type vectors and `projection_types` [expert]
- [x] 5.1 Confirm done checks 1 to 3 with the plan's `rg` commands

## Phase 3: Verification
- [x] 6.1 Run Checklist (build, unit tests, golden SQL diff, local E2E, fmt-check, lint)
- [x] 6.2 Scenario coverage audit and manual testing

## Phase 4: Review Fixes
- [x] 7.1 Delete `ExasolType::is_date` from `types/exasol_type.rs` and the test `date_family_is_date_only` from `types/exasol_type_tests.rs`
- [x] 7.2 Rename `ExasolType::JSON_VARCHAR` to `ExasolType::WIDEST_VARCHAR` with the widest-`VARCHAR` doc comment; rename `only_the_json_fallback_varchar_is_left_uncast` to `only_the_widest_varchar_is_left_uncast` in `support_tests.rs`
- [x] 7.3 Add doc comments to `ExasolType::is_decimal` and to the `ascii` field of `Char` and `Varchar` in `types/exasol_type.rs`
- [x] 7.4 Saturate an out-of-range timestamp precision to `u8::MAX` in the `"timestamp"` arm of `exasol_type_from_json`; add `exasol_type_from_json_saturates_an_out_of_range_timestamp_precision` to `mapping_tests.rs`
- [x] 7.5 Index the declared fields by uppercased name once in `ColumnTypes::of` (first field per name wins) instead of `find` with per-comparison `to_uppercase`
- [x] 7.6 Take the first projected column directly in the fallback of `referenced_column_projection` in `joins/sql_builders.rs`
- [x] 7.7 Add `ExasolType::Varchar { size: 10, ascii: false }` after the `VARCHAR(2000000)` entry of `EXASOL_TYPE_SPELLINGS` (array length 12) in `test_support_tests.rs`
- [x] 7.8 Add `ProjectedColumn` to the import list of `pushdown_surface_probe_tests.rs` between `JoinSides` and `RefusedColumn`
- [x] 7.9 Make the `col_types()` fixture in `request_shape_tests.rs` return `ColumnTypes` and pass `&col_types()` at its 15 call sites
- [x] 7.10 Move `col_types` into `FanOutProjection` (validated in `FanOutProjection::new` against every projected `Column`), drop the parameter and its `too_many_arguments` allow from `build_qualified_single_table_fallback_sql`, update callers and tests, add `fan_out_projection_refuses_a_projected_column_the_map_lacks` [expert]
