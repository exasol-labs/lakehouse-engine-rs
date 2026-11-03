# Code Review Findings: refactor-exasol-column-type

## Summary
- Files reviewed: 40
- Total findings: 10 (standard: 9, expert: 1)
- Behavior preservation holds for every variant match that replaced a string parse. The reviewer compared each consumer against `main` for every variant's rendered string: `guard_like_subject`, `coerce_string_position_arg`, `is_bare_decimal_column`, `declared_timestamp_digits`, `is_valid_emits_output_type`, `cast_to_declared_type`, `char_width`, `sum_emit_type`, `validate_agg_col_types`, `col_type_for`, `extend_projection_with_sort_keys`, `render_join_sort_target`, and the binary map built per table and per leg. The one input-edge change that no spec delta records is the timestamp precision above 255, listed below.
- Done checks 1 to 3 pass: the plan's three searches return the expected lines, and `git diff main -- crates/lakehouse-engine/src/adapter/pushdown/testdata/` is empty.

## Standard fixes

### crates/lakehouse-engine/src/types/exasol_type.rs

#### [UNUSED_FUNCTION] `ExasolType::is_date` has no production caller
- Location: `ExasolType::is_date`
- Issue: Only the test `date_family_is_date_only` calls `is_date`. The two production date checks, in `guard_like_subject` and `coerce_string_position_arg`, match `ExasolType::Date` directly.
- Fix: In crates/lakehouse-engine/src/types/exasol_type.rs, delete the method `ExasolType::is_date`. In crates/lakehouse-engine/src/types/exasol_type_tests.rs, delete the test `date_family_is_date_only`.

#### [IMPLEMENTATION_IN_NAME] `JSON_VARCHAR` names one use of the widest VARCHAR, and its doc comment is wrong for the others
- Location: associated constant `ExasolType::JSON_VARCHAR`
- Issue: The doc comment says the constant is "the catch-all for every type Exasol cannot hold", whose values the scan serializes to JSON text. Several producers use it for plain text that is never JSON: `DataType::Utf8` and `DataType::LargeUtf8` in `compatible_exasol_type`, Iceberg `String` in `iceberg_primitive_to_exasol`, a sizeless `varchar` in `exasol_type_from_json`, and the stringified `GK_{i}` group-key columns in `build_grouped_aggregate_scan_sql`. A reader of `DataType::Utf8 => ExasolType::JSON_VARCHAR` concludes that string columns are JSON-serialized.
- Fix: In crates/lakehouse-engine/src/types/exasol_type.rs, rename the constant `ExasolType::JSON_VARCHAR` to `ExasolType::WIDEST_VARCHAR` with Serena `rename_symbol`, which updates every production and test reference. Replace its doc comment with `/// \`VARCHAR(2000000)\`, the widest \`VARCHAR\`: the declaration of an unbounded string column, and of the JSON text the scan writes for every type Exasol cannot hold.` In crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs, rename the test `only_the_json_fallback_varchar_is_left_uncast` to `only_the_widest_varchar_is_left_uncast`. No rendered SQL changes.

#### [MISSING_DOC_COMMENT] `is_decimal` and the `ascii` fields carry no doc comment
- Location: `ExasolType::is_decimal`; field `ascii` of `ExasolType::Char` and `ExasolType::Varchar`
- Issue: `is_decimal` is a public method without a doc comment, unlike its sibling `is_character`. The public `ascii` field does not state that `false` means Exasol's default `UTF8` character set. That rule is documented only on the private `character_set_suffix`.
- Fix: In crates/lakehouse-engine/src/types/exasol_type.rs, add `/// \`DECIMAL\` at any precision and scale.` above `pub fn is_decimal`. Add `/// Declares the \`ASCII\` character set. \`false\` is Exasol's default, \`UTF8\`.` above the `ascii` field of both `Char` and `Varchar`.

### crates/lakehouse-engine/src/types/mapping.rs

#### [SWALLOWED_ERROR] A timestamp precision above 255 silently reads as bare `TIMESTAMP`
- Location: `exasol_type_from_json`, `"timestamp"` arm, `.and_then(|digits| u8::try_from(digits).ok())`
- Issue: The arm discards the `u8` conversion failure and returns `Timestamp { precision: None }`. `None` is a valid, different declaration: Exasol reads bare `TIMESTAMP` as `TIMESTAMP(3)`. On `main`, `fractionalSecondsPrecision: 300` declared `TIMESTAMP(300)`, which Exasol rejects. On the branch it declares `TIMESTAMP`, and `declared_timestamp_digits` then answers 3 instead of 300, so `timestamp_literal_floor` floors a 6-digit literal that `main` left unchanged. No spec delta records this change, and no test covers it. Exasol caps the precision at 9, so no well-formed request reaches it.
- Fix: In crates/lakehouse-engine/src/types/mapping.rs, in the `"timestamp"` arm of `exasol_type_from_json`, replace `.and_then(|digits| u8::try_from(digits).ok())` with `.map(|digits| u8::try_from(digits).unwrap_or(u8::MAX))`. Put the comment `// Saturates, so an out-of-range precision stays a declaration Exasol rejects and never reads as bare TIMESTAMP.` above that line. Add the test `exasol_type_from_json_saturates_an_out_of_range_timestamp_precision` to crates/lakehouse-engine/src/types/mapping_tests.rs. It asserts that `exasol_type_from_json(&json!({"type": "timestamp", "fractionalSecondsPrecision": 300}))` equals `ExasolType::Timestamp { precision: Some(255) }`.

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [PERFORMANCE_ISSUE] `ColumnTypes::of` matches names in quadratic time with one allocation per comparison
- Location: `ColumnTypes::of`, `declared.iter().find(|field| field.name.to_uppercase() == *name)`
- Issue: For each involved column, `of` scans the declared fields and allocates an uppercased copy of each field name it compares. The work grows with the square of the table width. On `main`, `BinaryColumns::widths` made one pass over the declared fields and uppercased only the binary ones. The reviewer measured a standalone replica of the lookup (`rustc -O`, mean of 5 runs, field names `l_column_name_{i}`): 8.1 ms per table at 1,000 columns and 119 ms at 4,000 columns. A one-pass uppercase index took 0.07 ms and 0.33 ms for the same inputs. `TableScanResolver::resolve` runs `of` once per table per query, so a join pays the cost once per leg.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, in `ColumnTypes::of`, build `let mut declared_by_name: std::collections::HashMap<String, &LogicalField>` before mapping `involved.types`. Insert each `declared` field under `field.name.to_uppercase()` with `declared_by_name.entry(key).or_insert(field)`, so the first field per name wins, as `find` does today. Replace `declared.iter().find(|field| field.name.to_uppercase() == *name)` with `declared_by_name.get(name).copied()`. The test `the_column_type_map_takes_each_binary_layout_from_the_declared_field` must pass unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [SHRINKABLE] The first-column fallback of `referenced_column_projection` selects the first entry through `selected`
- Location: `referenced_column_projection`, `all_cols.selected(all_cols.iter().take(1).map(|(column, _)| column))`
- Issue: To keep the map's first column, the fallback re-looks-up that column through `selected` and `binds_column`, and needs a `mut` rebinding. Taking the first projected column states the same rule directly.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, in `referenced_column_projection`, replace the `let mut referenced = ...;`, `if referenced.is_empty() { ... }`, and `full_projection(&referenced)` lines with: `let referenced = all_cols.narrowed(|column| names.contains(&column.name)); if referenced.is_empty() { return full_projection(all_cols).into_iter().take(1).collect(); } full_projection(&referenced)`. The tests `referenced_column_projection_falls_back_to_first_column` and `referenced_column_projection_narrows_with_a_real_select_list` must pass unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs

#### [MISSING_BOUNDARY_TEST] `EXASOL_TYPE_SPELLINGS` has no `VARCHAR(n)` narrower than 2,000,000
- Location: `EXASOL_TYPE_SPELLINGS`
- Issue: The doc comment promises one value per spelling the SQL rendering takes. The list holds `VARCHAR(2000000)` and `VARCHAR(2000000) ASCII` but no plain `VARCHAR(n)` with `n` below 2,000,000. `cast_to_declared_type` exempts only `VARCHAR(2000000)`, so the narrower spelling is the transition case of that exemption. `only_the_json_fallback_varchar_is_left_uncast` iterates this list and never asserts that a `VARCHAR(10)` target is cast, and no other test does.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs, add `ExasolType::Varchar { size: 10, ascii: false }` to `EXASOL_TYPE_SPELLINGS` directly after the `VARCHAR(2000000)` entry, and change the array type from `[ExasolType; 11]` to `[ExasolType; 12]`. Run the pin tests in support_tests.rs and grouped_agg_tests.rs that iterate the list. The cast pin must then assert `CAST(x AS VARCHAR(10))`.

### crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs

#### [OUTDATED_COMMENT] The in-crate probe states 29 items but lists 28 and omits `ProjectedColumn`
- Location: module doc comment, line 2, and the `use crate::adapter::pushdown::{...}` list
- Issue: The doc comment says "The list holds 29 items". The import list holds 28. `ProjectedColumn`, which `adapter::pushdown` re-exports and the external probe in crates/lakehouse-engine/tests/it/pushdown_public_surface.rs imports, is missing. The external probe's claim to be "a subset of the in-crate probe's 29" is therefore false.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs, add `ProjectedColumn` to the `use crate::adapter::pushdown::{...}` list between `JoinSides` and `RefusedColumn`, so the list holds the 29 items its doc comment states.

### crates/lakehouse-engine/src/adapter/pushdown/request_shape_tests.rs

#### [SHRINKABLE] Every use of the `col_types` fixture wraps it in the same call
- Location: fixture `col_types()` and its 15 call sites
- Issue: `col_types()` returns a tuple list, and all 15 call sites pass `&column_types(&col_types(), &[])`. The fixture exists only to feed that one constructor.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/request_shape_tests.rs, change `col_types()` to return `ColumnTypes` by wrapping its current tuple list in `column_types(&[...], &[])`. Replace each of the 15 `&column_types(&col_types(), &[])` arguments with `&col_types()`.

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [INFORMATION_COUPLING] The single-table fallback reads its projected columns from two parameters that nothing keeps in agreement
- Location: `build_qualified_single_table_fallback_sql`, parameter `col_types` and `let cols_per_leg = vec![col_types.selected(projected_columns)];`
- Issue: The fan-out `EMITS` list comes from `fan_out.columns`. The wrapper's full-row select list comes from `col_types.selected(...)` over the fan-out's `Column` items, and `selected` silently skips a projected column the map lacks. A caller whose map misses a projected column gets a full-row wrapper shorter than the fan-out. On `main`, both lists came from the fan-out alone. The new parameter also raises the arity to 8 and adds the only `#[allow(clippy::too_many_arguments)]` this change introduces. Seven test call sites pass the map separately from the `FanOutProjection` they build.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, add the field `pub col_types: &'a ColumnTypes` to `FanOutProjection` and a third parameter `col_types: &'a ColumnTypes` to `FanOutProjection::new`. In `new`, after the existing length check, return `Err(UdfError::User(format!(...)))` naming the column when a `ProjectionItem::Column(column)` of `spec.common.projection` has no entry in the map (`col_types.get(column).is_none()`). In `build_qualified_single_table_fallback_sql`, delete the `col_types` parameter and its `#[allow(clippy::too_many_arguments)]`, and build `cols_per_leg` from `fan_out.col_types.selected(projected_columns)`. In `qualified_single_table_fallback_pushdown`, pass `col_types` to `FanOutProjection::new` instead of to `build_qualified_single_table_fallback_sql`. Update the `FanOutProjection::new` and `build_qualified_single_table_fallback_sql` calls in `grouped_undecomposable_falls_back_to_qualified_wrapper` (crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs), in `golden_grouped_qualified_fallback_sql_unchanged`, `fallback_projection_narrows_to_referenced_columns`, and `declined_filter_wrapper_sql` (crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs), and in `multi_count_distinct_declines_to_qualified_wrapper` and `declined_filter_wrapper_sql` (crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs). Add the test `fan_out_projection_refuses_a_projected_column_the_map_lacks` to sql_builders_tests.rs: it asserts that `FanOutProjection::new` returns `Err` for a spec projecting `ProjectionItem::Column("Z".into())` against a map without `Z`. Every golden SQL string and `dispatch_golden` fixture must stay byte-identical.
