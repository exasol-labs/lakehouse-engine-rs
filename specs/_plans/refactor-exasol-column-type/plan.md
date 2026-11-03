# Plan: refactor-exasol-column-type

## Summary

Replace every `String` that carries an Exasol SQL type in `lakehouse-engine` with one owned enum, read once at each input edge and rendered by one SQL renderer and one `dataType` JSON renderer, and merge `ColumnTypes` and `BinaryColumns` into one per-column type map keyed by `ColumnRef`. Every declared column type, `EMITS` clause, `CAST` target, and golden SQL string stays byte-identical.

## Context

- Governing issue: #404 and its three addenda. #563 follows this plan. #564 is blocked by it.
- Implementation builds on `main` at `a4a2ca6d`, after PR #580 (#578) and PR #581 (#577) merged. That base has `ColumnNode`, `ColumnRef`, `ColumnTypes`, and the shared request-JSON test builders in `adapter/pushdown/test_support_tests.rs`. Line numbers in the census are against that commit.
- Today an Exasol type travels as `String`, and each consumer parses the part it needs: `classify_exa_type`, `parse_decimal_args`, `sum_emit_type`, `is_numeric_exasol_type`, `char_width`, the `TIMESTAMP(n)` read in `support.rs`, and `exasol_type_to_json`.
- The issue's evidence is partly stale on this branch. `arrow_to_exasol_type` has two production callers, `unity_type_name_to_exasol` no longer exists, and most line numbers moved. The census below replaces the issue's examples.
- A binary column declares the same Exasol type as a text column, so the pushdown keeps a second per-column map, `BinaryColumns`, to compare raw bytes. Both maps share the key and the lookup.
- Risk 1, byte-exact strings: 24 `dispatch_golden` fixtures (18 with `EMITS`) compare full SQL strings, about 700 host unit-test lines assert type strings, and E2E tests assert `EMITS` substrings and `SYS.EXA_ALL_COLUMNS` types.
- Risk 2, wrong rows: Exasol does not re-check a predicate whose capability the adapter advertised. A type check that answers differently for one type changes the pushed SQL and the rows, not the error.
- Format specs: the plan changes no source-to-Exasol mapping. Iceberg table spec § Primitive Types defines `decimal(P,S)` as "Fixed-point decimal; precision P, scale S" with "Scale is fixed, precision must be 38 or less", and `timestamp` as "Timestamp, microsecond precision". Delta protocol § Schema Serialization Format types `timestamp` as "Microsecond precision timestamp elapsed since the Unix epoch". The existing deviations stay as they are: a decimal beyond precision 36 and every nested type declare `VARCHAR(2000000)` JSON, and `timestamptz` declares plain `TIMESTAMP`. Each is a trade-off forced by an Exasol type limit.
- Architecture: `architecture.md` in this directory records that type-mapping owns the Exasol type, that the pushdown planner owns the per-column type map, and the new string-free constraint.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| type-mapping | CHANGED | `scan-types/type-mapping/spec.md` |
| type-mapping-timestamp-precision | CHANGED | `scan-types/type-mapping-timestamp-precision/spec.md` |
| type-relaxation | CHANGED | `scan-types/type-relaxation/spec.md` |
| create-virtual-schema-declaration-details | CHANGED | `vs-adapter/create-virtual-schema-declaration-details/spec.md` |
| pushdown-planning-char-type-declaration | CHANGED | `pushdown-types/pushdown-planning-char-type-declaration/spec.md` |

## Impact

None for users. Declared column types, pushdown SQL, and emitted values do not change. One input that no well-formed request reaches changes its answer, and the `create-virtual-schema-declaration-details` delta names it: the `dataType` JSON of a `CHAR` or `ASCII` type.

## Requirements

| Requirement | Details |
|-------------|---------|
| Done check 1 | `BinaryColumns` no longer exists, and `ColumnTypes` is the one per-column map |
| Done check 2 | No `String` or `&str` carries an Exasol type outside SQL text generation in `crates/lakehouse-engine` |
| Done check 3 | `ExaTypeClass`, `CompatibleExaType`, `classify_exa_type`, `parse_decimal_args`, `is_numeric_exasol_type`, and `exasol_type_to_arrow` are gone. `arrow_to_exasol_type` returns the owned type (decision-log [2]) |
| Done check 4 | The DECIMAL disagreement cannot occur: `ExasolType::Decimal` always carries precision and scale, and the numeric check of task 2.2 matches every `Decimal` and `DoublePrecision` |

## Census of Exasol type strings

Paths are relative to `crates/lakehouse-engine/src/`. S is `adapter/pushdown/support.rs`, G is `adapter/pushdown/grouped_agg.rs`, J is `adapter/pushdown/joins/sql_builders.rs`, R is `adapter/pushdown/joins/rendering.rs`.

- Input edges (producers):
  - `types/mapping.rs`: `exasol_type_from_json` with `character_set_suffix` (VS `dataType`), `arrow_to_exasol_type` and `compatible_exasol_type` (Arrow), `hex_varchar`, `catalog_decimal_to_exasol`, `TimestampPrecision::declaration`, `iceberg_primitive_to_exasol`, `iceberg_type_to_exasol`, `column_source_type_to_exasol`, `spark_type_to_exasol`, `spark_primitive_to_exasol`.
  - Pushdown literals and defaults: S:889 `project_columns` (`VARCHAR(2000000)` for an unresolved column), S:1019 `declared_select_type`, G:220 (`VARCHAR(2000000)` default), G:440 `partial_emits_items` (`DECIMAL(20,0)`, `DOUBLE PRECISION`), G:484 `col_type_for`, `scalar_over_agg.rs`:13 `NESTED_AGGREGATE_PLAN_TYPE`, `scalar_over_agg.rs`:34 `fold_aggregate_plan`, `single_group_agg.rs`:99 (`DOUBLE PRECISION` defaults).
  - VS `dataType` reads in the pushdown: S:336 `involved_columns` (the only source of column types), G:67 `constant_projection_sql`, G:187 `group_key_exasol_types`.
- Consumers that parse or match the string: S:470 `column_exa_type` with `classify_exa_type` at S:484, S:564, S:655; S:761 `declared_timestamp_digits` reads `TIMESTAMP(n)` after trim and uppercase; S:828 `is_valid_emits_output_type`; S:1032 compares with `VARCHAR(2000000)`; G:256 `char_width`; G:489 `sum_emit_type`; G:522 `is_numeric_exasol_type`; `types/mapping.rs` `exasol_type_to_json`; `scan/join_scan.rs`:232 `render_join_sort_target` (`classify_exa_type(&arrow_to_exasol_type(dt))`).
- Carriers (fields and pass-through): `ColumnTypes` S:434 (built at `adapter/pushdown/mod.rs`:201, `joins/mod.rs`:187, S:823, R:22, R:162); `InvolvedColumns.types` S:314; S:45; G:63; J:43 `RenderedJoinPushdown.projection_types`; J:938 `EmptyRelation.cols_per_leg`; `project_columns` S:838 and its callers S:821, J:81, `joins/mod.rs`:187; J:507, J:768, J:807; R:157, R:254; `joins/planning.rs`:251; `topn.rs`:216; `adapter/pushdown/mod.rs`:272; `request_shape.rs`:42; `empty_result.rs`:22; `SingleGroupItem::ScalarOverAggregate.declared_type` (`single_group_agg.rs`); `GroupedSelectItem::ScalarOverAggregate.declared_type` (G); the `declared` parameters of `cast_to_declared_type` (S), `col_type_for` (G), and `fold_aggregate_plan` (`scalar_over_agg.rs`).
- Type vectors (one `String` per projected column, plan slot, aggregate, or group key): `proj_types` in `build_dispatch_sql` (`adapter/pushdown/mod.rs`), `build_scan_driving_sql` and `build_row_scan_sql` (S), `extend_projection_with_sort_keys` (`topn.rs`), `empty_result_sql` and `empty_pushdown_sql` (`empty_result.rs`), `FanOutProjection` (J); `plan_types` in `AggregateMergeInputs` (S), `GroupedAggregateDetection` (G), `single_group_merge_select` (`single_group_agg.rs`), `fold_aggregate_plan` (`scalar_over_agg.rs`); `aggregate_types` in `build_grouped_aggregate_scan_sql` and `partial_emits_items` (G), `cast_merge_items` (`scalar_over_agg.rs`), `empty_grouped_sql` (`empty_result.rs`); `group_key_types` in `blank_pad_char_group_keys` and `build_grouped_aggregate_scan_sql` (G), `empty_grouped_sql`.
- Renderers: `EMITS` at S:145, S:175, S:262, G:340, G:458; `CAST` at S:1032, G:73, G:353, G:379, `scalar_over_agg.rs`:333, `single_group_agg.rs`:139, `empty_result.rs`:61, 78, 81, 85, 98, J:966; `dataType` JSON at `adapter/mod.rs`:476 `build_listing_virtual_tables`.
- Binary map: `BinaryColumns` (`adapter/pushdown/binary_filter.rs`:18) built at `adapter/pushdown/mod.rs`:202, `format/mod.rs`:130, J:380, R:38 `leg_binary_columns`; taken by 8 signatures (`binary_filter.rs`:73, 87, 172, 177, S:793, S:806, R:157, J:58). `ColumnTypes` is taken by 14 signatures in S.
- No Exasol type string: `adapter/pushdown/format/`, `types/widening.rs`, `types/hive_type.rs`, `adapter/column_notes.rs`, `adapter/direct_storage.rs` (calls `needs_json_fallback` on Arrow types), `scan/` except `join_scan.rs`, and `crates/lakehouse-catalog`.
- `crates/vs-expression` `render_cast_target` (`lib.rs`:312) renders a translated `CAST` target from `dataType` JSON inside SQL text generation.
- `crates/lakehouse-e2e` has no engine dependency and renders no type. It reads declared types back from Exasol (`tests/common/e2e_harness.rs`:836 `declared_types`) and cuts the `EMITS` clause out of generated SQL (`tests/e2e_local/fixtures.rs`:124). The type files `tests/e2e_local/emit_declaration.rs`, `timestamp_precision.rs`, and `complex_type.rs` assert rendered strings, `capability.rs`:3147-3345 asserts `CHAR(20)` in pushdown SQL, and `type_relaxation.rs` and `int96_timestamp.rs` assert none. None of these files changes.

## Implementation Tasks

Each stage leaves `cargo test --workspace` green.

### 1. Owned Exasol type and its edges

- [ ] 1.1 Add `types/exasol_type.rs` with `ExasolType`: `Boolean`, `Decimal { precision, scale }` (scale signed, as Arrow's), `DoublePrecision`, `Date`, `Timestamp { precision: Option<_> }` (`None` renders bare `TIMESTAMP`), `TimestampWithLocalTimeZone`, `Char { size, ascii }`, `Varchar { size, ascii }`. `Display` is the one SQL renderer. Add the family predicates that replace `ExaTypeClass` (character is `Char` or `Varchar`). In `exasol_type_tests.rs`, pin one rendered string per spelling the census producers emit today, byte for byte.
- [ ] 1.2 In `types/mapping.rs`, make every input edge return `ExasolType` under its current name: `exasol_type_from_json` (same defaults and caps), `arrow_to_exasol_type`, `compatible_exasol_type` (returns `Option<ExasolType>`, replacing `CompatibleExaType`; `needs_json_fallback` keeps its signature and answers), `hex_varchar`, `catalog_decimal_to_exasol`, `TimestampPrecision::declaration`, and the Iceberg, Spark, and column-source producers. `exasol_type_to_json(&ExasolType)` becomes the one JSON renderer, the inverse of `exasol_type_from_json` for every variant, which the new unit test `exasol_type_to_json_round_trips_every_variant` pins. Delete `exasol_type_to_arrow`. Switch `build_listing_virtual_tables` (`adapter/mod.rs`:476) and `render_join_sort_target` (`scan/join_scan.rs`:232) to the owned type. Pushdown callers call `to_string()` at the boundary until stage 2. Update `types/mapping_tests.rs`: expected strings compare against the rendered type, `exasol_type_to_json` takes enum inputs, and the compile-time pin in `arrow_input_resolver_stays_outside_the_timestamp_version_gate` becomes `fn(&DataType) -> ExasolType`.

### 2. Pushdown consumers read the owned type

- [ ] 2.1 Carry `ExasolType` in every pushdown producer and carrier the census lists, render it through `Display` at every `EMITS` and `CAST` site, and remove the stage-1 `to_string()` calls. The shared test builder `rendered_join` (`adapter/pushdown/test_support_tests.rs`) takes `(ProjectionItem, ExasolType)` pairs in place of `&str` types. [expert]
- [ ] 2.2 Replace each string consumer the census lists with a match on the variant. Each consumer keeps, for every variant, the answer it gives today for that variant's rendered string. The numeric check accepts `DoublePrecision` and every `Decimal`. Delete `classify_exa_type`, `ExaTypeClass`, `parse_decimal_args`, and `is_numeric_exasol_type`. Pin each consumer's answer per variant in its sibling `_tests.rs` file. [expert]

### 3. One per-column type map

- [ ] 3.1 Give `ColumnTypes` the value `ColumnType { exa_type, binary }`, where `binary` is the raw-byte layout the scan reads (variable length, or a fixed width in bytes) and is absent for a non-binary column. One constructor, which replaces `ColumnTypes::unqualified`, takes the involved table's `InvolvedColumns`, the declared `LogicalField`s, and `binary_as_string`, with the width rule of `BinaryColumns::widths`. `per_relation` takes each leg's alias and map and qualifies the map with the alias. `TableScanResolver::resolve` (`scan_resolution.rs`) and `resolve_one_join_side` take the involved table's `InvolvedColumns` (its Exasol types and notes) in place of its `TableNotes`. After `declared_schema()`, `TableScanResolver::resolve` builds the map, retypes the filter's binary literals with `with_binary_literals` and the map, and passes the retyped filter to `ResolvedScan::resolve`. `ResolvedScan::resolve` takes neither `binary_as_string` nor the map, and `format/mod.rs` no longer imports `BinaryColumns` or `with_binary_literals`. `TableScanResolver::resolve` returns the map beside the `ResolvedScan`, and `ResolvedJoinSide` carries it (decision-log [1]). [expert]
- [ ] 3.2 Switch every reader to that map. Each signature below takes `&ColumnTypes` in place of a tuple slice, and a join path takes one map per leg:
  - Filters and binary comparison: `classify_where_filter`, `type_accepted_rewrite`, `compare_binary_bytes`, `with_binary_literals`, and `type_screened_leg_filter` (`joins/rendering.rs`).
  - Single-table dispatch: `build_dispatch_sql` (`adapter/pushdown/mod.rs`); the `EMITS` builders `build_scan_driving_sql`, `build_aggregate_scan_sql`, `build_count_distinct_scan_sql`, and `build_distinct_fan_out` (`support.rs`); `extend_projection_with_sort_keys` (`topn.rs`); `classify_request_shape` (`request_shape.rs`); `empty_result_sql` (`empty_result.rs`).
  - Aggregates (`grouped_agg.rs`): `build_grouped_aggregate_scan_sql`, `partial_emits_items`, `col_type_for`, and `validate_agg_col_types`.
  - Broadcast join: `render_broadcast_join` reads one leg map from the resolved sides, which replaces `leg_column_types` and `leg_binary_columns`.
  - Join planning (`joins/mod.rs`): `plan_join`'s per-side columns and its empty-side union, and `ensure_no_side_refuses_a_referenced_column`.
  - Join rendering: `referenced_leg_columns` (`joins/rendering.rs`), which returns a narrowed map. In `joins/sql_builders.rs`: `n_full_row_qualified_items`, `n_scan_join_select_items`, `outer_wrapper_clauses`, `build_n_scan_join_sql`, `build_side_fan_out_sql`, `referenced_column_projection`, `build_qualified_single_table_fallback_sql` (its one-leg map narrows the resolved map to the projected columns, in projection order), `qualified_single_table_fallback_pushdown`, and `EmptyRelation` with `single_scan` and `join` (`cols_per_leg` holds one map per leg).

  The join paths read each side's map from its `ResolvedJoinSide`, so `involved_table_columns` (`joins/planning.rs`) is removed. The single-table path calls `project_columns` after resolution with the resolved map, and `extract_projection` is removed. Delete `BinaryColumns`. Tests build the map through one helper in `adapter/pushdown/test_support_tests.rs`, beside the `col`, `table_col`, and `aliased_col` builders. It replaces the local map builders `columns` and `columns_under` (`binary_filter_tests.rs`) and `shared_name_universe` (`support_tests.rs`). [expert]

### 4. Projections carry their type

- [ ] 4.1 Add `ProjectedColumn { item: ProjectionItem, exa_type: ExasolType }`. `project_columns` returns `(Vec<ProjectedColumn>, bool)`. Replace the parallel type vectors and `RenderedJoinPushdown.projection_types` at the census sites with that one vector, and remove the zip and unzip code. `rendered_join` in the pushdown test-support module takes `ProjectedColumn`s. [expert]

### 5. Done checks

- [ ] 5.1 Confirm done checks 1 to 3 with `rg -n 'BinaryColumns|ExaTypeClass|CompatibleExaType|classify_exa_type|parse_decimal_args|is_numeric_exasol_type|exasol_type_to_arrow|projection_types' crates/` (no output) and `rg -n '"(DECIMAL|VARCHAR|CHAR|TIMESTAMP|DOUBLE PRECISION|BOOLEAN|DATE)' crates/lakehouse-engine/src -g '!*_tests.rs'` (hits only in `types/exasol_type.rs`, in the VS `dataType` JSON read of a `CAST` node in `rewrite_decimal_stringifications` and its write in `wrap_cast_to_varchar` (`support.rs`), and in the `DATE` error messages of `scan/convert.rs`). Confirm that no tuple slice or `String` carries a column type in the pushdown with `rg -n "\(String, (String|ExasolType)\)|declared_type: String|declared: Option<(&str|String)>|_types: (&('a )?(mut )?)?(Vec<String>|\[String\])" crates/lakehouse-engine/src/adapter/pushdown -g '!*_tests.rs'`. Expected output: two lines, `InvolvedColumns.types` in `support.rs` (the request read that the one `ColumnTypes` constructor consumes) and `first_refused_member` in `format/iceberg.rs` (a field path and a refusal cause, not a type). Then run the Checklist.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Exasol type and edges | 1.1-1.2 | — | spec deltas `scan-types/type-mapping`, `scan-types/type-mapping-timestamp-precision`, `scan-types/type-relaxation`, `vs-adapter/create-virtual-schema-declaration-details`; `crates/lakehouse-engine/src/types/`, `adapter/mod.rs`, `scan/join_scan.rs`, `types/mapping_tests.rs` |
| B: Pushdown typing | 2.1-5.1 | A (consumes `ExasolType` and the edge signatures) | spec delta `pushdown-types/pushdown-planning-char-type-declaration`; `crates/lakehouse-engine/src/adapter/pushdown/` (`support.rs`, `grouped_agg.rs`, `binary_filter.rs`, `scan_resolution.rs`, `format/mod.rs`, `joins/`, `topn.rs`, `single_group_agg.rs`, `scalar_over_agg.rs`, `empty_result.rs`, `request_shape.rs`) and their `_tests.rs` files, `testdata/dispatch_golden/` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `exasol_type_to_arrow` (`types/mapping.rs`:69) | No production caller |
| Enum | `CompatibleExaType` (`types/mapping.rs`:23) | `compatible_exasol_type` returns `Option<ExasolType>` |
| Enum, function | `ExaTypeClass`, `classify_exa_type` (`types/mapping.rs`) | Replaced by `ExasolType` predicates |
| Function | `parse_decimal_args` (`types/mapping.rs`:123) | No type string left to parse |
| Function | `is_numeric_exasol_type` (`adapter/pushdown/grouped_agg.rs`:522) | Match on the variant |
| Struct | `BinaryColumns` and its impl (`adapter/pushdown/binary_filter.rs`:18) | Absorbed by `ColumnTypes` |
| Function | `leg_binary_columns` (`adapter/pushdown/joins/rendering.rs`:38) | One leg map from the resolved sides |
| Function | `extract_projection` (`adapter/pushdown/support.rs`:817) | Its body was one `project_columns` call |
| Function | `ColumnTypes::unqualified` (`adapter/pushdown/support.rs`) | Replaced by the one constructor of task 3.1 |
| Function | `involved_table_columns` (`adapter/pushdown/joins/planning.rs`) | The join paths read the resolved side's map |
| Field | `RenderedJoinPushdown.projection_types` (`adapter/pushdown/joins/sql_builders.rs`:43) | Replaced by `ProjectedColumn` |
| Test | `types/mapping_tests.rs`: the six `exasol_type_to_arrow_*` tests, `exasol_type_to_json_absent_decimal_scale_becomes_scale_zero_decimal`, `exasol_type_to_json_out_of_range_decimal_args_become_varchar`, `exasol_type_to_json_malformed_decimal_arg_lists_stay_varchar`, the malformed-string loop of `exasol_type_to_json_renders_timestamp_fractional_seconds_precision`, `classify_exa_type_matches_pushdown_guard_predicates` | Each tests a string parse that no longer exists; the predicate pins of task 2.2 replace the last one |
| Test | `adapter/pushdown/grouped_agg_tests.rs`: `sum_emit_type_absent_scale_widens_to_a_scale_zero_decimal`, `sum_emit_type_never_echoes_a_non_canonical_scale_text`, `sum_emit_type_declines_every_precision_the_parser_rejects` | Each feeds a type string that no variant can hold |

## Out of scope

- `LogicalField.arrow_type` stays a `String` tag, and the scan-spec wire format does not change.
- The SDK's `ExaType` stays as it is.
- `vs-expression`'s `render_cast_target` keeps rendering `CAST` targets from `dataType` JSON in its own crate, including its `DOUBLE` spelling.
- The pushdown type checks keep treating a `TIMESTAMP` column as an unknown type; changing that changes behavior.
- Per-query settings threaded through the pushdown paths stay with #563.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| type-mapping: A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR | Unit (pure mapping) | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `catalog_decimal_guard_is_shared_by_both_source_kinds` |
| type-mapping: The Iceberg-to-Arrow logical mapping reads the same catalog-decimal guard | Unit (pure mapping) | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `catalog_decimal_guard_is_shared_by_both_source_kinds` |
| type-mapping-timestamp-precision: A declared TIMESTAMP(p) EMITS column maps back to the Arrow unit of that precision | E2E | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_digit` |
| create-virtual-schema-declaration-details: A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision | E2E | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` |

- The `CHAR` and `ASCII` step of the last scenario is unreachable through Exasol, so the unit test `exasol_type_to_json_round_trips_every_variant` in `crates/lakehouse-engine/src/types/mapping_tests.rs` proves it.
- The Background-only deltas (`type-relaxation`, `pushdown-planning-char-type-declaration`, and the Background of `type-mapping-timestamp-precision` and `create-virtual-schema-declaration-details`) change no behavior.
- Regression guard (`specs/testing.md` § Regression guards): every existing assertion, golden SQL string, and `dispatch_golden` fixture stays unchanged, apart from call sites and the tests listed under Dead Code Removal.

### Manual Testing

`$DSN` is `exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0`, with the Makefile's values, after `make e2e-up`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| type-mapping, type-relaxation, type-mapping-timestamp-precision | `exapump sql "SELECT COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS ORDER BY 1, 2, 3" -d "$DSN"` after `make test-e2e` provisioned its virtual schemas, on `main` and on the branch | Identical output |
| create-virtual-schema-declaration-details | `scripts/capture-pushdown-payload.sh 'SELECT C_TS, C_DECIMAL_A, C_VARCHAR FROM {table}'` on `main` and on the branch | Identical `EMITS` clause, with `TIMESTAMP(6)` for `C_TS` on Exasol 2025.x |
| pushdown-planning-char-type-declaration | `scripts/capture-pushdown-payload.sh "SELECT CAST(C_VARCHAR AS CHAR(20)) FROM {table}"` on `main` and on the branch | Identical pushdown SQL with `CHAR(20)` in `EMITS` |

### Checklist

How the suites are built and run: `specs/mission.md` § Commands and `specs/testing.md` § E2E suites.

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace` | 0 failures |
| Golden SQL unchanged | `git diff --exit-code main -- crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/` | Exit 0 |
| E2E, local, Exasol 2025.x | `make e2e-up test-e2e` | 0 failures |
| E2E, local, Exasol 8.x | `EXASOL_IMAGE=exasol/docker-db:8.29.13 make e2e-up test-e2e` | 0 failures |
| E2E, Lakekeeper | `make lakekeeper-up test-e2e-lakekeeper` | 0 failures |
| E2E, Unity Catalog | `make unity-up test-e2e-unity` | 0 failures |
| E2E, Glue (needs AWS credentials, else the `E2E (Glue)` CI job) | `make glue-up test-e2e-glue` | 0 failures |
| Lint and format | `make fmt-check lint` | Exit 0 |
