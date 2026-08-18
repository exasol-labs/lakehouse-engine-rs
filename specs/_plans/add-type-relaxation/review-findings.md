# Code Review Findings: add-type-relaxation

## Summary
- Files reviewed: 20
- Total findings: 14 (standard: 10, expert: 4)

Verification run during review: `cargo fmt --check` clean; `cargo clippy --all-targets` clean;
`cargo clippy --all-targets --features exasol-e2e,unity-e2e` clean; the new/changed unit suites
(`scan::type_relaxation`, `delta_schema::tests`, `iceberg::tests`) all pass. No E2E stack was
available, so every E2E finding below is derived from the vendored fixture's own `_delta_log`
statistics rather than from a live run.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs

#### [OUTDATED_COMMENT] `build_delta_table_schema`'s doc claims a `UdfError` has exactly one cause
- Location: lines 42-44
- Issue: the doc still reads "A [`UdfError`] surfaces from this call only when a MAPPABLE column
  carries a malformed column-mapping annotation, below." That is false since this change: the new
  `recorded_type_changes(field)?` at line 83 propagates `malformed_type_change` for a malformed
  `delta.typeChanges` annotation, which is a second, unrelated cause and fires BEFORE the
  binding-key lookup the sentence points at.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs, rewrite the
  `build_delta_table_schema` doc sentence at lines 42-44 so it names BOTH `UdfError` causes: a
  MAPPABLE column carrying a malformed column-mapping annotation, and any column carrying a
  malformed `delta.typeChanges` annotation. State that an UNSUPPORTED (as opposed to malformed)
  recorded change refuses only its own column and never fails the call.

#### [OUTDATED_COMMENT] `ClassifiedDeltaColumn`'s doc asserts a single-meaning `UdfError`
- Location: lines 218-219
- Issue: "That leaves a [`UdfError`] out of [`build_delta_table_schema`] meaning exactly one thing:
  a MAPPABLE column carries a malformed column-mapping annotation." The malformed-`delta.typeChanges`
  path added at line 83 makes this a second meaning, so the stated invariant no longer holds.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs, update the
  `ClassifiedDeltaColumn` doc comment at lines 218-219 to say a `UdfError` out of
  `build_delta_table_schema` means a MALFORMED annotation — either column-mapping or
  `delta.typeChanges` — and that a refused column is still answered as a value, never as an error.

#### [DEAD_FLEXIBILITY] `RecordedTypeChange::field_path` is written and never read
- Location: lines 327, 383-392, 442-444
- Issue: `field_path` is parsed, type-checked, stored on the struct, and never read by any production
  code path — `is_supported_type_change`'s own doc (lines 442-444) states "`field_path` is
  deliberately not read", and `specs/_plans/add-type-relaxation/vs-adapter/delta-type-mapping/spec.md`
  requires only that an entry carrying a `fieldPath` be validated by its pair alone. The only
  consumers are three test assertions. Its sole production effect is that a non-string `fieldPath`
  fails the whole table for a key the design deliberately ignores, while an unrecognized key such as
  `tableVersion` is silently ignored — an inconsistency with no spec backing.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs, delete the
  `field_path` member from `RecordedTypeChange` and stop capturing it in `parse_type_change_entry`;
  keep the non-string `fieldPath` shape check as a validation-only step that returns no value, and
  update the `RecordedTypeChange` doc comment accordingly. In
  crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs, drop the
  `changes[i].field_path` assertions in `parses_fromtype_totype_ignoring_the_superseded_tableversion_key`
  and `parses_multiple_entries_and_an_optional_field_path` (rename the latter to
  `parses_multiple_entries_and_ignores_an_optional_field_path`), and build the two literals in
  `an_entry_carrying_a_field_path_is_validated_by_its_pair_alone` through the existing `type_change`
  helper instead of the struct literal. Keep
  `a_non_string_field_path_is_refused_naming_the_column` unchanged.

#### [DEAD_FLEXIBILITY] New `delta.typeChanges` items are `pub(super)` with no caller outside the module
- Location: lines 324, 325-327, 348, 449
- Issue: `RecordedTypeChange` (and each of its members), `recorded_type_changes`, and
  `is_supported_type_change` are declared `pub(super)`, exporting them to the whole
  `adapter::pushdown::format` module, but a repo-wide grep finds no user outside
  `delta_schema.rs` itself. The sibling test module (`#[path = "delta_schema_tests.rs"] mod tests`)
  is a CHILD of `delta_schema` and reaches private items through `use super::*` already, so the
  widened visibility buys nothing and invites a second, unintended consumer.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs, drop `pub(super)`
  from `RecordedTypeChange`, from each of its members, from `recorded_type_changes`, and from
  `is_supported_type_change`, making all four private to the module. Leave `build_delta_table_schema`
  `pub(super)` — `delta_format_reader.rs` calls it.

#### [MIXED_ABSTRACTION_LEVEL] `build_delta_table_schema`'s loop inlines the type-change lookup
- Location: lines 83-92
- Issue: the loop body delegates two of its three decisions to named helpers (`classify_delta_type`,
  `binding_key`) but inlines the third as a `recorded_type_changes(field)?.into_iter().find(...)`
  chain plus a manual `RefusedColumn` push, mixing orchestration with the detail of how an
  unsupported change is located.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs, extract lines 83-85
  into a private helper `fn unsupported_type_change(field: &StructField) -> Result<Option<RecordedTypeChange>, UdfError>`
  that returns the first recorded change failing `is_supported_type_change`, and rewrite the loop
  body to `if let Some(change) = unsupported_type_change(field)? { ... }` so all three decisions read
  at one level.

### crates/lakehouse-engine/tests/common/type_promotion_fixtures.rs

#### [UNUSED_VARIABLE] Six source/target-type constants have no consumer
- Location: lines 46-62
- Issue: `INT_LONG_SOURCE_TYPE`, `INT_LONG_TARGET_TYPE`, `FLOAT_DOUBLE_SOURCE_TYPE`,
  `FLOAT_DOUBLE_TARGET_TYPE`, `DECIMAL_DECIMAL_SOURCE_TYPE`, and `DECIMAL_DECIMAL_TARGET_TYPE` are
  referenced nowhere in the repository — `tests/e2e_type_relaxation_test.rs` imports only the column
  names, the physical-type constants, the table/namespace names, and the row arrays. The module-level
  `#![allow(dead_code)]` inherited from `tests/common/mod.rs` hides them from the compiler, so this
  is leftover from the parallel-group wiring rather than a warning anyone would see.
- Fix: In crates/lakehouse-engine/tests/common/type_promotion_fixtures.rs, delete the six unused
  constants at lines 46-62 together with their doc comments. Keep the Iceberg source/target types
  documented in the module-level `//!` header instead, since the fixture SQL is the authority for
  them.

#### [SUPPRESSED_WARNING] `clippy::approx_constant` is silenced instead of resolved
- Location: line 115
- Issue: `#[allow(clippy::approx_constant)]` is attached to `POST_PROMOTION_ROWS` purely because the
  two post-promotion `float_double` values happen to be pi and e. Nothing about the fixture requires
  those particular constants — the stated requirement (in the SQL header and in the array's own doc)
  is only that a value need more than binary32's 24 mantissa bits. The lint is therefore silenced
  rather than removed.
- Fix: In crates/lakehouse-engine/tests/common/type_promotion_fixtures.rs, replace the two
  `float_double` values in `POST_PROMOTION_ROWS` with `1.234_567_890_123_457` (id 3) and
  `-9.876_543_210_987_654` (id 4), and delete the `#[allow(clippy::approx_constant)]` attribute at
  line 115. In lockstep, update
  scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql: change the two
  `CAST(... AS DOUBLE)` literals in the post-promotion `INSERT` to `1.234567890123457` and
  `-9.876543210987654`, and update the ground-truth table in the header comment (rows 3 and 4 of the
  `float_double` column) to match.

### crates/lakehouse-engine/src/scan/emit_tests.rs

#### [MISSING_BOUNDARY_TEST] The emit-boundary relaxation scenario has no test at all
- Location: file unchanged by this plan
- Issue: `specs/_plans/add-type-relaxation/datafusion-scan/type-relaxation/spec.md`'s scenario
  "A relaxed column crosses the emit boundary at its declared Exasol type" is mapped by plan.md's
  Verification table to `crates/lakehouse-engine/src/scan/emit_tests.rs::a_relaxed_column_coerces_to_its_declared_exatype_without_a_relaxation_branch`.
  A grep of the repository finds no such test and no relaxation/widening/promotion reference anywhere
  in `emit_tests.rs`. The scenario's load-bearing claim — that a widened value survives
  `coerce_batch_to_exa_types`' `safe: true` cast without being NULLed and without any
  relaxation-aware branch — is currently unasserted.
- Fix: In crates/lakehouse-engine/src/scan/emit_tests.rs, add
  `a_relaxed_column_coerces_to_its_declared_exatype_without_a_relaxation_branch`: build a
  `RecordBatch` whose column already carries the CURRENT (widened) Arrow type for each of
  `Int64`, `Float64`, `Decimal128(20,5)`, and `Timestamp(Microsecond, None)` holding a value at the
  narrow source type's boundary, run it through `coerce_batch_to_exa_types` against the EMITS types
  `exasol_type_to_arrow` derives from `DECIMAL(20,0)` / `DOUBLE PRECISION` / `DECIMAL(20,5)` /
  `TIMESTAMP`, and assert every value round-trips unchanged with no NULL introduced.

### crates/lakehouse-engine/tests/build_convention.rs

#### [MISSING_BOUNDARY_TEST] The fixture/suite wiring scenario has no test
- Location: file unchanged by this plan
- Issue: `specs/_plans/add-type-relaxation/packaging/iceberg-type-promotion-fixture/spec.md`'s
  scenario "The new fixture and its suite are wired into the paths that actually run" is mapped by
  plan.md's Verification table to
  `crates/lakehouse-engine/tests/build_convention.rs::the_type_relaxation_suite_and_fixture_are_wired_into_run_fixtures_and_make_test_e2e`.
  That file contains only `host_release_build_documented_unloadable`. Both wirings were in fact made
  (`scripts/spark-fixtures/run_fixtures.sh` gained its `spark-sql -f` line, the `Makefile`'s
  `test-e2e` target gained `--test e2e_type_relaxation_test`) but nothing prevents either from being
  dropped, which is exactly the "present but never executed" failure the scenario exists to catch.
- Fix: In crates/lakehouse-engine/tests/build_convention.rs, add
  `the_type_relaxation_suite_and_fixture_are_wired_into_run_fixtures_and_make_test_e2e`: read
  `scripts/spark-fixtures/run_fixtures.sh` and assert it contains
  `create_iceberg_type_promotion_fixture.sql`, read `Makefile` and assert its `test-e2e` target line
  contains `--test e2e_type_relaxation_test`, and assert
  `scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql` exists. Resolve both paths from
  `CARGO_MANIFEST_DIR` joined with `../..`, following the existing test's path convention.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [MISSING_DOC_COMMENT] The new widened-read E2E test carries no `Scenario:` traceability comment
- Location: line 1100
- Issue: `unity_delta_type_widening_returns_the_widened_types_across_both_files` has no doc comment,
  while every neighbouring test in the file (including
  `unity_delta_unsupported_reader_feature_fails_the_query_loud` directly above it) opens with a
  `/// Scenario: ...` line naming the spec scenario it proves. Its scenario —
  "A type-widened Delta table returns its current wider types across the widening boundary" in
  `e2e-harness/unity-catalog-e2e-harness-delta-queries` — has no link from the code.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, add a doc comment above
  `unity_delta_type_widening_returns_the_widened_types_across_both_files` at line 1100 opening with
  `/// Scenario: A type-widened Delta table returns its current wider types across the widening
  boundary.` and stating that the table's two live data files straddle commit 2's widening, that
  eleven of its thirteen columns are protocol-supported, and that `byte_decimal`/`short_decimal` are
  refused per column per decision [15].

## Expert fixes

### crates/lakehouse-engine/src/types/mapping_tests.rs

#### [ASSERTION_FREE_TEST] The exhaustiveness "tripwire" asserts nothing that can ever fail
- Location: lines 1003-1052 (assertion at 1037, discarded call at 1046)
- Issue: three defects compound in one test.
  (1) `assert_eq!(every_variant.len(), 16, "...a dependency upgrade that adds or removes a variant
  changes this count and is caught here")` is tautological: `every_variant` is a 16-element array
  literal written in the test body, so its length is 16 by construction and can never change when
  `iceberg::spec::PrimitiveType` changes. The guarantee the message claims does not exist.
  (2) The block comment at lines 1006-1012 further claims the test catches "DELETING an arm" of the
  production match — deleting an arm from an exhaustive `match` is a compile error, so that claim is
  also false.
  (3) `let _arrow_type: DataType = iceberg_primitive_to_arrow(variant);` at line 1046 discards its
  result with no assertion; `iceberg_primitive_to_arrow` is total and cannot fail, so that line
  asserts nothing. The only surviving assertion — `!exasol_type.is_empty()` — is near-vacuous too,
  since every arm of `iceberg_primitive_to_exasol` returns a non-empty literal.
  The result is a test that passes while claiming a coverage guarantee it does not provide, which is
  precisely what `vs-adapter/iceberg-type-promotion`'s scenario "The unknown primitive type is
  unrepresentable, and the mapping is the tripwire" asks the suite to establish
  ("SHALL assert that `iceberg_primitive_to_exasol` and `iceberg_primitive_to_arrow` each match every
  `PrimitiveType` variant EXHAUSTIVELY with no catch-all arm").
- Fix: In crates/lakehouse-engine/src/types/mapping_tests.rs, rewrite
  `iceberg_primitive_mappings_are_exhaustive_so_a_new_variant_breaks_the_build` so it is a real
  compile-time tripwire: replace the array literal and the length assertion with a
  `fn expected_mapping(pt: &PrimitiveType) -> (&'static str, DataType)` inside the test module whose
  body is an EXHAUSTIVE `match` over every `PrimitiveType` variant with NO catch-all arm, returning
  the expected Exasol type string and the expected Arrow `DataType` for each; drive it from the same
  variant list and assert `iceberg_primitive_to_exasol(v)` and `iceberg_primitive_to_arrow(v)` equal
  that pair for every variant. Delete the `let _arrow_type` discard and the
  `!exasol_type.is_empty()` assertion. Rewrite the block comment at lines 1003-1012 to state only
  what is true: the production matches are exhaustive so a new `iceberg` variant fails the BUILD,
  and this test's own exhaustive `match` fails the build alongside them while pinning each variant's
  answer — remove the false claims about the length count and about deleted arms.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs

#### [MISSING_BOUNDARY_TEST] No test proves per-column refusal over the real vendored widening fixture
- Location: after `every_shipped_fixture_whose_reader_features_are_allow_listed_still_resolves`
  (line 694)
- Issue: plan.md's Verification table maps `vs-adapter/delta-type-mapping`'s scenario to an
  Integration test `an_unsupported_recorded_type_change_refuses_only_its_own_column` in this file;
  no such test exists. `every_shipped_fixture_whose_reader_features_are_allow_listed_still_resolves`
  gained `"type-widening"` but only asserts the snapshot opens and its log replays — it never builds
  the table schema, so it cannot see a refused column. Decision [15]'s central claim (eleven of the
  fixture's thirteen recorded changes are supported; `byte_decimal` `byte`→`decimal(4,1)` and
  `short_decimal` `short`→`decimal(6,1)` derive a negative `k1` against the protocol's base-10
  precision and are refused) is currently proven ONLY by the Unity E2E test, which needs a live
  stack. The fixture is checked into the repo at `scripts/unity/fixtures/type-widening/` and its
  commit-2 `schemaString` carries all thirteen `delta.typeChanges` entries, so this is provable in
  `cargo test`.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs, add
  `an_unsupported_recorded_type_change_refuses_only_its_own_column`: open the vendored
  `type-widening` fixture with `DeltaSnapshot::open(local_store(), &fixture_root("type-widening"))`,
  pass its schema and column-mapping mode to
  `super::super::delta_schema::build_delta_table_schema`, and assert the call returns `Ok`; assert
  the returned `logical_fields` are exactly the eleven names `byte_long`, `int_long`, `float_double`,
  `byte_double`, `short_double`, `int_double`, `decimal_decimal_same_scale`,
  `decimal_decimal_greater_scale`, `int_decimal`, `long_decimal`, `date_timestamp_ntz` in schema
  order; and assert the returned `refused_columns` are exactly `byte_decimal` and `short_decimal`,
  each reason naming its column and both its Delta type spellings (`byte`/`decimal(4,1)` and
  `short`/`decimal(6,1)`). Do not derive the expected sets from the call's own output.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [MISSING_BOUNDARY_TEST] Five of the eleven projected widened columns are never value-asserted
- Location: lines 1138-1207
- Issue: `unity_delta_type_widening_returns_the_widened_types_across_both_files` projects eleven
  columns but value-asserts only indices 0, 1, 2, 8, 9 and 10. Indices 3-7 —
  `BYTE_DOUBLE`, `SHORT_DOUBLE`, `INT_DOUBLE`, `DECIMAL_DECIMAL_SAME_SCALE`,
  `DECIMAL_DECIMAL_GREATER_SCALE` — have their DECLARED Exasol type checked and their values never
  read, so the classic widening-cast failure (a pre-widening value arriving as NULL, or a decimal
  arriving unrescaled) passes undetected for all five. `DECIMAL_DECIMAL_GREATER_SCALE` is the worst
  omission: `decimal(10,2)` → `decimal(20,5)` is the ONLY scale-growing pair in the whole supported
  set, it is the pair plan.md's Manual Testing table calls out by name, and it is the only column
  whose pre-widening value must be visibly RESCALED rather than merely re-tagged.
  Ground truth from the fixture's own `_delta_log` `add` statistics: the pre-widening row (commit 0)
  is `byte_double` 5, `short_double` 6, `int_double` 7, `decimal_decimal_same_scale` 123.45,
  `decimal_decimal_greater_scale` 67.89; the post-widening row (commit 2) is `byte_double`,
  `short_double`, `int_double` all 1.234567890123, `decimal_decimal_same_scale` 12345678901234.56,
  `decimal_decimal_greater_scale` 12345678901.23456.
  Separately, the two existing decimal assertions are mutually inconsistent and one of them is very
  likely wrong: `rows[9][PRE]` (`LONG_DECIMAL`, declared `DECIMAL(21,1)`) is asserted as `"4"` while
  `rows[9][POST]` on the SAME column is asserted as `"123456789012345678.9"`. If the WebSocket
  rendering carries the declared scale, the first must be `"4.0"`; if it trims trailing zeros, the
  first is right. The same doubt applies to `rows[8][PRE]` asserted as `"3"` for `DECIMAL(11,1)`.
  Per the project's verification-discipline rule this must be settled against a running Exasol
  instance, not assumed.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, extend
  `unity_delta_type_widening_returns_the_widened_types_across_both_files` with value assertions for
  the five unasserted columns using the ground truth above: assert `parse_numeric(&rows[3][PRE])`,
  `rows[4][PRE]`, `rows[5][PRE]` are 5.0, 6.0, 7.0 and the corresponding `[POST]` values are
  1.234567890123 (tolerance 1e-9); assert `value_to_string(&rows[6][PRE])` and `&rows[6][POST]` are
  the `DECIMAL(20,2)` renderings of 123.45 and 12345678901234.56; and assert
  `value_to_string(&rows[7][PRE])` and `&rows[7][POST]` are the `DECIMAL(20,5)` renderings of 67.89
  and 12345678901.23456, with a message stating that the pre-widening value proves the
  `decimal(10,2)` → `decimal(20,5)` RESCALE rather than a re-tag. Before committing, run
  `make test-e2e-unity` against the live Unity stack and take the exact decimal string rendering from
  that run for all four decimal columns, correcting the existing `rows[8][PRE]` / `rows[9][PRE]`
  expectations (`"3"` / `"4"`) if the driver carries the declared scale. Do not weaken any assertion
  to `contains` or to a parsed float to sidestep the rendering question.

### crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs

#### [MISSING_BOUNDARY_TEST] No test asserts a readable Iceberg promotion carries the promoted type
- Location: lines 787-830
- Issue: `vs-adapter/iceberg-type-promotion`'s scenario "A promotion this engine reads resolves
  through the shared relaxation cast" requires that "the adapter SHALL build the logical schema from
  `table.metadata().current_schema()`, so the promoted column's `LogicalField` carries the PROMOTED
  type rather than any data file's type", and plan.md maps that to a unit test named
  `a_readable_iceberg_promotion_plans_normally_and_carries_the_current_type`. The three tests that
  were written instead — `an_int_to_long_promotion_history_plans_normally`,
  `a_float_to_double_promotion_history_plans_normally`,
  `a_decimal_precision_widening_history_plans_normally` — call `refuse_date_promotion` directly and
  assert only that the DATE gate does not fire. None of them resolves a scan, and none inspects a
  `LogicalField`. The "carries the promoted type" half of the scenario is therefore proven nowhere
  below the E2E suite, which needs the Spark fixture stack. The file already contains the machinery
  to do this: `resolve_promoted_date_table` shows the `RecordingCatalog` + `CatalogSession` +
  `IcebergFormatReader::resolve_scan` pattern, and `load_table_body_with_promoted_date_column` shows
  how to serve a synthetic `loadTable` body.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs, add
  `a_readable_iceberg_promotion_plans_normally_and_carries_the_current_type`: build a `loadTable`
  body (reusing the shape of `load_table_body_with_promoted_date_column`, with no snapshot) whose
  schema history is schema 0 declaring `amount` `int`, `reading` `float`, `price` `decimal(10,2)`
  and current schema 1 declaring the same field ids as `long`, `double`, `decimal(20,2)`; serve it
  through `RecordingCatalog::spawn` and drive `IcebergFormatReader::resolve_scan(None)` exactly as
  `resolve_promoted_date_table` does; assert the call returns `Ok` and that the resulting
  `ResolvedScan`'s `logical_schema` carries `amount` as `int64`, `reading` as `float64`, and `price`
  as `decimal128(20,2)` — the CURRENT types — each keyed to its original field id. Assert the
  refused-column list is empty. Do not read the expected types back out of the resolved scan.
