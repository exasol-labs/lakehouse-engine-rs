# Code Review Findings: fix-iceberg-identity-partition-absent-column

## Summary
- Files reviewed: 19
- Total findings: 9 (standard: 9, expert: 0)
- Evidence: `cargo test --workspace --lib -- identity fallback hidden_identity unlisted bound_file_column` passes 48 tests with 0 failures. `cargo clippy --workspace --all-targets --all-features -- -D warnings` reports no warnings.

## Standard fixes

### crates/lakehouse-engine/src/scan/partition_values.rs

#### [SWALLOWED_ERROR] A fallback whose file side is not a column silently drops the identity value
- Location: line 388, `IdentityFallbackExpr::reduce_for_file`
- Issue: When `self.file_column` is not a `Column`, `reduce_for_file` returns the file side unchanged. It never consults the per-file literal. For a file that lacks the column, the scan then reads NULL, which is the silent wrong result this plan fixes. The sibling invariant violation, a hidden column that is not a literal, already returns `DataFusionError::Internal`. No test covers the branch.
- Fix: In `crates/lakehouse-engine/src/scan/partition_values.rs`, in `IdentityFallbackExpr::reduce_for_file`, replace the `else { return Ok(Arc::clone(&self.file_column)); }` arm with `else { return Err(DataFusionError::Internal(format!("the identity fallback {self} reached the file adapter with a file side that is not a column"))); }`. In `partition_values_tests.rs`, add `a_fallback_whose_file_side_is_not_a_column_fails`. The test takes `let file = entry_with(&[("region", Some("eu"))]);` and `let identity = identity_split(std::slice::from_ref(&file)).partition_values_for(&file).expect("a valid entry").remove(0);`. It builds `IdentityFallbackExpr::new(Arc::new(Literal::new(ScalarValue::Utf8(None))), Arc::new(Literal::new(identity)))`, calls `reduce_for_file(|_| false)`, and asserts an error whose text contains `identity_fallback`.

### crates/lakehouse-engine/src/scan/partition_values_tests.rs

#### [IMPLEMENTATION_COUPLED_TEST] Two tests assert the hidden column's private struct encoding
- Location: line 536, `each_file_encodes_no_value_a_null_or_its_identity_value`; line 429, `an_unlisted_partition_value_key_stays_a_file_column_beside_a_hidden_identity_column` (the assertions after `partition_names`)
- Issue: The plan makes `IdentityFallbackExpr` the sole owner of the hidden column's per-file encoding (null struct, null child, non-null child). Both tests unpack that struct directly: its child count, child type, child nullability, and `StructArray::is_null`. A change of encoding that keeps every read correct would fail them. The observable behavior is what `reduce_for_file` yields for each file.
- Fix: In `crates/lakehouse-engine/src/scan/partition_values_tests.rs`, rewrite `each_file_encodes_no_value_a_null_or_its_identity_value` as `each_file_reduces_to_its_value_a_null_or_its_file_column`. For each of the four files, take `opened_projection(&split, file)[1]`, downcast it to `IdentityFallbackExpr`, and call `reduce_for_file(|_| false)`. Assert `Literal` `Utf8(Some("eu"))` for the first file, `Literal` `Utf8(None)` for the explicit null and for the empty string, and a `Column` named `region` for the file without the key. Delete `hidden_struct` once nothing uses it. In `an_unlisted_partition_value_key_stays_a_file_column_beside_a_hidden_identity_column`, delete every assertion after `assert_eq!(partition_names(&split), ["__identity_1"]);`.

### crates/lakehouse-engine/src/adapter/pushdown/format/iceberg.rs

#### [TOO_MANY_ARGUMENTS] `plan_files_from_table` grew to five parameters
- Location: line 471, `plan_files_from_table`
- Issue: The change adds `logical_fields` as a fifth parameter. It is only forwarded to `planned_file_entry`. The function now both plans the iceberg-rust scan and builds the scan-spec entries.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg.rs`, remove the `logical_fields` parameter from `plan_files_from_table`. Rename the function to `plan_file_tasks` and return `Result<Vec<iceberg::scan::FileScanTask>, UdfError>` (the collected `tasks`). In `plan_checked_table`, map the returned tasks with `planned_file_entry(task, declared)`, collect them, and apply `.map_err(|refusal| refusal.into_error(&table_name))`. Keep the `relativize_shards_to_root` comment beside that mapping. Update the call in `iceberg_tests.rs` `an_unreadable_manifest_list_fails_planning_naming_its_cause` to `plan_file_tasks(table, "db.t", None, &[])`.

### crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs

#### [UNTESTED_ERROR_PATH] The literal-type mismatch and non-primitive refusals have no test
- Location: `iceberg.rs` line 662 (`file_column_value`, `_ => return None`) and line 583 (`identity_entry`, the `Some(_)` arm)
- Issue: Both paths refuse planning with `PlannedFileRefusal::UnreadableIdentityValue`. The only refusal test, `an_identity_value_its_declared_type_cannot_hold_fails_planning_naming_the_column`, covers the cast failure alone.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs`, add `an_identity_literal_of_another_type_than_its_column_fails_planning_naming_the_column`. Build a task with `partitioned_scan_task(&identity_partition_spec(0, &[("id", "id", Transform::Identity)]), vec![Some(Literal::string("x"))])`. Call `planned_file_entry(&task, &identity_partitioned_logical_fields())`. Assert that `user_message(refusal.into_error("db.t"))` contains `db.t`, `'id'`, and `long`. In the same test, repeat the call with `vec![Some(Literal::Struct(iceberg::spec::Struct::from_iter([Some(Literal::long(1))])))]` and assert that the message contains `non-primitive`.

#### [MISSING_DOC_COMMENT] The Iceberg reader test lacks its CHANGED scenario line
- Location: line 60, `iceberg_reader_owns_resolution_and_keeps_its_encoding`
- Issue: The plan's Scenario Coverage table maps the CHANGED `delta/delta-table-planning` scenario to this test. AGENTS.md requires one `/// Scenario:` line per implemented scenario. The test carries none.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs`, add `/// Scenario: The Iceberg format reader owns file resolution and binds columns by field-id` directly above the `#[tokio::test]` attribute of `iceberg_reader_owns_resolution_and_keeps_its_encoding`.

### crates/lakehouse-engine/src/scan/field_id_projection_tests.rs

#### [MISSING_DOC_COMMENT] Three tests lack the scenario lines the coverage table maps to them
- Location: line 701 `bound_file_column_wins_over_identity_value`, line 585 `absent_nullable_without_default_is_null_filled`, line 614 `absent_required_without_default_errors_cleanly`
- Issue: The Scenario Coverage table maps the CHANGED scenario "A materialized partition column is a first-class scan column" (its no-skip clause) to `bound_file_column_wins_over_identity_value`. That test carries only the S3 line. It also maps two CHANGED absent-fields scenarios to the two existing tests. Neither of those carries a `/// Scenario:` line, and no other test in `crates/` carries these titles.
- Fix: In `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs`, add `/// Scenario: A materialized partition column is a first-class scan column` below the existing `/// Scenario:` line of `bound_file_column_wins_over_identity_value`. Add `/// Scenario: Added nullable column absent from a file with no initial-default is NULL-filled` above the `#[test]` of `absent_nullable_without_default_is_null_filled`. Add `/// Scenario: Added required column absent from a file with no initial-default errors cleanly` above the `#[test]` of `absent_required_without_default_errors_cleanly`.

### crates/lakehouse-e2e/tests/common/identity_partition_fixtures.rs

#### [OUTDATED_COMMENT] The table-layout doc comment is attached to the id column constant
- Location: line 115, the doc comment above `TYPES_ID_COLUMN`
- Issue: The comment describes the `ident_part_types` table: which columns carry an `identity` field and how the three rows were written. It documents `TYPES_ID_COLUMN`, a column name, so it does not match the item it is attached to.
- Fix: In `crates/lakehouse-e2e/tests/common/identity_partition_fixtures.rs`, move the doc comment `/// Every column but id carries an identity partition field. Row 1 is imported, row 2 is native with the same values, row 3 is imported with every partition value null.` from `TYPES_ID_COLUMN` to `TYPES_TABLE`, and leave `TYPES_ID_COLUMN` without a doc comment like the other name constants.

### crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs

#### [MAGIC_NUMBER] The id column's declared type is a literal outside the ground-truth module
- Location: line 439, `reconstructed_identity_values_equal_stored_values_for_every_type`
- Issue: The plan places the fixture ground truth in `identity_partition_fixtures.rs`, and `TypesColumn.declared` holds every other column's declared Exasol type there. The `id` column's declared type is the inline literal `"DECIMAL(10,0)"` in the test.
- Fix: In `crates/lakehouse-e2e/tests/common/identity_partition_fixtures.rs`, add `pub const TYPES_ID_DECLARED: &str = "DECIMAL(10,0)";` after `TYPES_ID_COLUMN`. In `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs`, import it and replace the literal `"DECIMAL(10,0)"` in the `reads(&upper[0], ...)` call with `TYPES_ID_DECLARED`.

#### [STANDARD_LIBRARY_DUPLICATE] Expected group counts use a hand-written find-or-insert loop
- Location: line 494, `pushdown_shapes_see_reconstructed_identity_value`
- Issue: The loop over `ABSENT_ROWS` searches a `Vec` linearly for each key, then sorts it. `BTreeMap::entry` counts per key and yields sorted pairs.
- Fix: In `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs`, replace the `expected_counts` loop and its `sort()` with `let mut counted: BTreeMap<Option<String>, i64> = BTreeMap::new(); for row in &ABSENT_ROWS { *counted.entry(row.region.map(str::to_string)).or_insert(0) += 1; } let expected_counts: Vec<(Option<String>, i64)> = counted.into_iter().collect();`, and add `BTreeMap` to the existing `std::collections` import.

## Expert fixes
[none]
