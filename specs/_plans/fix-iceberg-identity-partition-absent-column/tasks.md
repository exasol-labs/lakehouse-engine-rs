# Tasks: fix-iceberg-identity-partition-absent-column

## Phase 2: Implementation (Group A: Identity values from plan to scan)
- [x] 1.1 Pass logical fields to `plan_files_from_table` / `planned_file_entry` in `adapter/pushdown/format/iceberg.rs`; write each `identity` partition value into `FileEntry.partition_values` (null as explicit absent value)
- [x] 1.2 Render a partition `Literal` at the declared Arrow type via `CastExpr::new_with_target_field`; unit-test S1 and S7 in `iceberg_tests.rs`
- [x] 1.3 `PartitionedScanSchema::split` takes file entries; fallback column per unlisted `partition_values` key with hidden `__identity_<idx>` partition column; fallback expression type; replace `remap_projection` with a projection-expression builder [expert]
- [x] 1.4 `FieldIdExprAdapter::rewrite` reduces fallback expressions before the `initial-default` intercept; unit-test S3 and S4 in `field_id_projection_tests.rs` [expert]

## Phase 2: Implementation (Group B: E2E fixture and suite)
- [x] 2.1 Add `scripts/spark-fixtures/create_identity_partition_fixture.sql`, `run_fixtures.sh` line, and ground truth in `common/identity_partition_fixtures.rs`
- [x] 2.2 Add `e2e_local/identity_partition.rs` with fixture guard and four E2E tests; add `/// Scenario:` lines to existing E2E tests
- [x] 2.3 Update `specs/testing.md` § Fixtures

## Phase 3: Verification
- [x] 3.1 `make cross-udf-build`
- [x] 3.2 Spark fixtures job
- [x] 3.3 `cargo test --workspace`
- [x] 3.4 `make test-e2e`
- [x] 3.5 `make fmt-check lint`
- [x] 3.6 Manual testing table

## Phase 4: Review Fixes
- [x] 4.1 In `scan/partition_values.rs`, make `IdentityFallbackExpr::reduce_for_file` return `DataFusionError::Internal` when the file side is not a `Column`; add `a_fallback_whose_file_side_is_not_a_column_fails` in `partition_values_tests.rs`
- [x] 4.2 In `partition_values_tests.rs`, rewrite `each_file_encodes_no_value_a_null_or_its_identity_value` as `each_file_reduces_to_its_value_a_null_or_its_file_column` over `reduce_for_file`, delete `hidden_struct`, and drop the struct-encoding assertions from `an_unlisted_partition_value_key_stays_a_file_column_beside_a_hidden_identity_column`
- [x] 4.3 In `adapter/pushdown/format/iceberg.rs`, replace `plan_files_from_table` with `plan_file_tasks` (no `logical_fields`, returns the `FileScanTask`s); map them with `planned_file_entry` in `plan_checked_table`; update `iceberg_tests.rs`
- [x] 4.4 In `iceberg_tests.rs`, add `an_identity_literal_of_another_type_than_its_column_fails_planning_naming_the_column` covering the type-mismatch and non-primitive refusals
- [x] 4.5 In `iceberg_tests.rs`, add the `/// Scenario: The Iceberg format reader owns file resolution and binds columns by field-id` line to `iceberg_reader_owns_resolution_and_keeps_its_encoding`
- [x] 4.6 In `field_id_projection_tests.rs`, add the three missing `/// Scenario:` lines to `bound_file_column_wins_over_identity_value`, `absent_nullable_without_default_is_null_filled`, and `absent_required_without_default_errors_cleanly`
- [x] 4.7 In `identity_partition_fixtures.rs`, move the table-layout doc comment from `TYPES_ID_COLUMN` to `TYPES_TABLE`
- [x] 4.8 Add `TYPES_ID_DECLARED` to `identity_partition_fixtures.rs` and use it in `e2e_local/identity_partition.rs` instead of the `"DECIMAL(10,0)"` literal
- [x] 4.9 In `e2e_local/identity_partition.rs`, count expected groups with a `BTreeMap` instead of the find-or-insert loop
