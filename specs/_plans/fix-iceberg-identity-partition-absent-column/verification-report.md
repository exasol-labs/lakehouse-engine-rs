# Verification Report: fix-iceberg-identity-partition-absent-column

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The scan reads an Iceberg `identity` partition value for a column the data file lacks. Unit tests (2042) and the local E2E suite (327) pass. |
| Code review | 9 findings, 9 fixed |

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
| Unit and host integration (`cargo test --workspace`) | 2044 | 2042 | 2 |
| E2E (`make test-e2e`, local Docker Exasol) | 327 | 327 | 0 |

The 2 ignored host tests are existing micro-benchmarks.

The first E2E run failed 16 tests, because `make test-e2e` does not run the Spark fixture job and the INT96, positional-delete, and type-promotion tables did not exist. After `docker compose up -d spark-iceberg-fixtures` finished with exit 0, the suite passed in full. The 5 identity-partition tests passed in both runs.

### Manual Tests

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL SELECT ts FROM INT96_TS_FAR_FUTURE`: files argument carries no `partition_values` | ✓ |
| `EXPLAIN VIRTUAL SELECT id, region FROM IDENT_PART_ABSENT`: files argument carries `region` values, common argument names no partition column | ✓ |
| `IDENT_PART_ABSENT`, `IDENT_PART_REQUIRED`, `IDENT_PART_TYPES` row values and `GROUP BY region` | ✓ by E2E tests `identity_value_fills_absent_column_per_file_spec`, `required_identity_column_reads_partition_value`, `reconstructed_identity_values_equal_stored_values_for_every_type` |

## Tool Evidence

### Linter

```
make fmt-check lint: exit 0
cargo clippy --workspace --all-targets --all-features -- -D warnings: no warnings
```

### Formatter

```
cargo fmt --all -- --check: no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| scan-read-path | scan-execution-field-id-projection-absent-fields | The VS carries each data file's identity partition values from the file's own partition spec | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `identity_values_come_from_each_files_own_spec` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | An identity partition value fills a column the data file lacks, per the file's own spec | `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs` | `identity_value_fills_absent_column_per_file_spec` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | A column the data file holds wins over the file's identity partition value | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs`, `partition_values_tests.rs` | `bound_file_column_wins_over_identity_value`, `unreduced_fallback_evaluates_its_file_column` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | An identity partition value precedes the initial-default, and a null partition value is present | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `identity_value_precedes_initial_default_and_null_is_present` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | A required identity column without a physical counterpart reads its partition value | `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs` | `required_identity_column_reads_partition_value` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | A reconstructed identity value reads the same as a stored one for every admitted type | `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs` | `reconstructed_identity_values_equal_stored_values_for_every_type` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | Every admitted identity value survives the scan-spec round trip at the column's declared type | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `identity_value_round_trips_at_declared_type_for_every_admitted_type` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | Pushed-down filters, aggregates, TopN, and joins see the reconstructed value | `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs` | `pushdown_shapes_see_reconstructed_identity_value` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | Added nullable column absent from a file with no initial-default is NULL-filled (CHANGED) | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `absent_nullable_without_default_is_null_filled` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | Absent field with a defined initial-default returns the default value per file (CHANGED) | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_added_columns_initial_default_fill_all_types` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | Added required column absent from a file with no initial-default errors cleanly (CHANGED) | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `absent_required_without_default_errors_cleanly` | Pass |
| scan-read-path | scan-execution-partition-values | The logged partition value wins over a physically present partition column (CHANGED) | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `partition_key_colliding_with_a_parquet_column_overrides_it` | Pass |
| scan-read-path | scan-execution-partition-values | A materialized partition column is a first-class scan column (CHANGED) | `crates/lakehouse-e2e/tests/e2e_unity_test.rs`, `field_id_projection_tests.rs` | `unity_delta_partitioned_table_returns_partition_values` (Unity suite, not run), `bound_file_column_wins_over_identity_value` | Pass (unit); Unity not run |
| scan-read-path | scan-execution-partition-values | A scan with no partition columns is unchanged (CHANGED) | `crates/lakehouse-engine/tests/it/scan_partition_values.rs` | `scan_without_partition_columns_is_byte_identical` | Pass |
| delta | delta-table-planning | The Iceberg format reader owns file resolution and binds columns by field-id (CHANGED) | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `iceberg_reader_owns_resolution_and_keeps_its_encoding` | Pass |

## Notes

- `make test-e2e` does not start the Spark fixture job. Run `docker compose up -d spark-iceberg-fixtures` and `docker wait lakehouse-engine-rs-spark-iceberg-fixtures-1` first, or Spark-authored tables are missing.
- The Unity suite (`make test-e2e-unity`) was not run, per the project hook. The edit to `unity_delta_partitioned_table_returns_partition_values` adds a `/// Scenario:` line only. The Lakekeeper, Glue, and Azure suites were not run, because the plan changes no catalog or storage behavior.
- Filters on an identity column do not push into the Parquet scan, because DataFusion's pushdown checker refuses a conjunct over the struct-typed hidden column. Such filters stay in `FilterExec`, and results are correct. Iceberg plan-time pruning on manifest partition values is unchanged.
- The Manual Testing expectation "files argument carries `region` values for imported files only" differs from the observed output: natively written files with an `identity` field also carry their `region` value. The scan reads the file column for such a file, so results are unchanged.
- A null partition value on a required identity column is not covered by a scenario or test. It builds a null literal under a non-nullable field, and Arrow is expected to reject the batch with an error.
- An unlisted `partition_values` key that names no declared field is ignored, and a key that matches two declared fields is refused.
