# Plan: fix-iceberg-identity-partition-absent-column

## Summary

The scan fills a column that an Iceberg data file lacks from the file's manifest `identity` partition value, per the Iceberg spec's "Column Projection" rule (1), instead of reading NULL (#99). A column the file holds still reads from the file, and a null partition value reads as NULL.

## Context

- A metadata-only Hive migration (Spark `migrate`, `snapshot`, or `add_files`) registers data files without the partition column. Files written natively hold it, and a migrated table that receives new writes mixes both.
- The bug is reproduced on the Docker Exasol (`decision-log.md` § Repro). Rows of imported files read `region` as NULL, so `WHERE` drops them and `GROUP BY` counts them under NULL, with no error.
- The Iceberg reader reads only path, size, format, and deletes from each `FileScanTask` (`adapter/pushdown/format/iceberg.rs` `planned_file_entry`). The pinned iceberg-rust already fills `FileScanTask::partition()` and `partition_spec()` from the spec the file was written with.
- The scan's partition-value path (`scan/partition_values.rs`) removes a partition column from the file schema, so the logged value always wins. That is the Delta rule, not the Iceberg rule.
- DataFusion 55 replaces partition columns with per-file literals before it creates the expression adapter, and the adapter factory receives only the logical and physical schema, never the file.
- Iceberg plan-time pruning runs inside iceberg-rust on the same manifest partition values (`plan_files_from_table`, `with_filter`), so pruning needs no change.
- Architecture: no change. The plan widens the meaning of an existing scan-spec field inside the format-readers, scan-spec, and datafusion-scan components.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| scan-execution-field-id-projection-absent-fields | CHANGED | `scan-read-path/scan-execution-field-id-projection-absent-fields/spec.md` |
| scan-execution-partition-values | CHANGED | `scan-read-path/scan-execution-partition-values/spec.md` |
| scan-execution-field-id-projection | CHANGED (Background) | `scan-read-path/scan-execution-field-id-projection/spec.md` |
| delta-table-planning | CHANGED (Background, Iceberg reader scenario) | `delta/delta-table-planning/spec.md` |

## Impact

- Queries over an Iceberg table with imported data files return each file's partition value instead of NULL. Filters, aggregates, COUNT(DISTINCT), TopN, and broadcast joins change their results the same way. This is the intended correction of a silent wrong result.
- A required `identity` column that an imported file lacks now reads its partition value instead of failing with the required-absent error.
- The per-shard file argument of an Iceberg table with `identity` partition fields grows by one small map per data file. Scan specs of every other table stay byte-identical.
- No property, capability, or declared column type changes.

## Implementation Tasks

**A. Identity values from plan to scan** (spec deltas: absent-fields, partition-values, field-id-projection, delta-table-planning)

- [ ] 1.1 In `adapter/pushdown/format/iceberg.rs`, pass the scan's logical fields to `plan_files_from_table` and `planned_file_entry`. For each `FileScanTask`, walk `partition_spec()` and `partition()`, and for each `identity` field whose `source_id` is the field id of a top-level logical field, write the value into `FileEntry.partition_values` under the field's logical name, with null as an explicit absent value. Skip every other transform and every column refused at plan time. Leave `declaration()` and `partition_columns` unchanged. Every caller of the reader, both join legs included, passes its logical fields.
- [ ] 1.2 In the same module, add one function that renders a partition `Literal` at the column's declared Arrow type, then serializes the string that `scan::partition_values::partition_scalar` parses back. It casts with the expression that the scan's `DefaultPhysicalExprAdapter` inserts for a column whose type differs: `CastExpr::new_with_target_field(column, declared_field, None)` in DataFusion 55.2 (`schema_rewriter.rs:619-623`). The reader evaluates that expression over a one-row batch of the Arrow type that a Parquet column of the file's Iceberg type reads as, for example `Time64(Microsecond)` for `time`. `None` selects `CastExpr`'s default options (`safe: false`), so the reader and the scan share one cast function and one option set, and the reader makes no `arrow::compute::cast` call of its own. Unit-test scenarios S1 and S7 in `iceberg_tests.rs`. S7 takes each type's expected value from that same expression over a one-element array of the file's type, and calls `partition_scalar` for the inverse.
- [ ] 1.3 In `scan/partition_values.rs`, let `PartitionedScanSchema::split` take the shard's file entries. Each `partition_values` key that `partition_columns` does not list becomes a fallback column with one hidden DataFusion partition column. Add the fallback expression type here, over the file column and the hidden column. It alone owns the hidden column's per-file encoding of "no value", "null", and "a value". The hidden column is a nullable `Struct` with one nullable child of the column's declared type: a null struct is "no value", a null child is "null", and a non-null child is the value. Every file therefore carries one struct per hidden column, which DataFusion 55.2 requires, because the opener pairs each file's `partition_values` with the partition fields by position and substitutes each value as a literal (`opener/mod.rs:776-801`). The split names a hidden column `__identity_<logical index>` and appends `_` until the name equals no field name of the scan's logical schema, because `replace_columns_with_literals` (`schema_rewriter.rs:64-83`) substitutes by name. A physical column name never meets the hidden name, because that substitution runs before the adapter binds names to the file. `partition_values_for` builds the struct through `non_null_partition_value` and `partition_scalar`, and the expression reduces itself, given whether its field is bound in a file, to the file column, a literal of the declared type, or the file column for the absent-field chain. An unreduced fallback expression evaluates to its file-column child, because DataFusion skips the adapter only when the file's physical schema equals the logical file schema, which binds the field. Unit-test that evaluation in `partition_values_tests.rs`. The expression exposes both inputs as children, so DataFusion's partition-literal substitution reaches the hidden column, also inside a predicate pushed into the scan. Replace `remap_projection` with a builder of projection expressions, pass them to the file source in `ParquetScanTable::scan`, pass the files from `register_file_list`, and update both doc comments. [expert]
- [ ] 1.4 In `scan/field_id_projection.rs`, `FieldIdExprAdapter::rewrite` reduces each fallback expression before the `initial-default` intercept, passing whether this file binds the field, so rule (1) precedes rule (3) and a bound field keeps its file column. Unit-test scenarios S3 and S4 in `field_id_projection_tests.rs`. The S3 test also rewrites a filter predicate over the column and expects it to read the file column, not the partition value. [expert]

**B. E2E fixture and suite** (spec delta: absent-fields)

- [ ] 2.1 Add `scripts/spark-fixtures/create_identity_partition_fixture.sql`, made idempotent like the INT96 script (`DROP TABLE IF EXISTS`, overwrite of the source directories), with its own `spark-sql -f` line in `run_fixtures.sh` that adds INT96's `hadoop-aws` package and `fs.s3.impl` setting. It writes three tables in `e2e_lakehouse`:
  - `ident_part_absent` from the Appendix SQL of `decision-log.md`, plus an imported row `(7, 70, NULL)` in the Hive default partition, then `DROP PARTITION FIELD region` and one unpartitioned `add_files` import of `(8, 80)` without `region`.
  - `ident_part_required`, with `region STRING NOT NULL` and `identity(region)`, holding imported files only.
  - `ident_part_types`, with `identity` fields on `boolean`, `int`, `bigint`, `float`, `double`, `decimal(10,2)`, `decimal(38,6)`, `date`, and `string` columns. Row 1 is imported, row 2 is written natively with the same values, and row 3 is imported with all partition values null.
  - Ground truth lives in `crates/lakehouse-e2e/tests/common/identity_partition_fixtures.rs`, declared in `common/mod.rs`.
- [ ] 2.2 Add `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs`, declared in `e2e_local/main.rs`. It holds one fixture guard, which reads through iceberg-rust and the file footers that imported files hold no source column, that their manifest partition structs carry the values, and that `ident_part_absent` carries two partition spec ids. It also holds the four E2E tests of the Scenario Coverage table, each tagged with its `/// Scenario:` line. Each test proves through `EXPLAIN VIRTUAL` that its query reaches the scan UDF. Add the `/// Scenario:` line of each CHANGED partition-values scenario to the existing E2E test that the Scenario Coverage table maps it to.
- [ ] 2.3 In `specs/testing.md` § Fixtures, add the identity-partition script to the list of Spark-authored fixtures, name it beside INT96 as an `add_files` script that adds INT96's settings, and add one bullet that describes its three tables.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Identity values from plan to scan | 1.1-1.4 | none | spec deltas `scan-read-path/scan-execution-field-id-projection-absent-fields`, `scan-read-path/scan-execution-partition-values`, `scan-read-path/scan-execution-field-id-projection`, `delta/delta-table-planning`; `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg.rs`, `iceberg_tests.rs`, `crates/lakehouse-engine/src/scan/partition_values.rs`, `partition_values_tests.rs`, `field_id_projection.rs`, `field_id_projection_tests.rs`, `scan_table.rs`, `raw_scan.rs` |
| B: E2E fixture and suite | 2.1-2.3 | A (the E2E tests pass only on the built `.so`) | spec deltas `scan-read-path/scan-execution-field-id-projection-absent-fields`, `scan-read-path/scan-execution-partition-values`; `scripts/spark-fixtures/`, `crates/lakehouse-e2e/tests/common/identity_partition_fixtures.rs`, `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs`, `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` and `crates/lakehouse-e2e/tests/e2e_unity_test.rs` (`/// Scenario:` lines only), `specs/testing.md` § Fixtures |

- Group B can author the fixture and tests while group A runs. Its tests are run after group A lands.

## Out of scope

- Iceberg equality deletes (#11) stay refused. When #11 applies them, a delete key that is an `identity` source absent from the data file must match on the reconstructed value, so #11 depends on this plan or must refuse that case.
- An `identity` source nested inside a struct keeps rules (2) to (4). The absent-fields delta records this as a scoped exception, because a Hive import produces only top-level partition columns.
- Delta keeps the log-wins precedence, because its protocol makes the transaction log authoritative.
- `delta/delta-table-planning`'s Background exceeds five bullets; trimming it needs its facts moved to the scenarios and the declaration-checks sibling first.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/scan/partition_values.rs` `PartitionedScanSchema::remap_projection` | Replaced by the projection-expression builder of task 1.3. Its unit tests in `partition_values_tests.rs` move to the builder with their expected values unchanged. |
| Spec text | Background bullets that call rule (1) unimplemented, in absent-fields, field-id-projection, and delta-table-planning | Replaced by the deltas |

No existing test becomes redundant.

## Verification

### Scenario Coverage

Labels S1 to S8 name the eight new scenarios of the absent-fields delta in their order.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The VS carries each data file's identity partition values from the file's own partition spec (S1) | Unit (wire contract) | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `identity_values_come_from_each_files_own_spec` |
| An identity partition value fills a column the data file lacks, per the file's own spec (S2) | E2E | `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs` | `identity_value_fills_absent_column_per_file_spec` |
| A column the data file holds wins over the file's identity partition value (S3) | Unit | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `bound_file_column_wins_over_identity_value` |
| A column the data file holds wins over the file's identity partition value (S3), file the adapter skips | Unit | `crates/lakehouse-engine/src/scan/partition_values_tests.rs` | `unreduced_fallback_evaluates_its_file_column` |
| An identity partition value precedes the initial-default, and a null partition value is present (S4) | Unit | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `identity_value_precedes_initial_default_and_null_is_present` |
| A required identity column without a physical counterpart reads its partition value (S5) | E2E | `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs` | `required_identity_column_reads_partition_value` |
| A reconstructed identity value reads the same as a stored one for every admitted type (S6) | E2E | `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs` | `reconstructed_identity_values_equal_stored_values_for_every_type` |
| Every admitted identity value survives the scan-spec round trip at the column's declared type (S7) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `identity_value_round_trips_at_declared_type_for_every_admitted_type` |
| Pushed-down filters, aggregates, TopN, and joins see the reconstructed value (S8) | E2E | `crates/lakehouse-e2e/tests/e2e_local/identity_partition.rs` | `pushdown_shapes_see_reconstructed_identity_value` |
| Added nullable column absent from a file with no initial-default is NULL-filled (CHANGED) | Unit, existing | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `absent_nullable_without_default_is_null_filled` |
| Absent field with a defined initial-default returns the default value per file (CHANGED) | E2E, existing | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_added_columns_initial_default_fill_all_types` |
| Added required column absent from a file with no initial-default errors cleanly (CHANGED) | Unit, existing | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `absent_required_without_default_errors_cleanly` |
| The logged partition value wins over a physically present partition column (CHANGED) | E2E, existing | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `partition_key_colliding_with_a_parquet_column_overrides_it` |
| A materialized partition column is a first-class scan column (CHANGED) | E2E, existing | `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `unity_delta_partitioned_table_returns_partition_values` |
| A materialized partition column is a first-class scan column (CHANGED), no skip on an `identity` value | Unit | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `bound_file_column_wins_over_identity_value` (its filter-predicate case) |
| A scan with no partition columns is unchanged (CHANGED) | Host, existing | `crates/lakehouse-engine/tests/it/scan_partition_values.rs` | `scan_without_partition_columns_is_byte_identical` |
| The Iceberg format reader owns file resolution and binds columns by field-id (CHANGED) | Unit, existing | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `iceberg_reader_owns_resolution_and_keeps_its_encoding` |

- S3 and S4 are proved by unit tests because they are pure rewrite decisions over a synthesized logical and physical schema. DataFusion 55.2 skips the adapter for a file whose physical schema equals the logical file schema when the scan has no predicate (`datafusion-datasource-parquet` `opener/mod.rs:1009-1017`). `unreduced_fallback_evaluates_its_file_column` therefore evaluates an unreduced fallback expression over a `RecordBatch` and expects the file column's values. The E2E S2 test reads imported and native files in one scan, because the single-node Docker stack plans one shard.
- Spark 3.5 cannot write `time`, `timestamp_ns`, or `timestamptz_ns` columns. Iceberg 1.10.1 `add_files` cannot import a `timestamp` or `timestamptz` identity partition, because `Conversions.fromPartitionString` has no case for either type and throws `UnsupportedOperationException`. The S7 unit test is therefore the only proof for those five types.
- The existing `e2e_partition_filter_prunes_and_returns_correct_rows` (`e2e_local/scan.rs`) over the natively written `regions` table guards that natively written identity-partitioned files read and prune as before.

### Manual Testing

Run after `make cross-udf-build`, the stack start, the Spark fixture job, and the VS deployment that `make test-e2e` performs.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-execution-field-id-projection-absent-fields | `exapump sql "SELECT id, region FROM MY_LAKEHOUSE.IDENT_PART_ABSENT ORDER BY id"` | ids 1 and 2 `eu`, 3 and 4 `us`, 5 `apac`, 6 `eu`, 7 and 8 NULL |
| scan-execution-field-id-projection-absent-fields | `exapump sql "SELECT region, COUNT(*) FROM MY_LAKEHOUSE.IDENT_PART_ABSENT GROUP BY region ORDER BY region"` | `apac` 1, `eu` 3, `us` 2, NULL 2 |
| scan-execution-field-id-projection-absent-fields | `exapump sql "SELECT id, region FROM MY_LAKEHOUSE.IDENT_PART_REQUIRED ORDER BY id"` | each row carries its partition value, no required-absent error |
| scan-execution-field-id-projection | `exapump sql "SELECT * FROM MY_LAKEHOUSE.IDENT_PART_TYPES ORDER BY id"` | rows 1 and 2 equal in every column, row 3 NULL in every partition column |
| scan-execution-partition-values | `exapump sql "EXPLAIN VIRTUAL SELECT ts FROM MY_LAKEHOUSE.INT96_TS_FAR_FUTURE"` | the per-shard files argument carries no `partition_values` |
| delta-table-planning | `exapump sql "EXPLAIN VIRTUAL SELECT id, region FROM MY_LAKEHOUSE.IDENT_PART_ABSENT"` | the files argument carries `region` values for imported files only, and the common argument names no partition column |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Spark fixtures | `docker compose up -d spark-iceberg-fixtures` then `docker wait lakehouse-engine-rs-spark-iceberg-fixtures-1` | Exit code 0 |
| Unit tests | `cargo test --workspace` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| Lint and format | `make fmt-check lint` | 0 errors or warnings, no format changes |
