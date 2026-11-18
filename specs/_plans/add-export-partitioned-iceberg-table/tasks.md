# Tasks: add-export-partitioned-iceberg-table

- [x] 1. `PARTITION_BY` grammar and column resolution in `export/partition_by.rs` (Implementation 1; unit case table in `partition_by_tests.rs`)
- [x] 2. Transform-to-type check and bound spec build in `partition_by.rs` (Implementation 1; unit case table of transform-to-type pairs)
- [x] 3. `ExportOptions` accepts `PARTITION_BY` and lists it in the unknown-option error (Implementation 2; `options_tests.rs`)
- [x] 4. Hook parses and resolves `PARTITION_BY` before any catalog call, refuses it for `TableMode::Append`, and `WriterSpec` carries `partition_by` (Implementation 2)
- [x] 5. `create_table` and `stage_table` take an optional `UnboundPartitionSpec` and `creation_request` sends it (Implementation 3; `iceberg_write_tests.rs`)
- [x] 6. `WriterTable::New` checks transform-to-type validity and passes the spec to the catalog (Implementation 3)
- [x] 7. Budget function and write-out policy in `export/fanout.rs` (Implementation 4; `fanout_tests.rs`)
- [x] 8. `IcebergSink` buffers per partition through the fan-out and writes one data file per write-out (Implementation 4)
- [x] 9. `RecordedLocations` places a file under its partition path; `partition_key` and its doc comment go (Implementation 5, Dead Code Removal)
- [x] 10. `refuse_unappendable` accepts partitioned tables and keeps the version 3 and void-named-like-column refusals (Implementation 6, Dead Code Removal; `iceberg_rest_tests.rs`)
- [x] 11. `docs/export.md`: `PARTITION_BY`, the Partitioned tables section, the refusal list (Implementation 7)
- [x] 12. Spark fixture `create_export_partition_fixture.sql`, `run_fixtures.sh` entry, ground truth `export_partition_fixtures.rs` (Implementation 8)
- [x] 13. Harness helper `data_file_partitions` in `common/e2e_harness.rs` (Implementation 9)
- [x] 14. E2E `e2e_local/export_partition.rs` with every scenario of export-partitioned-iceberg-table and the fixture guard (Implementation 9, Scenario Coverage)
- [x] 15. Retire the partitioned-table refusal case and the `partitioned` Spark table from `export_append` (Implementation 10, Dead Code Removal)
- [x] 16. `specs/testing.md` § Correctness oracles and § Fixtures (Implementation 11)
- [x] 17. Verification: build, unit tests, E2E, coverage, lint, format, manual steps
