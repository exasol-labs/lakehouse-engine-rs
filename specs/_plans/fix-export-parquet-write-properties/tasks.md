# Tasks: fix-export-parquet-write-properties

- [x] 1. `creation_request` adds the three write properties; extend its unit test (Implementation 1)
- [x] 2. Write-property validation helper: names the key and raw value of an unusable property, refuses `lzo` and a row group size of 0; unit tests (Implementation 2)
- [x] 3. `IcebergSink::open` builds the Parquet writer from the table's properties and rolls at `write.target-file-size-bytes` (Implementation 2)
- [x] 4. Unpartitioned sink streams into one open data file; `fanout.rs` drops `Split` and its unpartitioned test (Implementation 3, 4, Dead Code Removal)
- [x] 5. E2E `export_created_table_carries_write_properties` (Scenario: A table the export creates carries its write properties)
- [x] 6. E2E `export_append_follows_the_table_write_properties` (Scenarios: An append writes data files by the table's write properties; An export into an unpartitioned table rolls its data files at the target file size)
- [x] 7. E2E `export_refuses_unusable_write_properties` (Scenario: An unusable write property fails the export before any data is written)
- [x] 8. `docs/export.md` Data files subsection and partitioned buffer paragraph (Implementation 5)
