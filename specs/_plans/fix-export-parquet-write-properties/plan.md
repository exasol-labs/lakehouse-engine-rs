# Plan: fix-export-parquet-write-properties

## Summary

The export writes its Parquet data files by the table's `write.parquet.*` and `write.target-file-size-bytes` properties, a table it creates carries `zstd`, 128 MiB row groups, and 256 MiB files, and an export into an unpartitioned table streams its rows into one rolling data file without the partition buffer (#670).

## Context

- `IcebergSink::open` (`crates/lakehouse-engine/src/export/iceberg_sink.rs`) builds the Parquet writer from `WriterProperties::default()`: no compression, row groups capped at 1,048,576 rows and not in bytes, and no table property read.
- The rolling writer rolls at the partition buffer budget (64 to 512 MiB), not at `write.target-file-size-bytes`.
- Since #568, every export goes through `PartitionBuffer` (`fanout.rs`). An unpartitioned table has one partition, so the buffer holds up to the budget of rows before it writes any, and only adds memory.
- iceberg-rust at the pinned revision has `ParquetWriterBuilder::from_table_properties`, which maps the codec, compression level, row group bytes, page size, page row limit, and dictionary size, and `TableProperties::write_target_file_size_bytes`. Both return an error for a value they cannot parse.
- `creation_request` (`crates/lakehouse-catalog-write/src/iceberg_write.rs`) builds the properties of both the created and the staged (`SINGLE_COMMIT`) table.
- Depends on #568 (merged as #659). Lands before the measurements of #572.
- Architecture delta: `architecture.md` in this directory (export component, writer data flow, writer memory constraint).

## Features

| Feature | Status | Spec |
|---------|--------|------|
| export-iceberg-table | CHANGED | `export/export-iceberg-table/spec.md` |
| export-append-iceberg-table | CHANGED | `export/export-append-iceberg-table/spec.md` |
| export-partitioned-iceberg-table | CHANGED | `export/export-partitioned-iceberg-table/spec.md` |

## Impact

- Exported data files are `zstd`-compressed, so exported tables shrink and reads of them move fewer bytes.
- A table the export creates carries three write properties, which other engines also follow when they write to it.
- An append follows the target table's write properties. An append into a table with an unusable write property, such as `write.parquet.compression-codec = lzo`, now fails, where it used to succeed with uncompressed files. This is the one behavior change that can break a working statement.
- An export into an unpartitioned table holds one row group in progress instead of up to 512 MiB of rows, and its files roll at 256 MiB instead of at the buffer budget.
- No option, CONNECTION field, or virtual schema property changes.

## Dependencies

- iceberg-rust at the pinned revision: `ParquetWriterBuilder::from_table_properties`, `TableProperties`. No new crate.

## Implementation

1. `lakehouse-catalog-write`: `creation_request` adds `write.parquet.compression-codec = zstd`, `write.parquet.row-group-size-bytes = 134217728`, and `write.target-file-size-bytes = 268435456`, using the `TableProperties::PROPERTY_*` key constants. Extend the existing `creation_request` unit test in `iceberg_write_tests.rs` to assert the three entries.
2. `IcebergSink::open`: read `TableProperties` from the opened table's metadata properties, build the writer with `ParquetWriterBuilder::from_table_properties`, and pass `write_target_file_size_bytes()` to `RollingFileWriterBuilder` in place of the budget. An unusable value fails `open` with `could not prepare the data files of table <table>: invalid write property <key> = '<value>': <cause>`. iceberg-rust's errors do not always name the key (an out-of-range compression level, the `content-defined-chunking` keys), so the sink calls each `TableProperties` accessor that `from_table_properties` uses, in turn, and names the key of the first that fails, with its raw value. `write.parquet.row-group-size-bytes = 0` makes parquet-rs panic in `set_max_row_group_bytes`, so the sink refuses 0 before it calls `from_table_properties`. `lzo` parses and maps to `Compression::LZO`, but parquet-rs fails on it only at the first row group, so `open` also refuses `parquet_compression_codec() == CompressionCodec::Lzo` through the same error path. The workspace's `parquet` default features enable every other codec. `open` runs at each writer's first row, before any write, so every writer fails and none commits.
3. `IcebergSink`: hold either a `PartitionBuffer` (partitioned spec) or one open `DataFileWriter` built on the null partition key (unpartitioned spec). `write` sends an unpartitioned batch straight to the open writer, and `finish` closes it and adds its data files. Move `null_key` into the sink or keep it `pub(super)` in `fanout.rs`; `PartitionBuffer::new` refuses an unpartitioned spec only by `debug_assert`, as the sink never builds one.
4. `fanout.rs`: drop `Split::Unpartitioned` and the `Split` enum. `PartitionBuffer` holds the `RecordBatchPartitionSplitter` directly.
5. `docs/export.md`: in "The table", add a "Data files" subsection: Parquet whatever `write.format.default` says, the six `write.parquet.*` properties and `write.target-file-size-bytes`, Iceberg's defaults, the three properties of a new table and why files are 256 MiB, that an append follows the table's properties and never changes them, that an unusable property fails the export, that a row group also ends at 1,048,576 rows, that content-defined chunking follows its table properties, and that a larger row group size costs writer memory. In "Partitioned tables", rewrite the buffer paragraph (line 137) so it applies to partitioned tables only and states that each write-out gets files of its own, which can stay below `write.target-file-size-bytes`. State that an unpartitioned export buffers no rows.
6. `specs/architecture.md` changes only through the delta at record time.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Enum | `Split` in `crates/lakehouse-engine/src/export/fanout.rs` | The buffer serves partitioned specs only |
| Test | `an_unpartitioned_spec_buffers_every_row_under_one_key` in `fanout_tests.rs` | Unpartitioned rows no longer pass the buffer |
| Import | `parquet::file::properties::WriterProperties` in `iceberg_sink.rs` | Replaced by `from_table_properties` |
| Comment | "A write-out is at most the budget, so it rolls over ..." in `IcebergSink::open` | Files roll at the target file size |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A table the export creates carries its write properties | Integration | `crates/lakehouse-e2e/tests/e2e_local/export.rs` | `export_created_table_carries_write_properties` |
| An export into an unpartitioned table rolls its data files at the target file size | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `export_append_follows_the_table_write_properties` (the same run asserts both scenarios) |
| An append writes data files by the table's write properties | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `export_append_follows_the_table_write_properties` |
| An unusable write property fails the export before any data is written | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `export_refuses_unusable_write_properties` |

Test notes:

- `export_created_table_carries_write_properties`: a case table of two runs, `WRITERS_PER_NODE = '4'` and `SINGLE_COMMIT = 'true'`. Assert the three properties exactly from the REST catalog's table metadata, and read every data file's footer with `read_footer` (`common/pruning_cases.rs`): every column chunk is `ZSTD`, and no row group's compressed size exceeds 134217728 by more than a tenth.
- `export_append_follows_the_table_write_properties`: create the table by an export, then set the three properties through the REST catalog's `set-properties` table update. Grep `crates/lakehouse-e2e/tests/` for an existing REST update helper first; put a new one in `common/e2e_harness.rs`. Source: about 200,000 rows of 100 random characters, built in Exasol with `CONNECT BY LEVEL`, well within the Docker 1 GiB temp volume. Assert every column chunk is `SNAPPY`, no row group's compressed size exceeds 1 MiB by more than a tenth, more than one data file, each at most 4 MiB plus 1 MiB, and the table properties after the append equal those before.
- `export_refuses_unusable_write_properties`: three cases, `write.parquet.compression-codec = lzo`, `write.parquet.row-group-size-bytes = 0`, and `write.target-file-size-bytes = big`, set through `set-properties` on an exported table. Assert the error contains the table, the key, and the value, and that the snapshot count is unchanged.

### Manual Testing

Against the local stack (`make e2e-up`, `.so` built with `make cross-udf-build` and deployed):

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| export-iceberg-table | `exapump sql "EXPORT (SELECT LEVEL AS ID, 'v' \|\| LEVEL AS LABEL FROM DUAL CONNECT BY LEVEL <= 100000) INTO SCRIPT LAKEHOUSE.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS' TABLE_NAME = 'manual_670.created'"`, then `curl -s localhost:18181/v1/namespaces/manual_670/tables/created \| jq .metadata.properties` | Row count 100000; properties hold `write.parquet.compression-codec: zstd`, `write.parquet.row-group-size-bytes: 134217728`, `write.target-file-size-bytes: 268435456` |
| export-append-iceberg-table | `curl -s -X POST localhost:18181/v1/namespaces/manual_670/tables/created -d '{"requirements":[],"updates":[{"action":"set-properties","updates":{"write.parquet.compression-codec":"snappy"}}]}'`, run the EXPORT again, download the newest data file and print its footer with `python3 -c "import pyarrow.parquet as pq,sys; print(pq.ParquetFile(sys.argv[1]).metadata.row_group(0).column(0).compression)" <file>` | `SNAPPY`; the earlier files stay `ZSTD`; properties still read `snappy` |
| export-append-iceberg-table | Set `write.parquet.compression-codec` to `lzo` the same way and run the EXPORT again | The statement fails naming `manual_670.created`, `write.parquet.compression-codec`, and `lzo`; snapshot count unchanged |
| export-partitioned-iceberg-table | Run `export_partition` E2E module (`cargo test --workspace --test e2e_local export_partition -- --test-threads=1`) | Passes unchanged; files of a new partitioned table are `ZSTD` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace --lib` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info`, then `.claude/skills/test-strategy-review/scripts/changed_line_coverage.py` | At least 90% of changed lines |
| Lint and format | `make fmt-check lint` | 0 errors or warnings, no changes |
