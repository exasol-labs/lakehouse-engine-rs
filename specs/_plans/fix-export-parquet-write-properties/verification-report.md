# Verification Report: fix-export-parquet-write-properties

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Exported data files follow the table's write properties, a new table carries `zstd`, 128 MiB row groups, and 256 MiB files, and an unpartitioned export streams its rows into one rolling data file. |
| Code review | 4 findings — 4 fixed |

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
| Unit | 92.8% of changed lines (142/153), 91.73% of workspace lines |
| Integration | Not measured; the E2E suites run against the deployed `.so` |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit | 2,099 | 2,099 | 0 |
| Integration | 433 | 433 | 0 |

The full local E2E suite (433) ran before two changes: boxing the partition buffer for a clippy size lint, and reading the chunking sizes with `.ok()?` in `refused`. Neither changes behavior. After the first, the `.so` was rebuilt and the export suites (`export`, `export_append`, `export_partition`, 29 tests) passed again. The second came after that rerun and is covered by the unit tests, which ran on the final code.

### Manual Tests

| Test | Result |
|------|--------|
| Export of 100,000 rows into a new table: the REST catalog shows `write.parquet.compression-codec: zstd`, `write.parquet.row-group-size-bytes: 134217728`, `write.target-file-size-bytes: 268435456` | ✓ |
| `set-properties` to `snappy`, then the same export again: the new file is `SNAPPY` (1,241,595 bytes), the earlier file stays `ZSTD(1)` (575,214 bytes), and the property still reads `snappy` | ✓ |
| `set-properties` to `lzo`, then the same export: fails with `could not prepare the data files of table manual_670_1795086516.created: invalid write property write.parquet.compression-codec = 'lzo': the export cannot write lzo`; snapshot count stays 2 | ✓ |
| Export with `PARTITION_BY = 'GRP'` into a new table: three files, one per partition, each `ZSTD(1)` | ✓ |

The plan's manual commands lacked `ALLOW_HTTP = 'true'`, which the local plain-HTTP catalog needs. The footers were read with a throwaway reader on the `parquet` crate, because no Parquet tool is installed on the host.

## Tool Evidence

### Linter

```
make lint: cargo clippy --workspace --all-targets --all-features -- -D warnings
exit 0
```

### Formatter

```
make fmt-check: cargo fmt --all -- --check
exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| export | export-iceberg-table | A table the export creates carries its write properties | `crates/lakehouse-e2e/tests/e2e_local/export.rs` | `export_created_table_carries_write_properties` | Pass |
| export | export-iceberg-table | An export into an unpartitioned table rolls its data files at the target file size | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `export_append_follows_the_table_write_properties` | Pass |
| export | export-append-iceberg-table | An append writes data files by the table's write properties | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `export_append_follows_the_table_write_properties` | Pass |
| export | export-append-iceberg-table | An unusable write property fails the export before any data is written | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `export_refuses_unusable_write_properties` | Pass |

The naming of every unusable property, including the compression level and the content-defined chunking keys, is unit-tested in `crates/lakehouse-engine/src/export/iceberg_sink_tests.rs` (`an_unusable_write_property_is_named_with_its_value`).

## Notes

- A row group can end slightly above `write.parquet.row-group-size-bytes`, because parquet-rs estimates how many rows fit. A 1 MiB limit gave a 1,076,745-byte row group. The scenarios, the test, and `docs/export.md` allow up to a tenth above the limit (intent direction D3).
- `refused` copies parquet-rs's content-defined chunking mask formula, so a chunking setup that parquet-rs would refuse only at the first write fails when the writer opens the table. A change to that formula in a later parquet-rs needs the same change there.
- The rule that an unpartitioned writer holds no rows beyond the row group in progress has no direct test (intent direction D5). It follows from the code: an unpartitioned table's rows bypass the partition buffer.
- The manual run left namespace `manual_670_1795086516` in the local REST catalog.
