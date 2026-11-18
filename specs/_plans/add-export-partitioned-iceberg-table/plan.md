# Plan: add-export-partitioned-iceberg-table

## Summary

The export writes into partitioned Iceberg tables: each writer splits its rows by the table's current partition spec and writes each partition's rows into its own data files, within a memory budget. A new `PARTITION_BY` option creates a new table with a partition spec (#568).

## Context

- After #567 the export appends only to unpartitioned tables, and refuses a table whose current spec has a field other than `void` (`refuse_unappendable` in `crates/lakehouse-engine/src/export/iceberg_rest.rs`).
- Every writer opens one `DataFileWriter` with an all-null `PartitionKey` (`partition_key` in `iceberg_sink.rs`), and `RecordedLocations` ignores the partition when it places a file.
- Rows reach the writers round-robin (`GROUP BY MOD(ROWNUM, G)`), so every writer can see every partition. An uncapped fan-out holds one Parquet writer and its upload buffer per partition.
- The pinned iceberg-rust (`da5cc19`) has `RecordBatchPartitionSplitter`, which computes every transform's partition values and splits a batch by them. Its `FanoutWriter` keeps every partition writer open until close and has no budget, and each open Parquet and storage writer has a fixed memory cost, so the export buffers rows per partition instead of keeping files open.
- `lakehouse-catalog-write`'s `create_table` and `stage_table` send `partition_spec: None`. The commit already pins the default partition spec ID (#651).
- The hook receives only the source column names, not their types (`ExportSpec`), so only a writer can check a transform against a column type.
- Structural changes: `architecture.md` in this plan directory (export component, writer data flow, writer budget constraint).
- Follow-up plans: none.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| EXPORT INTO SCRIPT Into a Partitioned Iceberg Table | NEW | `export/export-partitioned-iceberg-table/spec.md` |
| EXPORT INTO SCRIPT Into an Existing Iceberg Table | CHANGED | `export/export-append-iceberg-table/spec.md` |
| EXPORT INTO SCRIPT Options and Writer Count | CHANGED | `export/export-options/spec.md` |
| EXPORT INTO SCRIPT Into a New Iceberg Table | CHANGED | `export/export-iceberg-table/spec.md` |

## Impact

- An export into a partitioned table succeeds where it failed before. A version 3 table and a `void` field named like a column are still refused.
- New optional export option `PARTITION_BY`. An export that sets it into an existing table fails before reading any row.
- A writer whose rows exceed its memory budget writes a partition's rows into several data files. An unpartitioned writer's data files are therefore at most the budget (64 to 512 MiB) instead of 128 MiB.
- No breaking change. `docs/export.md` changes in the same PR.

## Implementation

1. **`PARTITION_BY` grammar** (`crates/lakehouse-engine/src/export/partition_by.rs`, new; tests in `partition_by_tests.rs`). It parses the option into an ordered list of `(transform, source column name, field name)`: blanks around tokens are ignored, and a column is a bare name or a double-quoted one with `""` for a quote. It resolves each column against the source column names (exact, then the single case-insensitive match). In the hook it refuses an empty or malformed list, an unknown transform, a width below 1, an unknown or ambiguous column, duplicate field names, a field name equal to another source column's name, and two time transforms on one column, each with an error that names `PARTITION_BY`, the field, and the accepted form. The last two mirror iceberg-rust's `PartitionSpecBuilder` rules, which refuse them only at bind time. In the writer, a second function checks each field's transform against the column's Exasol type (the § Background table) and builds the spec with `PartitionSpecBuilder` bound to the export's own Iceberg schema, so every library rule fails in this module before `create_table` or `stage_table`. It owns the grammar, the naming rule, and the type table, and nothing else does.
2. **Options and hook** (`options.rs`, `mod.rs`). `ExportOptions` accepts `PARTITION_BY` and the unknown-option error lists it. The hook parses and resolves it before any catalog call. When `prepare` answers `TableMode::Append` and `PARTITION_BY` is set, the hook refuses, naming the table. `WriterSpec` carries the resolved fields as `partition_by` (rendered and parsed strictly, as today).
3. **New-table creation** (`lakehouse-catalog-write/src/iceberg_write.rs`). `create_table` and `stage_table` take an optional `UnboundPartitionSpec`, and `creation_request` sends it. `WriterTable::New` passes it from the writer spec after the writer checks transform-to-type validity, so a type mismatch fails before any catalog write.
4. **Partition buffer** (`crates/lakehouse-engine/src/export/fanout.rs`, new; tests in `fanout_tests.rs`). `IcebergSink` holds a partition buffer instead of one `DataFileWriter`. It splits each batch with `RecordBatchPartitionSplitter::try_new_with_computed_values` over the table's current schema and default spec, and appends each split batch to the buffer of its `PartitionKey`, counting its `get_array_memory_size()`. While the counted total exceeds the budget, it writes the partition with the largest total into one data file (a rolling `DataFileWriter` built for that key, closed at once) and frees its buffer, so at most one data file is open at a time. `finish` writes every remaining buffer. An unpartitioned spec is one key, so the same path serves it. The budget is a pure function of `ctx.memory_limit()` (D2), passed in when the sink opens. The policy that picks which partitions to write is a pure function over `(key, bytes)` pairs and the budget, returning partitions largest first until the rest fit.
5. **File placement** (`iceberg_sink.rs`). `RecordedLocations::generate_location` passes the partition key to `DefaultLocationGenerator`, so a file lies under the partition path (D3). `partition_key` goes.
6. **Append target** (`iceberg_rest.rs`). `refuse_unappendable` drops the partitioned-table branch and keeps the format-version and void-named-like-column checks.
7. **Docs** (`docs/export.md`). Add `PARTITION_BY` to the statement, the options table, and the refusal list. Add a "Partitioned tables" section: the transforms, the type table, null partitions, the file layout, and that unordered rows over many partitions give more files. Delete the paragraph that refuses partitioned tables.
8. **Spark fixture** (`scripts/spark-fixtures/create_export_partition_fixture.sql`, listed in `run_fixtures.sh`; ground truth in `crates/lakehouse-e2e/tests/common/export_partition_fixtures.rs`). Namespace `e2e_export_partition`, format version 2:
   - `by_value`: identity on `string`, `long`, `date`; `bucket` on `int`, `long`, `decimal(20,2)`, `string`, `date`, `timestamp`, `timestamptz`; `truncate` on `int`, `long`, `decimal(20,2)`, `string`.
   - `by_time`: `year(date)`, `month(timestamp)`, `day(timestamptz)`, `hour(timestamp)`, `hour(timestamptz)`.
   - `evolved`: `identity(region)` with rows, then replaced by `bucket(8, id)`.
   - Rows chosen to discriminate: negative numbers (truncate rounds toward negative infinity), dates and timestamps before 1970, multibyte strings (truncate counts code points), values on bucket and truncate boundaries, one row NULL in every partition column, and rows NULL in one partition column each. Several rows per partition of each field.
   - In `by_value`, the rows of every `bucket` partition span the filter literals of the pruning test (values below and above each one), so the files of other buckets have column bounds that hold the literal and only partition values can exclude them.
   - Each test resets each table's `main` branch to the Spark snapshot before it appends, as `export_append` does.
   - A fixture guard, `test_export_partition_fixture_shape`, reads each table's partition spec from the catalog's REST API and, through the step 9 helper, asserts that every partition of every field holds at least two rows and that the `bucket` partitions span the pruning literals.
9. **E2E** (`crates/lakehouse-e2e/tests/e2e_local/export_partition.rs`, declared in `main.rs`). A shared helper in `common/e2e_harness.rs`, `data_file_partitions(namespace, table, snapshot filter)`, reads each data file's path, its partition values as text, and its row ids through iceberg-rust's scan planner and reader in the test process, and returns harness-owned types. The export sources are `SELECT` reads of the Spark tables through the virtual schema, so the exported rows hold Spark's values and ids. For each id, the export's file tuple must equal Spark's file tuple. Spark is the oracle that computes the transforms.
10. **Retire the old refusal** (`export_append.rs`, `create_export_append_fixture.sql`, `export_append_fixtures.rs`): delete the "a partitioned table" case and its `partitioned` Spark table.
11. **Testing rules** (`specs/testing.md`). § Correctness oracles: an export scenario about which partition a data file belongs to may read the table's manifests and data files through iceberg-rust in the test process, because Exasol shows no file's partition values (D9). § Fixtures: add the `export_partition` Spark fixture (tables, transforms, discriminating rows, guard) and drop "a partitioned table" from the `export_append` entry.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `partition_key` in `crates/lakehouse-engine/src/export/iceberg_sink.rs` | The fan-out builds a key per partition |
| Branch | the partitioned-table refusal in `refuse_unappendable` (`iceberg_rest.rs`) and its cases in `iceberg_rest_tests.rs` | Partitioned tables are accepted |
| Test case | "a partitioned table" in `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | The refusal is gone |
| Fixture | `partitioned` table in `scripts/spark-fixtures/create_export_append_fixture.sql` and `PARTITIONED` in `crates/lakehouse-e2e/tests/common/export_append_fixtures.rs` | Only the deleted case used it |
| Comment | `RecordedLocations::generate_location` doc ("no data file has a partition value") | No longer true |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| An append places every row in the partition that the table's spec assigns | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_append_places_rows_like_spark` (case table: `by_value`, `by_time`; several writers, `SINGLE_COMMIT`) |
| A row with a NULL partition column lands in the partition whose value is null | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_null_partition_columns_land_in_null_partitions` |
| A filter on a partition column reads only the exported files of matching partitions | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_partition_filter_prunes_exported_files` (one row per transform: the `EXPLAIN VIRTUAL` file set equals the export's files whose Spark-assigned partition value and column bounds can hold the literal, and rows match a native Exasol copy of the source. The `bucket` rows discriminate partition pruning from min/max pruning, per step 8) |
| An append writes under the table's current partition spec | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_append_writes_under_current_spec` |
| PARTITION_BY creates a partitioned table | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_partition_by_creates_partitioned_table` (spec read from the catalog's REST API; tuples against Spark's per transform and value) |
| An invalid PARTITION_BY fails the export before any data is read | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_invalid_partition_by_is_refused_before_reading` (case table, `failing_source`, `assert_refused_before_reading`) |
| A transform that the column's type does not take fails the export before any data is written | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_partition_by_transform_type_mismatch_is_refused` |
| PARTITION_BY into an existing table is refused | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_partition_by_into_existing_table_is_refused` |
| An export into many partitions completes within a bounded amount of memory | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_many_partitions_export_completes` (`PARTITION_BY = 'K'`, `K` cycling through 1,000 values row by row, 560,000 rows of 40 `DECIMAL` columns, over 600 MB of Arrow memory, above the 512 MiB maximum budget. A 1 GB source with a `VARCHAR` filler, as D8 planned, overflows the Docker image's 1 GiB temp volume (measured: the statement aborts with `FULL_DEVICE`), while narrow numeric rows cost Arrow about seven times the bytes Exasol stores. Asserts the data file count from the catalog's REST API exceeds 1,000) |
| A format-version 3 table is refused before any data is written | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | the existing refusal case table, minus the partitioned case, with the scenario line renamed |

Unit tests, each for logic E2E cannot isolate:

- `partition_by_tests.rs`: one case table of valid and invalid `PARTITION_BY` strings (field order, names, letter case, blanks, quoted names, ambiguity, duplicates, a field name equal to another column, two time transforms on one column, widths) and one of transform-to-type pairs.
- `fanout_tests.rs`: the budget function at a known, a small, a large, and an unknown limit, and the write-out policy returns partitions largest first until the rest fit the budget.
- `options_tests.rs`: `PARTITION_BY` is accepted and listed in the unknown-option error.
- `iceberg_rest_tests.rs`: a partitioned spec is no longer refused; version 3 and void-named-like-column still are.
- `iceberg_write_tests.rs`: `creation_request` carries the partition spec.

### Manual Testing

`DSN='exasol://sys:exasol@localhost:28563?validateservercertificate=0'` after `make e2e-up test-e2e`. `W` stands for `WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS' ALLOW_HTTP = 'true'`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| EXPORT INTO SCRIPT Into a Partitioned Iceberg Table | `exapump sql -d "$DSN" "EXPORT (SELECT 1 AS ID, DATE '2024-01-15' AS D UNION ALL SELECT 2, NULL) INTO SCRIPT LHVS.LAKEHOUSE_EXPORT W TABLE_NAME = 'manual_part.t' PARTITION_BY = 'day(D), bucket(4, ID)'"` | Row count 2; `curl -s localhost:18181/v1/namespaces/manual_part/tables/t \| jq '.metadata["partition-specs"]'` shows `D_day` (day) and `ID_bucket` (bucket[4]) |
| EXPORT INTO SCRIPT Into an Existing Iceberg Table | The same statement with `SINGLE_COMMIT = 'true'` and without `PARTITION_BY` | Row count 2; the table holds 4 rows, and data files lie under `data/D_day=2024-01-15/...` and `data/D_day=null/...` |
| EXPORT INTO SCRIPT Options and Writer Count | The same statement with `PARTITION_BY = 'D'` | Fails before reading, naming `manual_part.t` and stating that `PARTITION_BY` applies only to a table the export creates |
| EXPORT INTO SCRIPT Into a New Iceberg Table | `... TABLE_NAME = 'manual_part.u' PARTITION_BY = 'hour(D)'` | Fails naming `D_hour`, column `D`, and type `DATE`; `manual_part.u` does not exist |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` then `python3 .claude/skills/test-strategy-review/scripts/changed_line_coverage.py <scratchpad>/lcov.info` | ≥ 90% of changed lines |
| Lint | `make lint` | 0 errors/warnings |
| Format | `make fmt-check` | No changes |
