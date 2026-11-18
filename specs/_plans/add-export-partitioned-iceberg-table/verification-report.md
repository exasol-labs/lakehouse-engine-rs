# Verification Report: add-export-partitioned-iceberg-table

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The export writes into partitioned Iceberg tables and creates them with `PARTITION_BY`; every scenario has a passing E2E test against the local stack, and every check passes. |
| Code review | 7 findings — 7 fixed |

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
| Unit | 93.8% of changed production lines (407/434, gate 90%); 91.87% workspace line coverage |
| Integration | Not measured: `cargo llvm-cov` counts only the unit tests |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit | 2177 | 2177 | 0 |
| Integration | 411 | 411 | 0 |

### Manual Tests

DSN `exasol://sys:exasol@localhost:28563?validateservercertificate=0`, source `(SELECT 1 AS ID, DATE '2024-01-15' AS D UNION ALL SELECT 2, NULL)`.

| Test | Result |
|------|--------|
| EXPORT INTO SCRIPT Into a Partitioned Iceberg Table: `PARTITION_BY = 'day(D), bucket(4, ID)'` into `manual_part.t` | ✓ Statement OK; the catalog's spec 0 holds `D_day` (`day`) and `ID_bucket` (`bucket[4]`) |
| EXPORT INTO SCRIPT Into an Existing Iceberg Table: the same source with `SINGLE_COMMIT = 'true'` | ✓ Statement OK; `total-records` 4; data files under `data/D_day=2024-01-15/ID_bucket=3/` and `data/D_day=null/ID_bucket=3/` |
| EXPORT INTO SCRIPT Options and Writer Count: `PARTITION_BY = 'D'` into the existing table | ✓ `table manual_part.t exists, and PARTITION_BY applies only to a table the export creates; remove PARTITION_BY to append to the table` |
| EXPORT INTO SCRIPT Into a New Iceberg Table: `PARTITION_BY = 'hour(D)'` into `manual_part.u` | ✓ `PARTITION_BY field D_hour applies hour to column D of type DATE, which hour does not take; hour takes TIMESTAMP`; `manual_part.u` answers HTTP 404 |

## Tool Evidence

### Linter

```
make lint  (cargo clippy --workspace --all-targets --all-features -- -D warnings)
exit 0, no warnings
```

### Formatter

```
make fmt-check  (cargo fmt --all -- --check)
exit 0, no changes
```

### Build

```
make cross-udf-build
exit 0; cargo exasol-udf validate passed
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| export | export-partitioned-iceberg-table | An append places every row in the partition that the table's spec assigns | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_append_places_rows_like_spark` | Pass |
| export | export-partitioned-iceberg-table | A row with a NULL partition column lands in the partition whose value is null | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_null_partition_columns_land_in_null_partitions` | Pass |
| export | export-partitioned-iceberg-table | A filter on a partition column reads only the exported files of matching partitions | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_partition_filter_prunes_exported_files` | Pass |
| export | export-partitioned-iceberg-table | An append writes under the table's current partition spec | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_append_writes_under_current_spec` | Pass |
| export | export-partitioned-iceberg-table | PARTITION_BY creates a partitioned table | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_partition_by_creates_partitioned_table` | Pass |
| export | export-partitioned-iceberg-table | An invalid PARTITION_BY fails the export before any data is read | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_invalid_partition_by_is_refused_before_reading` | Pass |
| export | export-partitioned-iceberg-table | A transform that the column's type does not take fails the export before any data is written | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_partition_by_transform_type_mismatch_is_refused` | Pass |
| export | export-partitioned-iceberg-table | PARTITION_BY into an existing table is refused | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_partition_by_into_existing_table_is_refused` | Pass |
| export | export-partitioned-iceberg-table | An export into many partitions completes within a bounded amount of memory | `crates/lakehouse-e2e/tests/e2e_local/export_partition.rs` | `test_many_partitions_export_completes` | Pass |
| export | export-append-iceberg-table | A format-version 3 table is refused before any data is written | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `export_refuses_appends_the_table_cannot_take` | Pass |

The fixture guard `test_export_partition_fixture_shape` also passes.

## Notes

- The many-partitions test exports 560,000 rows of 40 `DECIMAL` columns, over 600 MB of Arrow memory, instead of the 1 GB `VARCHAR` source D8 planned. Measured: the 1 GB source aborts the statement with `FULL_DEVICE`, because the Docker image's temp volume is 1 GiB and the writer's input takes about 1.6 to 2.5 bytes of temp per source byte. The UDF memory limit there is 1,280 MiB, so the budget is the 512 MiB cap, and the narrow rows exceed it. `plan.md`'s Scenario Coverage row states the new shape.
- The plan's Dead Code table removes `partition_key`. iceberg-rust's `RecordBatchPartitionSplitter` refuses a spec without a non-`void` field, so the all-null key survives as `null_key` in `fanout.rs`, and its unit test moved with it.
- `refuse_unappendable` refuses a partition field named like a column unless it is that column's identity, the rule iceberg-rust's commit applies. The spec's `void` case is a subset of it. A Spark identity field such as `region` therefore appends.
- The full E2E run passed 409 of 411. The other 2 (`export_errors_carry_no_connection_password`, `export_refuses_invalid_statements_before_reading`) failed only on expected values that this change alters: the unknown-option list now names `PARTITION_BY`, and a forged writer spec needs the new `partition_by` field. After those test edits, both pass on rerun.
- `table_exists` moved from `export.rs` to `common/table_state.rs`, because the new suite needs it too.
- Changed-line coverage was measured over the uncommitted working tree against the merge base `92bab541`. Untracked files count in full.
