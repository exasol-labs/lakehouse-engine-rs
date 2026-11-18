# Intent: add-export-partitioned-iceberg-table

## Interview

- Q: How to bound memory over many partitions? → A: writer-side cap, close the largest open file
- Q: Include the PARTITION_BY option for new tables? → A: Include it
- Q: E2E oracle for row placement? → A: Spark-created tables as oracle
- Q: PARTITION_BY when the table exists? → A: Refuse always
- Q: PARTITION_BY syntax? → A: Spark/Iceberg-style transform list

## Directions

- [D1] A writer buffers rows per partition and writes out the largest buffer at the budget, not one open file per partition, because each open Parquet and storage writer has a fixed cost and interleaved rows would give one file per row.
- [D2] Budget is min(max((memory limit − 200 MB) × 0.5, 64 MiB), 512 MiB), 512 MiB when unknown, counted as Arrow memory of buffered batches; capped because larger buffers only make larger files.
- [D3] Data files go under the table's partition path (`data/<field>=<value>/`), Iceberg's default location layout, so tools that list by directory see the partitions.
- [D4] `PARTITION_BY` syntax and column names are checked in the export hook, before any catalog call. Transform-to-type validity is checked by each writer before it creates the table, because the hook does not receive the source column types.
- [D5] Two fields with the same derived name are refused rather than renamed with a suffix, so a field's name always follows from its definition.
- [D6] The existing refusal of a `void` partition field named like a column stays: iceberg-rust's commit still refuses that shape.
- [D7] The long Background of export-append-iceberg-table is kept as it is, apart from the one bullet this plan changes; shortening it would move behavior into new scenarios, which is outside this issue.
- [D8] The bounded-memory E2E exports about 1 GB over 1,000 interleaved partitions, twice the maximum budget, and asserts more data files than partitions; an unbounded buffer would write exactly one per partition.
- [D9] The placement scenarios read each data file's partition tuple and rows through iceberg-rust in the test process, an oracle `specs/testing.md` § Correctness oracles does not yet name, because Exasol cannot show a file's partition tuple.
