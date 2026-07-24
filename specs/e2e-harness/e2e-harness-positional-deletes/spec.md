# Feature: End-to-End Harness — Positional Deletes

Extends the end-to-end harness (`e2e-harness/e2e-harness`) with a matrix that drives Iceberg
merge-on-read positional-delete tables through the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet data + positional-delete files in MinIO —
verifying that the post-delete row set is returned, that deletes compose with projection/filter/
LIMIT/aggregation, that both `file` and `partition` delete granularity work, that a partition-scoped
delete spanning multiple partitions is applied correctly and its result is invariant to fan-out
placement of the affected data files, that delete-free tables do not regress, and that unsupported
delete mechanisms fail loud.

## Background

* All positional-delete tables are produced by the Spark fixtures (see
  `packaging/positional-delete-fixtures`); the harness reads them through the shared REST catalog
  over MinIO.
* Every scenario MUST fail (not skip) if the Exasol Docker container, Spark service, REST catalog,
  or MinIO is unavailable.
* Correctness is asserted against the recorded deleted-row set (equivalently, the single-node
  DataFusion result over the same post-delete data).

## Scenarios

### Scenario: End-to-end query over a file-granularity delete table returns post-delete rows

* *GIVEN* the Docker stack is running with a Spark-produced `write.delete.granularity=file` merge-on-read table and the VS adapter and scan UDF installed
* *WHEN* a `SELECT` over that virtual table is issued
* *THEN* the returned rows MUST exactly match the seeded rows minus the recorded deleted rows
* *AND* no deleted row SHALL appear in the result
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end query over a partition-granularity delete table returns post-delete rows

* *GIVEN* the Docker stack is running with a Spark-produced `write.delete.granularity=partition` merge-on-read table and the VS installed
* *WHEN* a `SELECT` over that virtual table is issued
* *THEN* the returned rows MUST exactly match the seeded rows minus the recorded deleted rows, with each partition-scoped delete file applied only to the data files it references
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end query over a multi-partition-spanning delete returns the exact post-delete set

* *GIVEN* the Docker stack is running with the Spark-produced partition-granularity table whose positional-delete files reference data files spanning multiple partitions (at least two partitions, each with at least two data files) and the VS installed
* *WHEN* a `SELECT` over that virtual table is issued
* *THEN* the returned rows MUST exactly match the seeded rows minus the recorded deleted rows, with each partition-scoped delete file applied only to the data files it references across every affected partition
* *AND* no deleted row from any affected partition SHALL appear in the result
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: Post-delete result is invariant across fan-out placement of affected data files

* *GIVEN* the Docker stack is running with the multi-partition-spanning partition-granularity delete table and the VS installed
* *WHEN* the same `SELECT` is issued under two configurations the test forces via the shard count / parallelism factor — one that co-locates all affected data files in the SAME UDF/shard and one that splits them across DIFFERENT UDFs/shards
* *THEN* both configurations MUST return the same post-delete row set, equal to the seeded rows minus the recorded deleted rows, so correctness is invariant to whether a shared delete file is read by one UDF or independently by several
* *AND* the test MUST deterministically force both the same-shard and the different-shard placements by controlling the shard count / parallelism factor, and MUST NOT rely on hash-partitioning luck
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end deletes compose with projection, filter, and LIMIT

* *GIVEN* the Docker stack is running with a merge-on-read positional-delete table and the VS installed
* *WHEN* a `SELECT` projecting a subset of columns with a WHERE predicate and a LIMIT is issued against that virtual table
* *THEN* the returned rows MUST equal the same projection/filter/LIMIT evaluated over the post-delete data on a single node
* *AND* no deleted row SHALL appear in the result
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end deletes compose with aggregation

* *GIVEN* the Docker stack is running with a merge-on-read positional-delete table and the VS installed
* *WHEN* a single-group and a grouped aggregate query are issued against that virtual table
* *THEN* the aggregate results MUST equal the same aggregates evaluated over the post-delete data on a single node (deleted rows contribute to no count, sum, or group)
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end unsupported delete mechanism fails loud

* *GIVEN* the Docker stack is running with a table whose snapshot carries an unsupported delete mechanism (an equality-delete or a Puffin / v3 deletion-vector table)
* *WHEN* a query over that virtual table is issued
* *THEN* the query MUST fail at plan time with a clean error naming the unsupported delete mechanism, and MUST NOT return any rows
* *AND* the error MUST NOT contain any storage access key, secret key, or session token
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end delete-free table shows no regression

* *GIVEN* the Docker stack is running with a delete-free Iceberg table and the VS installed
* *WHEN* the existing projection/filter/LIMIT and aggregate queries are issued against that virtual table
* *THEN* the results MUST be identical to the pre-feature results, confirming the unified `ParquetSource`-backed provider does not regress the delete-free path
* *AND* the test MUST fail (not skip) if the stack is unavailable
