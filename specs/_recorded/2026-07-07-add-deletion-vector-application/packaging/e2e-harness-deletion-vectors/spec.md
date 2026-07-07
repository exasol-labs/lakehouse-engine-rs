# Feature: End-to-End Harness — Deletion Vectors

Extends the end-to-end harness (`packaging/e2e-harness`) with a matrix that drives Iceberg
format-version-3 deletion-vector tables through the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet data files + `deletion-vector-v1` Puffin blobs
in MinIO — verifying that the post-delete row set is returned, that deletion vectors compose with
projection/filter/LIMIT/aggregation, and that a mixed table whose data files are split across
positional-delete and deletion-vector mechanisms returns the correct combined post-delete set
regardless of fan-out placement of the affected data files.

## Background

* All deletion-vector tables are produced by the Spark fixtures (see
  `packaging/deletion-vector-fixtures`); the harness reads them through the shared REST catalog
  over MinIO.
* Every scenario MUST fail (not skip) if the Exasol Docker container, Spark service, REST catalog,
  or MinIO is unavailable.
* Correctness is asserted against the recorded deleted-row set (equivalently, the single-node
  DataFusion result over the same post-delete data).
* These scenarios exercise the SUCCESS path for deletion vectors; the plan-time fail-loud path for
  the mechanisms that remain unsupported (equality deletes, ORC/Avro) is covered by
  `packaging/e2e-harness-positional-deletes`.

## Scenarios

### Scenario: End-to-end query over a deletion-vector table returns post-delete rows

* *GIVEN* the Docker stack is running with a Spark-produced `format-version=3` merge-on-read deletion-vector table and the VS adapter and scan UDF installed
* *WHEN* a `SELECT` over that virtual table is issued
* *THEN* the returned rows MUST exactly match the seeded rows minus the recorded deleted rows
* *AND* no deleted row SHALL appear in the result
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end deletion vectors compose with projection, filter, and LIMIT

* *GIVEN* the Docker stack is running with a deletion-vector table and the VS installed
* *WHEN* a `SELECT` projecting a subset of columns with a WHERE predicate and a LIMIT is issued against that virtual table
* *THEN* the returned rows MUST equal the same projection/filter/LIMIT evaluated over the post-delete data on a single node
* *AND* no deleted row SHALL appear in the result
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end deletion vectors compose with aggregation

* *GIVEN* the Docker stack is running with a deletion-vector table and the VS installed
* *WHEN* a single-group and a grouped aggregate query are issued against that virtual table
* *THEN* the aggregate results MUST equal the same aggregates evaluated over the post-delete data on a single node (deleted rows contribute to no count, sum, or group)
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end query over a mixed positional-delete and deletion-vector table returns the combined post-delete set

* *GIVEN* the Docker stack is running with the Spark-produced mixed-mechanism table whose data files are split so some are backed by Parquet positional deletes and others by v3 deletion vectors, and the VS installed
* *WHEN* a `SELECT` over that virtual table is issued
* *THEN* the returned rows MUST exactly match the seeded rows minus the recorded deleted rows across both mechanisms
* *AND* no row deleted by either the positional-delete files or the deletion vectors SHALL appear in the result
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: Mixed-mechanism post-delete result is invariant across fan-out placement

* *GIVEN* the Docker stack is running with the mixed-mechanism table and the VS installed
* *WHEN* the same `SELECT` is issued under two configurations the test forces via the shard count / parallelism factor — one that co-locates the positional-delete-backed and DV-backed data files in the SAME UDF/shard and one that splits them across DIFFERENT UDFs/shards
* *THEN* both configurations MUST return the same combined post-delete row set, equal to the seeded rows minus the recorded deleted rows across both mechanisms
* *AND* the test MUST deterministically force both the same-shard and the different-shard placements by controlling the shard count / parallelism factor, and MUST NOT rely on hash-partitioning luck
* *AND* the test MUST fail (not skip) if the stack is unavailable
