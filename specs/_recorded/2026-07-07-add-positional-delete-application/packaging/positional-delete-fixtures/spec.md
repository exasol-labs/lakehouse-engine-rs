# Feature: Positional-Delete E2E Fixtures (Apache Spark)

Adds Iceberg merge-on-read positional-delete fixtures to the end-to-end test stack, produced by
Apache Spark (Iceberg Spark runtime, `write.delete.mode=merge-on-read`), so the delete-application
scan path can be validated full-stack. Two fixtures are produced — one at
`write.delete.granularity=file` and one at `write.delete.granularity=partition` — because the two
granularities exercise different scan-time behavior (a file-scoped delete file versus a
partition-scoped delete file that references many data files). Each fixture records the set of
rows it deletes so tests can assert the post-delete result exactly.

## Background

* Positional-delete fixtures are produced by Apache Spark because iceberg-rust 0.10 has no
  position-delete writer (upstream apache/iceberg-rust #340) and pyiceberg is copy-on-write only;
  the fixture engine MUST be an official Apache Iceberg ecosystem engine. The Spark fixture
  carries an upstream-tracking comment with #340 as its drop condition.
* Fixtures are written against the SAME shared REST catalog + MinIO used by the rest of the E2E
  stack, so the VS resolves them through its normal catalog path.
* Equality deletes and v3/Puffin deletion vectors are explicitly NOT produced by these fixtures —
  they are out of scope for this feature and are covered instead by the plan-time fail-loud gate.
* The fixture data files and delete files are all Parquet; no ORC or Avro fixtures are produced.

## Scenarios

### Scenario: Spark produces a file-granularity positional-delete fixture

* *GIVEN* the E2E stack is running with the shared REST catalog over MinIO and an Apache Spark service with the Iceberg Spark runtime
* *WHEN* the fixture step creates a merge-on-read table (`write.delete.mode=merge-on-read`, `write.delete.granularity=file`) and issues a `DELETE`/`MERGE` that removes a known subset of rows
* *THEN* the fixture SHALL commit a snapshot carrying Parquet positional-delete files scoped at file granularity
* *AND* the fixture SHALL record the exact set of deleted rows so a test can assert the post-delete result
* *AND* the fixture step SHALL fail (not skip) if the Spark service, REST catalog, or MinIO is unavailable

### Scenario: Spark produces a partition-granularity positional-delete fixture

* *GIVEN* the E2E stack is running with the shared REST catalog over MinIO and an Apache Spark service with the Iceberg Spark runtime
* *WHEN* the fixture step creates a partitioned merge-on-read table (`write.delete.mode=merge-on-read`, `write.delete.granularity=partition`) laid out across at least two partitions each holding at least two data files, and issues a `DELETE`/`MERGE` removing a known subset of rows whose affected data files span more than one partition
* *THEN* the fixture SHALL commit a snapshot carrying partition-scoped Parquet positional-delete files whose `file_path` columns collectively reference data files across multiple partitions (each delete file referencing multiple data files within its partition)
* *AND* the fixture SHALL record the exact set of deleted rows so a test can assert the post-delete result
* *AND* the fixture step SHALL fail (not skip) if the Spark service, REST catalog, or MinIO is unavailable
