# Feature: Deletion-Vector E2E Fixtures (Apache Spark)

Adds Iceberg format-version-3 **deletion-vector** fixtures to the end-to-end test stack, produced
by Apache Spark (Iceberg Spark runtime), so the deletion-vector application scan path can be
validated full-stack. A `format-version=3` merge-on-read `DELETE` makes Iceberg commit a
`deletion-vector-v1` Puffin blob (one per data file) instead of a Parquet positional-delete file.
Two fixtures are produced: a **DV-only** table (all deletes as DVs) and a **mixed-mechanism** table
whose data files are split so some are backed by legacy Parquet positional deletes and others by
v3 DVs — the realistic v2→v3 migration shape. Each fixture records the exact set of rows it deletes
so tests can assert the post-delete result exactly.

## Background

* Deletion-vector fixtures are produced by Apache Spark because it is the reference v3 writer
  (v3 DVs became GA in Apache Iceberg 1.10.0) and iceberg-rust 0.10 has no deletion-vector WRITE
  support; the fixture engine MUST be an official Apache Iceberg ecosystem engine.
* This feature REPURPOSES the existing `mor_dv_unsupported` fixture (previously created only to
  exercise the plan-time fail-loud path) into a positive DV fixture: its already-recorded ground
  truth (10 rows `id 1..=10`, deleted `id IN (3, 7)`, one data file) is retained, and it gains a
  post-delete row/id ground-truth set so a successful read can be asserted — exactly like the two
  positional-delete fixtures (`mor_pos_file`, `mor_pos_partition`) already do.
* The mixed-mechanism fixture is laid out so its data files are split across delete mechanisms:
  at least one data file whose deletes are Parquet positional deletes (written under
  `format-version=2` semantics) and at least one whose deletes are a v3 DV, within the same table
  snapshot the VS reads, so a single scan invocation can be forced to see both mechanisms.
* Fixtures are written against the SAME shared REST catalog + MinIO used by the rest of the E2E
  stack, so the VS resolves them through its normal catalog path.
* Equality deletes and ORC/Avro fixtures are explicitly NOT produced by this feature — they remain
  out of scope and are covered instead by the plan-time fail-loud gate.
* The fixture data files are all Parquet; the DV blobs are Puffin `deletion-vector-v1`.

## Scenarios

### Scenario: Spark produces a deletion-vector fixture

* *GIVEN* the E2E stack is running with the shared REST catalog over MinIO and an Apache Spark service with the Iceberg Spark runtime
* *WHEN* the fixture step creates a `format-version=3` merge-on-read table and issues a `DELETE` that removes a known subset of rows
* *THEN* the fixture SHALL commit a snapshot carrying a `deletion-vector-v1` Puffin blob (one per affected data file) rather than a Parquet positional-delete file
* *AND* the fixture SHALL record the exact set of deleted rows and the expected post-delete row set so a test can assert the read result
* *AND* the fixture step SHALL fail (not skip) if the Spark service, REST catalog, or MinIO is unavailable

### Scenario: Spark produces a mixed positional-delete and deletion-vector fixture

* *GIVEN* the E2E stack is running with the shared REST catalog over MinIO and an Apache Spark service with the Iceberg Spark runtime
* *WHEN* the fixture step creates a merge-on-read table laid out so that at least one data file's deletes are Parquet positional deletes and at least one other data file's deletes are a v3 `deletion-vector-v1` Puffin blob, all visible in the snapshot the VS reads
* *THEN* the fixture SHALL commit a snapshot in which one data file is backed by a Parquet positional-delete file and another data file is backed by a Puffin deletion vector
* *AND* the fixture SHALL record the exact set of deleted rows across both mechanisms so a test can assert the combined post-delete result
* *AND* the fixture step SHALL fail (not skip) if the Spark service, REST catalog, or MinIO is unavailable

### Scenario: Deletion-vector fixture ground truth stays in lockstep with the test constants

* *GIVEN* the deletion-vector and mixed-mechanism fixtures with their recorded seeded rows and deleted rows
* *WHEN* the fixture SQL and the Rust test fixture constants (`tests/common/pos_delete_fixtures.rs`) are compared
* *THEN* the table name, seeded row set, deleted row set, and expected post-delete row set SHALL match between the fixture SQL and the Rust constants
* *AND* the fixture SQL SHALL document that its ground truth MUST NOT be changed without updating the Rust constants in the same change
