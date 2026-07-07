# Decision Log: add-positional-delete-application

Date: 2026-07-06

## Interview

**Q:** Custom `TableProvider` vs `ListingTable`? Attaching a per-file `ParquetAccessPlan` requires
building a `FileScanConfig` directly, which `ListingTable` doesn't permit.
**A:** Unified — build the custom `TableProvider` over DataFusion's `ParquetSource` always
(replacing `ListingTable` in `register_files`), keeping the code clean and simple, UNLESS a
plan-shape/perf check shows a noticeable regression on the delete-free path, in which case fall
back to conditional (`ListingTable` for delete-free, custom provider only when deletes present).
Default to unified; the plan-shape test is the gate that justifies staying unified.

**Q:** How to handle unsupported delete types/formats (equality deletes, Puffin/v3 deletion
vectors, ORC/Avro data or delete files) — today the engine silently returns pre-delete rows?
**A:** Fail immediately at PLAN TIME with a clear error, so invalid results are never returned.
Plan-time detection (adapter, at the manifest/`DataFile` level where the Puffin discriminator is
still visible) is the authoritative gate. A lightweight scan-time backstop is acceptable as cheap
defense-in-depth only if it stays simple, but plan-time is the required gate.

**Q:** Issue tracking?
**A:** New issue #68, linked to #11 (already created). Equality deletes + deletion vectors remain
future work under #11.

## Design Decisions

### [1] Keep DataFusion `ParquetSource`; apply positional deletes via a per-file base `ParquetAccessPlan`

- **Decision:** Do NOT swap in iceberg-rust's `ArrowReader`. Keep DataFusion's `ParquetSource` as
  the scan engine and apply positional deletes by attaching a per-data-file `ParquetAccessPlan`
  (base row selection) via `PartitionedFile::with_extensions`; the Parquet opener intersects
  predicate/bloom/row-group/page pruning on top of the injected selection.
- **Alternatives:** iceberg-rust `ArrowReader` / `iceberg-datafusion` `IcebergTableScan` (the
  rejected broader plan) — rejected because they lose DataFusion projection/filter/LIMIT pushdown,
  row-group/page pruning, statistics and streaming, and re-plan files inside the scan (breaking
  file-level work assignment + resolve-once).
- **Rationale:** DataFusion 54 exposes the access-plan seam natively (verified at
  `datafusion-datasource-parquet-54.0.0` opener/mod.rs:896/:1348/:1097-1121/:2303-2323 and
  access_plan.rs:228-236); the injected selection composes with pushdown rather than defeating it,
  preserving performance (cf. apache/iceberg-rust#2376).
- **Promotes to ADR:** yes

### [2] Unified custom provider on all paths, gated by a plan-shape/pruning-preservation test

- **Decision:** Build the custom `ParquetSource`-backed `TableProvider` for every scan (delete-free
  and MOR alike), replacing `ListingTable`; keep it unless the plan-shape/pruning test shows a
  noticeable regression, then fall back to conditional.
- **Alternatives:** Conditional from the start (`ListingTable` for delete-free) — rejected as
  default for code cleanliness, retained as the documented fallback.
- **Rationale:** One code path is simpler; the plan-shape/pruning-preservation test (plan Task 4.2)
  is the objective gate for the decision.
- **Promotes to ADR:** yes

### [3] Plan-time fail-loud at the manifest/`DataFile` level is the authoritative correctness gate

- **Decision:** Detect unsupported delete mechanisms (equality deletes, Puffin/v3 deletion vectors,
  ORC/Avro data or delete files) at plan time in the adapter, at the manifest/`DataFile` level,
  before building scan-driving SQL; add a cheap read-time backstop in the scan.
- **Alternatives:** Read-time-only detection on `FileScanTaskDeleteFile` — rejected as the sole
  guard because `plan_files` drops the Puffin discriminator, so a deletion vector is
  indistinguishable from a Parquet positional delete at read time.
- **Rationale:** Invalid results must never be returned; reliable early detection needs
  manifest-level access where the discriminator and file format are still visible.
- **Promotes to ADR:** yes

### [4] Minimal ScanSpec surface: per-file positional-delete refs only

- **Decision:** Add ONLY per-file positional-delete refs (path, byte size, delete content type) to
  the per-shard `files` argument; keep `logical_schema` + `FieldIdExprAdapter` exactly as-is; do
  NOT carry a serialized iceberg `Schema` or a `BoundPredicate`. Legacy `(path, size)` entries
  deserialize with an empty delete list (backward-compatible serde).
- **Alternatives:** Carry serialized iceberg `Schema` + bound `BoundPredicate` (the rejected plan) —
  rejected as unnecessary weight, since DataFusion does its own pushdown from the SQL filter and the
  existing field-id adapter already handles schema evolution.
- **Rationale:** This is the key divergence from the rejected plan; a lean surface keeps the wire
  format simple and the field-id path untouched.
- **Promotes to ADR:** yes

### [5] Scope: positional deletes only; equality + deletion vectors deferred under #11

- **Decision:** Support Parquet data + Parquet positional-delete files at `write.delete.granularity`
  ∈ {`file`, `partition`}; equality deletes, Puffin/v3 deletion vectors, and ORC/Avro are explicit
  non-goals that fail loud and follow later under #11.
- **Alternatives:** The rejected broader plan (`add-iceberg-delete-application`) attempted positional
  + equality together via `ArrowReader` — rejected for performance and scope.
- **Rationale:** Narrower, correct, shippable scope tracked as #68 (refs #11).
- **Promotes to ADR:** no

### [6] Vendor `build_deletes_row_selection` rather than depend on it

- **Decision:** Lift/reimplement iceberg-rust's `build_deletes_row_selection` (positions +
  per-row-group counts → `RowSelection`) with attribution + an upstream-tracking comment, consuming
  a `DeleteVector` (`RoaringTreemap`) in ascending order.
- **Alternatives:** Depend on it directly — blocked because it is `pub(super)` in iceberg (verify
  during implementation).
- **Rationale:** Reuse the verified row-group-boundary algorithm without a visibility dependency.
- **Promotes to ADR:** no

### [7] Double-footer-read mitigation via a shared `ParquetFileReaderFactory` / metadata cache

- **Decision:** Prefer installing a shared reader factory / cached metadata reader so a
  delete-carrying data file's footer parses once for both access-plan construction and the opener;
  accept one extra footer range GET as the fallback. Never a HEAD (preserves the no-HEAD guarantee).
- **Alternatives:** Always read the footer twice — acceptable but wasteful.
- **Rationale:** Building the `ParquetAccessPlan` needs per-row-group row counts (the footer), which
  the opener also reads; caching avoids a redundant parse.
- **Promotes to ADR:** no

### [8] Positional-delete fixtures via Apache Spark (file + partition granularity)

- **Decision:** Produce positional-delete fixtures with Apache Spark (Iceberg Spark runtime,
  `write.delete.mode=merge-on-read`) at both `write.delete.granularity=file` and `partition`; a
  fast no-container scan-level test complements them for quick feedback.
- **Alternatives:** iceberg-rust native writer (no position-delete writer, #340); pyiceberg
  (copy-on-write only) — both rejected.
- **Rationale:** Spark is an official Apache Iceberg ecosystem engine and produces committed
  positional-delete snapshots; the two granularities exercise different scan-time behavior.
- **Drop condition (code comment):** native position-delete writer apache/iceberg-rust #340.
- **Promotes to ADR:** no

### [9] Partition fixture must span multiple partitions; assert fan-out placement invariance

- **Decision:** Strengthen the partition-granularity fixture to lay data across ≥2 partitions (each
  with ≥2 data files) and commit positional-delete file(s) whose `file_path` references data files
  across multiple partitions — not merely multiple files within one partition. Add an E2E test that
  deterministically forces both same-shard and different-shard placement of the affected data files
  (by controlling the shard count / parallelism factor) and asserts the post-delete result is
  identical in both cases.
- **Alternatives:** Rely on the existing single-partition, multi-file fixture and on hash-partition
  luck to occasionally split affected files across shards — rejected: it never proves the
  cross-UDF (different-shard) case and gives no invariance guarantee.
- **Rationale:** A partition-scoped delete file is read independently by every UDF whose assigned
  data files it references; each UDF filters delete rows by `file_path` and applies only the `pos`
  values for the data file it is reading, with no cross-UDF coordination. Correctness MUST therefore
  be invariant to whether a shared delete file lands in one shard or is split across many — a
  property only a multi-partition-spanning fixture plus a forced same-shard/split-shard matrix can
  prove. The scan already preserves the planner's delete↔file association and filters by
  `file_path`, so no scan/adapter spec change is needed.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
