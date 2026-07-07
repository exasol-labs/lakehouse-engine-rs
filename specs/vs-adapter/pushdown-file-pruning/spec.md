# Feature: Pushdown File Pruning

Translates the soundly-translatable conjuncts of the Exasol WHERE predicate into an
`iceberg::expr::Predicate` that is applied to the Iceberg table scan at file-resolution
time, so `plan_files` prunes data files on partition values and per-file min/max bounds
before any S3 I/O — while the DataFusion scan keeps applying the full filter as the sole
source of row-level correctness. This delta extends the same resolve-once seam to preserve
each data file's associated positional-delete files and v3 deletion vectors, and to fail
loud on the delete mechanisms the engine still cannot apply.

## Background

* The pruning predicate is sound-not-complete: every emitted conjunct is logically implied
  by the user predicate; any node that cannot be translated soundly is dropped, so the scan
  prunes less rather than skipping a file that could contain matching rows.
* Under `AND`, a dropped conjunct only widens the surviving file set.
* Under `OR`, an untranslatable branch forces the whole `OR` to impose no constraint.
* `NOT` of an untranslatable child imposes no constraint.
* DataFusion always applies the full `ScanSpec.filter`, so pruning only narrows which files
  are opened and never changes the result set.
* Exasol pre-normalises `>`→`<` and `>=`→`<=`, so only LESS/LESSEQUAL comparison nodes
  reach the adapter.
* A data file's associated positional-delete files (as resolved by `plan_files` per the
  Iceberg sequence-number rules) MUST be preserved into the scan spec, never discarded.
* A data file's associated positional-delete files and v3 deletion vector, when present, MUST be
  preserved into the scan spec by building the normalized per-shard wire: an interned `deleteFiles`
  pool (each physical delete file/container once per shard, with `path`, `size`, `type`, `format`)
  and `df`-indexed `deletes` references on each `dataFiles` entry (see
  `datafusion-scan/scan-execution-spec-reconstitution` for the wire shape). Because iceberg-rust's
  `plan_files`/`FileScanTask.deletes` does not surface deletion-vector files, DV references MUST be
  sourced from the manifest / `DataFile`-level walk (the same walk that gates unsupported
  mechanisms), which is the only place the DV discriminator and the DV-specific coordinates
  (`content_offset`, `content_size_in_bytes`, and the `referenced_data_file` used only to associate
  the DV with its data file) are visible.
* The `referenced_data_file` is used ONLY to attach each deletion vector to the correct `dataFiles`
  entry (as a `df`-indexed `deletes` reference carrying the blob's `offset`/`length`); it is NOT
  serialized onto the wire. The association is structural — it lives on the data file's `deletes`
  list — and the scan-side decoder re-derives and cross-checks it from the Puffin `BlobMetadata`.
* Delete mechanisms this engine still cannot apply — equality deletes and ORC/Avro data or
  delete files — MUST be detected at plan time and fail the request loud. v3 / Puffin
  deletion vectors are NO LONGER in this unsupported set: they are applied on read (see
  `datafusion-scan/scan-execution-deletion-vectors`).
* See `vs-adapter/pushdown-planning` for the broader pushdown plan and the scenario that
  covers wiring of the Iceberg predicate alongside the DataFusion filter string.

## Scenarios

### Scenario: Equality on a partition column prunes data files

* *GIVEN* a virtual schema over a partitioned Iceberg table backed by MinIO whose data files are distributed across partition values
* *AND* a query with a `WHERE partition_col = <value>` predicate over a column the adapter can translate
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL set an `iceberg::expr::Predicate` equality term on the table scan before calling `plan_files`
* *AND* the resolved data-file list SHALL contain only files whose partition value matches `<value>`
* *AND* the data files belonging to non-matching partitions SHALL NOT appear in the scan-driving SQL

### Scenario: Range predicate prunes files via per-file min/max bounds

* *GIVEN* a virtual schema over an Iceberg table whose data files carry disjoint per-file min/max column statistics
* *AND* a query with a `WHERE col <= <value>` (or `BETWEEN`) predicate over a column the adapter can translate
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL apply the translated Iceberg predicate so `plan_files` evaluates each file's column bounds
* *AND* a data file whose min/max bounds provably exclude `<value>` SHALL NOT appear in the resolved file list
* *AND* a data file whose bounds overlap `<value>` SHALL remain in the resolved file list

### Scenario: Untranslatable conjunct disables pruning for that conjunct only

* *GIVEN* a query whose WHERE clause is `<translatable predicate> AND <untranslatable predicate>` (for example `col = 5 AND name LIKE 'A%'`)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL emit an Iceberg pruning predicate carrying only the translatable conjunct
* *AND* the adapter SHALL drop the untranslatable conjunct from the pruning predicate
* *AND* the full original predicate SHALL still be present in `ScanSpec.filter` for DataFusion to apply
* *AND* the query result SHALL be identical to the result without any Iceberg pruning

### Scenario: An untranslatable branch of an OR disables pruning entirely

* *GIVEN* a query whose WHERE clause is `<translatable predicate> OR <untranslatable predicate>` (for example `col = 5 OR name LIKE 'A%'`)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL NOT apply any Iceberg pruning predicate derived from that `OR`, because a row satisfying the untranslatable branch MAY live in any file
* *AND* the resolved file list SHALL equal the unpruned file list for that `OR`
* *AND* the query result SHALL be correct because DataFusion applies the full predicate

### Scenario: Positional-delete files are preserved into the scan spec

* *GIVEN* a virtual schema over an Iceberg merge-on-read table whose resolved `FileScanTask`s carry associated Parquet positional-delete files, at either `write.delete.granularity=file` or `write.delete.granularity=partition`
* *WHEN* Exasol sends the corresponding pushdown request and the adapter resolves the file list once
* *THEN* the adapter MUST NOT discard a data file's associated positional-delete files, and SHALL intern each physical delete file EXACTLY ONCE into the per-shard `deleteFiles` pool (`type` `POS_DEL`, `format` `PARQUET`) and attach a `df`-indexed `deletes` reference (with no `offset`/`length`) to each `dataFiles` entry it applies to
* *AND* a partition-granularity delete file referenced by several data files SHALL appear only ONCE in the `deleteFiles` pool, with each referencing data file carrying a `deletes` entry whose `df` points at that one pool slot
* *AND* the association between a data file and its delete files SHALL follow the Iceberg sequence-number rules exactly as `plan_files` resolved them, so a delete file is referenced by exactly the data files it applies to and no others
* *AND* the adapter SHALL carry the delete-file references as the ONLY delete-related addition to the per-shard files argument — it MUST NOT add a serialized Iceberg schema or a bound predicate to the spec for delete support

### Scenario: Deletion-vector files are preserved into the scan spec

* *GIVEN* a virtual schema over an Iceberg `format-version=3` merge-on-read table whose current snapshot carries a `deletion-vector-v1` Puffin blob referencing a data file
* *WHEN* Exasol sends the corresponding pushdown request and the adapter walks the snapshot's manifests to resolve the file list once
* *THEN* the adapter SHALL intern the Puffin container into the per-shard `deleteFiles` pool (`type` `DV`, `format` `PUFFIN`, with the Puffin file path and byte size) and attach, to the `dataFiles` entry for the referenced data file, a `deletes` reference whose `df` indexes that pool slot and which carries the blob's `offset` and `length` within the Puffin file
* *AND* the adapter SHALL associate each deletion vector with exactly the data file the manifest's `referenced_data_file` names and no other, because the v3 spec guarantees at most one deletion vector per data file — but SHALL NOT serialize `referenced_data_file` onto the wire, because the association is structural (it lives on the data file's `deletes` list)
* *AND* a single Puffin container referenced by many data files SHALL appear only ONCE in the `deleteFiles` pool, each referencing data file carrying its own `df`-indexed `deletes` entry with that blob's `offset`/`length`
* *AND* the adapter MUST NOT discard the deletion-vector reference, so a DV-backed data file is never read as if it had no deletes

### Scenario: An unsupported delete mechanism fails loud at plan time

* *GIVEN* a virtual schema over an Iceberg table whose current snapshot carries a delete mechanism this engine cannot apply — an equality-delete file, or a delete file (or data file) in ORC or Avro format
* *WHEN* Exasol sends the corresponding pushdown request and the adapter inspects the snapshot's delete files at the manifest / `DataFile` level, where the file format is still visible
* *THEN* the adapter SHALL fail the request at plan time with a clean error naming the unsupported delete mechanism, BEFORE building or returning any scan-driving SQL and before fan-out
* *AND* the adapter MUST NOT silently return pre-delete rows nor emit scan-driving SQL for that request, and the error message MUST NOT contain any storage access key, secret key, or session token
* *AND* a v3 / Puffin deletion vector SHALL NOT trigger this failure — it is a supported mechanism applied on read
* *AND* this plan-time detection SHALL be the authoritative correctness gate; any scan-time check is a secondary backstop only
