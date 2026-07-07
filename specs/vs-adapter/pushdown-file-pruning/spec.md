# Feature: Pushdown File Pruning

Translates the soundly-translatable conjuncts of the Exasol WHERE predicate into an
`iceberg::expr::Predicate` that is applied to the Iceberg table scan at file-resolution
time, so `plan_files` prunes data files on partition values and per-file min/max bounds
before any S3 I/O — while the DataFusion scan keeps applying the full filter as the sole
source of row-level correctness. This delta extends the same resolve-once seam to preserve
each data file's associated positional-delete files and to fail loud on delete mechanisms
the engine cannot apply.

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
* Delete mechanisms this engine cannot apply — equality deletes, Puffin/v3 deletion
  vectors, and ORC/Avro data or delete files — MUST be detected at plan time and fail the
  request loud.
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
* *THEN* the adapter MUST NOT discard a data file's associated positional-delete files, and SHALL carry each delete file's path, byte size, and delete content type into the per-shard file entry for the data file(s) it applies to
* *AND* the association between a data file and its delete files SHALL follow the Iceberg sequence-number rules exactly as `plan_files` resolved them, so a delete file is carried for exactly the data files it applies to and no others
* *AND* the adapter SHALL carry the delete-file references as the ONLY delete-related addition to the per-shard files argument — it MUST NOT add a serialized Iceberg schema or a bound predicate to the spec for delete support

### Scenario: An unsupported delete mechanism fails loud at plan time

* *GIVEN* a virtual schema over an Iceberg table whose current snapshot carries a delete mechanism this engine cannot apply — an equality-delete file, a v3 / Puffin deletion vector, or a delete file (or data file) in ORC or Avro format
* *WHEN* Exasol sends the corresponding pushdown request and the adapter inspects the snapshot's delete files at the manifest / `DataFile` level, where the Puffin discriminator and file format are still visible
* *THEN* the adapter SHALL fail the request at plan time with a clean error naming the unsupported delete mechanism, BEFORE building or returning any scan-driving SQL and before fan-out
* *AND* the adapter MUST NOT silently return pre-delete rows and MUST NOT emit scan-driving SQL for that request
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token
* *AND* this plan-time detection SHALL be the authoritative correctness gate; any scan-time check is a secondary backstop only
