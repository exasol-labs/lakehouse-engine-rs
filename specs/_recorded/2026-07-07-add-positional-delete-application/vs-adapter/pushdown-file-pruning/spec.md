# Feature: Pushdown File Pruning

Translates the soundly-translatable conjuncts of the Exasol WHERE predicate into an Iceberg
predicate applied to the table scan at file-resolution time, so Iceberg prunes data files on
partition values and per-file min/max bounds before any S3 I/O — while the DataFusion scan keeps
applying the full filter as the sole source of row-level correctness. This delta extends the same
resolve-once seam to preserve each data file's associated positional-delete files and to fail loud
on delete mechanisms the engine cannot apply.

## Background

* File pruning narrows which files are opened; it never changes the result set, because DataFusion
  applies the full predicate.
* A data file's associated positional-delete files (as resolved by `plan_files` per the Iceberg
  sequence-number rules) MUST be preserved into the scan spec, never discarded.
* Delete mechanisms this engine cannot apply — equality deletes, Puffin/v3 deletion vectors, and
  ORC/Avro data or delete files — MUST be detected at plan time and fail the request loud.
* Credentials MUST NOT appear in any returned SQL string or error message.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Positional-delete files are preserved into the scan spec

* *GIVEN* a virtual schema over an Iceberg merge-on-read table whose resolved `FileScanTask`s carry associated Parquet positional-delete files, at either `write.delete.granularity=file` or `write.delete.granularity=partition`
* *WHEN* Exasol sends the corresponding pushdown request and the adapter resolves the file list once
* *THEN* the adapter MUST NOT discard a data file's associated positional-delete files, and SHALL carry each delete file's path, byte size, and delete content type into the per-shard file entry for the data file(s) it applies to
* *AND* the association between a data file and its delete files SHALL follow the Iceberg sequence-number rules exactly as `plan_files` resolved them, so a delete file is carried for exactly the data files it applies to and no others
* *AND* the adapter SHALL carry the delete-file references as the ONLY delete-related addition to the per-shard files argument — it MUST NOT add a serialized Iceberg schema or a bound predicate to the spec for delete support
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: An unsupported delete mechanism fails loud at plan time

* *GIVEN* a virtual schema over an Iceberg table whose current snapshot carries a delete mechanism this engine cannot apply — an equality-delete file, a v3 / Puffin deletion vector, or a delete file (or data file) in ORC or Avro format
* *WHEN* Exasol sends the corresponding pushdown request and the adapter inspects the snapshot's delete files at the manifest / `DataFile` level, where the Puffin discriminator and file format are still visible
* *THEN* the adapter SHALL fail the request at plan time with a clean error naming the unsupported delete mechanism, BEFORE building or returning any scan-driving SQL and before fan-out
* *AND* the adapter MUST NOT silently return pre-delete rows and MUST NOT emit scan-driving SQL for that request
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token
* *AND* this plan-time detection SHALL be the authoritative correctness gate; any scan-time check is a secondary backstop only
<!-- /DELTA:NEW -->
