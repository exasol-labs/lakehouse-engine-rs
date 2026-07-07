# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg
data-file list once, captures the requested projection, filter, LIMIT, and any supported aggregate,
extracts the table's current Iceberg schema for field-id-based projection, and emits the SQL that
drives the DataFusion scan SET UDF over exactly those files. This delta extends the resolve-once
seam to also associate each data file's positional-delete files and carry them minimally in the
per-shard argument.

## Background

* The data-file list, each file's byte size, and each file's associated positional-delete files are
  resolved exactly once, at the same seam; the scan UDF never discovers files or delete files.
* The scan-driving SQL passes shard-invariant parts (projection, filter, LIMIT, logical schema,
  credentials, table root) once in the common argument, and each shard's per-file subset in the
  per-shard argument.
* Delete support keeps the wire surface minimal — per-file delete references only, with no
  serialized Iceberg schema and no bound predicate added to the spec.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Positional-delete file references are carried in the per-shard files argument

* *GIVEN* a virtual schema over an Iceberg merge-on-read table backed by MinIO, where `plan_files` associates each data file with its applicable Parquet positional-delete files (at `file` or `partition` granularity)
* *WHEN* Exasol sends the corresponding pushdown request
* *THEN* the adapter SHALL resolve the data-file list, each file's byte size, and each file's associated positional-delete files exactly once, at the same resolve-once seam, and MUST NOT require the scan UDF to discover delete files itself
* *AND* the adapter SHALL carry each data file's associated positional-delete file references (path, byte size, delete content type) in the per-shard files argument alongside the data-file entry, keeping the wire surface minimal — no serialized Iceberg schema and no bound predicate are added for delete support
* *AND* the shard-invariant common spec (logical schema, projection, filter, LIMIT, credentials, table root) SHALL be unchanged by delete support, so a delete-free table produces a byte-identical common spec to before this feature
<!-- /DELTA:NEW -->
