# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and any supported aggregate, extracts the table's current Iceberg schema for field-id-based projection, and emits the SQL that drives the DataFusion scan SET UDF — sharded across cluster nodes — over exactly those files. The scan-driving SQL passes the shard-invariant parts (projection, filter, LIMIT, logical schema, credentials, and the Iceberg table root) once as the UDF's common argument and each shard's per-file `(path, size)` subset as the per-shard argument.

## Background

* The data-file list, each file's byte size (from the Iceberg manifest), and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The Iceberg table root (`table.metadata().location()`, already resolved at the resolve-once seam as the vended-credential anchor) is a shard-invariant value, so it is carried ONCE in the common scan-spec argument — never repeated per shard.
* Each per-shard file entry carries both the file path and its byte size, so the scan UDF never re-discovers a size the adapter already resolved.
* A file path is emitted RELATIVE to the table root only when the root is an actual prefix of the path; any path not under the root is emitted unchanged as an absolute URI.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot, data-file list, and each file's byte size exactly once, and at that same seam extract the table's current Iceberg schema (from `current_schema()`) into a logical schema carrying, per column, its `field_id`, current name, Arrow type, and nullability
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the scan SET UDF, carrying the logical schema AND the Iceberg table root in the shard-invariant common spec argument (each serialized once) and the resolved data-file list as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Table root is carried once and paths under it are emitted relative

* *GIVEN* a resolved data-file list in which every data-file URI lies under the Iceberg table root (`table.metadata().location()`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL carry the table root exactly once in the shard-invariant common spec argument
* *AND* for each file whose URI begins with the table root, the adapter SHALL strip that root prefix and emit only the remaining relative path in the per-shard argument, so the repeated table-location prefix is shipped once rather than once per file
* *AND* the reconstructed absolute path (table root joined with the relative entry) SHALL equal the original resolved data-file URI
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A data-file path not under the table root is carried as an absolute path

* *GIVEN* a resolved data-file list containing at least one data-file URI that does NOT lie under the Iceberg table root (for example a `write.data.path` / `write.object-storage.enabled` hash-injected, migrated, or Databricks layout)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit that file's full absolute URI unchanged in the per-shard argument, stripping the table root ONLY from paths for which the root is an actual prefix
* *AND* the adapter MUST NOT strip a partial or non-prefix match, so no absolute path is ever corrupted into an unresolvable relative path
* *AND* a per-shard payload MAY mix relative entries (paths under the root) and absolute entries (paths not under the root) within the same query
<!-- /DELTA:NEW -->
