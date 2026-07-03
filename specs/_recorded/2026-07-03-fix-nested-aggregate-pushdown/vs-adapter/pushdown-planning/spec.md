# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and any supported aggregate, extracts the table's current Iceberg schema for field-id-based projection, and emits the SQL that drives the DataFusion scan SET UDF — sharded across cluster nodes — over exactly those files. The scan-driving SQL passes the shard-invariant parts (projection, filter, LIMIT, logical schema, credentials, and the Iceberg table root) once as the UDF's common argument and each shard's per-file `(path, size)` subset as the per-shard argument. See `vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute path encoding rules.

## Background

* The data-file list, each file's byte size (from the Iceberg manifest), and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The logical schema carried into the common scan-spec argument identifies each column by its Iceberg field-id, current name, Arrow type, and nullability.
* The scan-driving SQL serializes the shard-invariant common spec once (projection, filter, LIMIT, aggregates, group keys, logical schema, EMITS types, credentials, tuning knobs, and the Iceberg table root) and carries only each shard's per-file `(path, size)` subset per shard.
* Each per-shard file entry carries both the file path and its byte size, so the scan UDF never re-discovers a size the adapter already resolved.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Composed pushdown request never renders a scan spec that references a non-source column

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a user query whose pushdown request composes an outer aggregate over an inner grouped-aggregate sub-select — e.g. `SELECT COUNT(*) FROM (SELECT L_ORDERKEY, COUNT(*) AS cnt FROM {vs_table} GROUP BY L_ORDERKEY) t` — so that some `selectList`, `groupBy`, or `filter` node does not resolve to a plain column of the involved source table's current Iceberg schema
* *WHEN* Exasol sends the corresponding `pushdown` request and the adapter builds the scan spec and the scan-driving SQL
* *THEN* every column reference in the per-shard scan-driving SQL the adapter emits SHALL name a column present in the involved table's resolved logical schema (or an aggregate/group-key expression rendered from one), and the adapter MUST NOT emit a scan spec whose rendered SQL references a phantom identifier such as `NULL` that is absent from the source schema
* *AND* when the adapter cannot map every `selectList`/`groupBy` node of a composed request onto a supported single-group aggregate, grouped aggregate, or projection over the source table's own columns, it SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field) so Exasol applies the outer computation on the returned rows using its own engine
* *AND* the query MUST NOT fail with a DataFusion `Schema error: No field named ...` (or any planning-time SQL-generation error) raised from inside the scan UDF
<!-- /DELTA:NEW -->
