# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and any supported aggregate, extracts the table's current Iceberg schema for field-id-based projection, and emits the SQL that drives the DataFusion scan SET UDF — sharded across cluster nodes — over exactly those files. The scan-driving SQL passes the shard-invariant parts (projection, filter, LIMIT, logical schema, credentials) once as the UDF's common argument and each shard's file subset as the per-shard argument.

## Background

* The data-file list and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The logical schema carried into the common scan-spec argument identifies each column by its Iceberg field-id, current name, Arrow type, and nullability.
* The scan-driving SQL serializes the shard-invariant common spec once (projection, filter, LIMIT, aggregates, group keys, logical schema, EMITS types, credentials, tuning knobs) and carries only each shard's file subset per shard.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot and data-file list exactly once, and at that same seam extract the table's current Iceberg schema (from `current_schema()`) into a logical schema carrying, per column, its `field_id`, current name, Arrow type, and nullability
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the scan SET UDF, carrying the logical schema in the shard-invariant common spec argument (serialized once) and the resolved data-file list as the per-shard files argument
* *AND* the adapter MUST NOT require the scan UDF to discover files itself
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a query that selects only some of the table's columns
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF, in the shard-invariant common spec argument shared by all shards
* *AND* the projected column names SHALL be the current Iceberg logical names carried in the common spec's logical schema, so the UDF's registered table exposes them and the field-id adapter maps each to the correct physical column per file
* *AND* the UDF's declared EMITS column list SHALL match the projected columns in order and type
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, omitting (never mistranslating) any node it cannot render
* *AND* the adapter SHALL ALSO translate the soundly-translatable conjuncts into an `iceberg::expr::Predicate` applied to the Iceberg table scan as a file-pruning filter, dropping any node it cannot translate soundly rather than skipping a file that could match
* *AND* the DataFusion scan SHALL always apply the full common-spec filter, so the Iceberg pruning filter only narrows which files are opened and never changes the result set
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIMIT is pushed into the scan spec

* *GIVEN* a query with a LIMIT clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the shard-invariant common spec passed to the UDF SHALL carry the row limit
* *AND* because the common spec is shared by every shard, each row-scan shard invocation SHALL observe the same limit
* *AND* the generated SQL MAY also retain the LIMIT at the Exasol level as a correctness backstop
<!-- /DELTA:CHANGED -->
