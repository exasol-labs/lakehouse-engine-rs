# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and any supported aggregate, extracts the table's current Iceberg schema for field-id-based projection, and emits the SQL that drives the DataFusion scan SET UDF — sharded across cluster nodes — over exactly those files.

## Background

* The data-file list and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The logical schema carried into the scan spec identifies each column by its Iceberg field-id, current name, Arrow type, and nullability.
* Credentials MUST NOT appear in any returned SQL string or error message.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding pushdown request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot and data-file list exactly once, and at that same seam extract the table's current Iceberg schema (from `current_schema()`) into a logical schema carrying, per column, its `field_id`, current name, Arrow type, and nullability
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the scan SET UDF and passes both the resolved data-file list AND the logical schema as explicit arguments in the scan spec
* *AND* the adapter MUST NOT require the scan UDF to discover files itself
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a query that selects only some of the table's columns
* *WHEN* Exasol sends the pushdown request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF
* *AND* the projected column names SHALL be the current Iceberg logical names carried in the scan spec's logical schema, so the UDF's registered table exposes them and the field-id adapter maps each to the correct physical column per file
* *AND* the UDF's declared EMITS column list SHALL match the projected columns in order and type
<!-- /DELTA:CHANGED -->
