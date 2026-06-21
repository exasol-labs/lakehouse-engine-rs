# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it
resolves the Iceberg data-file list once, captures the requested projection, filter,
and LIMIT, and emits the SQL that drives the DataFusion scan SET UDF over exactly
those files. This is the thin seam that keeps execution in DataFusion and metadata
resolution out of the per-node UDF.

## Background

* The adapter receives a `pushdown` request carrying the projection, filter, and
  LIMIT that Exasol was able to delegate.
* Iceberg snapshot and data-file resolution happens exactly once here, in the planning
  layer — never inside the scan UDF. This is the seam later multi-node sharding will
  exploit; for this PoC the whole file list is assigned to a single UDF invocation.
* The scan UDF is the second entry point of the same `.so`; the adapter references it
  by its registered SET-script name.
* Connection properties (catalog endpoint, S3 endpoint/region, credentials) are passed
  to the scan UDF so it can register the files without re-resolving them.

## Scenarios

### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query that projects a subset of columns
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL resolve the Iceberg snapshot and its data-file list exactly once
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the scan SET UDF
* *AND* that SQL MUST pass the resolved data-file list as an explicit argument to the UDF
* *AND* the adapter MUST NOT require the scan UDF to discover files itself

### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a query that selects only some of the table's columns
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF
* *AND* the UDF's declared EMITS column list SHALL match the projected columns in order and type

### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the scan spec passed to the UDF
* *AND* a predicate the adapter cannot translate SHALL be omitted from the scan spec rather than produce an incorrect result

### Scenario: LIMIT is pushed into the scan spec

* *GIVEN* a query with a LIMIT clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the scan spec passed to the UDF SHALL carry the row limit
* *AND* the generated SQL MAY also retain the LIMIT at the Exasol level as a correctness backstop
