# Feature: Create Virtual Schema

Lets an Exasol user register an Iceberg table (resolved through an Iceberg REST
catalog over S3-compatible storage) as a queryable virtual schema, so the table's
columns appear to Exasol with correctly mapped SQL types.

## Background

* The virtual schema is stateless: it persists no metadata of its own and resolves
  everything from the Iceberg REST catalog on demand.
* The Iceberg REST catalog and the S3-compatible object store (MinIO) are reachable
  from the adapter via connection properties supplied at `CREATE VIRTUAL SCHEMA` time.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so`; it speaks the
  Exasol virtual-schema JSON protocol (request in, JSON response out).
* Schema mapping (C.2) MUST use the same mapping as the scan, defined in the
  `datafusion-scan/type-mapping` feature. Columns whose Arrow type Exasol cannot
  represent (list, struct, map, binary, out-of-range decimal, and the other incompatible
  types) are declared as `VARCHAR(2000000)` — they MUST NOT cause `createVirtualSchema`
  to error.

## Scenarios

### Scenario: Adapter reports its pushdown capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the adapter SHALL return a JSON response of type `getCapabilities`
* *AND* the capabilities list MUST include column projection, filter predicates, and LIMIT
* *AND* the capabilities list MUST NOT include aggregation or join pushdown

### Scenario: Create virtual schema maps the Iceberg table schema

* *GIVEN* an Iceberg table exists in the REST catalog backed by MinIO
* *AND* the catalog and storage connection properties are supplied to the adapter
* *WHEN* Exasol sends a `createVirtualSchema` request naming that table
* *THEN* the adapter SHALL resolve the table's current Iceberg schema from the catalog
* *AND* the adapter SHALL return a JSON response describing one virtual table whose columns map each Iceberg field to an Exasol SQL type per the `datafusion-scan/type-mapping` table, declaring any Exasol-incompatible type as `VARCHAR(2000000)` rather than failing
* *AND* the adapter MUST NOT persist any catalog metadata between requests

### Scenario: Create virtual schema fails clearly when the catalog is unreachable

* *GIVEN* the supplied Iceberg REST catalog endpoint cannot be reached
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached
* *AND* the error message MUST NOT contain storage access keys or secret keys
