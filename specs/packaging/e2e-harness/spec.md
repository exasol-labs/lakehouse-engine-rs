# Feature: End-to-End Harness

Wires a local Docker stack — Exasol, an Iceberg REST catalog, and MinIO — and proves
the whole path: a plain `SELECT` with projection, filter, and LIMIT against the virtual
schema returns the correct rows scanned by DataFusion inside the UDF.

## Background

* The stack runs Exasol (`exasol/docker-db`), an Iceberg REST catalog, and MinIO on a
  shared Docker network, mirroring the sibling project's compose conventions.
* The Rust SLC and the single `.so` are uploaded to BucketFS (HTTPS API, port 2581);
  `SCRIPT_LANGUAGES` is set to register the Rust language.
* E2E tests run under the `exasol-e2e` feature gate and MUST fail (not skip) if the
  stack is unavailable.
* All DSN/connection strings include `validateservercertificate=0`.

## Scenarios

### Scenario: End-to-end projection + filter + LIMIT query returns correct rows

* *GIVEN* the Docker stack is running with a seeded Iceberg table in the REST catalog over MinIO
* *AND* the Rust SLC and the `.so` are installed and the virtual schema is created
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>`
* *THEN* the query SHALL return exactly the rows that satisfy the predicate, capped at `n`, projected to the selected columns
* *AND* the returned values SHALL match the seeded source data

### Scenario: E2E suite fails when the stack is unavailable

* *GIVEN* the Exasol container is not reachable
* *WHEN* the `exasol-e2e` test suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed
