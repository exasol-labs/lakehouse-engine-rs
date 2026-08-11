# Feature: Unity Catalog E2E Harness

End-to-end test suite that creates a virtual schema against a local native Unity Catalog and asserts it lists the fixture's tables and their column metadata, proving the native Unity Catalog client and the `CATALOG_KIND` seam work against a real Unity Catalog server rather than only against mocked HTTP. The suite stops at createVirtualSchema — it lists tables and columns and runs no scan, because Delta scan execution lands in #319/#320. It runs behind its own `unity-e2e` cargo feature and follows the project's fail-not-skip contract.

## Background

The suite runs against the #325 fixture harness: the `docker-compose.unity.yml` overlay stands up MinIO plus an OSS Unity Catalog server whose authentication is disabled, and `make unity-up` seeds the vendored Delta fixtures onto MinIO and registers them in Unity Catalog under the `unity.delta_e2e` catalog and schema. The UDF reaches Unity Catalog at `http://unitycatalog:8080` over the docker network. The CONNECTION address is that Unity Catalog host and its password supplies no auth field, because the OSS server needs none. The suite provisions the adapter script through the shared harness definition so the script DDL is byte-identical to every other E2E binary. The suite's column assertion presumes the OSS #325 fixture's `GET /tables` returns each table's `columns[]` inline by default; because that inline-columns behavior was verified live only against Databricks (`demo_sales_catalog.sales`), the implementation SHALL confirm it against the running `make unity-up` fixture before authoring the column assertion rather than assume OSS parity, so a missing inline `columns[]` on the OSS list endpoint surfaces as a caught precondition rather than a red suite with no code bug.

## Scenarios

### Scenario: Harness brings up Unity Catalog and seeds the Delta fixtures

* *GIVEN* the `docker-compose.unity.yml` overlay and the `make unity-up` target
* *WHEN* the harness provisions the stack before any test
* *THEN* the harness SHALL bring up MinIO and the OSS Unity Catalog server and seed the vendored Delta fixtures under the `unity.delta_e2e` catalog and schema
* *AND* the seed SHALL be idempotent and SHALL abort non-zero on any failure, so a partially-seeded stack fails the suite rather than yielding a partial listing

### Scenario: Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns

* *GIVEN* a running Unity Catalog stack seeded with the fixtures and an Exasol CONNECTION whose address is `http://unitycatalog:8080`, whose password supplies no auth field, and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `ICEBERG_NAMESPACE` property is `unity.delta_e2e`
* *WHEN* the suite issues createVirtualSchema against that CONNECTION
* *THEN* the created virtual schema SHALL expose one virtual table per seeded fixture table, each named by the shared flatten-and-uppercase rule
* *AND* each virtual table SHALL declare its columns with Exasol types mapped from the Unity Catalog column types, so a seeded fixture's columns appear with the expected Exasol types
* *AND* the suite SHALL assert the presence of a representative fixture table and its column set, so a regression in enumeration or column mapping fails the suite
* *AND* the suite SHALL run no scan, because #318 stops at catalog metadata

### Scenario: The Unity Catalog E2E suite fails when the stack is unavailable

* *GIVEN* the Unity Catalog server, MinIO, or Exasol service is not reachable
* *WHEN* the `unity-e2e` suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: The Unity Catalog E2E suite leaks no credential value

* *GIVEN* a createVirtualSchema request whose CONNECTION or resolved auth carries a value, on any failure path the suite exercises
* *WHEN* the suite surfaces an error or prints diagnostic output
* *THEN* no resolved bearer token, OAuth client secret, or vended storage credential SHALL appear in any returned SQL string, error message, or test output
