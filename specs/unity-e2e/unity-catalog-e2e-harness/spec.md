# Feature: Unity Catalog E2E Harness

End-to-end coverage of the Virtual Schema against a native Unity Catalog OSS server backed by MinIO
and seeded with the vendored Delta fixtures, run through the shared harness so the script DDL is
byte-identical to every other E2E binary. The suite fails, never skips, when the stack is unavailable.

## Background

The suite runs against the #325 fixture harness: the `docker-compose.unity.yml` overlay stands up MinIO plus an OSS Unity Catalog server whose authentication is disabled, and `make unity-up` seeds the vendored Delta fixtures onto MinIO and registers them in Unity Catalog under the `unity.delta_e2e` catalog and schema. The UDF reaches Unity Catalog at `http://unitycatalog:8080` over the docker network. The CONNECTION address is that Unity Catalog host and its password supplies no auth field, because the OSS server needs none. The suite provisions the adapter script through the shared harness definition so the script DDL is byte-identical to every other E2E binary. The suite's column assertion presumes the OSS #325 fixture's `GET /tables` returns each table's `columns[]` inline by default; because that inline-columns behavior was verified live only against Databricks (`demo_sales_catalog.sales`), the implementation SHALL confirm it against the running `make unity-up` fixture before authoring the column assertion rather than assume OSS parity, so a missing inline `columns[]` on the OSS list endpoint surfaces as a caught precondition rather than a red suite with no code bug.

* **This delta is issue #320 and lifts the suite's scan-execution ceiling.** The suite stopped at
  catalog metadata and plan-time scan resolution before; it now issues real queries through Exasol and
  asserts the rows they return.
* The seeded fixtures this delta queries are `multi_part_stats` (5 files, 5 rows, delete-free,
  unpartitioned), `table_with_dv` (1 file, 10 physical rows, a UUID-relative deletion vector of
  cardinality 2), `cm_id_mode` and `cm_name_mode` (`col-<uuid>` physical names under `id` and `name`
  column mapping), and `basic_partitioned` (6 files, 6 rows, partitioned by `letter`, one file under
  the Hive default-partition directory).
* No new fixture, Makefile target, or test tier is added by this delta. The scenarios extend the
  existing `make test-e2e-unity` suite.
* **This delta changes ONE scenario, adds ONE, and revises the feature description; it is issue
  #319.** The suite gains the plan-time half of the Delta path: catalog resolution, credential
  vending, and transaction-log replay against the seeded fixtures.
* **SUPERSEDES the description sentence "The suite stops at createVirtualSchema — it lists tables and
  columns and runs no scan, because Delta scan execution lands in #319/#320."** The suite no longer
  stops at createVirtualSchema. "Runs no scan" still holds and is retained: #319 issues no scan UDF
  invocation and no scan-driving query. What it adds is plan-time object-storage reads of
  `_delta_log`.
* **The new test lands in the existing `e2e_unity_test.rs` binary**, because the CI job and the
  Makefile target both name that one `--test` target, and the authority comment in
  `.github/workflows/ci.yml` requires the Makefile's cargo line to stay flag-identical to it. A second
  test binary would run in neither without editing both, which is #328's settled territory rather than
  this plan's.
* **The Delta log-replay logic itself is covered OFFLINE, not here.** The vendored fixtures live in
  the repository, so replay correctness — active-file selection across commits, partition values,
  deletion-vector references, column mapping — is asserted by a plain `cargo test` integration test
  over a local-filesystem object store. This suite covers what only the live stack can prove: the
  Unity Catalog resolve, the temporary-table-credentials vend, and reading `_delta_log` over S3.
* **This suite is `resolve_uc_vended_storage`'s first live exercise.** The OSS Unity Catalog server
  vends in static-key mode — it echoes its configured keys rather than calling STS — which is enough to
  prove the vend request, the response parse, and the shared vended-storage policy end to end.
* Exasol stays a precondition of the suite even though the added test needs no database, because the
  `unity-e2e` feature gates one stack and splitting it would fork the fail-not-skip contract.
* **Split, issue #320: the Delta query-result scenarios moved to
  `unity-e2e/unity-catalog-e2e-harness-delta-queries`.** This feature's scenario count crossed this
  library's per-spec organization threshold once those scenarios landed; they now live in the sibling
  feature, which shares this suite's stack, binary, and the virtual schema the storage-credential
  scenario below creates.

## Scenarios

### Scenario: Harness brings up Unity Catalog and seeds the Delta fixtures

* *GIVEN* the `docker-compose.unity.yml` overlay and the `make unity-up` target
* *WHEN* the harness provisions the stack before any test
* *THEN* the harness SHALL bring up MinIO and the OSS Unity Catalog server and seed the vendored Delta fixtures under the `unity.delta_e2e` catalog and schema
* *AND* the seed SHALL be idempotent and SHALL abort non-zero on any failure, so a partially-seeded stack fails the suite rather than yielding a partial listing

### Scenario: Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns

* *GIVEN* a running Unity Catalog stack seeded with the fixtures and an Exasol CONNECTION whose address is `http://unitycatalog:8080`, whose password supplies no auth field, and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `NAMESPACE` property is `unity.delta_e2e`
* *WHEN* the suite issues createVirtualSchema against that CONNECTION
* *THEN* the created virtual schema SHALL expose one virtual table per seeded fixture table, each named by the shared flatten-and-uppercase rule
* *AND* each virtual table SHALL declare its columns with Exasol types mapped from the Unity Catalog column types, so a seeded fixture's columns appear with the expected Exasol types
* *AND* the suite SHALL assert the presence of a representative fixture table and its column set, so a regression in enumeration or column mapping fails the suite
* *AND* this scenario SHALL keep issuing no scan-driving query of its own, SUPERSEDING the recorded clause that bounded the WHOLE SUITE to no scan execution: the scenarios below now run real queries, so what this scenario asserts is enumeration alone rather than the suite's ceiling

### Scenario: The suite resolves a seeded Delta table's scan spec over MinIO under both credential modes

* *GIVEN* a running Unity Catalog stack seeded with the vendored Delta fixtures on MinIO, and the
  partitioned fixture registered as `unity.delta_e2e.basic_partitioned`
* *WHEN* the suite resolves that table's scan through the Delta format reader, once with
  `use_vended_credentials` enabled and once with the CONNECTION's static MinIO credentials
* *THEN* BOTH runs SHALL return the same file list, the same per-file partition values, and the same
  table root, so the credential mode changes how object storage is reached and nothing about what is
  resolved
* *AND* the vended run SHALL request temporary table credentials from the Unity Catalog server scoped
  to that table's catalog-assigned vending key, and SHALL read `_delta_log` from MinIO with the
  credentials that response returns
* *AND* the suite SHALL inject the MinIO endpoint client-side, because the OSS Unity Catalog server
  performs no server-side S3 access and vends no endpoint
* *AND* the suite SHALL assert the resolved file count and the set of partition values, so a
  regression in catalog resolution, credential vending, or log replay over S3 fails the suite
* *AND* the suite SHALL additionally resolve the deletion-vector fixture registered as
  `unity.delta_e2e.table_with_dv` and assert its single active data file carries a deletion-vector
  reference, so the DV reference survives the whole live chain rather than only the offline replay test
* *AND* the suite MUST fail (not skip) when the Unity Catalog server or MinIO is unreachable
* *AND* no vended or static credential value SHALL appear in any assertion message or test output

### Scenario: The Unity Catalog E2E suite fails when the stack is unavailable

* *GIVEN* the Unity Catalog server, MinIO, or Exasol service is not reachable
* *WHEN* the `unity-e2e` suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: The Unity Catalog E2E suite leaks no credential value

* *GIVEN* a createVirtualSchema request whose CONNECTION or resolved auth carries a value, on any failure path the suite exercises
* *WHEN* the suite surfaces an error or prints diagnostic output
* *THEN* no resolved bearer token, OAuth client secret, or vended storage credential SHALL appear in any returned SQL string, error message, or test output

### Scenario: The suite's virtual schema carries the storage credentials a UDF-side scan needs

* *GIVEN* the seeded fixtures on MinIO and an OSS Unity Catalog server that vends no object-storage endpoint
* *WHEN* the suite creates the virtual schema the query scenarios below run against
* *THEN* that virtual schema's CONNECTION SHALL supply the MinIO endpoint and static storage credentials, so the scan UDF running inside Exasol reaches MinIO with a credential resolved from the CONNECTION rather than from a test-process injection
* *AND* the suite SHALL provision the scan UDF script through the SAME shared harness definition every other E2E binary uses, so the scan script DDL is byte-identical across suites
* *AND* adding those credentials MUST NOT change the enumeration scenario's result, because listing reads catalog metadata and no object storage
* *AND* the vended-versus-static planning scenario SHALL keep running unchanged, so credential vending stays covered where the OSS server can serve it

> The scenarios asserting the ROWS a query returns over these fixtures — delete-free, deletion-vector,
> column-mapped, partitioned, join/aggregate, and unplannable-type — live in
> `unity-e2e/unity-catalog-e2e-harness-delta-queries`.
