# Feature: Unity Catalog E2E Harness

End-to-end test suite that creates a virtual schema against a local native Unity Catalog and asserts it lists the fixture's tables and their column metadata, proving the native Unity Catalog client and the `CATALOG_KIND` seam work against a real Unity Catalog server rather than only against mocked HTTP. The suite also resolves a seeded Delta table's scan spec at plan time — reading its transaction log from MinIO through catalog-vended and static storage credentials — and still runs no scan, because Delta scan execution lands in #320. It runs behind its own `unity-e2e` cargo feature and follows the project's fail-not-skip contract.

## Background

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

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns

* *GIVEN* a running Unity Catalog stack seeded with the fixtures and an Exasol CONNECTION whose address is `http://unitycatalog:8080`, whose password supplies no auth field, and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `ICEBERG_NAMESPACE` property is `unity.delta_e2e`
* *WHEN* the suite issues createVirtualSchema against that CONNECTION
* *THEN* the created virtual schema SHALL expose one virtual table per seeded fixture table, each named by the shared flatten-and-uppercase rule
* *AND* each virtual table SHALL declare its columns with Exasol types mapped from the Unity Catalog column types, so a seeded fixture's columns appear with the expected Exasol types
* *AND* the suite SHALL assert the presence of a representative fixture table and its column set, so a regression in enumeration or column mapping fails the suite
* *AND* the suite SHALL run no scan — it SHALL issue no scan-driving query and no scan UDF invocation — SUPERSEDING the recorded reason "because #318 stops at catalog metadata": the suite now also resolves a Delta scan spec at plan time, so what bounds it is the absence of scan EXECUTION rather than the absence of everything past catalog metadata
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
