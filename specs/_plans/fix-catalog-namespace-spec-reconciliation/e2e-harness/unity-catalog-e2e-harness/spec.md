# Feature: Unity Catalog E2E Harness

End-to-end coverage of the Virtual Schema against a native Unity Catalog OSS server backed by MinIO
and seeded with the vendored Delta fixtures, run through the shared harness so the script DDL is
byte-identical to every other E2E binary. The suite fails, never skips, when the stack is unavailable.

## Background

* **This delta renames ONE VS property in the suite's createVirtualSchema DDL and changes nothing else, and is issue #324.** `ICEBERG_NAMESPACE` becomes `NAMESPACE`; the fixture catalog and schema (`unity.delta_e2e`), the seeded tables, the asserted column sets, and every query the suite issues are unchanged.
* **The suite is what pins the rename end to end for the Unity Catalog kind.** It creates a real virtual schema through a real `createVirtualSchema` against a live server, so a rename that reached only the adapter constant and not the DDL fails here rather than in production.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns

* *GIVEN* a running Unity Catalog stack seeded with the fixtures and an Exasol CONNECTION whose address is `http://unitycatalog:8080`, whose password supplies no auth field, and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `NAMESPACE` property is `unity.delta_e2e`
* *WHEN* the suite issues createVirtualSchema against that CONNECTION
* *THEN* the created virtual schema SHALL expose one virtual table per seeded fixture table, each named by the shared flatten-and-uppercase rule
* *AND* each virtual table SHALL declare its columns with Exasol types mapped from the Unity Catalog column types, so a seeded fixture's columns appear with the expected Exasol types
* *AND* the suite SHALL assert the presence of a representative fixture table and its column set, so a regression in enumeration or column mapping fails the suite
* *AND* this scenario SHALL keep issuing no scan-driving query of its own, SUPERSEDING the recorded clause that bounded the WHOLE SUITE to no scan execution: the scenarios below now run real queries, so what this scenario asserts is enumeration alone rather than the suite's ceiling
<!-- /DELTA:CHANGED -->
