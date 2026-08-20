# Feature: Unity Catalog Create Virtual Schema

Enumerates a Unity Catalog namespace during createVirtualSchema and returns one virtual table per Delta base table — a listed entry whose Unity Catalog `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA`. Every other listed entry — a view, a non-`DELTA` format, or any other `table_type` — is excluded from the returned virtual tables and warned. Enumeration runs on the SAME kind-agnostic listing pipeline the Iceberg REST kind uses; the Delta-base decision lives inside the Unity Catalog client, so `data_source_format` never crosses the shared trait boundary. Mapping each `catalog.schema.table` identifier to an Exasol table name and each Unity Catalog column to an Exasol column type is sufficient to list the namespace and expose queryable column metadata; deeper Delta schema fidelity — reader-feature gating, timestamp precision, type widening, and variant types — is deferred to #322. This path reads only Unity Catalog catalog metadata; it does not read the Delta transaction log, so it never resolves a snapshot or a file list.

## Background

* **This delta discharges the rename this feature itself deferred, and is issue #324.** The recorded Background sentence "the property keeps its Iceberg-era name under this plan and a catalog-neutral rename is deferred to #324" is SUPERSEDED: the property is now named `NAMESPACE`, and the deferral note is removed rather than left standing against a rename that has landed.
* **The property was always catalog-neutral; only its name was not.** It names a namespace for BOTH kinds — an Iceberg REST namespace under `vs-adapter/create-virtual-schema`, and the `catalog.schema` pair this feature dot-splits into segments. One name for one concept is what stops a reader from inferring a second, Unity-specific property that does not exist.
* **No enumeration, Delta-base filtering, skip-warn, `TABLE_MAP`, collision, case-fold, column-type-mapping, or credential-redaction behavior changes.** Only the string that selects the namespace changes.
* **`ICEBERG_NAMESPACE` is REMOVED, not aliased.** No deployed virtual schema needs migrating, so a request still supplying the old name fails loudly with the required-property error naming `NAMESPACE`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema enumerates every table in the configured Unity Catalog namespace

* *GIVEN* a Unity Catalog reachable through the CONNECTION named by `CATALOG_CONNECTION` and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `NAMESPACE` property names a `catalog.schema`
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL list every table in that schema by calling the shared `CatalogClient` list operation on the constructed Unity Catalog client, and SHALL return one virtual table per listed DELTA BASE table — an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA`
* *AND* the adapter SHALL exclude from the returned virtual tables every other listed entry — a view, a base table whose `data_source_format` is not `DELTA`, or an entry of any other `table_type` — and MUST NOT record an excluded entry in `TABLE_MAP`, so no non-Delta or non-base entry becomes a queryable virtual table
* *AND* the adapter SHALL name each returned virtual table by flattening the segments below the configured namespace plus the table name with `__` and uppercasing the result through the shared case-fold site
* *AND* the adapter SHALL source every listed entry's columns, `table_type`, and `data_source_format` from the single paginated `GET /tables` list sweep — issuing no per-table `GET /tables/{full_name}`, reading no Delta transaction log, and resolving no snapshot — because this path stops at catalog metadata
* *AND* the namespace property SHALL be the SAME `NAMESPACE` property the Iceberg REST kind reads, so neither kind carries a namespace property of its own
<!-- /DELTA:CHANGED -->
