# Feature: Catalog Kind Selection

The adapter reads the `CATALOG_KIND` virtual-schema property to choose the catalog kind: Iceberg REST, native Unity Catalog, direct storage, or native AWS Glue. It then builds the matching catalog client, and every createVirtualSchema listing runs through the shared `CatalogClient` trait. An absent property selects Iceberg REST.

## Background

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Absent CATALOG_KIND resolves the Iceberg REST catalog kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties do not include `CATALOG_KIND`
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL select the Iceberg REST catalog kind and MUST NOT read `CATALOG_KIND` from the CONNECTION password JSON
* *AND* enumerating a namespace SHALL cost ONE OAuth2 grant under the client-credentials mode and ZERO under the no-auth and static-token modes, even for an empty namespace (see `catalog/pushdown-catalog-session`). A virtual schema over an empty namespace whose credentials would fail a grant therefore succeeds ONLY in the no-auth and static-token modes
* *AND* an Iceberg REST pushdown SHALL plan files from the Iceberg table metadata that the table's `loadTable` GET returns
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A pushdown request under the Unity Catalog kind is planned as a Delta scan

* *GIVEN* a pushdown request whose virtual schema was created with `CATALOG_KIND` set to `UNITY_CATALOG`, over a seeded Delta table
* *WHEN* the adapter handles the pushdown request
* *THEN* the adapter SHALL plan the table that Unity Catalog returns as a Delta scan, and SHALL return a scan-driving SQL response, SUPERSEDING the recorded refusal that Unity Catalog scan execution is not yet supported
* *AND* the adapter MUST NOT send an Iceberg REST request for the table, because a Unity Catalog Delta table has no Iceberg metadata to plan from
* *AND* a pushdown request whose Delta table cannot be planned SHALL fail with the plan-time error that names why the table cannot be planned, rather than a refusal of the catalog kind, so an unreadable table and an unsupported catalog kind are distinguishable
* *AND* no error message on any of these paths SHALL contain a credential value
<!-- /DELTA:CHANGED -->
