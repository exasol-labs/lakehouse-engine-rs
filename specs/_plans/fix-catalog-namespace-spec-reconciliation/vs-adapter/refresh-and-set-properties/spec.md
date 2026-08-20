# Feature: Refresh And Set Properties

Lets an Exasol user re-read the Iceberg catalog for an existing virtual schema in place — through `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET` — so a namespace's added, dropped, renamed, or type-changed tables and columns become queryable without a `DROP ... CASCADE` + `CREATE`, which loses dependent views and grants.

## Background

* **This delta renames ONE VS property in ONE scenario and changes nothing else.** Issue #324 renames `ICEBERG_NAMESPACE` to `NAMESPACE`; the property names a namespace in both catalog kinds, so its Iceberg-era name misdescribed it. The merge precedence, the `null`-unsets rule, the re-enumeration, the response `type` labels, and the `requestedTables` echo are all unchanged.
* **The required-property set is unchanged in membership, only in spelling.** A `setProperties` request that leaves the namespace property or `CATALOG_CONNECTION` unset still returns the same clear error; the namespace property is now named `NAMESPACE` in that error.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Set properties overrides persisted properties and re-enumerates

* *GIVEN* a virtual schema created with property `NAMESPACE` set to one namespace, whose persisted properties arrive in `schemaMetadataInfo.properties`
* *WHEN* Exasol sends a request of type `setProperties` (the literal protocol string for `ALTER VIRTUAL SCHEMA ... SET`) whose `properties` object sets `NAMESPACE` to a different namespace
* *THEN* the adapter SHALL treat the request's `properties` as overriding the persisted `schemaMetadataInfo.properties` on conflict — the newly set value wins — and a `null` value in the request's `properties` SHALL unset that property
* *AND* the adapter SHALL re-enumerate using the effective merged properties and return a JSON response of type `setProperties` whose `schemaMetadata` describes the tables of the newly targeted namespace
* *AND* a `setProperties` request that leaves a required property (`NAMESPACE` or `CATALOG_CONNECTION`) unset SHALL return a clear error naming the missing property
<!-- /DELTA:CHANGED -->
