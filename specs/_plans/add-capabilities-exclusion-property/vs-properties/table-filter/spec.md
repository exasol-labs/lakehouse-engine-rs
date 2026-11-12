# Feature: Table Filter

The virtual schema properties `TABLE_INCLUDE` and `TABLE_EXCLUDE` choose which source tables a virtual schema exposes, by glob patterns over the Exasol virtual table name (#544). A user can expose most tables and hide a few, or expose a few and hide the rest, without naming every table. The filter narrows every listing that `vs-adapter/create-virtual-schema`, `vs-adapter/refresh-and-set-properties`, `vs-properties/create-virtual-schema-adapter-notes`, `unity-catalog/unity-catalog-create-virtual-schema`, `glue/glue-catalog-client`, and `direct-storage/direct-storage-table-discovery` describe: what those features state about every table of the source applies to the tables the filter exposes.

## Background

* Both properties are optional. Each holds a comma-separated list of glob patterns, such as `'AUDIT_LOG, TMP_*'`.
* A virtual table name is the namespace segments below the configured namespace plus the table name, joined with `__` and uppercased (`vs-adapter/create-virtual-schema-declaration-details`), so the table `orders` of the child namespace `staging` is `STAGING__ORDERS`.
* `vs-properties/property-name-validation` owns the name check, and `vs-properties/property-value-validation` owns the error format and the request lifecycle of every value check.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: An empty pattern entry fails the statement

* *GIVEN* a `createVirtualSchema` or `setProperties` request whose effective properties hold a `TABLE_INCLUDE` or `TABLE_EXCLUDE` value with an entry that is empty once the whitespace around it is removed, such as `'ORDERS,,AUDIT_LOG'` or `'ORDERS, '`
* *WHEN* the adapter checks the properties
* *THEN* the request SHALL fail with the entry `invalid value '<value>' for '<name>': expected a comma-separated list of non-empty table name patterns` in the one error of `vs-properties/property-value-validation`
<!-- /DELTA:CHANGED -->
