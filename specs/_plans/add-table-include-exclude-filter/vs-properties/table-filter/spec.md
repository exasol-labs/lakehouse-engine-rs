# Feature: Table Filter

The virtual schema properties `TABLE_INCLUDE` and `TABLE_EXCLUDE` choose which source tables a virtual schema exposes, by glob patterns over the Exasol virtual table name (#544). A user can expose most tables and hide a few, or expose a few and hide the rest, without naming every table. The filter narrows every listing that `vs-adapter/create-virtual-schema`, `vs-adapter/refresh-and-set-properties`, `vs-properties/create-virtual-schema-adapter-notes`, `unity-catalog/unity-catalog-create-virtual-schema`, `glue/glue-catalog-client`, and `direct-storage/direct-storage-table-discovery` describe: what those features state about every table of the source applies to the tables the filter exposes.

## Background

* Both properties are optional. Each holds a comma-separated list of glob patterns, such as `'AUDIT_LOG, TMP_*'`.
* A virtual table name is the namespace segments below the configured namespace plus the table name, joined with `__` and uppercased (`vs-adapter/create-virtual-schema-declaration-details`), so the table `orders` of the child namespace `staging` is `STAGING__ORDERS`.
* `vs-properties/property-name-validation` owns the name check, and `vs-properties/property-value-validation` owns the error format and the request lifecycle of every value check.

## Scenarios

### Scenario: Include and exclude patterns choose the exposed tables, and exclude wins

* *GIVEN* an Iceberg namespace with the tables `orders`, `orders_2024`, `orders_tmp`, and `audit_log`, and a child namespace `staging` with the table `orders`
* *WHEN* Exasol sends a `createVirtualSchema` request with `TABLE_INCLUDE = 'ORDERS*'` and `TABLE_EXCLUDE = 'ORDERS_TMP'`
* *THEN* the response SHALL declare `ORDERS` and `ORDERS_2024` and no other table
* *AND* the adapter SHALL expose a table exactly when `TABLE_INCLUDE` is absent or the table's virtual table name matches at least one of its patterns, and the name matches none of the `TABLE_EXCLUDE` patterns, so a table that matches both lists stays hidden
* *AND* `TABLE_INCLUDE = 'AUDIT_LOG'` alone SHALL declare `AUDIT_LOG` only
* *AND* `TABLE_EXCLUDE = 'STAGING__*, ORDERS_*'` alone SHALL hide every table of the child namespace `staging` and declare `AUDIT_LOG` and `ORDERS`

### Scenario: A pattern matches the whole virtual table name without regard to case

* *GIVEN* an entry of a `TABLE_INCLUDE` or `TABLE_EXCLUDE` value and a virtual table name
* *WHEN* the adapter compares them
* *THEN* in a pattern `*` SHALL match any sequence of characters, the empty one included, `?` SHALL match exactly one character, and every other character, `[`, `]`, and `\` included, SHALL match only itself, so the adapter accepts every non-empty entry as a pattern
* *AND* a pattern SHALL match the whole name, so `ORDERS` matches `ORDERS` and neither `ORDERS_2024` nor `STAGING__ORDERS`
* *AND* the adapter SHALL remove the whitespace around the entry and uppercase it with the fold the virtual table name receives before it compares them, so the entry ` orders_?0* ` matches `ORDERS_2024`

### Scenario: A hidden table is never loaded and never recorded as skipped

* *GIVEN* a source with a table `conflict` whose load fails the statement, such as a direct-storage directory whose files store one column at two types that no widening rule folds
* *AND* a table `empty` that the listing would skip, such as a direct-storage directory that holds no data file
* *WHEN* Exasol sends a `createVirtualSchema`, `refresh`, or `setProperties` request whose effective properties hold `TABLE_EXCLUDE = 'CONFLICT, EMPTY'`
* *THEN* the statement SHALL succeed and declare every other table of the source
* *AND* the adapter MUST NOT send a request or read an object for a hidden table beyond the namespace listing that names it: no Iceberg `loadTable`, no Unity Catalog `GET /tables/{full_name}`, no Glue `GetTable`, no Iceberg metadata-file read, and no read below a hidden direct-storage directory, so a hidden table cannot fail the statement
* *AND* `SKIPPED_TABLES` SHALL NOT hold an entry for a hidden table

### Scenario: Setting either property re-lists the exposed tables

* *GIVEN* a virtual schema created without `TABLE_INCLUDE` and `TABLE_EXCLUDE` over a source that lists `orders` and `orders_tmp`
* *WHEN* the user runs `ALTER VIRTUAL SCHEMA ... SET TABLE_EXCLUDE = 'ORDERS_TMP'`
* *THEN* the `setProperties` response SHALL omit `ORDERS_TMP`, so Exasol drops that virtual table, and `ORDERS` SHALL stay declared
* *AND* a following full `REFRESH` SHALL apply the persisted filter and keep `ORDERS_TMP` hidden
* *AND* a following `SET TABLE_EXCLUDE = null` SHALL declare `ORDERS_TMP` again, because a `null` value unsets a property per `vs-properties/property-value-validation`

### Scenario: A partial refresh treats a hidden table as one the source does not serve

* *GIVEN* a virtual schema whose `TABLE_EXCLUDE` hides the table `conflict` of a source that also lists the table `good`
* *WHEN* the user runs `ALTER VIRTUAL SCHEMA ... REFRESH TABLES GOOD CONFLICT`
* *THEN* the statement SHALL succeed, and the response SHALL declare `GOOD` and omit `CONFLICT`, so `REFRESH TABLES` cannot add a hidden table back
* *AND* the adapter SHALL remove every hidden identifier from the identifier listing before it resolves the requested names, so a requested hidden table resolves as a name that matches no listed identifier per `vs-adapter/refresh-and-set-properties`, and the adapter never loads it

### Scenario: An empty pattern entry fails the statement

* *GIVEN* a `createVirtualSchema`, `refresh`, or `setProperties` request whose effective properties hold a `TABLE_INCLUDE` or `TABLE_EXCLUDE` value with an entry that is empty once the whitespace around it is removed, such as `'ORDERS,,AUDIT_LOG'` or `'ORDERS, '`
* *WHEN* the adapter checks the properties
* *THEN* the request SHALL fail with the entry `invalid value '<value>' for '<name>': expected a comma-separated list of non-empty table name patterns` in the one error of `vs-properties/property-value-validation`
