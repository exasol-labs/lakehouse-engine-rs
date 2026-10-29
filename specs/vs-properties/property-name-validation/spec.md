# Feature: Virtual Schema Property Name Validation

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... REFRESH`, and `ALTER VIRTUAL SCHEMA ... SET` reject a property name the adapter does not read, with one error that names every such property (#534). This feature owns the complete list of virtual schema property names. `vs-properties/property-value-validation` owns the checks of tuning values and the request lifecycle around both checks.

## Background

* Exasol passes each property name of `CREATE VIRTUAL SCHEMA ... WITH` and `ALTER VIRTUAL SCHEMA ... SET` to the adapter without checking it, so only the adapter can reject an unknown name.
* A request's effective properties are its own properties merged with the persisted ones, per the precedence `vs-adapter/refresh-and-set-properties` states for each request type.
* The required-property checks of `CATALOG_CONNECTION` and `NAMESPACE` belong to `vs-adapter/create-virtual-schema` and `direct-storage/direct-storage-properties`, and run after the name check.

## Scenarios

### Scenario: An unknown property name fails the statement

* *GIVEN* a `createVirtualSchema`, `refresh` (with or without `requestedTables`), or `setProperties` request whose effective properties hold a name outside the known property names
* *WHEN* the adapter receives the request
* *THEN* the adapter SHALL fail the request with an error that names each unknown property as `unknown property '<name>'`
* *AND* the adapter SHALL match names exactly and case-sensitively, so `"Namespace"` is unknown even though `NAMESPACE` is known
* *AND* the error MUST NOT contain the value of an unknown property, because a misspelled property can hold any text
* *AND* the adapter SHALL fail before it reads the CONNECTION or contacts any catalog or object store, so a request that also names a CONNECTION that does not exist fails with the unknown-property error

### Scenario: The error suggests the nearest known name for a close misspelling

* *GIVEN* a request that fails because of an unknown property name
* *WHEN* the adapter builds the error
* *THEN* for each unknown name that is at most two single-character edits (insertion, deletion, or substitution) from a known name, compared without regard to case, the error SHALL append `(did you mean '<known name>'?)` with the nearest known name, and on a tie the alphabetically first one
* *AND* the error SHALL append no suggestion for an unknown name more than two edits from every known name, so `MADE_UP_PROPERTY` gets none
* *AND* `NAMESPCE` SHALL suggest `NAMESPACE`, `PARALLELISM_FACTR` SHALL suggest `PARALLELISM_FACTOR`, `PERMISSION_CHEK` SHALL suggest `PERMISSION_CHECK`, and `"Namespace"` SHALL suggest `NAMESPACE`

### Scenario: Every property the adapter reads is known under every catalog kind


* *GIVEN* a `createVirtualSchema` request under any `CATALOG_KIND`
* *WHEN* the adapter checks the property names
* *THEN* the adapter SHALL accept exactly these 20 names: `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `ALLOW_HTTP`, `PARALLELISM_FACTOR`, `DATAFUSION_THREADING_MODE`, `DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `JOIN_BROADCAST_MAX_BYTES`, `S3_MAX_CONNECTIONS`, `MERGE_SCHEMA`, `HIVE_PARTITIONING`, `BINARY_AS_STRING`, `PERMISSION_CHECK`, `USER_MAPPING`, `TABLE_INCLUDE`, and `TABLE_EXCLUDE`
* *AND* the set SHALL NOT depend on the catalog kind, so `MERGE_SCHEMA` under the Iceberg REST kind and `USER_MAPPING` without `PERMISSION_CHECK` are accepted and have no effect

### Scenario: A misspelled required property reports the misspelling

* *GIVEN* a `createVirtualSchema` request that sets `CATALOG_CONECTION` and not `CATALOG_CONNECTION`
* *WHEN* the adapter receives the request
* *THEN* the error SHALL name `CATALOG_CONECTION` as unknown and suggest `CATALOG_CONNECTION`
* *AND* the adapter SHALL report it instead of the `CATALOG_CONNECTION is required` error, because the name check runs before every required-property check and before the value checks the Background names
* *AND* each property whose own feature checks its value SHALL be reported once, by that feature's check, and never also by this feature

### Scenario: Pushdown and drop never validate properties

* *GIVEN* a virtual schema whose persisted properties hold an unknown name or an invalid tuning value, and whose `adapterNotes` hold valid tuning entries
* *WHEN* a user queries one of its virtual tables, or drops it
* *THEN* the `pushdown` request SHALL plan from the `adapterNotes` as before and the query SHALL return the table's rows
* *AND* `DROP VIRTUAL SCHEMA ... CASCADE` SHALL succeed
* *AND* the adapter MUST NOT run this feature's name check or tuning-value check on a `pushdown`, `getCapabilities`, or `dropVirtualSchema` request, and the value checks that `vs-adapter/catalog-kind-selection`, `vs-adapter/lakekeeper-permission-check`, and `direct-storage/direct-storage-properties` run on pushdown stay unchanged
