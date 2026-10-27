# Feature: Virtual Schema Property Validation

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... REFRESH`, and `ALTER VIRTUAL SCHEMA ... SET` reject a property name the adapter does not read and a property value the adapter cannot use, with one error that names every such property (#534). The feature does nothing more: an absent property still takes its default. This feature owns the complete list of virtual schema property names.

## Background

* Exasol passes each property name of `CREATE VIRTUAL SCHEMA ... WITH` and `ALTER VIRTUAL SCHEMA ... SET` to the adapter without checking it, so only the adapter can reject an unknown name.
* A request's effective properties are its own properties merged with the persisted ones, per the precedence `vs-adapter/refresh-and-set-properties` states for each request type.
* The value checks of `CATALOG_KIND`, `PERMISSION_CHECK`, `USER_MAPPING`, `NAMESPACE`, `MERGE_SCHEMA`, and `HIVE_PARTITIONING`, and the required-property checks of `CATALOG_CONNECTION` and `NAMESPACE`, belong to `vs-adapter/catalog-kind-selection`, `vs-adapter/lakekeeper-permission-check`, `vs-adapter/create-virtual-schema`, and `direct-storage/direct-storage-properties`.
* The effect of each tuning property belongs to `vs-adapter/create-virtual-schema-adapter-notes-resources`, `scan-runtime/scan-execution-threading`, `scan-runtime/scan-execution-connection-concurrency`, and `pushdown-joins/pushdown-planning-join`.

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
* *THEN* the adapter SHALL accept exactly these 17 names: `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `ALLOW_HTTP`, `PARALLELISM_FACTOR`, `DATAFUSION_THREADING_MODE`, `DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `JOIN_BROADCAST_MAX_BYTES`, `S3_MAX_CONNECTIONS`, `MERGE_SCHEMA`, `HIVE_PARTITIONING`, `PERMISSION_CHECK`, and `USER_MAPPING`
* *AND* the set SHALL NOT depend on the catalog kind, so `MERGE_SCHEMA` under the Iceberg REST kind and `USER_MAPPING` without `PERMISSION_CHECK` are accepted and have no effect

### Scenario: An invalid tuning value fails the statement and names the accepted values

* *GIVEN* a `createVirtualSchema`, `refresh`, or `setProperties` request whose effective properties hold a tuning property with a non-empty value
* *WHEN* the adapter checks the value
* *THEN* the adapter SHALL fail the request for each rejected value with an error entry `invalid value '<value>' for '<name>': expected <accepted values>`
* *AND* the entry SHALL echo at most the first 100 characters of the value, followed by `…` when the value is longer, and SHALL double each `'` in the echoed text, so a long value or a value with a quote keeps the error readable
* *AND* the adapter SHALL reject a `MEMORY_POOL_FRACTION` value that is not a number greater than 0 and at most 1, expecting `a number greater than 0 and at most 1`, so `60` and `0` fail and `1` passes
* *AND* the adapter SHALL reject a `DATAFUSION_THREADING_MODE` value other than `AUTO` or `FIXED`, expecting `AUTO or FIXED`, and an `ALLOW_HTTP` value other than `TRUE` or `FALSE`, expecting `TRUE or FALSE`, each compared without regard to case

### Scenario: A whole-number tuning value is checked against the range of its type

* *GIVEN* a `createVirtualSchema`, `refresh`, or `setProperties` request whose effective properties hold a whole-number tuning property with a non-empty value
* *WHEN* the adapter checks the value
* *THEN* the adapter SHALL accept only a whole number in the property's range: `PARALLELISM_FACTOR` from 1 to 255; `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_TARGET_PARTITIONS`, and `S3_MAX_CONNECTIONS` from 1 to 65535; `INSTANCE_OVERHEAD_MB` from 0 to 65535; `DATAFUSION_BATCH_SIZE` from 1 to 4294967295; and `JOIN_BROADCAST_MAX_BYTES` from 1 to 18446744073709551615
* *AND* the adapter SHALL reject every other value with the expectation `a whole number from <minimum> to <maximum>`, so `DATAFUSION_THREADS_PER_UDF = '100000'` fails with `expected a whole number from 1 to 65535`
* *AND* a value too large for the range, however many digits it has, SHALL fail with that same expectation
* *AND* `S3_MAX_CONNECTIONS = 'AUTO'` and `S3_MAX_CONNECTIONS = '0'` SHALL fail, because only an absent property selects the derived budget of `scan-runtime/scan-execution-connection-concurrency`

### Scenario: A tuning value is checked on its own and never replaced by its default

* *GIVEN* a `createVirtualSchema` request under the `AUTO` threading mode, which ignores a valid `DATAFUSION_TARGET_PARTITIONS` value, that sets `DATAFUSION_TARGET_PARTITIONS = '0'`
* *WHEN* the adapter checks the properties
* *THEN* the request SHALL fail with the invalid-value entry for `DATAFUSION_TARGET_PARTITIONS`, because the adapter checks each tuning value without regard to the other properties
* *AND* the adapter MUST NOT replace any rejected tuning value with the property's default

### Scenario: One error reports every unknown name and every invalid value

* *GIVEN* a `createVirtualSchema` request with the properties `CATALOG_CONNECTION = '<conn>'`, `NAMESPCE = 'sales'`, `PARALLELISM_FACTR = '64'`, `DATAFUSION_BATCH_SIZE = 'abc'`, and `MADE_UP_PROPERTY = 'x'`
* *WHEN* the adapter receives the request
* *THEN* the request SHALL fail with one error whose message starts with `invalid virtual schema properties: ` and lists every problem, separated by `; `
* *AND* the error SHALL list the unknown names first and the invalid values second, each group in alphabetical order of the property name, so the message reads `invalid virtual schema properties: unknown property 'MADE_UP_PROPERTY'; unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?); unknown property 'PARALLELISM_FACTR' (did you mean 'PARALLELISM_FACTOR'?); invalid value 'abc' for 'DATAFUSION_BATCH_SIZE': expected a whole number from 1 to 4294967295`

### Scenario: A misspelled required property reports the misspelling

* *GIVEN* a `createVirtualSchema` request that sets `CATALOG_CONECTION` and not `CATALOG_CONNECTION`
* *WHEN* the adapter receives the request
* *THEN* the error SHALL name `CATALOG_CONECTION` as unknown and suggest `CATALOG_CONNECTION`
* *AND* the adapter SHALL report it instead of the `CATALOG_CONNECTION is required` error, because the name check runs before every required-property check and before the value checks the Background names
* *AND* each property whose own feature checks its value SHALL be reported once, by that feature's check, and never also by this feature

### Scenario: An empty or null property value counts as absent

* *GIVEN* a request whose properties hold a name with an empty or `null` value
* *WHEN* the adapter checks the properties
* *THEN* the adapter SHALL treat the property as absent, report neither an unknown name nor an invalid value for it, and resolve a tuning property to its default

### Scenario: Set properties validates the merged properties after null unsets

* *GIVEN* a virtual schema whose persisted properties are valid
* *WHEN* Exasol sends a `setProperties` request that sets an unknown name, or sets a tuning property to an invalid value
* *THEN* the `ALTER VIRTUAL SCHEMA ... SET` statement SHALL fail with the error of this feature, and the virtual schema SHALL keep its previous properties, tables, and `adapterNotes`
* *AND* a `setProperties` request that sets a name to `null` SHALL remove it from the effective properties before the check, so `ALTER VIRTUAL SCHEMA ... SET <name> = null` clears an unknown name or an invalid value

### Scenario: A schema persisted with an invalid property refuses refresh until the property is unset

* *GIVEN* a virtual schema whose persisted properties hold an unknown name or an invalid tuning value, because an adapter without this check created or altered it
* *WHEN* Exasol sends a `refresh` request
* *THEN* the `ALTER VIRTUAL SCHEMA ... REFRESH` statement SHALL fail with the error of this feature, listing every such property
* *AND* one `ALTER VIRTUAL SCHEMA ... SET` that sets each listed property to `null`, or each invalid value to a valid one, SHALL succeed, and a later `REFRESH` SHALL succeed

### Scenario: Pushdown and drop never validate properties

* *GIVEN* a virtual schema whose persisted properties hold an unknown name or an invalid tuning value, and whose `adapterNotes` hold valid tuning entries
* *WHEN* a user queries one of its virtual tables, or drops it
* *THEN* the `pushdown` request SHALL plan from the `adapterNotes` as before and the query SHALL return the table's rows
* *AND* `DROP VIRTUAL SCHEMA ... CASCADE` SHALL succeed
* *AND* the adapter MUST NOT run this feature's name check or tuning-value check on a `pushdown`, `getCapabilities`, or `dropVirtualSchema` request, and the value checks that `vs-adapter/catalog-kind-selection`, `vs-adapter/lakekeeper-permission-check`, and `direct-storage/direct-storage-properties` run on pushdown stay unchanged
