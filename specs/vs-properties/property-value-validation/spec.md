# Feature: Virtual Schema Property Value Validation

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... REFRESH`, and `ALTER VIRTUAL SCHEMA ... SET` reject a tuning property value the adapter cannot use, with one error that lists every invalid value together with every unknown name (#534). An absent property still takes its default. `vs-properties/property-name-validation` owns the list of known names and the name check.

## Background

* Exasol itself rejects an empty or `null` value in `CREATE VIRTUAL SCHEMA ... WITH` with `Value of property <NAME> must not be null or empty.`, and it sends `ALTER VIRTUAL SCHEMA ... SET <name> = ''` and `SET <name> = null` to the adapter as a JSON `null`.
* A request's effective properties are its own properties merged with the persisted ones, per the precedence `vs-adapter/refresh-and-set-properties` states for each request type.
* The value checks of `CATALOG_KIND`, `PERMISSION_CHECK`, `USER_MAPPING`, `NAMESPACE`, `MERGE_SCHEMA`, and `HIVE_PARTITIONING` belong to `vs-adapter/catalog-kind-selection`, `vs-adapter/lakekeeper-permission-check`, `vs-adapter/create-virtual-schema`, and `direct-storage/direct-storage-properties`.
* The effect of each tuning property belongs to `vs-properties/create-virtual-schema-adapter-notes-resources`, `scan-runtime/scan-execution-threading`, `scan-runtime/scan-execution-connection-concurrency`, and `pushdown-joins/pushdown-planning-join`.

## Scenarios

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

### Scenario: An empty or null property value counts as absent

* *GIVEN* a `setProperties` request whose properties hold a name with a `null` value, which is what `ALTER VIRTUAL SCHEMA ... SET <name> = ''` and `SET <name> = null` send
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
