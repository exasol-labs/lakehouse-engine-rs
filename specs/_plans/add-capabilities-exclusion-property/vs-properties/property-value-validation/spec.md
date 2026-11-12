<!-- DELTA:CHANGED -->
# Feature: Virtual Schema Property Value Validation

`CREATE VIRTUAL SCHEMA` and `ALTER VIRTUAL SCHEMA ... SET` reject a tuning property value the adapter cannot use, with one error that lists every invalid value together with every unknown name (#534). An absent property still takes its default. `ALTER VIRTUAL SCHEMA ... REFRESH` reads the persisted properties without checking them. `vs-properties/property-name-validation` owns the list of known names and the name check.
<!-- /DELTA:CHANGED -->

## Background

* Exasol itself rejects an empty or `null` value in `CREATE VIRTUAL SCHEMA ... WITH` with `Value of property <NAME> must not be null or empty.`, and it sends `ALTER VIRTUAL SCHEMA ... SET <name> = ''` and `SET <name> = null` to the adapter as a JSON `null`.
* A request's effective properties are its own properties merged with the persisted ones, per the precedence `vs-adapter/refresh-and-set-properties` states for each request type.
* The value checks of `CATALOG_KIND`, `PERMISSION_CHECK`, `USER_MAPPING`, `NAMESPACE`, `MERGE_SCHEMA`, and `HIVE_PARTITIONING` belong to `vs-adapter/catalog-kind-selection`, `vs-adapter/lakekeeper-permission-check`, `vs-adapter/create-virtual-schema`, and `direct-storage/direct-storage-properties`.
* The effect of each tuning property belongs to `vs-properties/create-virtual-schema-adapter-notes-resources`, `scan-runtime/scan-execution-threading`, `scan-runtime/scan-execution-connection-concurrency`, and `pushdown-joins/pushdown-planning-join`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: An invalid tuning value fails the statement and names the accepted values

* *GIVEN* a `createVirtualSchema` or `setProperties` request whose effective properties hold a tuning property with a non-empty value
* *WHEN* the adapter checks the value
* *THEN* the adapter SHALL fail the request for each rejected value with an error entry `invalid value '<value>' for '<name>': expected <accepted values>`
* *AND* the entry SHALL echo at most the first 100 characters of the value, followed by `…` when the value is longer, and SHALL double each `'` in the echoed text, so a long value or a value with a quote keeps the error readable
* *AND* the adapter SHALL reject a `MEMORY_POOL_FRACTION` value that is not a number greater than 0 and at most 1, expecting `a number greater than 0 and at most 1`, so `60` and `0` fail and `1` passes
* *AND* the adapter SHALL reject a `DATAFUSION_THREADING_MODE` value other than `AUTO` or `FIXED`, expecting `AUTO or FIXED`, and an `ALLOW_HTTP` or `BINARY_AS_STRING` value other than `TRUE` or `FALSE`, expecting `TRUE or FALSE`, each compared without regard to case
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A whole-number tuning value is checked against the range of its type

* *GIVEN* a `createVirtualSchema` or `setProperties` request whose effective properties hold a whole-number tuning property with a non-empty value
* *WHEN* the adapter checks the value
* *THEN* the adapter SHALL accept only a whole number in the property's range: `PARALLELISM_FACTOR` and `METADATA_READ_CONCURRENCY` from 1 to 255; `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_TARGET_PARTITIONS`, and `S3_MAX_CONNECTIONS` from 1 to 65535; `INSTANCE_OVERHEAD_MB` from 0 to 65535; `DATAFUSION_BATCH_SIZE` from 1 to 4294967295; and `JOIN_BROADCAST_MAX_BYTES` from 1 to 18446744073709551615
* *AND* the adapter SHALL reject every other value with the expectation `a whole number from <minimum> to <maximum>`, so `DATAFUSION_THREADS_PER_UDF = '100000'` fails with `expected a whole number from 1 to 65535`
* *AND* a value too large for the range, however many digits it has, SHALL fail with that same expectation
* *AND* `S3_MAX_CONNECTIONS = 'AUTO'` and `S3_MAX_CONNECTIONS = '0'` SHALL fail, because only an absent property selects the derived budget of `scan-runtime/scan-execution-connection-concurrency`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A tuning value is checked on its own and never replaced by its default

* *GIVEN* a `createVirtualSchema` request under the `AUTO` threading mode, which ignores a valid `DATAFUSION_TARGET_PARTITIONS` value, that sets `DATAFUSION_TARGET_PARTITIONS = '0'`
* *WHEN* the adapter checks the properties
* *THEN* the request SHALL fail with the invalid-value entry for `DATAFUSION_TARGET_PARTITIONS`, because the adapter checks each tuning value without regard to the other properties
* *AND* the adapter MUST NOT replace a rejected tuning value of a `createVirtualSchema` or `setProperties` request with the property's default
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: A schema persisted with an invalid property refuses refresh until the property is unset

* *GIVEN* a virtual schema whose persisted properties hold an unknown name or an invalid tuning value, because an adapter without this check created or altered it
* *WHEN* Exasol sends a `refresh` request
* *THEN* the `ALTER VIRTUAL SCHEMA ... REFRESH` statement SHALL fail with the error of this feature, listing every such property
* *AND* one `ALTER VIRTUAL SCHEMA ... SET` that sets each listed property to `null`, or each invalid value to a valid one, SHALL succeed, and a later `REFRESH` SHALL succeed
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Refresh reads the persisted properties without checking them

* *GIVEN* a virtual schema whose persisted properties hold an unknown name, `MEMORY_POOL_FRACTION = '60'`, `METADATA_READ_CONCURRENCY = '0'`, and `EXCLUDED_CAPABILITIES = 'NO_SUCH_CAPABILITY'`, because an adapter without the name and value checks created or altered it
* *WHEN* the user runs `ALTER VIRTUAL SCHEMA ... REFRESH`, with or without `TABLES`
* *THEN* the statement SHALL succeed, because the adapter MUST NOT run the name check of `vs-properties/property-name-validation`, the value checks of this feature, the `EXCLUDED_CAPABILITIES` check of `vs-properties/excluded-capabilities`, or the permission checks of `vs-adapter/lakekeeper-permission-check` on a `refresh` request
* *AND* each value these checks would reject SHALL read as absent, so the schema-level `adapterNotes` record the default `0.6` for `MEMORY_POOL_FRACTION` and the default `16` for `METADATA_READ_CONCURRENCY`, and a rejected `TABLE_INCLUDE` or `TABLE_EXCLUDE` value hides no table
* *AND* the unknown name SHALL have no effect, and a query over the virtual schema SHALL return the table's rows
* *AND* a `refresh` SHALL still fail when `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `MERGE_SCHEMA`, or `HIVE_PARTITIONING` cannot be used, with the error of the feature that owns the property, because the adapter reads them to list the source and declare its tables
<!-- /DELTA:NEW -->
