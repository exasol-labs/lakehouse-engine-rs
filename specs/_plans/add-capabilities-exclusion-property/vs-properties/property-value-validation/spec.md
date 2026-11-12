# Feature: Virtual Schema Property Value Validation

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... REFRESH`, and `ALTER VIRTUAL SCHEMA ... SET` reject a tuning property value the adapter cannot use, with one error that lists every invalid value together with every unknown name (#534). An absent property still takes its default. `vs-properties/property-name-validation` owns the list of known names and the name check.

<!-- DELTA:CHANGED -->
## Background

* Exasol itself rejects an empty or `null` value in `CREATE VIRTUAL SCHEMA ... WITH` with `Value of property <NAME> must not be null or empty.`, and it sends `ALTER VIRTUAL SCHEMA ... SET <name> = ''` and `SET <name> = null` to the adapter as a JSON `null`.
* A request's effective properties are its own properties merged with the persisted ones, per the precedence `vs-adapter/refresh-and-set-properties` states for each request type.
* The value checks of `CATALOG_KIND`, `PERMISSION_CHECK`, `USER_MAPPING`, `NAMESPACE`, `MERGE_SCHEMA`, `HIVE_PARTITIONING`, and `EXCLUDED_CAPABILITIES` belong to `vs-adapter/catalog-kind-selection`, `vs-adapter/lakekeeper-permission-check`, `vs-adapter/create-virtual-schema`, `direct-storage/direct-storage-properties`, and `vs-properties/excluded-capabilities`.
* The effect of each tuning property belongs to `vs-properties/create-virtual-schema-adapter-notes-resources`, `scan-runtime/scan-execution-threading`, `scan-runtime/scan-execution-connection-concurrency`, and `pushdown-joins/pushdown-planning-join`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One error reports every unknown name and every invalid value

* *GIVEN* a `createVirtualSchema` request with the properties `CATALOG_CONNECTION = '<conn>'`, `NAMESPCE = 'sales'`, `PARALLELISM_FACTR = '64'`, `DATAFUSION_BATCH_SIZE = 'abc'`, and `MADE_UP_PROPERTY = 'x'`
* *WHEN* the adapter receives the request
* *THEN* the request SHALL fail with one error whose message starts with `invalid virtual schema properties: ` and lists every problem, separated by `; `
* *AND* the error SHALL list the unknown names first and the invalid values second, each group in alphabetical order of the property name, so the message reads `invalid virtual schema properties: unknown property 'MADE_UP_PROPERTY'; unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?); unknown property 'PARALLELISM_FACTR' (did you mean 'PARALLELISM_FACTOR'?); invalid value 'abc' for 'DATAFUSION_BATCH_SIZE': expected a whole number from 1 to 4294967295`
* *AND* a property whose value yields several entries, such as `EXCLUDED_CAPABILITIES`, SHALL keep its place in that alphabetical order and list its entries in the order its value holds them
<!-- /DELTA:CHANGED -->
