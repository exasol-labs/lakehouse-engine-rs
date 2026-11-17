# Feature: Virtual Schema Property Value Validation

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... REFRESH`, and `ALTER VIRTUAL SCHEMA ... SET` reject a tuning property value the adapter cannot use, with one error that lists every invalid value together with every unknown name (#534). An absent property still takes its default. `vs-properties/property-name-validation` owns the list of known names and the name check.

<!-- DELTA:CHANGED -->
## Background

* Exasol itself rejects an empty or `null` value in `CREATE VIRTUAL SCHEMA ... WITH` with `Value of property <NAME> must not be null or empty.`, and it sends `ALTER VIRTUAL SCHEMA ... SET <name> = ''` and `SET <name> = null` to the adapter as a JSON `null`.
* A request's effective properties are its own properties merged with the persisted ones, per the precedence `vs-adapter/refresh-and-set-properties` states for each request type.
* The value checks of `CATALOG_KIND`, `PERMISSION_CHECK`, `USER_MAPPING`, `NAMESPACE`, `MERGE_SCHEMA`, `HIVE_PARTITIONING`, `EXCLUDED_CAPABILITIES`, and `SESSION_TIME_ZONE` belong to `vs-adapter/catalog-kind-selection`, `vs-adapter/lakekeeper-permission-check`, `vs-adapter/create-virtual-schema`, `direct-storage/direct-storage-properties`, `vs-properties/excluded-capabilities`, and `pushdown/pushdown-planning-clock-functions`.
* `createVirtualSchema`, `refresh`, and `setProperties` check the properties. `pushdown` and `getCapabilities` read a rejected value as absent and an unknown name as unset (`vs-properties/property-name-validation`).
* The effect of each tuning property belongs to `vs-properties/tuning-resolution`, `scan-runtime/scan-execution-threading`, `scan-runtime/scan-execution-connection-concurrency`, and `pushdown-joins/pushdown-planning-join`.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: An empty or null property value counts as absent

* *GIVEN* a `setProperties` request whose properties hold a name with a `null` value, which is what `ALTER VIRTUAL SCHEMA ... SET <name> = ''` and `SET <name> = null` send
* *WHEN* the adapter checks the properties
* *THEN* the adapter SHALL treat the property as absent, report neither an unknown name nor an invalid value for it, and resolve a tuning property to its default
