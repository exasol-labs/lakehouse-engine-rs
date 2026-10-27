# Plan: fix-vs-property-validation

## Summary

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... REFRESH`, and `ALTER VIRTUAL SCHEMA ... SET` fail with one error when a property name is unknown or a tuning value is invalid, instead of ignoring the name or using the default (#534). One property catalog in the adapter defines the known names and each tuning value's rule, and both the check and the resolution of the tuning values use it.

## Context

- Issue #534: `CREATE VIRTUAL SCHEMA` and `ALTER VIRTUAL SCHEMA ... SET` accept property names the adapter does not read and ignore them. `PERMISSION_CHEK = 'LAKEKEEPER'` turns enforcement off. `NAMESPCE = 'sales'` lists the whole CONNECTION root under `CATALOG_KIND = 'DIRECT_STORAGE'`, and fails as a missing `NAMESPACE` under the other three kinds. The issue was found by code inspection and is not yet reproduced live.
- Exasol passes custom adapter property names through unchecked, according to the issue. The Exasol JDBC adapters (`virtual-schema-common-jdbc`) call `validateProperties()` on createVirtualSchema, refresh, and setProperties, never on pushdown, and `SupportedPropertiesValidator` reports only the first unknown name.
- `handle_create_virtual_schema` (`crates/lakehouse-engine/src/adapter/mod.rs`) serves createVirtualSchema, refresh (full and partial), and setProperties. It builds the effective properties with `get_properties`, or with `merge_set_properties` for setProperties, which removes `null` values. The pushdown branch of `dispatch` and `pushdown/mod.rs` call `get_properties` separately.
- The tuning parsers (`resolve_*` in `adapter/mod.rs`) use `.parse().ok().filter(range).unwrap_or(default)`. `DATAFUSION_THREADING_MODE` falls back to `AUTO` for anything but `FIXED`, `ALLOW_HTTP` to false for anything but `true`, and `DATAFUSION_BATCH_SIZE = '0'` clamps to 1. The pushdown's `adapterNotes` reads use the same chains.
- `CATALOG_KIND` (`catalog_kind.rs`), `PERMISSION_CHECK` and `USER_MAPPING` (`permission.rs`), and `MERGE_SCHEMA`, `HIVE_PARTITIONING`, and the direct-storage `NAMESPACE` (`direct_storage_properties.rs`) already reject invalid values. No other workspace code reads a virtual schema property; `connection.rs` reads only the CONNECTION JSON, and `lakehouse-catalog` reads none.
- The E2E test `e2e_added_columns_initial_default_fill_all_types` passes `SCAN_SCHEMA`, which the adapter no longer reads. `docs/tuning.md` lists `LAKEHOUSE_UDF_DEBUG_LEVEL` (an environment variable) in the property table and documents an invalid `S3_MAX_CONNECTIONS` as AUTO.
- Accepted ADRs checked: `allow-http-threaded-as-vended-selector-parameter` keeps `ALLOW_HTTP` false by default, and the plan keeps that default. `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute` is unaffected, because the notes' entries do not change.
- The plan touches no scanning, pushdown planning, schema, or type handling, so the Iceberg table spec and Delta protocol check does not apply.
- The architecture delta (`architecture.md` in this directory) adds the `vs-properties` component and states the closed property set under § Interfaces.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/property-validation | NEW | `vs-adapter/property-validation/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes-resources | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes/spec.md` |
| scan-runtime/scan-execution-threading | CHANGED | `scan-runtime/scan-execution-threading/spec.md` |
| scan-runtime/scan-execution-connection-concurrency | CHANGED | `scan-runtime/scan-execution-connection-concurrency/spec.md` |
| direct-storage/direct-storage-properties | CHANGED | `direct-storage/direct-storage-properties/spec.md` |

The validation is a new feature because it applies to three request types, which `vs-adapter/create-virtual-schema` and `vs-adapter/refresh-and-set-properties` each cover in part, and it owns the complete property list.

## Impact

- **Breaking:** a `CREATE VIRTUAL SCHEMA` or `ALTER VIRTUAL SCHEMA ... SET` that sets an unknown name or an invalid tuning value now fails. Before, it succeeded and used the default. `DATAFUSION_BATCH_SIZE = '0'`, which was clamped to 1, now fails.
- **Existing schemas:** a virtual schema whose persisted properties hold an unknown name or an invalid value keeps answering queries. Its `REFRESH` and `SET` fail until one `ALTER VIRTUAL SCHEMA ... SET <name> = null ...` statement unsets every property the error lists.
- Query results do not change, so `docs/semantic-differences.md` gets no entry.

## Implementation Tasks

### 1. Reproduce #534 against the local Docker Exasol

- [ ] 1.1 Start the local Docker stack, build with `make cross-udf-build`, and provision the adapter as the local E2E harness does. Run three statements with `exapump sql`. If an observation differs from the expectations below, stop and report it to the orchestrator.
  - Run the issue's `CREATE VIRTUAL SCHEMA TYPO ...` statement. Expect it to fail with `property 'NAMESPACE' is required`. Under the Iceberg REST kind of the local suite, the error names the missing property, not the typo.
  - Run the same statement with `NAMESPACE` spelled correctly, and keep `PARALLELISM_FACTR = '64'`, `DATAFUSION_BATCH_SIZE = 'abc'`, and `MADE_UP_PROPERTY = 'x'`. Confirm it succeeds and records `PARALLELISM_FACTOR` `1` and `DF_BATCH_SIZE` `8192` in `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. Confirm `ALTER VIRTUAL SCHEMA TYPO SET MEMORY_POOL_FRACTION = '60'` succeeds and records `0.6`.
  - Run a `CATALOG_KIND = 'DIRECT_STORAGE'` CREATE with `NAMESPCE`, using the direct-storage fixtures of `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs`. Confirm it succeeds and lists the tables from the CONNECTION root.
- [ ] 1.2 Probe the request shapes with a Python stub adapter in its own schema, following `LEGACY_ADAPTER_BODY` in `crates/lakehouse-e2e/tests/e2e_local/refresh.rs`, that fails with the received `properties` and `schemaMetadataInfo.properties` as its error text. Record how an unquoted lowercase name and a quoted mixed-case name arrive, and what `CREATE ... WITH <name> = ''`, `SET <name> = null`, and `SET <name> = ''` send. Record whether a `setProperties` response without `schemaMetadata` keeps the tables and their notes, and whether a failed `ALTER ... SET` leaves `SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` unchanged. If an observation contradicts `vs-adapter/property-validation`, for example Exasol rejecting unknown names itself, stop and report it to the orchestrator. Also stop and report if Exasol requires `schemaMetadata` in a `setProperties` response, because the stub swap of task 2.1 depends on it.

### 2. E2E tests (written first, failing against the current `.so`)

- [ ] 2.1 Add `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs`, declare it in `crates/lakehouse-e2e/tests/e2e_local/main.rs`, and write the E2E tests of § Scenario Coverage. Use raw `CREATE`/`ALTER` SQL with this module's own virtual schema names, and a separate adapter schema for the stub swap of `stray_persisted_properties_block_refresh_but_not_queries`, so no shared schema or script changes. The swap test creates the schema with the engine adapter, swaps in a stub whose `setProperties` persists an unknown name and `MEMORY_POOL_FRACTION = '60'`, swaps the engine adapter back, then queries a seeded fixture table, refreshes, unsets, refreshes again, and drops.
- [ ] 2.2 Remove the `SCAN_SCHEMA` clause from `e2e_added_columns_initial_default_fill_all_types` (`crates/lakehouse-e2e/tests/e2e_local/scan.rs:553`). The adapter reads the script schema from `ctx.script_schema()`.
- [ ] 2.3 Extend `malformed_connections_are_rejected_at_create_virtual_schema` (`crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs:1667`) to assert that the `MERGE_SCHEMA` rejection does not contain `invalid virtual schema properties:`, and tag it with the changed direct-storage scenario.

### 3. Property catalog module

- [ ] 3.1 Write unit tests first in `crates/lakehouse-engine/src/adapter/properties_tests.rs`: each of the 17 names passes; a name differing only in case is unknown; the suggestion rule (`NAMESPCE`, `PARALLELISM_FACTR`, `PERMISSION_CHEK`, `"Namespace"`, an alphabetical tie, `MADE_UP_PROPERTY` without suggestion); each rule's boundaries (`0` and `1` for the counts, `0` and `-1` for `INSTANCE_OVERHEAD_MB`, `0`, `1`, `1.5`, `60`, `NaN`, and `inf` for the fraction, `fixed` and `garbage` for the mode, `True` and `yes` for `ALLOW_HTTP`); empty and `null` values as absent; the exact message of the issue example; an unknown property's value absent from the message.
- [ ] 3.2 Implement `crates/lakehouse-engine/src/adapter/properties.rs`: one table of every property name with its value rule, name-only for the seven properties the owning modules check. The check returns the typed tuning values or one `UdfError::User` in the format of `vs-adapter/property-validation`. Each rule also exposes a lenient parse for the pushdown's `adapterNotes` reads. Write the edit distance in the module, with no new dependency.
- [ ] 3.3 Move every property name constant into `properties.rs`. `catalog_kind.rs`, `permission.rs`, `direct_storage_properties.rs`, and `adapter/mod.rs` import the names, and their value checks and error texts stay unchanged.

### 4. Adapter wiring

- [ ] 4.1 Write unit tests first in `crates/lakehouse-engine/src/adapter/adapter_tests.rs`: a createVirtualSchema, refresh, and setProperties request with an unknown name fails with the unknown-property error when the `TestContext` has no CONNECTION; a setProperties request that sets a persisted unknown name to `null` passes the check; `CATALOG_CONECTION` alone fails with the unknown-property error, not `CATALOG_CONNECTION is required`. Update the lenient tests listed under § Dead Code Removal, and change `create_virtual_schema_rejects_old_namespace_alias_without_replacement` to expect `unknown property 'ICEBERG_NAMESPACE'`.
- [ ] 4.2 In `handle_create_virtual_schema`, run the check on the effective properties before `PermissionSettings::parse`, and pass its typed tuning values to the create-time resolution (`resolve_parallelism_factor` through `resolve_s3_max_connections`), which no longer parses strings. The create path drops the `DATAFUSION_BATCH_SIZE` clamp to 1.
- [ ] 4.3 Make `resolve_connection_config` read `ALLOW_HTTP` through the catalog's rule, keeping `false` for an invalid persisted value on the pushdown path. Make `handle_pushdown_request` read its `adapterNotes` through the catalog's lenient parses with today's defaults. The `pushdown` branch of `dispatch` runs no check.

### 5. Documentation

- [ ] 5.1 In `docs/tuning.md` § Parameters, state that `CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... SET`, and `REFRESH` fail on an unknown property name or an invalid value, with one error that names each problem and suggests the nearest name. Give each tuning property's accepted values. Replace "To change one value, you must recreate the VS" with `ALTER VIRTUAL SCHEMA ... SET`. Move `LAKEHOUSE_UDF_DEBUG_LEVEL` out of the property table, because setting it as a property now fails. Change the `S3_MAX_CONNECTIONS` bullet "Absent, invalid, or `0`" to "Absent", and state that `0` or an invalid value fails.

### 6. Verification

- [ ] 6.1 Run `make fmt-check lint` and `cargo test`.
- [ ] 6.2 Run `make test-e2e`, `make test-e2e-lakekeeper`, and `make test-e2e-unity` against freshly started stacks. Run `make test-e2e-glue` and `make test-e2e-azure` when their credentials are available.
- [ ] 6.3 Re-run the statements of task 1.1 and compare the errors with § Manual Testing.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: property validation | 1.1-4.3, 6.1-6.3 | none | spec deltas `vs-adapter/property-validation`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, `vs-adapter/create-virtual-schema-adapter-notes`, `scan-runtime/scan-execution-threading`, `scan-runtime/scan-execution-connection-concurrency`, `direct-storage/direct-storage-properties`; `crates/lakehouse-engine/src/adapter/{mod.rs,properties.rs,catalog_kind.rs,permission.rs,direct_storage_properties.rs}`, `crates/lakehouse-engine/src/adapter/{adapter_tests.rs,properties_tests.rs}`, `crates/lakehouse-e2e/tests/e2e_local/{property_validation.rs,main.rs,scan.rs,direct_storage.rs,refresh.rs}`, `crates/lakehouse-e2e/tests/common/e2e_harness.rs` |
| B: documentation | 5.1 | none | spec delta `vs-adapter/property-validation`; `docs/tuning.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Constant | `PROP_CATALOG_KIND` in `crates/lakehouse-engine/src/adapter/catalog_kind.rs` | Moved to `properties.rs` |
| Constant | `PERMISSION_CHECK`, `USER_MAPPING` in `crates/lakehouse-engine/src/adapter/permission.rs` | Moved to `properties.rs` |
| Constant | `PROP_MERGE_SCHEMA`, `PROP_HIVE_PARTITIONING` in `crates/lakehouse-engine/src/adapter/direct_storage_properties.rs` | Moved to `properties.rs` |
| Constant | `PROP_*` constants in `crates/lakehouse-engine/src/adapter/mod.rs` | Moved to `properties.rs` |
| Function | `resolve_df_fixed_count` and the string parsing in `resolve_parallelism_factor`, `resolve_threading_mode`, `resolve_s3_max_connections`, `resolve_df_batch_size`, `resolve_memory_pool_fraction`, `resolve_instance_overhead_mb`, `resolve_join_broadcast_max_bytes` (`adapter/mod.rs`) | The check returns typed values, so the create path parses no string |
| Test | Invalid-value branches of `create_vs_records_parallelism_factor`, `df_target_partitions_falls_back_when_absent_or_invalid`, `df_batch_size_uses_supplied_value`, `df_threads_per_udf_falls_back_when_absent_or_invalid`, `resolve_memory_pool_fraction_defaults_and_validates`, `resolve_instance_overhead_mb_defaults_and_validates`, `resolve_join_broadcast_max_bytes_defaults_and_validates`, `threading_mode_defaults_to_auto`, `resolve_s3_max_connections_auto_scales_with_threads` (`adapter_tests.rs`) | They assert the default replaces an invalid value; `properties_tests.rs` asserts the rejection |
| Scenario tag | `/// Scenario:` lines of `fixed_mode_uses_supplied_values` and `resolve_s3_max_connections_auto_scales_with_threads` | E2E tests prove the changed scenarios (`specs/testing.md` § Coverage rule) |
| E2E clause | `SCAN_SCHEMA` in `crates/lakehouse-e2e/tests/e2e_local/scan.rs:553` | The adapter does not read it, and the CREATE would fail |

## Verification

### Scenario Coverage

All E2E tests below live in `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` unless the location says otherwise.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| An unknown property name fails the statement | Integration | `property_validation.rs` | `create_rejects_unknown_property_names_before_reading_the_connection` |
| The error suggests the nearest known name for a close misspelling | Integration | `property_validation.rs` | `create_suggests_the_nearest_known_property_name` |
| Every property the adapter reads is known under every catalog kind | Integration | `property_validation.rs` | `create_accepts_every_known_property_name` (all names except `CATALOG_KIND` and `PERMISSION_CHECK`, which the existing direct-storage and Lakekeeper suites set) |
| An invalid tuning value fails the statement and names the accepted values | Integration | `property_validation.rs` | `create_rejects_each_invalid_tuning_value` |
| A tuning value is checked on its own and never replaced by its default | Integration | `property_validation.rs` | `create_rejects_each_invalid_tuning_value` (sets `DATAFUSION_TARGET_PARTITIONS = '0'` without `DATAFUSION_THREADING_MODE`) |
| One error reports every unknown name and every invalid value | Integration | `property_validation.rs` | `create_reports_every_property_problem_in_one_error` |
| A misspelled required property reports the misspelling | Integration | `property_validation.rs` | `misspelled_required_property_reports_the_misspelling` (also asserts that `CATALOG_KIND = 'NOPE'` yields the catalog-kind error without the `invalid virtual schema properties:` prefix) |
| An empty or null property value counts as absent | Integration | `property_validation.rs` | `empty_property_values_count_as_absent` |
| Set properties validates the merged properties after null unsets | Integration | `property_validation.rs` | `set_properties_rejects_problems_and_keeps_the_schema` |
| A schema persisted with an invalid property refuses refresh until the property is unset | Integration | `property_validation.rs` | `stray_persisted_properties_block_refresh_but_not_queries` |
| Pushdown and drop never validate properties | Integration | `property_validation.rs` | `stray_persisted_properties_block_refresh_but_not_queries` |
| Adapter records the parallelism factor in the virtual-schema adapterNotes | Integration | `property_validation.rs` | `create_accepts_every_known_property_name`, `create_without_tuning_properties_records_the_defaults`, `create_rejects_each_invalid_tuning_value` |
| Adapter records the DataFusion threading mode in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| Adapter records the DataFusion target partition count in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| Adapter records the memory-pool fraction in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| FIXED mode uses the operator-supplied thread and partition values verbatim | Integration | `property_validation.rs` | `create_accepts_every_known_property_name`, `create_rejects_each_invalid_tuning_value` |
| Threading mode defaults to AUTO when the property is absent | Integration | `property_validation.rs` | `create_without_tuning_properties_records_the_defaults`, `create_rejects_each_invalid_tuning_value` |
| AUTO derivation sizes the per-instance budget from the resolved thread count | Integration | `property_validation.rs` | `create_without_tuning_properties_records_the_defaults` (asserts `S3_MAX_CONNECTIONS` = 4 × `DF_THREADS_PER_UDF`), `create_rejects_each_invalid_tuning_value` |
| Adapter derives the per-node core count from available_parallelism() on every request | Integration | `property_validation.rs` | `create_rejects_unknown_property_names_before_reading_the_connection` (`NR_OF_CORES`), `create_without_tuning_properties_records_the_defaults` (no `NR_OF_CORES` note) |
| An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted | Integration | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `malformed_connections_are_rejected_at_create_virtual_schema` |
| Suggestion rule, value-rule boundaries, message format (logic, no scenario tag) | Unit | `crates/lakehouse-engine/src/adapter/properties_tests.rs` | tests of task 3.1 |
| Check order and null unset in dispatch (logic, no scenario tag) | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | tests of task 4.1 |

### Manual Testing

`DSN` is `exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0`, as in the Makefile. `<adapter>` is the adapter script the local E2E harness installs, and `<conn>` and `<ns>` are its CONNECTION and namespace.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/property-validation | `printf '%s' "CREATE VIRTUAL SCHEMA TYPO USING <adapter> WITH CATALOG_CONNECTION = '<conn>' NAMESPCE = '<ns>' PARALLELISM_FACTR = '64' DATAFUSION_BATCH_SIZE = 'abc' MADE_UP_PROPERTY = 'x'" \| exapump sql -d "$DSN"` | Fails with `invalid virtual schema properties: unknown property 'MADE_UP_PROPERTY'; unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?); unknown property 'PARALLELISM_FACTR' (did you mean 'PARALLELISM_FACTOR'?); invalid value 'abc' for 'DATAFUSION_BATCH_SIZE': expected a whole number of at least 1` |
| vs-adapter/property-validation | Create `OK_VS` with valid properties, then `printf '%s' "ALTER VIRTUAL SCHEMA OK_VS SET MEMORY_POOL_FRACTION = '60'" \| exapump sql -d "$DSN"`, then `SELECT PROPERTY_NAME, PROPERTY_VALUE FROM SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES WHERE SCHEMA_NAME = 'OK_VS'` | The ALTER fails with `invalid value '60' for 'MEMORY_POOL_FRACTION': expected a number greater than 0 and at most 1`; the properties are unchanged |
| vs-adapter/create-virtual-schema-adapter-notes-resources, scan-runtime/scan-execution-threading, scan-runtime/scan-execution-connection-concurrency | `printf '%s' "CREATE VIRTUAL SCHEMA DEF_VS USING <adapter> WITH CATALOG_CONNECTION = '<conn>' NAMESPACE = '<ns>' ALLOW_HTTP = 'true'" \| exapump sql -d "$DSN"`, then `SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'DEF_VS'` | Succeeds; notes hold `PARALLELISM_FACTOR` `1`, `DF_THREADING_MODE` `AUTO`, `MEMORY_POOL_FRACTION` `0.6`, `INSTANCE_OVERHEAD_MB` `200`, and `S3_MAX_CONNECTIONS` equal to 4 × `DF_THREADS_PER_UDF` |
| vs-adapter/create-virtual-schema-adapter-notes | The `DEF_VS` statement with `NR_OF_CORES = '64'` added | Fails with `invalid virtual schema properties: unknown property 'NR_OF_CORES'` |
| direct-storage/direct-storage-properties | A `CATALOG_KIND = 'DIRECT_STORAGE'` CREATE with `MERGE_SCHEMA = 'maybe'` | Fails with the existing `'MERGE_SCHEMA' value 'maybe' is neither a true nor a false spelling ...` error and no `invalid virtual schema properties:` prefix |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e`, `make test-e2e-lakekeeper`, `make test-e2e-unity` | 0 failures |
| Lint | `make lint` | 0 errors or warnings |
| Format | `make fmt-check` | No changes |
