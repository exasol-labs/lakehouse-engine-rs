# Plan: fix-vs-property-validation

## Summary

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... REFRESH`, and `ALTER VIRTUAL SCHEMA ... SET` reject an unknown property name or an invalid property value with one error (#534). The fix does nothing more: an absent property still takes its default. One property catalog in the adapter lists the 17 property names and parses each tuning value into a fixed-width type whose range is the accepted range. The plan also removes the remaining references to retired properties and lists every property in one documentation table.

## Context

- Issue #534: `CREATE VIRTUAL SCHEMA` and `ALTER VIRTUAL SCHEMA ... SET` accept property names the adapter does not read and ignore them. `PERMISSION_CHEK = 'LAKEKEEPER'` turns enforcement off. `NAMESPCE = 'sales'` lists the whole CONNECTION root under `CATALOG_KIND = 'DIRECT_STORAGE'`, and fails as a missing `NAMESPACE` under the other three kinds. The issue was found by code inspection and is not yet reproduced live.
- Exasol passes custom adapter property names through unchecked, according to the issue. The Exasol JDBC adapters (`virtual-schema-common-jdbc`) call `validateProperties()` on createVirtualSchema, refresh, and setProperties, never on pushdown, and `SupportedPropertiesValidator` reports only the first unknown name.
- `handle_create_virtual_schema` (`crates/lakehouse-engine/src/adapter/mod.rs`) serves createVirtualSchema, refresh (full and partial), and setProperties. It builds the effective properties with `get_properties`, or with `merge_set_properties` for setProperties, which removes `null` values. The pushdown branch of `dispatch` and `pushdown/mod.rs` call `get_properties` separately.
- The tuning parsers (`resolve_*` in `adapter/mod.rs`) use `.parse().ok().filter(range).unwrap_or(default)` and check only a lower bound, so `DATAFUSION_THREADS_PER_UDF = '100000'` is accepted. `DATAFUSION_THREADING_MODE` falls back to `AUTO` for anything but `FIXED`, `ALLOW_HTTP` to false for anything but `true`, and `DATAFUSION_BATCH_SIZE = '0'` clamps to 1. `S3_MAX_CONNECTIONS = 'AUTO'` works only because `AUTO` fails to parse and the fallback is the derived budget.
- `build_adapter_notes` in `adapter/mod.rs` writes the schema-level `adapterNotes`, and `handle_pushdown_request` in the same file reads them back with the same lenient chains.
- `CATALOG_KIND` (`catalog_kind.rs`), `PERMISSION_CHECK` and `USER_MAPPING` (`permission.rs`), and `MERGE_SCHEMA`, `HIVE_PARTITIONING`, and the direct-storage `NAMESPACE` (`direct_storage_properties.rs`) already reject invalid values. No other workspace code reads a virtual schema property; `connection.rs` reads only the CONNECTION JSON, and `lakehouse-catalog` reads none.
- The harness comment `Exasol rejects NAMESPACE = ''` (`crates/lakehouse-e2e/tests/common/e2e_harness.rs:296`) is unverified. What reaches the adapter for an empty or `null` property value is not known.
- `docs/tuning.md` lists 13 of the 17 properties. `MERGE_SCHEMA` and `HIVE_PARTITIONING` appear only in `docs/catalogs.md`, and `PERMISSION_CHECK` and `USER_MAPPING` only in `docs/security.md`. The table shows `AUTO` as the default of `S3_MAX_CONNECTIONS` and documents an invalid value or `0` as AUTO. `docs/tuning.md` and `docs/architecture.md:46` say a tuning change needs a new virtual schema, although `ALTER VIRTUAL SCHEMA ... SET` re-resolves the notes.
- The table also lists `LAKEHOUSE_UDF_DEBUG_LEVEL`, which no adapter or UDF code reads. Only `bench/run.sh` reads it, to write the `%udf_debug_level` directive into the scan script source, and `ctx.debug_level()` returns the level that directive sets. `specs/mission.md` and `scan-runtime/scan-execution-telemetry` name the variable as the switch.
- Retired names remain in tests, specs, and docs: `SCAN_SCHEMA` in `e2e_added_columns_initial_default_fill_all_types` and `pushdown/pushdown-planning`; `NR_OF_CORES` in `create_vs_omits_cluster_nodes_from_adapter_notes` (`scan.rs:1033`), `vs-adapter/create-virtual-schema-adapter-notes`, `AGENTS.md:53`, `specs/udf-context.md:11`, and `bench/requirements.md:18`; `ICEBERG_NAMESPACE` in `create_virtual_schema_rejects_old_namespace_alias_without_replacement`; the removed note key `CLUSTER_NODES` in `create_vs_omits_cluster_nodes_from_adapter_notes` (`scan.rs:1038`), `build_adapter_notes_merges_existing` and `adapter_notes_omit_cluster_nodes` (`adapter_tests.rs:358-381`), `pushdown/pushdown-planning`, `docs/architecture.md:37`, and the log greps of `bench/sweep.sh:27` and `bench/parallelism_sweep.sh:29`. No engine or UDF code emits `CLUSTER_NODES`, so those greps match nothing.
- Accepted ADRs checked: `allow-http-threaded-as-vended-selector-parameter` keeps `ALLOW_HTTP` false by default, and the plan keeps that default. `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute` is unaffected: a refresh keeps an inherited entry, as the ADR decides.
- The plan touches no scanning, pushdown planning, schema, or type handling, so the Iceberg table spec and Delta protocol check does not apply.
- The architecture delta (`architecture.md` in this directory) adds the `vs-properties` component, keeps the notes format with `vs-adapter`, and points § Interfaces to the property list of `vs-adapter/property-validation`.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/property-validation | NEW | `vs-adapter/property-validation/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes-resources | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes/spec.md` |
| scan-runtime/scan-execution-threading | CHANGED | `scan-runtime/scan-execution-threading/spec.md` |
| scan-runtime/scan-execution-connection-concurrency | CHANGED | `scan-runtime/scan-execution-connection-concurrency/spec.md` |
| scan-runtime/scan-execution-telemetry | CHANGED | `scan-runtime/scan-execution-telemetry/spec.md` |
| direct-storage/direct-storage-properties | CHANGED | `direct-storage/direct-storage-properties/spec.md` |
| pushdown/pushdown-planning | CHANGED | `pushdown/pushdown-planning/spec.md` |

The validation is a new feature because it applies to three request types, which `vs-adapter/create-virtual-schema` and `vs-adapter/refresh-and-set-properties` each cover in part, and it owns the complete property list. The telemetry and pushdown-planning deltas change wording only: they name the actual debug switch and drop a step that names a retired property.

## Impact

- **Breaking:** a `CREATE VIRTUAL SCHEMA` or `ALTER VIRTUAL SCHEMA ... SET` that sets an unknown name or an invalid tuning value now fails. Before, it succeeded and used the default. This includes `DATAFUSION_BATCH_SIZE = '0'`, which was clamped to 1, `S3_MAX_CONNECTIONS = 'AUTO'` or `'0'`, which selected the derived budget, and a whole number above the property's range, such as `PARALLELISM_FACTOR = '300'`.
- **Existing schemas:** a virtual schema whose persisted properties hold an unknown name or an invalid value keeps answering queries. Its `REFRESH` and `SET` fail until one `ALTER VIRTUAL SCHEMA ... SET <name> = null ...` statement unsets every property the error lists.
- Query results do not change, so `docs/semantic-differences.md` gets no entry.

## Implementation Tasks

### 1. Reproduce #534 against the local Docker Exasol

- [ ] 1.1 Start the local Docker stack, build with `make cross-udf-build`, and provision the adapter as the local E2E harness does. Run three statements with `exapump sql`. If an observation differs from the expectations below, stop and report it to the orchestrator.
  - Run the issue's `CREATE VIRTUAL SCHEMA TYPO ...` statement. Expect it to fail with `property 'NAMESPACE' is required`. Under the Iceberg REST kind of the local suite, the error names the missing property, not the typo.
  - Run the same statement with `NAMESPACE` spelled correctly, and keep `PARALLELISM_FACTR = '64'`, `DATAFUSION_BATCH_SIZE = 'abc'`, and `MADE_UP_PROPERTY = 'x'`. Confirm it succeeds and records `PARALLELISM_FACTOR` `1` and `DF_BATCH_SIZE` `8192` in `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. Confirm `ALTER VIRTUAL SCHEMA TYPO SET MEMORY_POOL_FRACTION = '60'` succeeds and records `0.6`.
  - Run a `CATALOG_KIND = 'DIRECT_STORAGE'` CREATE with `NAMESPCE`, using the direct-storage fixtures of `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs`. Confirm it succeeds and lists the tables from the CONNECTION root.
- [ ] 1.2 Probe the request shapes with a Python stub adapter in its own schema, following `LEGACY_ADAPTER_BODY` in `crates/lakehouse-e2e/tests/e2e_local/refresh.rs`, that fails with the received `properties` and `schemaMetadataInfo.properties` as its error text.
  - Record how an unquoted lowercase name and a quoted mixed-case name arrive.
  - For `NAMESPACE` and for `MEMORY_POOL_FRACTION`, run `CREATE ... WITH <name> = ''`, `ALTER ... SET <name> = ''`, `ALTER ... SET <name> = null`, and an `ALTER ... REFRESH` after each. For each statement, record whether Exasol rejects it before it calls the adapter, with Exasol's error text, or which value reaches the adapter: no key, `""`, or JSON `null`. This checks the comment at `crates/lakehouse-e2e/tests/common/e2e_harness.rs:296`.
  - Record whether a `setProperties` response without `schemaMetadata` keeps the tables and their notes, and whether a failed `ALTER ... SET` leaves `SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` unchanged.
  - Stop and report to the orchestrator if Exasol rejects unknown names itself, if Exasol requires `schemaMetadata` in a `setProperties` response (the stub swap of task 2.1 depends on it), or if the received properties hold any key the statement did not set.
  - If Exasol rejects an empty or `null` value itself, state that in Background bullet 1 of this plan's `vs-adapter/property-validation` delta, and limit its scenario "An empty or null property value counts as absent" to the values that reach the adapter. Keep the comment at `e2e_harness.rs:296` if the observation confirms it, and correct it otherwise.

### 2. E2E tests (written first, failing against the current `.so`)

- [ ] 2.1 Add `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs`, declare it in `crates/lakehouse-e2e/tests/e2e_local/main.rs`, and write the E2E tests of § Scenario Coverage. Use raw `CREATE`/`ALTER` SQL with this module's own virtual schema names, and a separate adapter schema for the stub swap of `stray_persisted_properties_block_refresh_but_not_queries`, so no shared schema or script changes.
  - `create_rejects_each_invalid_tuning_value` sets, for each whole-number property, its maximum plus 1 and a 21-digit value, and also `S3_MAX_CONNECTIONS = 'AUTO'`, `S3_MAX_CONNECTIONS = '0'`, and a 150-character `DATAFUSION_BATCH_SIZE` value that contains `'`. It asserts each range expectation, the 100-character cut with `…`, and the doubled quote.
  - `empty_and_null_property_values_count_as_absent` pins the observations of task 1.2 for CREATE with `''`, SET with `''`, SET with `null`, and the REFRESH after each. Where Exasol rejects the statement, the test asserts Exasol's error. Where the value reaches the adapter, the test asserts that the statement succeeds and the notes hold the defaults.
  - The swap test creates the schema with the engine adapter, swaps in a stub whose `setProperties` persists an unknown name and `MEMORY_POOL_FRACTION = '60'`, swaps the engine adapter back, then queries a seeded fixture table, refreshes, unsets, refreshes again, and drops.
- [ ] 2.2 Remove the `SCAN_SCHEMA` clause from `e2e_added_columns_initial_default_fill_all_types` (`crates/lakehouse-e2e/tests/e2e_local/scan.rs:553`), and tag the test with the changed `pushdown/pushdown-planning` scenario "Scan-driving UDF invocations are schema-qualified from the running adapter script's schema". The adapter reads the script schema from `ctx.script_schema()`.
- [ ] 2.3 Extend `malformed_connections_are_rejected_at_create_virtual_schema` (`crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs:1667`) to assert that the `MERGE_SCHEMA` rejection does not contain `invalid virtual schema properties:`, and tag it with the changed direct-storage scenario.
- [ ] 2.4 In `create_vs_omits_cluster_nodes_from_adapter_notes` (`crates/lakehouse-e2e/tests/e2e_local/scan.rs:1000`), replace the `NR_OF_CORES` and `CLUSTER_NODES` absence checks with one assertion on the exact set of note keys of the changed scenario "createVirtualSchema adapterNotes omit the cluster node count", and tag the test with that scenario and with "Adapter derives the per-node core count from available_parallelism() on every request". After the CREATE, run `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET PARALLELISM_FACTOR = '1'`, and assert the same key set after each. Tag `pushdown_shards_from_handshake_node_count_without_note` (`scan.rs:1187`) with the changed `pushdown/pushdown-planning` scenario "Pushdown reads the cluster node count from the UDF handshake".

### 3. Property catalog module

- [ ] 3.1 Write unit tests first in `crates/lakehouse-engine/src/adapter/properties_tests.rs`:
  - each of the 17 names passes; a name differing only in case is unknown;
  - the suggestion rule (`NAMESPCE`, `PARALLELISM_FACTR`, `PERMISSION_CHEK`, `"Namespace"`, an alphabetical tie, `MADE_UP_PROPERTY` without suggestion);
  - for each whole-number property: its minimum, its maximum, the value below the minimum, the maximum plus 1, a 21-digit value, `-1`, and `abc`, each rejected value with the expectation that names the type's range;
  - `0`, `1`, `1.5`, `60`, `NaN`, and `inf` for `MEMORY_POOL_FRACTION`, `fixed` and `garbage` for the mode, `True` and `yes` for `ALLOW_HTTP`;
  - the echo: a 100-character value echoed whole, a 101-character value cut to 100 characters and `…`, a multi-byte character counted as one character, and `'` doubled;
  - the exact message of the issue example, and an unknown property's value absent from the message.
- [ ] 3.2 Implement `crates/lakehouse-engine/src/adapter/properties.rs`: one table of every property name with its value rule, name-only for the seven properties the owning modules check. Parse `PARALLELISM_FACTOR` into `NonZeroU8`; `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_TARGET_PARTITIONS`, and `S3_MAX_CONNECTIONS` into `NonZeroU16`; `INSTANCE_OVERHEAD_MB` into `u16`; `DATAFUSION_BATCH_SIZE` into `NonZeroU32`; and `JOIN_BROADCAST_MAX_BYTES` into `NonZeroU64`. Build each range expectation from the type's minimum and maximum, so the message and the parse cannot disagree, and map every parse error, overflow included, to that expectation. The check returns the typed tuning values, `None` for an absent property, or one `UdfError::User` in the format of `vs-adapter/property-validation`. The module parses properties only, never `adapterNotes`. Write the edit distance in the module, with no new dependency.
- [ ] 3.3 Move every property name constant into `properties.rs`. `catalog_kind.rs`, `permission.rs`, `direct_storage_properties.rs`, and `adapter/mod.rs` import the names, and their value checks and error texts stay unchanged.

### 4. Adapter wiring

- [ ] 4.1 Write unit tests first in `crates/lakehouse-engine/src/adapter/adapter_tests.rs`: a createVirtualSchema, refresh, and setProperties request with an unknown name fails with the unknown-property error when the `TestContext` has no CONNECTION; `CATALOG_CONECTION` alone fails with the unknown-property error, not `CATALOG_CONNECTION is required`. Update the lenient tests listed under § Dead Code Removal, delete `create_virtual_schema_rejects_old_namespace_alias_without_replacement`, make `build_adapter_notes_merges_existing` seed and assert only `OTHER_KEY` as the inherited entry, delete `adapter_notes_omit_cluster_nodes`, because the exact-key-set assertion of task 2.4 proves the same behavior, and tag `resolve_s3_max_connections_auto_scales_with_threads` with "AUTO derivation yields the single-core budget when the core count cannot be detected". Empty and `null` values get no unit test; task 2.1 proves them end to end.
- [ ] 4.2 In `handle_create_virtual_schema`, run the check on the effective properties before `PermissionSettings::parse`, and pass its typed tuning values to the create-time resolution (`resolve_parallelism_factor` through `resolve_s3_max_connections`), which no longer parses strings and widens each value to the type `TuningNotes` holds. `resolve_s3_max_connections` derives the budget only for an absent value. The create path drops the `DATAFUSION_BATCH_SIZE` clamp to 1.
- [ ] 4.3 Make `resolve_connection_config` parse `ALLOW_HTTP` with the catalog's rule and treat a rejected value as `false`, which only a pushdown on a schema persisted before this check reaches. `handle_pushdown_request` keeps its `adapterNotes` reads and their defaults unchanged in `adapter/mod.rs`, beside `build_adapter_notes`; they use no catalog rule. The `pushdown` branch of `dispatch` runs no check.

### 5. Documentation and retired names

- [ ] 5.1 Update `docs/tuning.md` and `docs/architecture.md`. Add no migration notes.
  - In `docs/tuning.md` § Parameters, list all 17 properties in one table. Add rows for `MERGE_SCHEMA` and `HIVE_PARTITIONING`, linking to their direct-storage section in `docs/catalogs.md`, and for `PERMISSION_CHECK` and `USER_MAPPING`, linking to `docs/security.md` § Lakekeeper permission check and § USER_MAPPING. Each row names the accepted values, using the ranges of `vs-adapter/property-validation`, and links to the page that explains the property.
  - Remove the `LAKEHOUSE_UDF_DEBUG_LEVEL` row, because it is not a property.
  - State that `CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... SET`, and `REFRESH` fail on an unknown property name or an invalid value, with one error that names each problem and suggests the nearest name. Replace "Each value is fixed at `CREATE VIRTUAL SCHEMA` time. To change one value, you must recreate the VS" with `ALTER VIRTUAL SCHEMA ... SET`.
  - For `S3_MAX_CONNECTIONS`, show the default as absent, with the derived budget of 4 × the resolved thread count. Change the bullet "Absent, invalid, or `0`" to "Absent", state that `AUTO`, `0`, or any other value outside 1 to 65535 fails, and change "a change from `AUTO`" in "When to change it" to "a change from the derived budget".
  - At `docs/tuning.md:72`, name the `%udf_debug_level debug` directive in the `LAKEHOUSE_SCAN` script source as the switch, and state that the bench variable `LAKEHOUSE_UDF_DEBUG_LEVEL` only writes that directive.
  - At `docs/architecture.md:46`, replace "A change to one of them requires a new VS" with `ALTER VIRTUAL SCHEMA ... SET`.
  - At `docs/architecture.md:37`, replace `G = CLUSTER_NODES × PARALLELISM_FACTOR` with `G = node_count × PARALLELISM_FACTOR`.
- [ ] 5.2 Remove retired names and the old debug switch outside `docs/`, and add the no-trace rule.
  - In `AGENTS.md:53` and `specs/udf-context.md:11`, replace "sized to `NR_OF_CORES`" with "sized to the node's core count".
  - In `bench/requirements.md:18`, remove the clause that names the `NR_OF_CORES` property and the `BENCH_NR_OF_CORES` variable, and keep the statement that the adapter detects the core count on its executing node.
  - In `specs/mission.md` Core Capability 5, replace "enabled through `LAKEHOUSE_UDF_DEBUG_LEVEL` (`ctx.debug_level()`)" with "enabled through the `%udf_debug_level` script directive (`ctx.debug_level()`)".
  - In the `grep -E` patterns of `bench/sweep.sh:27` and `bench/parallelism_sweep.sh:29`, remove the `CLUSTER_NODES` alternative and keep the other alternatives unchanged. The pattern targets the removed note key, which no engine or UDF code emits, so no other token replaces it.
  - Tag `telemetry_reports_three_phases_when_enabled` (`crates/lakehouse-engine/tests/it/scan_telemetry.rs:146`) with the changed telemetry scenario.
  - Add to `AGENTS.md` § Workflow: "A removed property, option, or note key leaves no trace. No spec step, test, or doc names it."

### 6. Verification

- [ ] 6.1 Run `make fmt-check lint` and `cargo test`.
- [ ] 6.2 Run `make test-e2e`, `make test-e2e-lakekeeper`, and `make test-e2e-unity` against freshly started stacks. Run `make test-e2e-glue` and `make test-e2e-azure` when their credentials are available.
- [ ] 6.3 Re-run the statements of task 1.1 and the commands of § Manual Testing, and compare the output with § Manual Testing.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: property validation | 1.1-4.3, 6.1-6.3 | none | spec deltas `vs-adapter/property-validation`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, `vs-adapter/create-virtual-schema-adapter-notes`, `scan-runtime/scan-execution-threading`, `scan-runtime/scan-execution-connection-concurrency`, `direct-storage/direct-storage-properties`, `pushdown/pushdown-planning` (base `specs/pushdown/pushdown-planning/spec.md`); `crates/lakehouse-engine/src/adapter/{mod.rs,properties.rs,catalog_kind.rs,permission.rs,direct_storage_properties.rs}`, `crates/lakehouse-engine/src/adapter/{adapter_tests.rs,properties_tests.rs}`, `crates/lakehouse-e2e/tests/e2e_local/{property_validation.rs,main.rs,scan.rs,direct_storage.rs,refresh.rs}`, `crates/lakehouse-e2e/tests/common/e2e_harness.rs` |
| B: documentation and retired names | 5.1-5.2 | none | spec delta `scan-runtime/scan-execution-telemetry`, and the ranges in `vs-adapter/property-validation` (read only); `docs/tuning.md`, `docs/architecture.md` (`:37`, `:46`), `AGENTS.md`, `specs/udf-context.md`, `specs/mission.md`, `bench/requirements.md`, `bench/sweep.sh`, `bench/parallelism_sweep.sh`, `crates/lakehouse-engine/tests/it/scan_telemetry.rs` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Constant | `PROP_CATALOG_KIND` in `crates/lakehouse-engine/src/adapter/catalog_kind.rs` | Moved to `properties.rs` |
| Constant | `PERMISSION_CHECK`, `USER_MAPPING` in `crates/lakehouse-engine/src/adapter/permission.rs` | Moved to `properties.rs` |
| Constant | `PROP_MERGE_SCHEMA`, `PROP_HIVE_PARTITIONING` in `crates/lakehouse-engine/src/adapter/direct_storage_properties.rs` | Moved to `properties.rs` |
| Constant | `PROP_*` constants in `crates/lakehouse-engine/src/adapter/mod.rs` | Moved to `properties.rs`; the `NOTE_*` constants stay |
| Function | `resolve_df_fixed_count` and the string parsing in `resolve_parallelism_factor`, `resolve_threading_mode`, `resolve_s3_max_connections`, `resolve_df_batch_size`, `resolve_memory_pool_fraction`, `resolve_instance_overhead_mb`, `resolve_join_broadcast_max_bytes` (`adapter/mod.rs`) | The check returns typed values, so the create path parses no string |
| Test | Invalid-value branches of `create_vs_records_parallelism_factor`, `df_target_partitions_falls_back_when_absent_or_invalid`, `df_batch_size_uses_supplied_value`, `df_threads_per_udf_falls_back_when_absent_or_invalid`, `resolve_memory_pool_fraction_defaults_and_validates`, `resolve_instance_overhead_mb_defaults_and_validates`, `resolve_join_broadcast_max_bytes_defaults_and_validates`, `threading_mode_defaults_to_auto`, `resolve_s3_max_connections_auto_scales_with_threads` (`adapter_tests.rs`) | They assert the default replaces an invalid value; `properties_tests.rs` asserts the rejection |
| Test | `create_virtual_schema_rejects_old_namespace_alias_without_replacement` (`adapter_tests.rs:170`) | It names the retired `ICEBERG_NAMESPACE`; the unknown-name rule covers it |
| Test | `CLUSTER_NODES` entry and assertion in `build_adapter_notes_merges_existing` (`adapter_tests.rs:358`, `:367-371`) | It names the removed note key; the `OTHER_KEY` entry proves the merge |
| Test | `adapter_notes_omit_cluster_nodes` (`adapter_tests.rs:374-387`) | It names the removed note key; the exact-key-set assertion of task 2.4 proves the same behavior |
| Bench pattern | `CLUSTER_NODES` alternative in the log greps of `bench/sweep.sh:27` and `bench/parallelism_sweep.sh:29` | No engine or UDF code emits the removed note key, so the alternative matches nothing |
| E2E assertion | `NR_OF_CORES` and `CLUSTER_NODES` absence checks in `create_vs_omits_cluster_nodes_from_adapter_notes` (`scan.rs:1032-1042`) | The exact set of note keys replaces them |
| E2E clause | `SCAN_SCHEMA` in `crates/lakehouse-e2e/tests/e2e_local/scan.rs:553` | The adapter does not read it, and the CREATE would fail |
| Scenario tag | `/// Scenario:` lines of `fixed_mode_uses_supplied_values` and `auto_mode_uses_every_core_with_twice_as_many_partitions` (`adapter_tests.rs:926`), and the "AUTO derivation sizes the per-instance budget" line of `resolve_s3_max_connections_auto_scales_with_threads` | E2E tests prove the changed scenarios (`specs/testing.md` § Coverage rule) |
| Doc row | `LAKEHOUSE_UDF_DEBUG_LEVEL` in the property table of `docs/tuning.md` | It is a bench variable, not a property |

## Verification

### Scenario Coverage

All E2E tests below live in `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` unless the location says otherwise.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| An unknown property name fails the statement | Integration | `property_validation.rs` | `create_rejects_unknown_property_names_before_reading_the_connection` |
| The error suggests the nearest known name for a close misspelling | Integration | `property_validation.rs` | `create_suggests_the_nearest_known_property_name` |
| Every property the adapter reads is known under every catalog kind | Integration | `property_validation.rs` | `create_accepts_every_known_property_name` (all names except `CATALOG_KIND` and `PERMISSION_CHECK`, which the existing direct-storage and Lakekeeper suites set) |
| An invalid tuning value fails the statement and names the accepted values | Integration | `property_validation.rs` | `create_rejects_each_invalid_tuning_value` (includes the 150-character quoted value) |
| A whole-number tuning value is checked against the range of its type | Integration | `property_validation.rs` | `create_rejects_each_invalid_tuning_value` (maximum plus 1 and a 21-digit value per property, `S3_MAX_CONNECTIONS = 'AUTO'` and `'0'`) |
| A tuning value is checked on its own and never replaced by its default | Integration | `property_validation.rs` | `create_rejects_each_invalid_tuning_value` (sets `DATAFUSION_TARGET_PARTITIONS = '0'` without `DATAFUSION_THREADING_MODE`) |
| One error reports every unknown name and every invalid value | Integration | `property_validation.rs` | `create_reports_every_property_problem_in_one_error` |
| A misspelled required property reports the misspelling | Integration | `property_validation.rs` | `misspelled_required_property_reports_the_misspelling` (also asserts that `CATALOG_KIND = 'NOPE'` yields the catalog-kind error without the `invalid virtual schema properties:` prefix) |
| An empty or null property value counts as absent | Integration | `property_validation.rs` | `empty_and_null_property_values_count_as_absent` (CREATE, SET, and REFRESH, per task 1.2) |
| Set properties validates the merged properties after null unsets | Integration | `property_validation.rs` | `set_properties_rejects_problems_and_keeps_the_schema` |
| A schema persisted with an invalid property refuses refresh until the property is unset | Integration | `property_validation.rs` | `stray_persisted_properties_block_refresh_but_not_queries` |
| Pushdown and drop never validate properties | Integration | `property_validation.rs` | `stray_persisted_properties_block_refresh_but_not_queries` |
| Adapter records the parallelism factor in the virtual-schema adapterNotes | Integration | `property_validation.rs` | `create_accepts_every_known_property_name`, `create_without_tuning_properties_records_the_defaults`, `create_rejects_each_invalid_tuning_value` |
| Adapter records the DataFusion threading mode in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| Adapter records the DataFusion target partition count in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| Adapter records the memory-pool fraction in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes | Integration | `property_validation.rs` | same three tests |
| createVirtualSchema adapterNotes omit the cluster node count | Integration | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes` (exact set of note keys after CREATE, REFRESH, and SET); the kept inherited entry is proven by the tests of `vs-adapter/refresh-and-set-properties` "Refresh rebuilds every identifier note and preserves other adapter notes" |
| Adapter derives the per-node core count from available_parallelism() on every request | Integration | `crates/lakehouse-e2e/tests/e2e_local/scan.rs`, `property_validation.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes` (no core-count key), `create_without_tuning_properties_records_the_defaults` (threads derived with no property set) |
| AUTO mode gives the node's single scan instance every core | Integration | `property_validation.rs` | `create_without_tuning_properties_records_the_defaults` (asserts `DF_TARGET_PARTITIONS` = 2 × `DF_THREADS_PER_UDF`), `create_rejects_each_invalid_tuning_value` |
| FIXED mode uses the operator-supplied thread and partition values verbatim | Integration | `property_validation.rs` | `create_accepts_every_known_property_name`, `create_rejects_each_invalid_tuning_value` |
| Threading mode defaults to AUTO when the property is absent | Integration | `property_validation.rs` | `create_without_tuning_properties_records_the_defaults` (asserts `DF_THREADING_MODE` = `AUTO`), `create_rejects_each_invalid_tuning_value` |
| FIXED value overrides the AUTO derivation at createVirtualSchema | Integration | `property_validation.rs` | `create_accepts_every_known_property_name` (asserts the `S3_MAX_CONNECTIONS` note equals the supplied value) |
| AUTO derivation sizes the per-instance budget from the resolved thread count | Integration | `property_validation.rs` | `create_without_tuning_properties_records_the_defaults` (asserts `S3_MAX_CONNECTIONS` = 4 × `DF_THREADS_PER_UDF`), `create_rejects_each_invalid_tuning_value` |
| AUTO derivation yields the single-core budget when the core count cannot be detected | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolve_s3_max_connections_auto_scales_with_threads` (one-thread case; no Exasol request can force a core count of 1) |
| An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted | Integration | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `malformed_connections_are_rejected_at_create_virtual_schema` |
| Scan-driving UDF invocations are schema-qualified from the running adapter script's schema | Integration | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_added_columns_initial_default_fill_all_types` (creates its schema without `SCAN_SCHEMA`) |
| Pushdown reads the cluster node count from the UDF handshake | Integration | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `pushdown_shards_from_handshake_node_count_without_note` (existing, tagged by task 2.4) |
| Enabling telemetry emits the three phase timings for a completed scan | Scan UDF test (`TestContext`) | `crates/lakehouse-engine/tests/it/scan_telemetry.rs` | `telemetry_reports_three_phases_when_enabled` (existing; the delta changes wording only) |
| Suggestion rule, type ranges, echo cut and quoting, message format (logic, no scenario tag) | Unit | `crates/lakehouse-engine/src/adapter/properties_tests.rs` | tests of task 3.1 |
| Check order in dispatch (logic, no scenario tag) | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | tests of task 4.1 |

### Manual Testing

`DSN` is `exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0`, as in the Makefile. `<adapter>` is the adapter script the local E2E harness installs, `<scan script>` its `LAKEHOUSE_SCAN` script, and `<conn>`, `<ns>`, and `<table>` its CONNECTION, namespace, and a seeded table.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/property-validation | `printf '%s' "CREATE VIRTUAL SCHEMA TYPO USING <adapter> WITH CATALOG_CONNECTION = '<conn>' NAMESPCE = '<ns>' PARALLELISM_FACTR = '64' DATAFUSION_BATCH_SIZE = 'abc' MADE_UP_PROPERTY = 'x'" \| exapump sql -d "$DSN"` | Fails with `invalid virtual schema properties: unknown property 'MADE_UP_PROPERTY'; unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?); unknown property 'PARALLELISM_FACTR' (did you mean 'PARALLELISM_FACTOR'?); invalid value 'abc' for 'DATAFUSION_BATCH_SIZE': expected a whole number from 1 to 4294967295` |
| vs-adapter/property-validation | `printf '%s' "CREATE VIRTUAL SCHEMA RANGE_VS USING <adapter> WITH CATALOG_CONNECTION = '<conn>' NAMESPACE = '<ns>' ALLOW_HTTP = 'true' DATAFUSION_THREADS_PER_UDF = '100000' S3_MAX_CONNECTIONS = 'AUTO'" \| exapump sql -d "$DSN"` | Fails with `invalid virtual schema properties: invalid value '100000' for 'DATAFUSION_THREADS_PER_UDF': expected a whole number from 1 to 65535; invalid value 'AUTO' for 'S3_MAX_CONNECTIONS': expected a whole number from 1 to 65535` |
| vs-adapter/property-validation | Create `OK_VS` with valid properties, then `printf '%s' "ALTER VIRTUAL SCHEMA OK_VS SET MEMORY_POOL_FRACTION = '60'" \| exapump sql -d "$DSN"`, then `SELECT PROPERTY_NAME, PROPERTY_VALUE FROM SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES WHERE SCHEMA_NAME = 'OK_VS'` | The ALTER fails with `invalid value '60' for 'MEMORY_POOL_FRACTION': expected a number greater than 0 and at most 1`; the properties are unchanged |
| vs-adapter/create-virtual-schema-adapter-notes-resources, vs-adapter/create-virtual-schema-adapter-notes, scan-runtime/scan-execution-threading, scan-runtime/scan-execution-connection-concurrency | `printf '%s' "CREATE VIRTUAL SCHEMA DEF_VS USING <adapter> WITH CATALOG_CONNECTION = '<conn>' NAMESPACE = '<ns>' ALLOW_HTTP = 'true'" \| exapump sql -d "$DSN"`, then `SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'DEF_VS'` | Succeeds; the notes hold exactly `PARALLELISM_FACTOR` `1`, `DF_THREADING_MODE` `AUTO`, `DF_TARGET_PARTITIONS` equal to 2 × `DF_THREADS_PER_UDF`, `DF_THREADS_PER_UDF`, `DF_BATCH_SIZE` `8192`, `MEMORY_POOL_FRACTION` `0.6`, `INSTANCE_OVERHEAD_MB` `200`, `S3_MAX_CONNECTIONS` equal to 4 × `DF_THREADS_PER_UDF`, `JOIN_BROADCAST_MAX_BYTES` `134217728`, and `SKIPPED_TABLES` |
| pushdown/pushdown-planning | `printf '%s' "SELECT COUNT(*) FROM DEF_VS.<table>" \| exapump sql -d "$DSN"` in a new session that has opened no schema | Returns the table's row count |
| scan-runtime/scan-execution-telemetry | Recreate `<scan script>` with the line `%udf_debug_level debug` in its source, run `nc -l -p <port>` on a host the container reaches, then `printf '%s' "ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<host>:<port>'; SELECT COUNT(*) FROM DEF_VS.<table>" \| exapump sql -d "$DSN"` | The listener receives one phase-timing line per scan invocation, with the startup, object-storage import, and send-back durations |
| direct-storage/direct-storage-properties | A `CATALOG_KIND = 'DIRECT_STORAGE'` CREATE with `MERGE_SCHEMA = 'maybe'` | Fails with the existing `'MERGE_SCHEMA' value 'maybe' is neither a true nor a false spelling ...` error and no `invalid virtual schema properties:` prefix |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e`, `make test-e2e-lakekeeper`, `make test-e2e-unity` | 0 failures |
| Lint | `make lint` | 0 errors or warnings |
| Format | `make fmt-check` | No changes |
