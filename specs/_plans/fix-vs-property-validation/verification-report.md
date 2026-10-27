# Verification Report: fix-vs-property-validation

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | createVirtualSchema, refresh, and setProperties reject unknown property names and invalid tuning values with one error. Every check passes against the local Docker Exasol. |
| Code review | 12 findings (11 standard, 1 expert), 12 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit and scan UDF (`cargo test`) | 2025 | 2023 | 2 (existing micro-benchmarks) |
| E2E local (`make test-e2e`) | 319 | 319 | 0 |

### Manual Tests

All statements ran against the new `.so` on the local Docker Exasol, with adapter `LHVS.LAKEHOUSE_ADAPTER`.

| Test | Result |
|------|--------|
| `CREATE VIRTUAL SCHEMA TYPO` with `NAMESPCE`, `PARALLELISM_FACTR`, `DATAFUSION_BATCH_SIZE = 'abc'`, and `MADE_UP_PROPERTY`: the statement fails with the expected message, word for word | ✓ |
| `CREATE VIRTUAL SCHEMA RANGE_VS` with `DATAFUSION_THREADS_PER_UDF = '100000'` and `S3_MAX_CONNECTIONS = 'AUTO'`: the statement fails and names both ranges `1 to 65535` | ✓ |
| `ALTER VIRTUAL SCHEMA OK_VS SET MEMORY_POOL_FRACTION = '60'`: the statement fails with `expected a number greater than 0 and at most 1`, and the properties stay `ALLOW_HTTP`, `CATALOG_CONNECTION`, `NAMESPACE` | ✓ |
| `CREATE VIRTUAL SCHEMA DEF_VS` without tuning properties: the notes hold exactly `PARALLELISM_FACTOR` 1, `DF_THREADING_MODE` AUTO, `DF_THREADS_PER_UDF` 4, `DF_TARGET_PARTITIONS` 8, `DF_BATCH_SIZE` 8192, `MEMORY_POOL_FRACTION` 0.6, `INSTANCE_OVERHEAD_MB` 200, `S3_MAX_CONNECTIONS` 16, `JOIN_BROADCAST_MAX_BYTES` 134217728, and `SKIPPED_TABLES` | ✓ |
| `SELECT COUNT(*) FROM DEF_VS.EVENTS` in a new session: the query returns 20 | ✓ |
| `CATALOG_KIND = 'DIRECT_STORAGE'` with `MERGE_SCHEMA = 'maybe'`: the statement fails with the existing `neither a true nor a false spelling` error and no `invalid virtual schema properties:` prefix | ✓ |
| Telemetry over `SCRIPT_OUTPUT_ADDRESS`: not run by hand. The delta changes wording only, and `telemetry_reports_three_phases_when_enabled` covers the behavior | n/a |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

### Formatter

```
cargo fmt --all -- --check  (no changes)
```

## Scenario Coverage

All E2E tests passed in the `make test-e2e` run. `property_validation.rs` carries 24 `/// Scenario:` tags.

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | property-validation | An unknown property name fails the statement | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_rejects_unknown_property_names_before_reading_the_connection` | Pass |
| vs-adapter | property-validation | The error suggests the nearest known name for a close misspelling | `property_validation.rs` | `create_suggests_the_nearest_known_property_name` | Pass |
| vs-adapter | property-validation | Every property the adapter reads is known under every catalog kind | `property_validation.rs` | `create_accepts_every_known_property_name` | Pass |
| vs-adapter | property-validation | An invalid tuning value fails the statement and names the accepted values | `property_validation.rs` | `create_rejects_each_invalid_tuning_value` | Pass |
| vs-adapter | property-validation | A whole-number tuning value is checked against the range of its type | `property_validation.rs` | `create_rejects_each_invalid_tuning_value` | Pass |
| vs-adapter | property-validation | A tuning value is checked on its own and never replaced by its default | `property_validation.rs` | `create_rejects_each_invalid_tuning_value` | Pass |
| vs-adapter | property-validation | One error reports every unknown name and every invalid value | `property_validation.rs` | `create_reports_every_property_problem_in_one_error` | Pass |
| vs-adapter | property-validation | A misspelled required property reports the misspelling | `property_validation.rs` | `misspelled_required_property_reports_the_misspelling` | Pass |
| vs-adapter | property-validation | An empty or null property value counts as absent | `property_validation.rs` | `empty_and_null_property_values_count_as_absent` | Pass |
| vs-adapter | property-validation | Set properties validates the merged properties after null unsets | `property_validation.rs` | `set_properties_rejects_problems_and_keeps_the_schema` | Pass |
| vs-adapter | property-validation | A schema persisted with an invalid property refuses refresh until the property is unset | `property_validation.rs` | `stray_persisted_properties_block_refresh_but_not_queries` | Pass |
| vs-adapter | property-validation | Pushdown and drop never validate properties | `property_validation.rs` | `stray_persisted_properties_block_refresh_but_not_queries` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes-resources | Adapter records the parallelism factor, threading mode, target partition count, threads-per-UDF count, memory-pool fraction, and instance-overhead megabytes in the virtual-schema adapterNotes (6 scenarios) | `property_validation.rs` | `create_accepts_every_known_property_name`, `create_without_tuning_properties_records_the_defaults`, `create_rejects_each_invalid_tuning_value` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes | createVirtualSchema adapterNotes omit the cluster node count | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes` | Pass |
| vs-adapter | create-virtual-schema-adapter-notes | Adapter derives the per-node core count from available_parallelism() on every request | `scan.rs`, `property_validation.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes`, `create_without_tuning_properties_records_the_defaults` | Pass |
| scan-runtime | scan-execution-threading | AUTO mode gives the node's single scan instance every core | `property_validation.rs` | `create_without_tuning_properties_records_the_defaults`, `create_rejects_each_invalid_tuning_value` | Pass |
| scan-runtime | scan-execution-threading | FIXED mode uses the operator-supplied thread and partition values verbatim | `property_validation.rs` | `create_accepts_every_known_property_name`, `create_rejects_each_invalid_tuning_value` | Pass |
| scan-runtime | scan-execution-threading | Threading mode defaults to AUTO when the property is absent | `property_validation.rs` | `create_without_tuning_properties_records_the_defaults`, `create_rejects_each_invalid_tuning_value` | Pass |
| scan-runtime | scan-execution-connection-concurrency | FIXED value overrides the AUTO derivation at createVirtualSchema | `property_validation.rs` | `create_accepts_every_known_property_name` | Pass |
| scan-runtime | scan-execution-connection-concurrency | AUTO derivation sizes the per-instance budget from the resolved thread count | `property_validation.rs` | `create_without_tuning_properties_records_the_defaults`, `create_rejects_each_invalid_tuning_value` | Pass |
| scan-runtime | scan-execution-connection-concurrency | AUTO derivation yields the single-core budget when the core count cannot be detected | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolve_s3_max_connections_auto_scales_with_threads` | Pass |
| direct-storage | direct-storage-properties | An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `malformed_connections_are_rejected_at_create_virtual_schema` | Pass |
| pushdown | pushdown-planning | Scan-driving UDF invocations are schema-qualified from the running adapter script's schema | `scan.rs` | `e2e_added_columns_initial_default_fill_all_types` | Pass |
| pushdown | pushdown-planning | Pushdown reads the cluster node count from the UDF handshake | `scan.rs` | `pushdown_shards_from_handshake_node_count_without_note` | Pass |
| scan-runtime | scan-execution-telemetry | Enabling telemetry emits the three phase timings for a completed scan | `crates/lakehouse-engine/tests/it/scan_telemetry.rs` | `telemetry_reports_three_phases_when_enabled` | Pass |

## Notes

- Only the local E2E suite ran. The user skipped `make test-e2e-lakekeeper`, `make test-e2e-unity`, `make test-e2e-glue`, and `make test-e2e-azure`, which task 6.2 lists. The local suite covers the Iceberg REST and direct-storage kinds, and `create_accepts_every_known_property_name` covers the full name list.
- Task 1.2 found that Exasol rejects an empty or `null` value in `CREATE` before it calls the adapter, with `Value of property <NAME> must not be null or empty.` `ALTER ... SET <name> = ''` and `= null` reach the adapter as JSON `null`. The `vs-adapter/property-validation` delta limits the scenario "An empty or null property value counts as absent" to that setProperties case.
- Exasol upper-cases unquoted property names and rejects quoted ones, so the `"Namespace"` suggestion example is reachable only through the unit tests.
- No coverage percentage was measured.
