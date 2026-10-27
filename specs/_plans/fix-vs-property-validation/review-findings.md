# Code Review Findings: fix-vs-property-validation

## Summary
- Files reviewed: 22
- Total findings: 12 (standard: 11, expert: 1)
- Baseline: `cargo test -p lakehouse-engine --lib adapter::` passes (1100 passed, 0 failed), `make fmt-check` reports no changes, and `make lint` reports no warnings.

## Standard fixes

### crates/lakehouse-engine/src/adapter/properties.rs

#### [COMMAND_QUERY_MIX] TuningChecker returns values and records rejections in one call
- Location: lines 93-109 (`check_properties`), lines 179-214 (`TuningChecker::tuning`, `TuningChecker::value`)
- Issue: `TuningChecker::tuning` returns the parsed values and also records each rejected value in `self.invalid`. `check_properties` must call `tuning()` before it reads `checker.invalid` (lines 95-97), and nothing enforces that order. Line 180 calls `self.value(PROP_ALLOW_HTTP, check_allow_http)` only for its side effect and discards the returned value.
- Fix: In crates/lakehouse-engine/src/adapter/properties.rs, add a private `fn check_tuning(props: &Json) -> Result<TuningProperties, Vec<String>>` that constructs the `TuningChecker`, calls `tuning()` once, and returns `Err(checker.invalid.into_values().collect())` when any value was rejected, otherwise `Ok(tuning)`. Rewrite `check_properties` to call `unknown_names` and `check_tuning`, and to build the error from the unknown-name entries followed by the `Err` entries of `check_tuning`, so `check_properties` no longer reads `TuningChecker::invalid`. Keep the message text and entry order unchanged, and run `cargo test -p lakehouse-engine --lib adapter::properties`.

#### [INFORMATION_COUPLING] The adapterNotes encoding of the threading mode lives in the property module
- Location: lines 67-74 (`impl ThreadingMode { fn as_note }`)
- Issue: `as_note` produces the `DF_THREADING_MODE` adapterNotes value. Its only caller is `build_adapter_notes` in crates/lakehouse-engine/src/adapter/mod.rs:551. The plan's architecture delta assigns the notes format to vs-adapter (`adapter/mod.rs`), and decision-log entry [1] states that the property module parses properties only, never adapterNotes. A change to the note encoding now needs an edit in properties.rs.
- Fix: Move the `impl ThreadingMode { fn as_note(self) -> &'static str }` block from crates/lakehouse-engine/src/adapter/properties.rs into crates/lakehouse-engine/src/adapter/mod.rs, next to `build_adapter_notes`, as a private inherent impl without `pub(super)`. Keep the `ThreadingMode` enum and `parse_threading_mode` in properties.rs.

#### [INFORMATION_COUPLING] The TRUE/FALSE spelling rule is implemented twice
- Location: properties.rs lines 281-293 (`parse_allow_http`, `check_allow_http`) and line 111 (doc of `resolve_allow_http`); crates/lakehouse-engine/src/adapter/direct_storage_properties.rs lines 78-88 (`parse_bool_property`); crates/lakehouse-engine/src/adapter/properties_tests.rs line 269
- Issue: The case-insensitive `TRUE`/`FALSE` rule exists in `parse_allow_http` for `ALLOW_HTTP` and again in `parse_bool_property` for `MERGE_SCHEMA` and `HIVE_PARTITIONING`. docs/tuning.md documents one rule, "`TRUE` or `FALSE`, any letter case", for all three properties. A change to the accepted spellings needs an edit in both modules. The `resolve_allow_http` doc and the test `resolve_allow_http_reads_the_catalog_rule_and_treats_a_rejected_value_as_false` call it "the catalog's rule", although "catalog" in the adapter names the lakehouse catalog (`CATALOG_KIND`, `CATALOG_CONNECTION`).
- Fix: In crates/lakehouse-engine/src/adapter/properties.rs, rename `parse_allow_http` to `parse_boolean`, make it `pub(super)`, and update its callers `resolve_allow_http` and `check_allow_http`. In crates/lakehouse-engine/src/adapter/direct_storage_properties.rs, replace the two `eq_ignore_ascii_case` arms of `parse_bool_property` with a call to `crate::adapter::properties::parse_boolean(value)`, and keep the existing error text for a `None` result unchanged. Change the first sentence of the `resolve_allow_http` doc to "Reads `ALLOW_HTTP` with the TRUE/FALSE rule that `check_properties` applies." In crates/lakehouse-engine/src/adapter/properties_tests.rs, rename the test to `resolve_allow_http_accepts_true_or_false_and_treats_a_rejected_value_as_false`.

### crates/lakehouse-engine/src/adapter/properties_tests.rs

#### [MISSING_BOUNDARY_TEST] No test reaches the empty or null branch of the check
- Location: no test; the branches are `unknown_names` at properties.rs:125 and `TuningChecker::value` at properties.rs:201
- Issue: `check_properties` treats an empty or `null` value as absent in two places: the `nonempty_str` filter of `unknown_names` and the `nonempty_str(...)?` of `TuningChecker::value`. No test at any layer reaches either branch. The E2E test `empty_and_null_property_values_count_as_absent` does not reach them, because Exasol rejects an empty or `null` value in `CREATE` and `merge_set_properties` removes each `null` key before the check. This change deleted the adapter tests that asserted an empty tuning value takes its default. Replacing `nonempty_str` with a plain string read in properties.rs fails no test.
- Fix: In crates/lakehouse-engine/src/adapter/properties_tests.rs, add the test `an_empty_or_null_value_counts_as_absent` without a scenario tag. It calls `check_properties` on `json!({"MADE_UP_PROPERTY": "", "NAMESPCE": null, "MEMORY_POOL_FRACTION": "", "DATAFUSION_BATCH_SIZE": null})` and asserts the result equals `Ok(TuningProperties::default())`.

#### [MISSING_BOUNDARY_TEST] Nothing proves that only the 17 names are known
- Location: `every_known_name_passes`, lines 60-84
- Issue: The scenario "Every property the adapter reads is known under every catalog kind" requires the adapter to accept exactly 17 names. `every_known_name_passes` proves that each of the 17 names is accepted. Its list assertion on line 81 compares the fixture with `ALL_NAMES`, and both are constants of the test file. An 18th entry in `PROPERTY_NAMES` fails no test.
- Fix: In `every_known_name_passes` in crates/lakehouse-engine/src/adapter/properties_tests.rs, add an assertion that `PROPERTY_NAMES` and `ALL_NAMES` hold the same names. Compare them as `BTreeSet<&str>` so the order does not matter, with the message "the adapter must accept exactly the 17 names of vs-adapter/property-validation".

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [DUPLICATE_TEST] Five tuning tests repeat assertions of other tests
- Location: `explicit_parallelism_factor_overrides_default` (line 630), `df_target_partitions_falls_back_when_absent` (line 640), `df_threads_per_udf_falls_back_when_absent` (line 705), `df_target_partitions_explicit_wins` (line 840), `df_threads_per_udf_explicit_wins` (line 850)
- Issue: With `resolve_df_fixed_count` removed, these tests call the same function with the same input shape as another test. The two `*_falls_back_when_absent` tests repeat the second assertion of `fixed_mode_uses_supplied_values`: FIXED mode with absent counts returns the AUTO values. The two `*_explicit_wins` tests repeat the first assertion of `df_target_partitions_uses_supplied_value` and `df_threads_per_udf_uses_supplied_value`: FIXED mode returns the supplied count. `explicit_parallelism_factor_overrides_default` repeats the supplied-factor assertion of `create_vs_records_parallelism_factor`. specs/testing.md allows deleting a test when another test keeps its coverage.
- Fix: Delete these five tests from crates/lakehouse-engine/src/adapter/adapter_tests.rs. Keep `default_parallelism_factor_is_one`, because it alone pins the default to the literal `1`. Run `cargo test -p lakehouse-engine --lib adapter::tests`.

### crates/lakehouse-e2e/tests/e2e_local/property_validation.rs

#### [SHRINKABLE] Five call sites rebuild the harness's query_error
- Location: lines 73, 491, 529, 568, 635
- Issue: Each line spells `error_text(&conn.try_execute(&sql), &sql)`. The harness function `query_error(conn, sql)` in crates/lakehouse-e2e/tests/common/e2e_harness.rs:844 has the same body, and refresh.rs already calls it six times.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/property_validation.rs, replace the five expressions with `query_error(conn, &sql)` inside `create_error`, `query_error(&mut conn, &sql)` at lines 491, 529, and 568, and `query_error(&mut conn, &refresh)` at line 635. Remove `error_text` from the `use crate::common::exasol_ws::{...}` import when it becomes unused.

#### [INFORMATION_COUPLING] Two new tests parse the schema-level adapterNotes themselves
- Location: property_validation.rs lines 88-103 (`notes`); crates/lakehouse-e2e/tests/e2e_local/scan.rs lines 986-1009 (`assert_exact_keys` closure in `create_vs_omits_cluster_nodes_from_adapter_notes`)
- Issue: Both tests query `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` and parse its JSON-string encoding inline. The harness doc of `skipped_entries` (e2e_harness.rs:724-725) states the convention "so no test parses the schema-level notes itself". After this change, the harness, scan.rs (twice, with the CPU-probe test at lines 1056-1071), and property_validation.rs each parse the encoding, so a change to it needs an edit in every one.
- Fix: In crates/lakehouse-e2e/tests/common/e2e_harness.rs, add `pub fn schema_notes(conn: &mut ExaConn, vs_name: &str) -> serde_json::Map<String, serde_json::Value>`. It runs the `SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS` query and panics with the schema name when the notes are absent, not JSON, or not a JSON object. Rewrite `skipped_entries` to read through it. In property_validation.rs, rewrite `notes` to build its `BTreeMap<String, String>` from `schema_notes`. In scan.rs, make `assert_exact_keys` take its keys from `schema_notes(conn, NOTES_VS)`, and make the CPU-probe test read `DF_THREADS_PER_UDF` through `schema_notes`.

### crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs

#### [UNTESTED_ERROR_PATH] The newly tagged test never checks the MERGE_SCHEMA error content
- Location: `malformed_connections_are_rejected_at_create_virtual_schema`, scenario tag at line 1652, assertions at lines 1755-1759
- Issue: This change tags the test with "An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted". The scenario requires the error to name the property, the rejected value, and the accepted spellings. The test asserts only that the CREATE fails and that the message lacks `invalid virtual schema properties:`, so a failure for any other reason also passes. No E2E test creates a schema with an unparseable `HIVE_PARTITIONING` value.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs, after the `MERGE_SCHEMA` `let msg = error_text(...)`, add `assert_mentions(&msg, &["'MERGE_SCHEMA' value 'NOTABOOL'", "'TRUE' and 'FALSE'"]);`. Then run the same `try_create_virtual_schema_with_password` call with `direct_vs("REJECTS_HIVE_PARTITIONING", "DIRECT_STORAGE_BAD_HIVE_PARTITIONING_CREDS").with_hive_partitioning("NOTABOOL")`. Assert that its error text mentions `'HIVE_PARTITIONING' value 'NOTABOOL'` and `'TRUE' and 'FALSE'`, and does not contain `invalid virtual schema properties:`.

### docs/tuning.md

#### [OUTDATED_COMMENT] The page says an empty or null value counts as absent in every statement
- Location: line 11
- Issue: "an empty or `null` value counts as absent" does not hold for `CREATE VIRTUAL SCHEMA`. Exasol rejects an empty or `null` value there with `Value of property <NAME> must not be null or empty.`. The `vs-adapter/property-validation` Background states this, and `empty_and_null_property_values_count_as_absent` asserts it. Only `ALTER VIRTUAL SCHEMA ... SET <name> = ''` or `= null` reaches the adapter, as a JSON `null` that removes the property.
- Fix: In docs/tuning.md line 11, replace "Names are case-sensitive, and an empty or `null` value counts as absent." with "Names are case-sensitive. `CREATE VIRTUAL SCHEMA` rejects an empty or `null` value. `ALTER VIRTUAL SCHEMA ... SET <name> = ''` or `SET <name> = null` removes the property, which then takes its default."

#### [OUTDATED_COMMENT] The NAMESPACE row omits the direct-storage form
- Location: line 17
- Issue: The row marks `NAMESPACE` as required and lists only the catalog namespace forms. Under `CATALOG_KIND = 'DIRECT_STORAGE'`, `handle_create_virtual_schema` skips the required check, and `parse_namespace` in direct_storage_properties.rs accepts a path relative to the CONNECTION address with no URI scheme and no leading `/`. docs/catalogs.md:583 documents that form. Task 5.1 requires each row to name the accepted values.
- Fix: In the `NAMESPACE` row of docs/tuning.md, set the Required cell to "yes, except under `DIRECT_STORAGE`". Append "or, under `DIRECT_STORAGE`, a path relative to the CONNECTION address with no URI scheme and no leading `/`" to the Accepted values cell. Append "See [Direct storage](catalogs.md#direct-storage-raw-parquet-no-catalog)." to the Effect cell.

## Expert fixes

### crates/lakehouse-engine/src/adapter/mod.rs

#### [SHALLOW_MODULE] Eight one-line resolve functions pass the tuning values through three representations
- Location: lines 202-211 and the `TuningNotes { ... }` literal before line 245 in `handle_create_virtual_schema`; lines 585-653 (`resolve_parallelism_factor`, `resolve_threading_mode`, `resolve_df_threading`, `resolve_s3_max_connections`, `resolve_df_batch_size`, `resolve_memory_pool_fraction`, `resolve_instance_overhead_mb`, `resolve_join_broadcast_max_bytes`)
- Issue: After this change, six of the eight `resolve_*` functions are one `map_or` or `unwrap_or` that applies a default to one `TuningProperties` field. `handle_create_virtual_schema` calls each once, holds nine locals, and copies them into a `TuningNotes` literal. The nine tuning values pass through `TuningProperties`, the locals, and `TuningNotes` inside a function that also reads the CONNECTION, lists the catalog, and builds the response. Each one-liner also has its own unit test in adapter_tests.rs, about 30 call sites in total.
- Fix: Apply this after the adapter_tests.rs `[DUPLICATE_TEST]` fix. In crates/lakehouse-engine/src/adapter/mod.rs, add `fn resolve_tuning_notes(checked: &TuningProperties, nr_of_cores: u32) -> TuningNotes` with a doc comment stating that it applies each absent property's default and the AUTO/FIXED derivation. Move the bodies of the eight functions into it and delete the eight functions. Keep `auto_df_threading` and `resolve_nr_of_cores`, which the pushdown path also calls. In `handle_create_virtual_schema`, replace the `nr_of_cores` line, the eight `resolve_*` lines, and the `TuningNotes { ... }` literal with `let tuning = resolve_tuning_notes(&checked, resolve_nr_of_cores());`. In crates/lakehouse-engine/src/adapter/adapter_tests.rs, rewrite each test that calls a deleted function to call `resolve_tuning_notes` and assert the matching `TuningNotes` field. Keep every asserted value: each default, FIXED counts used verbatim, AUTO ignoring supplied counts, absent FIXED counts falling back to the AUTO values, an explicit `S3_MAX_CONNECTIONS` winning, and the derived budget of 4 × threads. Keep the test name `resolve_s3_max_connections_auto_scales_with_threads` and its `/// Scenario:` tag, and drive its one-thread case with `nr_of_cores = 1`. Run `cargo test -p lakehouse-engine --lib adapter::` and `make fmt-check lint`.
