# Code Review Findings: add-unity-parquet-table-routing

## Summary
- Files reviewed: 35
- Total findings: 17 (standard: 15, expert: 2)

## Standard fixes

### crates/lakehouse-catalog/src/unity/client.rs

#### [REDUNDANT_COMMENT] Inline comment restates the `Ok(format)` match arm
- Location: lines 218-220, inside `list_tables`
- Issue: The three-line `//` comment above `Ok(format) => tables.push(neutral_table(ident, info, format))` says the admitted format is the outcome of the admission filter. The `match admission(..)` on the same statement already shows that. The comment also says the filter is "above", but the call is on the same `match` line. The user asked for no comments except a non-obvious why.
- Fix: In `crates/lakehouse-catalog/src/unity/client.rs`, `list_tables`, delete the three `//` comment lines that start with "An admitted entry's format is the outcome of the admission". Leave the match arm unchanged.

### crates/lakehouse-catalog/src/unity/client_tests.rs

#### [OUTDATED_COMMENT] Test doc and message say the admission filter is unchanged
- Location: the doc comment above `list_tables_tags_each_admitted_table_by_its_own_format` (lines 429-433), and the assertion message at line 499
- Issue: The doc says "while the admission filter itself is unchanged". This change widened the filter to admit `PARQUET`, which the same test asserts (`raw_orders` tagged `TableFormat::Parquet`). The assertion message at line 499 says "the admission filter is unchanged".
- Fix: In `crates/lakehouse-catalog/src/unity/client_tests.rs`, replace the five-line doc comment of `list_tables_tags_each_admitted_table_by_its_own_format` with the single line `/// Scenario: The client lists tables in a configured catalog and schema`. Change the assertion message at line 499 to `"an ICEBERG base table is skipped, not tagged"`.

#### [MISSING_BOUNDARY_TEST] Nothing tests that `partition_index` order can differ from column order
- Location: `list_tables_admits_a_parquet_base_table_with_its_partition_columns` (line 506); `neutral_table` in `unity/client.rs`
- Issue: `neutral_table` sorts partition columns by `partition_index` (`partition_columns.sort_by_key(..)`). Every fixture that reaches `neutral_table` declares its partition columns in `partition_index` order already: this test (`event_date` 0, then `region` 1), `single_table_body`, and the E2E registration. If someone deletes the sort, every test still passes.
- Fix: In `crates/lakehouse-catalog/src/unity/client_tests.rs`, `list_tables_admits_a_parquet_base_table_with_its_partition_columns`, reorder the `columns` JSON array to `region` (`partition_index` 1), then `payload`, then `event_date` (`partition_index` 0). Keep the asserted `vec!["event_date".to_string(), "region".to_string()]`, so the test now proves the sort.

#### [REDUNDANT_COMMENT] New test doc comments restate the test names across several lines
- Location: the doc comments above `list_tables_admits_a_parquet_base_table_with_its_partition_columns` (lines 503-505), `load_table_returns_format_tag_vending_key_partition_columns_and_ordered_columns` (lines 537-540), and `load_table_maps_the_uppercase_parquet_format_to_the_parquet_tag` (lines 585-588)
- Issue: Each doc says in prose what the test name already says. The project rule (CLAUDE.md) requires a one-line `/// Scenario: <fact>` doc on a test.
- Fix: In `crates/lakehouse-catalog/src/unity/client_tests.rs`, replace these three doc comments with one line each, as follows. For `list_tables_admits_a_parquet_base_table_with_its_partition_columns`, use `/// Scenario: The client admits a Parquet base table and reports its partition columns`. For `load_table_returns_format_tag_vending_key_partition_columns_and_ordered_columns`, use `/// Scenario: The client retrieves a table's metadata including its columns`. For `load_table_maps_the_uppercase_parquet_format_to_the_parquet_tag`, use `/// Scenario: The single-table load refuses a data source format the crate cannot name`.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs

#### [MISSING_DOC_COMMENT] The shared whole-table guard lost its doc comment when it became `pub(super)`
- Location: `ensure_table_has_a_mappable_column`, line 132
- Issue: This change removed the function's doc comment. The doc explained why an empty logical schema refuses the whole table: falling back to a data file's own schema would bind by physical order and name. The function is now `pub(super)`, and `unity_parquet_format_reader.rs` calls it too, so its reason is now part of a cross-module interface. The new `table_kind` parameter is not documented either.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs`, add a doc comment of at most three lines above `pub(super) fn ensure_table_has_a_mappable_column`. It must say that a table with no mappable column is refused as a whole, because `raw_scan` registers the logical schema as the table's schema and an empty one cannot be scanned, and that a table with at least one mappable column is left alone. It must also say that `table_kind` is the label that starts the refusal text.

### crates/lakehouse-engine/src/adapter/pushdown/format/unity_parquet_format_reader.rs

#### [TOO_MANY_ARGUMENTS] `plan` takes four arguments besides `self`
- Location: `UnityParquetFormatReader::plan`, line 54; its caller at line 90
- Issue: `plan(&self, table_root, storage, secrets, filter_json)` has four parameters. `secrets` is always `storage.secret_values()`, so it can be derived from `storage`.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/unity_parquet_format_reader.rs`, remove the `secrets: &[&str]` parameter from `UnityParquetFormatReader::plan`. Add `let secrets = storage.secret_values();` at the top of `plan`, and pass `&secrets` to `build_table_root_store`. At line 90 in `resolve_scan`, change the call to `.plan(table_root, &effective_storage, filter_json)`. Keep `let secrets = effective_storage.secret_values();` in `resolve_scan`, because the `redacted` call there still needs it.

### crates/lakehouse-engine/src/adapter/pushdown/format/unity_parquet_format_reader_tests.rs

#### [OUTDATED_COMMENT] `resolve_with` doc names a parameter the function does not take
- Location: lines 104-105
- Issue: The doc says the function resolves "against a store serving `keys` below the table root". `resolve_with` has no `keys` parameter. It takes an already-built `storage: &StorageBackend`.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/unity_parquet_format_reader_tests.rs`, replace the two-line doc of `resolve_with` with `/// Resolve \`table\`'s scan through the static credential against \`storage\`.`

#### [UNTESTED_ERROR_PATH] No test shows that a failed listing is redacted
- Location: `UnityParquetFormatReader::resolve_scan`, `.map_err(|error| redacted(error, &secrets))`; no counterpart test in this file
- Issue: The spec scenario "Storage is resolved through the table's own catalog exactly as for a Delta table" says every error the reader surfaces SHALL be redacted against the effective storage's secrets. The only credential assertion in this file (`vending_without_a_vending_key_errors_and_never_falls_back_to_static`) fails before any storage access, so no test covers the `redacted` step on `plan`'s result. The Delta reader has `a_failed_log_read_reports_no_static_credential_value`. This reader has no equivalent.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/unity_parquet_format_reader_tests.rs`, add a `#[tokio::test] async fn a_failed_listing_reports_no_static_credential_value()` with the doc `/// Scenario: Storage is resolved through the table's own catalog exactly as for a Delta table`. Build `StorageBackend::S3(StorageProps { endpoint: "http://127.0.0.1:1".into(), region: "us-east-1".into(), access_key: "AKIA-SENTINEL-ACCESS-0001".into(), secret_key: "sentinel-secret-value-0002".into(), allow_http: true, path_style: true, ..Default::default() })`, using two new file-level constants for the keys. Call `resolve_with(&id_table(), &storage, None).await.expect_err(..)` and convert the error with `user_message`. Assert that the message contains `"failed to list"` and `TABLE_PREFIX`, and contains neither sentinel value.

#### [UNTESTED_ERROR_PATH] Catalog columns that differ only in letter case have no test
- Location: `catalog_schema`, the `StructType::try_new(fields).map_err(..)` branch in `unity_parquet_format_reader.rs`
- Issue: `delta_kernel` `StructType::try_new` rejects field names that are equal ignoring case ("Duplicate field name (case-insensitive)"). The reader turns that into "the Unity Catalog columns of table {} do not form one Spark schema". No test reaches this branch.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/unity_parquet_format_reader_tests.rs`, add `#[tokio::test] async fn catalog_columns_equal_ignoring_letter_case_fail_the_plan()` with the doc `/// Scenario: The logical schema is the catalog's declared column list`. Build `sales_table(vec![catalog_column("id", json!("long")), catalog_column("ID", json!("long"))], &[])` and call `resolution_error(&table).await`. Assert that the message contains `TABLE_NAME` and `"do not form one Spark schema"`.

### crates/lakehouse-engine/src/adapter/unity_schema_tests.rs

#### [DEAD_FLEXIBILITY] `parquet_table_entry_typed` takes a `table_type` that its one caller always sets to `"EXTERNAL"`
- Location: `parquet_table_entry_typed`, line 212; its only call site in `lists_a_parquet_base_table_with_every_declared_column`
- Issue: The helper has one caller, which passes the literal `"EXTERNAL"`. The parameter is never varied, and the doc "A MANAGED or EXTERNAL Parquet base table list entry." describes flexibility that nothing uses.
- Fix: In `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs`, rename `parquet_table_entry_typed` to `parquet_table_entry`, remove its `table_type: &str` parameter, and hard-code `"table_type": "EXTERNAL"` in the JSON. Replace its doc with `/// An EXTERNAL Parquet base table list entry.` Update the call site in `lists_a_parquet_base_table_with_every_declared_column` to `parquet_table_entry("raw_events", vec![long_col("id"), string_col("region")])`.

### crates/lakehouse-engine/src/adapter/direct_storage_tests.rs

#### [OUTDATED_COMMENT] Assertion message says direct storage does not infer partition columns from the path
- Location: line 460, inside `hive_partitioning_reaches_the_seam_on_enumeration`
- Issue: The message reads "direct storage does not infer partition columns from the path". Under `HIVE_PARTITIONING = TRUE`, which this same loop exercises, direct storage does derive partition keys from `key=value` paths (`resolve_parquet_directory` → `declared_partition_keys`). What stays empty is `CatalogTable.partition_columns`, because this catalog kind declares none itself.
- Fix: In `crates/lakehouse-engine/src/adapter/direct_storage_tests.rs`, change the assertion message at line 460 to `"direct storage declares no catalog partition columns; its partition keys come from the listing, HIVE_PARTITIONING = {hive_partitioning}"`.

### crates/lakehouse-engine/src/scan/test_support_tests.rs

#### [INFORMATION_LEAKAGE] `column_binding_for` copies the raw scan's `FieldIdResolution` assembly
- Location: `column_binding_for`, line 45; production copy at `crates/lakehouse-engine/src/scan/raw_scan.rs:251`
- Issue: The helper says it builds the factory "the raw scan installs", but it rebuilds the `FieldIdResolution` field by field (`index_declared_physical_names`, `reconstruct_initial_defaults`, `index_nested_members`), just as `raw_scan.rs` does. Nothing makes the two stay the same. If the raw scan changes how it derives the resolution, the Unity Parquet reader tests that use this helper keep passing against a binding production no longer builds.
- Fix: In `crates/lakehouse-engine/src/scan/field_id_projection.rs`, add `impl FieldIdResolution { pub(crate) fn for_logical_schema(logical_schema: &[LogicalField], name_mapping: &[NameMappingEntry]) -> Result<Self, String> }`. Its body must be the struct literal now at `raw_scan.rs:251-256`, with `reconstruct_initial_defaults(logical_schema)?` and a one-line doc. In `crates/lakehouse-engine/src/scan/raw_scan.rs`, replace that literal with `FieldIdResolution::for_logical_schema(logical_schema, name_mapping).map_err(UdfError::User)?`. In `crates/lakehouse-engine/src/scan/test_support_tests.rs`, `column_binding_for`, replace the literal with `FieldIdResolution::for_logical_schema(logical_schema, &[]).expect("the logical schema's initial defaults reconstruct")`. Then remove the imports that become unused.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [OUTDATED_COMMENT] `SALES_PARQUET_LOCATION` doc contradicts its own value
- Location: lines 83-84
- Issue: The doc says the root is "unprefixed by the `s3://warehouse/` the Delta fixtures share", but the value is `s3://warehouse/unity_parquet/sales_parquet`, which is under `s3://warehouse/`. The Delta fixtures actually live under `s3://warehouse/delta/` (`scripts/unity/seed.sh`, `UC_PREFIX="delta"`).
- Fix: In `crates/lakehouse-engine/tests/e2e_unity_test.rs`, replace the doc of `SALES_PARQUET_LOCATION` with `/// Outside the \`s3://warehouse/delta/\` prefix \`scripts/unity/seed.sh\` gives the Delta fixtures, so the two fixture families never collide.`

#### [REDUNDANT_COMMENT] Fixture helper docs restate their bodies
- Location: the doc of `register_sales_parquet_table` (lines 205-210), and the doc of `sales_parquet_batch` (lines 162-164)
- Issue: The six-line `register_sales_parquet_table` doc restates the code ("via `DELETE` then `POST /tables`", "`year` and `region` additionally carry `partition_index`"). Its one reason, registering in-process because the bytes are written in-process, is buried in the middle. The `sales_parquet_batch` doc ends with "per the Background fixture description", a spec cross-reference that adds nothing.
- Fix: In `crates/lakehouse-engine/tests/e2e_unity_test.rs`, replace the doc of `register_sales_parquet_table` with `/// Registered in-process, unlike the Delta fixtures \`scripts/unity/seed.sh\` registers, because this fixture's bytes are also written in-process.` Replace the doc of `sales_parquet_batch` with `/// The in-file columns only: \`year\` and \`region\` are partition directories the catalog declares.`

### docs/catalogs.md

#### [OUTDATED_COMMENT] The Unity Parquet docs list the admitted types incompletely
- Location: line 251, the last bullet under **Parquet tables**
- Issue: The text says that a type "outside identity and the supported widening set (integer, floating-point, decimal, and date widening) fails every query that reads that column". The scan also admits a timestamp stored at the same or a coarser unit, and a text-rendered file type (binary, time) under a string column (`admits_timestamp`, `admits_as_text` in `field_id_projection.rs`). As written, a user would conclude that a millisecond timestamp file under a microsecond column fails, but it reads.
- Fix: In `docs/catalogs.md`, line 251, replace `(integer, floating-point, decimal, and date widening)` with `(integer, floating-point, decimal, and date widening, a timestamp stored at the same or a coarser unit, and a binary value under a \`STRING\` column)`.

## Expert fixes

### crates/lakehouse-engine/src/scan/field_id_projection.rs

#### [UNTESTED_ERROR_PATH] A primitive file column under a nested declaration skips admission and is cast silently
- Location: `ColumnBinding::refused_columns`, line 910 (`.filter(|(_, field)| !self.nested.contains_key(field.name()))`)
- Issue: `refused_columns` skips every column whose logical field declares a member tree, which is every key of `self.nested`. `bind_columns` inserts a `self.nested` entry for every claimed column with a declared tree, including one whose file stores a primitive. `resolve_nested_field` resolves such a column VERBATIM, `nested_columns()` leaves it out because a verbatim primitive does not need JSON rendering, and it goes to the delegate. The delegate then casts the primitive to the logical `Utf8`. A `struct` column (logical `utf8` plus a descriptor) that one file stores as `Int64` therefore reads the string `"1"`, with no refusal. The spec delta `datafusion-scan/type-relaxation` says the UDF SHALL hand a bound column to the cast only when its physical type is ADMITTED, and `admits(Int64, Utf8)` is false. The skip was meant for columns that are rendered, not for columns that are cast. No test covers this case, so the suite passes over the silent cast.
- Fix: In `crates/lakehouse-engine/src/scan/field_id_projection.rs`, `ColumnBinding::refused_columns`, replace the filter `!self.nested.contains_key(field.name())` with `!self.nested.get(field.name()).is_some_and(|resolution| needs_nested_json_rendering(resolution.resolved_field().data_type()))`. That skips exactly the columns `nested_columns()` renders. Update the doc sentence to say that a column the scan renders to JSON is left out, and that a declared tree the file contradicts with a primitive is judged like any other column. In `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs`, add `#[test] fn a_primitive_file_column_under_a_nested_declaration_is_judged_by_admission()` with the doc `/// Scenario: A physical type outside the admitted set is refused before any cast`. Build the resolution like `a_nested_physical_column_with_no_descriptor_fails_the_cast_rather_than_rendering` does: `FieldIdResolution { nested_members: HashMap::from([("addr".to_string(), NestedMembers::Struct { fields: vec![nested_by_physical_name("street", "street")] })]), ..bare_resolution() }`. Use a logical `field_no_id("addr", DataType::Utf8, true)`. Assert that `rewrite_with(.., Column::new("addr", 0))` over a physical `addr` of `DataType::Int64` fails, naming `'addr'`, `Int64`, `Utf8`, and `TABLE_ROOT`. Also assert that the same rewrite over a physical `addr` of `DataType::Utf8` succeeds.

#### [UNTESTED_ERROR_PATH] An all-NULL (`DataType::Null`) file column is now refused under every non-string declaration
- Location: `admits`, line 1021
- Issue: parquet-rs maps a column with the Parquet `UNKNOWN` (null) logical type to `DataType::Null` (`parquet-58.3.0/src/arrow/schema/primitive.rs:121`). pyarrow writes that type for an all-`None` pandas column. `admits(Null, logical)` is true only for a string logical type, through `needs_json_fallback(Null)`. `widen` has no `Null` row, and the other admission rules do not match either. So a file whose `id`/`amount`/`date` column is all NULL now fails every query that reads the column. Before this change the delegate cast it to NULLs. The plan refuses casts because they lose information ("A cast truncates `double` 1.7 to `int` 1"), and a cast from `Null` changes no value. No test covers `Null` in either direction.
- Fix: In `crates/lakehouse-engine/src/scan/field_id_projection.rs`, `admits`, add `physical == &DataType::Null ||` as the first disjunct. Update the doc comment of `admits` to name that case in the same sentence ("... or an all-NULL column, whose cast changes no value"). In `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs`, add `#[test] fn an_all_null_file_column_is_admitted_under_every_declared_type()` with the doc `/// Scenario: A physical type outside the admitted set is refused before any cast`. Call `assert_admitted(DataType::Null, logical)` for `DataType::Int64`, `DataType::Float64`, `DataType::Date32`, `DataType::Timestamp(TimeUnit::Microsecond, None)`, and `DataType::Utf8`. In `specs/_plans/add-unity-parquet-table-routing/datafusion-scan/type-relaxation/spec.md`, scenario "A physical type outside the admitted set is refused before any cast", add this admission clause to the THEN bullet: "and admitted for a physical `Null` column under every logical type, because an all-NULL column's cast changes no value".
