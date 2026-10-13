# Code Review Findings: add-column-source-notes

## Summary
- Files reviewed: 66 (60 code, test, script, and docs files reviewed against `af91305`; the 6 plan files read for context)
- Total findings: 36 (standard: 36, expert: 0)

## Standard fixes

### crates/lakehouse-engine/src/adapter/column_notes.rs

#### [SWALLOWED_ERROR] The note parse discards serde's error
- Location: lines 81-83 (`TableNotes::declared_schema`), lines 107-113 (`TableNotes::unreadable`)
- Issue: `note.and_then(|note| serde_json::from_str(note).ok())` drops the `serde_json::Error`. A note that exists but does not parse therefore gets the same message as an absent note. The message states neither the field that failed nor the constraint it broke. A note written by a different engine version is the case this error serves, and its cause (for example `missing field sourceType`) is lost.
- Fix: In `crates/lakehouse-engine/src/adapter/column_notes.rs`, change `unreadable(&self, column: &str)` to `unreadable(&self, column: &str, cause: &str)` and append ` ({cause})` after "rebuilds it" in its message. In `declared_schema`, replace the `and_then(...).ok_or_else(...)` chain with a `match note`: `None` yields `Err(self.unreadable(column, "the column carries no note"))`, and `Some(note)` yields `serde_json::from_str(note).map_err(|error| self.unreadable(column, &error.to_string()))?`. In `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs`, update `unreadable_message` (lines 748-749) to the new message the test prints for the note `{"sourceType":`. In `column_notes_tests.rs` `missing_or_unreadable_note_names_table_column_and_refresh`, assert that the unparseable note's message contains the serde error text and the absent note's message contains "the column carries no note".

#### [REDUNDANT_COMMENT] The `TableNotes` doc comment repeats the note limit as a literal
- Location: lines 64-66
- Issue: The comment spans 3 lines and spells out "2,000,000 characters", the value of `MAX_COLUMN_NOTE_CHARS`. AGENTS.md limits a comment to 1 or 2 lines, and the literal goes stale when the constant changes.
- Fix: Replace the doc comment of `TableNotes` with these two lines: `/// One involved table's columns and each raw note, `None` where Exasol sent none. Borrowed from` and `/// the request, so a note of up to [`MAX_COLUMN_NOTE_CHARS`] characters is never copied.`

### crates/lakehouse-engine/src/adapter/column_notes_tests.rs

#### [DUPLICATE_TEST] Two tests assert the same whole-table refusal
- Location: lines 133-149 (`a_table_whose_every_column_is_refused_is_refused_as_a_whole`), lines 224-254 (`a_table_is_refused_as_a_whole_only_when_every_column_is_refused`)
- Issue: Both tests parse a table whose every column carries `refused` and assert the exact message "virtual table 'SALES' has no mappable column; every column is refused: ...". The second test also covers the mappable-column and empty-table cases, so the first adds no coverage. Its scenario line, "Pushdown takes the declared schema from the column notes for every kind", is already carried by `resolver_assembles_the_declared_schema_from_each_tables_notes` in `scan_resolution_tests.rs`.
- Fix: In `crates/lakehouse-engine/src/adapter/column_notes_tests.rs`, delete `a_table_whose_every_column_is_refused_is_refused_as_a_whole` together with its `/// Scenario:` line (lines 133-149). Leave `a_table_is_refused_as_a_whole_only_when_every_column_is_refused` unchanged.

### crates/lakehouse-engine/src/adapter/direct_storage.rs

#### [REDUNDANT_COMMENT] The `resolve_columns` doc comment is 4 lines and opens with what the code does
- Location: lines 135-138
- Issue: AGENTS.md limits a comment to 1 or 2 lines of non-obvious why. The first sentence describes what each returned column carries, which the body shows.
- Fix: Replace the doc comment of `resolve_columns` with these two lines: `/// The tag and the note come from the one folded schema, so they cannot disagree. Nested or` and `/// unrepresentable columns tag as Utf8 (JSON text) instead of failing CREATE VIRTUAL SCHEMA.`

### crates/lakehouse-engine/src/adapter/parquet_directory.rs

#### [REDUNDANT_COMMENT] The `fill_partition_values` doc comment is 4 lines and restates the body
- Location: lines 319-322
- Issue: AGENTS.md limits a comment to 1 or 2 lines of non-obvious why. "One value map per file, in `files` order, keyed by the declared spelling" restates the return type and the loop.
- Fix: Replace the doc comment of `fill_partition_values` with these two lines: `/// Keys match their declared column under the uppercase fold, the deepest segment wins, and values` and `/// stay verbatim. The spelling check runs first, so no file-keep predicate can hide a conflict.`

### crates/lakehouse-engine/src/adapter/pushdown/format/catalog_parquet_format_reader.rs

#### [INFORMATION_LEAKAGE] The Delta CREATE-time declaration lives in the Parquet reader's module
- Location: lines 248-365 (`COLUMN_MAPPING_MODE_PROPERTY`, `declare`, `refused_placeholder`, `classify_columns`, `spark_field`)
- Issue: These five items run only at CREATE, REFRESH, and SET. Their one caller is `declare_columns` in `format/mod.rs`, and `CatalogParquetFormatReader` uses none of them. They hold the Unity Delta declaration, including the Delta table property `delta.columnMapping.mode`, inside the module of the catalog-declared Parquet reader. The Delta declaration tests (`declaration_carries_the_mode_selected_key_from_type_json`, `unusable_annotation_or_mode_refuses_the_column`) sit in `delta_schema_tests.rs`, so their code under test is in a Parquet reader file. A change to the Delta column-mapping rules edits the Parquet reader's module.
- Fix: Create `crates/lakehouse-engine/src/adapter/pushdown/format/catalog_declaration.rs` and move `COLUMN_MAPPING_MODE_PROPERTY`, `declare`, `refused_placeholder`, `classify_columns`, and `spark_field` into it unchanged, with the imports they need. Add `mod catalog_declaration;` to `format/mod.rs` and change `declare_columns` to call `catalog_declaration::declare(table)`. Replace the doc comment of `declare` with two lines: `/// The Unity Delta, Unity Parquet, and Glue Parquet arm of [`super::declare_columns`], one entry` and `/// per column in catalog order. Unusable metadata refuses columns, never the statement.` Remove the imports the compiler then reports unused in `catalog_parquet_format_reader.rs`. The tests reach this code through `declare_columns`, so they stay where they are.

#### [SHRINKABLE] The reader's `table` field exists only to feed the Glue arm
- Location: line 42 (`CatalogParquetFormatReader.table`), lines 60-74 (`ParquetFileSource::plan`), line 242 (`resolve_scan`)
- Issue: The Unity arm now lists against the declared partition columns, so `plan` takes both `table` and `partition_columns`, and each is read by one arm only. `table` reaches `plan` only for `GluePartitions`, while the `TableDirectory` arm's `UnityTableStorage` already holds its table. Before this change both arms read `table`. Plan.md § Requirements rules out a reader signature parameter that one arm alone reads.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/catalog_parquet_format_reader.rs`, add `table: &'a CatalogTable` to the `ParquetFileSource::GluePartitions` variant. Delete the `table` field of `CatalogParquetFormatReader` and the `table` parameter of `ParquetFileSource::plan`, and pass the variant's own `table` to `plan_glue_table`. Change `resolve_scan` to call `self.files.plan(partition_columns, &keep)`. Update the two constructions in `format/mod.rs` `format_reader` (lines 187 and 205) and the two in `catalog_parquet_format_reader_tests.rs` (`resolve_noted` line 133, `resolve_glue` line 622).

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs

#### [MIXED_ABSTRACTION_LEVEL] `checked_partition_fields` runs the whole declaration check under a partition-lookup name
- Location: lines 108-165
- Issue: The function runs three independent checks: each field's binding key against the mode, the partition columns against the log, and each non-nullable declaration against the log. It also returns the partition fields. Its name announces only the partition part, so a reader of `declared_files` cannot see that the binding-key and nullability checks run there.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs`, extract the binding-key check (from `let mode = snapshot.column_mapping_mode();` through its `return Err(disagreement(...))`) into `fn ensure_binding_keys_follow_mode(declared: &DeclaredSchema, mode: ColumnMappingMode) -> Result<(), UdfError>`. Extract the nullability check (the `relaxed` closure and its `match`) into `fn ensure_no_relaxed_nullability(schema: &StructType, declared: &DeclaredSchema) -> Result<(), UdfError>`. In `declared_files`, call `ensure_binding_keys_follow_mode(declared, snapshot.column_mapping_mode())?`, then `checked_partition_fields(&schema, snapshot, declared)?`, then `ensure_no_relaxed_nullability(&schema, declared)?`, so the check order and every message stay as they are. Leave `checked_partition_fields` with the partition lookup and order check only, and give each of the three functions a doc comment of at most 2 lines.

#### [REDUNDANT_COMMENT] The `declared_files` doc comment is 4 lines
- Location: lines 64-67
- Issue: AGENTS.md limits a comment to 1 or 2 lines. "The snapshot's active files for a scan planned from `declared`" restates the signature.
- Fix: Replace the doc comment of `declared_files` with these two lines: `/// Filter columns and partition values resolve through each column's recorded binding key, as` and `/// PROTOCOL.md § Column Mapping resolves "partition values and column level statistics".`

### crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs

#### [DEAD_FLEXIBILITY] The helpers return catalog request targets that no caller reads
- Location: lines 49-79 (`resolve_rest_declared`), lines 1035-1048 (`resolve_declared`)
- Issue: `resolve_rest_declared` returns `(Result<ResolvedScan, UdfError>, Vec<String>)`, where the `Vec<String>` is `catalog.targets()` (line 78). All five callers discard it with `let (resolved, _) = ...` (lines 37, 1060, 1083, 1109, 1129). The doc comment "also answers the catalog requests the plan sent" describes a value no test uses.
- Fix: In `iceberg_tests.rs`, change `resolve_rest_declared` and `resolve_declared` to return `Result<ResolvedScan, UdfError>` and return `resolved` instead of `(resolved, catalog.targets())`. Change the doc comment of `resolve_rest_declared` to `/// Like [`resolve_rest`], from `notes`.` Replace each `let (resolved, _) = ...` with `let resolved = ...`.

#### [MISSING_DOC_COMMENT] The field-id nullability test has no Scenario line
- Location: line 1101 (`nullability_is_compared_by_declared_field_id_and_a_tightened_column_plans`)
- Issue: The test implements the clause "the reader SHALL find the column in the current schema by its declared field id" of the scenario "A declared required column that the current schema marks optional fails the query". AGENTS.md requires a `/// Scenario:` line on a test that implements a spec scenario.
- Fix: In `iceberg_tests.rs`, add `/// Scenario: A declared required column that the current schema marks optional fails the query` directly above the `#[tokio::test]` of `nullability_is_compared_by_declared_field_id_and_a_tightened_column_plans`.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs

#### [MISSING_BOUNDARY_TEST] No case declares binding keys against a log without column mapping
- Location: lines 371-412 (`declaration_check_refuses_a_mode_or_partition_mismatch`)
- Issue: The binding-key check in `checked_partition_fields` refuses any field whose keys differ from the mode's selection, including "neither key under `none`". Every disagreement case uses a log in `name` or `id` mode, or tests the partition mismatch. The transition from a mapped declaration to an unmapped log is untested.
- Fix: In `delta_format_reader_tests.rs`, after the `disagreements` loop of `declaration_check_refuses_a_mode_or_partition_mismatch`, add a case: map each field of `logged_fields(&partitioned)` through `.with_metadata([("delta.columnMapping.id", MetadataValue::Number(<1-based index>)), ("delta.columnMapping.physicalName", MetadataValue::String(format!("col-{}", field.name())))])`, build `declaration(&annotated, Some("name"), &["letter"])`, and assert that the error of `planned(&partitioned, &declared, None)` contains `VIRTUAL_TABLE`, `column-mapping mode is 'none'`, and `ALTER VIRTUAL SCHEMA ... REFRESH`.

#### [NONDETERMINISTIC_TEST] Temporary Delta tables share one directory name across processes
- Location: line 334 (`temporary_table`)
- Issue: `std::env::temp_dir().join(format!("lh_delta_reader_{name}"))` is the same path in every process, and `remove_dir_all` runs before each write. Two concurrent runs, such as `cargo test` and `cargo llvm-cov`, delete each other's logs mid-test. `scan/type_relaxation_tests.rs` and `scan/raw_scan_tests.rs` append the process id for this reason.
- Fix: In `delta_format_reader_tests.rs` `temporary_table`, change the directory name to `format!("lh_delta_reader_{name}_{}", std::process::id())`.

#### [MISSING_DOC_COMMENT] A test implements two scenario clauses without Scenario lines
- Location: lines 495-498 (`partition_values_are_keyed_by_the_declared_partition_columns_in_the_logs_order`)
- Issue: The test asserts the out-of-order partition disagreement, a clause of "A declaration that disagrees with the Delta log fails the query". It also asserts that a nullable declaration of a required log column plans, a clause of "A declared non-nullable column that the Delta log marks nullable fails the query". It carries no `/// Scenario:` line.
- Fix: In `delta_format_reader_tests.rs`, add `/// Scenario: A declaration that disagrees with the Delta log fails the query` and `/// Scenario: A declared non-nullable column that the Delta log marks nullable fails the query` above the `#[test]` of `partition_values_are_keyed_by_the_declared_partition_columns_in_the_logs_order`, keeping its existing doc comment.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs

#### [MISSING_DOC_COMMENT] The binding-key translator test has no Scenario line
- Location: line 749 (`a_filter_column_is_emitted_under_its_paired_fields_current_name`)
- Issue: The test implements the translator half of "A filter column resolves through its declared binding key": the column is emitted under its paired field's current name, and an unpaired column constrains nothing. It carries no `/// Scenario:` line.
- Fix: In `delta_predicate_tests.rs`, add `/// Scenario: A filter column resolves through its declared binding key` directly above the `#[test]` of `a_filter_column_is_emitted_under_its_paired_fields_current_name`.

### crates/lakehouse-engine/src/adapter/pushdown/format/parquet_format_reader_tests.rs

#### [INLINE_COMMENT] An inline comment restates a case's expectation
- Location: line 366 (`a_partition_filter_prunes_the_listing_without_reading_any_footer`)
- Issue: `// Keeping no file still resolves, and the declared schema survives pruning every file.` describes what the `(equal("YEAR", "2099"), vec![])` case asserts. The guardrails ban inline comments.
- Fix: In `parquet_format_reader_tests.rs`, delete the comment at line 366. In the same loop, change the `column_names` assertion message from `"{filter}"` to `"{filter}: the declared schema survives pruning every file"`.

#### [WORK_TRACKING_COMMENT] An assertion message cites the superseded issue #412
- Location: line 322 (`plan_reads_no_footer_and_lists_every_file`)
- Issue: The message "a non-partition filter never prunes files (#412)" points at footer-statistics pruning. Decision [12] records #412 as closed and superseded, because no pushdown reads a footer.
- Fix: In `parquet_format_reader_tests.rs`, change the message at line 322 to `"a non-partition filter never prunes files, because planning reads no footer statistics"`.

#### [OUTDATED_COMMENT] An assertion message says the plan path renders the timezone tag
- Location: line 585 (`a_non_utc_timezone_column_plans_at_its_normalized_tag`)
- Issue: "the plan path renders the tz-aware tag, the same one enumeration declares" describes the removed pushdown-time derivation. `ParquetFormatReader` takes the logical schema from the notes that enumeration records.
- Fix: In `parquet_format_reader_tests.rs`, change the message at line 585 to `"the plan carries the tz-aware tag that enumeration recorded in the column's note"`.

### crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs

#### [SHRINKABLE] `notes_of` repeats `involved_notes` and names columns with placeholders
- Location: line 551 (`notes_of`); callers at lines 605, 613, 670, 751, 758
- Issue: `notes_of` builds a `TableNotes` the way `test_support::involved_notes` does, which this file already imports. It names the columns `"C1"` and `"C2"`, which are not the uppercase fold of each note's `sourceType.name`. Because it uses `zip`, any note past the second is dropped silently.
- Fix: In `scan_resolution_tests.rs`, delete `notes_of`. At each caller, build a `Vec<(String, String)>` pairing each note with the uppercase of its declared source name, for example `("ID".to_string(), note(declared_field("id", "int64"), None))`, and pass `&involved_notes("<TABLE>", &pairs)` to `resolver.resolve`.

#### [SHRINKABLE] `note` and `refused_note` are the same builder
- Location: line 530 (`note`), line 725 (`refused_note`)
- Issue: Both build `ColumnDeclaration { source_type, partition, refused }` and call `.encode()`. They differ only in `refused`: `None` against a fixed `Some("its type is refused")`. `refused_note` is defined about 60 lines after its first use.
- Fix: In `scan_resolution_tests.rs`, replace both functions with one `fn note(field: LogicalField, partition: Option<usize>, refused: Option<&str>) -> String` placed above its first caller, setting `refused: refused.map(str::to_string)`. Change every `note(f, p)` call to `note(f, p, None)` and every `refused_note(f, p)` call to `note(f, p, Some("its type is refused"))`.

#### [SHRINKABLE] `declared_field` is the fourth identical nullable `LogicalField` builder
- Location: line 539 (`declared_field`)
- Issue: `declared_field(name, arrow_type)` has the same body as `identity_bound` in `pushdown/format/catalog_parquet_format_reader_tests.rs` (line 169), `field` in `adapter/column_notes_tests.rs` (line 12), and `logical_field` in `scan/type_relaxation_tests.rs` (line 240). Four copies pass the Rule of Three. Two of them sit under `adapter::pushdown` and can share `test_support`.
- Fix: Add `pub(super) fn nullable_field(name: &str, arrow_type: &str) -> LogicalField` with that body to `crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs`. Delete `declared_field` from `scan_resolution_tests.rs` and `identity_bound` from `crates/lakehouse-engine/src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs`, and call `nullable_field` at their call sites.

#### [VAGUE_TEST_NAME] The test name claims behavior the test does not exercise
- Location: line 736 (`resolver_parses_notes_once_and_every_reader_reads_the_declaration`)
- Issue: For each of the four catalog kinds, the test passes three refusing declarations (an unreadable note, an all-refused table, a refused partition column) and asserts the exact error and that no catalog, Glue, or object endpoint was contacted. No reader receives a declaration, and nothing counts parses.
- Fix: In `scan_resolution_tests.rs`, rename the test to `a_refusing_declaration_fails_before_any_reader_loads_its_table_under_every_kind`. Update the test name in the "Every pushdown request shape resolves through the one format-reader seam" row of `specs/_plans/add-column-source-notes/plan.md` § Scenario Coverage to match.

### crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs

#### [SHRINKABLE] `binary_table_pushdown` copies `with_notes` inline and writes Iceberg notes onto the Delta request
- Location: lines 2169-2181 (`binary_table_pushdown`: the `notes` binding and the `if let Some((_, note)) = notes.iter().find(...)` block), lines 2190-2191
- Issue: The inline block does what `test_support::with_notes` does. It also runs for the Delta arm, which attaches Iceberg notes to `ID` and `B` before `with_notes(request, "T", &delta_notes(...))` overwrites them, so a reader must trace both steps to know which notes the Delta request carries.
- Fix: In `pushdown_tests.rs` `binary_table_pushdown`, delete the `notes` binding and the `if let Some(...)` block, and build each column as `serde_json::json!({"name": name, "dataType": data_type})`. In the Iceberg arm, before the pushdown call, add `let request = with_notes(request, "T", &load_table_body_notes(&serde_json::from_str(&body).expect("a JSON body")));`.

### crates/lakehouse-engine/src/adapter/pushdown/joins/joins_tests.rs

#### [UNTESTED_ERROR_PATH] No test covers a join where only one leg lacks notes
- Location: file level. The only note-less join case is `note_less_unity_join_request` in `pushdown_tests.rs` (line 1897), which drops the notes of both legs, and its loop asserts only the substring "is missing or unreadable; ALTER VIRTUAL SCHEMA ... REFRESH".
- Issue: Spec `vs-adapter/column-source-notes` requires each join leg to read the notes of its own involved table, and the error to name the virtual table and the column. A join-leg extraction (`involved_table` in `joins/planning.rs`) that read the other leg's notes would pass every current test.
- Fix: In `joins_tests.rs`, add `#[tokio::test(flavor = "multi_thread", worker_threads = 4)] async fn a_join_leg_without_notes_fails_naming_its_own_table_and_column()` with `/// Scenario: A column without a readable note fails the pushdown naming the fix`. Build the request as `delta_join_request(Json::Null)` does, but apply `with_notes` to `"CUSTOMER"` only (`with_notes(request, "CUSTOMER", &delta_notes(CUSTOMER_COLUMNS, &[]))`). Run it as `a_select_star_join_is_refused_by_the_side_declaring_the_refused_column` does: `delta_pushdown(&request, &unity_delta_catalog().await.uri, two_delta_legs_one_refusing_binary_col().await, "cat.sch.orders")`. Assert that `user_message(err)` contains `'O_CUSTKEY'`, `'ORDERS'`, and `ALTER VIRTUAL SCHEMA ... REFRESH`, and does not contain `'CUSTOMER'`.

### crates/lakehouse-engine/tests/common/e2e_harness.rs

#### [ASSERTION_FREE_TEST] The notes helper returns an empty list for a wrong table, so callers pass vacuously
- Location: line 532 (`persisted_column_notes`), used by `column_adapter_notes`
- Issue: For a wrong or missing table, `SYS.EXA_ALL_VIRTUAL_COLUMNS` returns zero rows and the helper returns `vec![]`. `unity_delta_and_parquet_columns_persist_source_notes` (`e2e_unity_test.rs` line 2034) then compares 0 with 0 and loops over nothing, so it passes without checking a note.
- Fix: In `persisted_column_notes`, after `query_columns`, add `assert!(!cols[0].is_empty(), "{vs_name}.{table}: no virtual columns persisted");` before building the result.

### crates/lakehouse-engine/tests/e2e_direct_storage_test.rs

#### [ASSERTION_FREE_TEST] The single-letter needle "N" cannot show that the error names the column
- Location: line 2182 (`source_change_after_refresh_plans_against_the_declaration`)
- Issue: `assert_mentions(..., &["N", "Int64", "Int32", ...])` checks for the bare letter `N`, which almost any error text contains. The scenario clause "naming the column" is therefore unchecked. The scan error quotes the column as `cannot read column 'N' of the table at ...` (`src/scan/field_id_projection.rs` line 838).
- Fix: In `source_change_after_refresh_plans_against_the_declaration`, replace the needle `"N"` with `"'N'"`.

#### [MAGIC_NUMBER] The literal 5 repeats the `{index:05}` pad width
- Location: line 2336 (`over_limit_struct_batch`)
- Issue: `format!("{index:05}{}", "m".repeat(MEMBER_NAME_CHARS - 5))` writes the pad width twice. Changing one without the other changes every member-name length and therefore the note length the test depends on.
- Fix: In `over_limit_struct_batch`, add `const INDEX_DIGITS: usize = 5;` beside `MEMBERS` and build the name as `format!("{index:0INDEX_DIGITS$}{}", "m".repeat(MEMBER_NAME_CHARS - INDEX_DIGITS))`.

#### [MISSING_DOC_COMMENT] Two changed hive-partitioning scenarios have no Scenario line on any test
- Location: line 1569 (`hive_segments_declare_varchar_partition_columns_with_decoded_values`), line 1622 (`mixed_layout_unions_partition_keys_and_nulls_the_missing_key`)
- Issue: The delta `direct-storage/direct-storage-hive-partitioning` changes both scenarios, and plan.md maps them to these two tests. No test in `crates/` carries a `/// Scenario:` line for either title, which breaks the AGENTS.md coverage rule and plan task 1.9.
- Fix: Add `/// Scenario: A key=value directory segment declares a VARCHAR partition column` directly above the `#[test]` of `hive_segments_declare_varchar_partition_columns_with_decoded_values`. Add `/// Scenario: A table's partition columns are the union of its files' keys` directly above the `#[test]` of `mixed_layout_unions_partition_keys_and_nulls_the_missing_key`.

### crates/lakehouse-engine/tests/e2e_refresh_test.rs

#### [ASSERTION_FREE_TEST] A column named "must" lets any error containing the word "must" pass
- Location: line 869 (`long_field(3, "must", true)`), line 912 (`dropped_iceberg_column_reads_what_older_files_store`)
- Issue: `required_absent.to_lowercase().contains("must")` matches any error that uses the English word "must", and production errors do (for example `src/scan/partition_values.rs` line 88). The test cannot show that the failure is the required-absent error of this column.
- Fix: In `dropped_iceberg_column_reads_what_older_files_store`, rename the field `"must"` to `"required_marker"` in `fields`, change the query to `SELECT ID, REQUIRED_MARKER FROM {table}`, and change the assertion to `required_absent.to_lowercase().contains("required_marker")`.

#### [SWALLOWED_ERROR] `fresh_table` treats a failed namespace check as an absent namespace
- Location: line 656 (`fresh_table`)
- Issue: `catalog.namespace_exists(&ns).await.unwrap_or(false)` discards the catalog error. The sibling `create_empty_table` (line 158) panics with context on the same call. An unreachable catalog then surfaces later as an unrelated seed or drop failure.
- Fix: In `fresh_table`, replace `.unwrap_or(false)` with `.unwrap_or_else(|e| panic!("check namespace {namespace}: {e}"))`.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [MISSING_DOC_COMMENT] The Unity Parquet partition-position assertions carry no Scenario line
- Location: line 1875 (`unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values`)
- Issue: Plan.md maps "A partition column's note carries its position among the partition columns" to `unity_parquet_partition_notes_follow_partition_index`, which does not exist. Its assertions ("CREATE records each partition column at its partition_index") are in this test, whose only scenario line is "Partition columns come from the catalog and partition values from the file paths".
- Fix: Add `/// Scenario: A partition column's note carries its position among the partition columns` beside the existing `/// Scenario:` line of `unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values`. In `specs/_plans/add-column-source-notes/plan.md` § Scenario Coverage, replace `unity_parquet_partition_notes_follow_partition_index` with `unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values`.

#### [ASSERTION_FREE_TEST] The needle "Name" cannot show that the refusal names the mode
- Location: line 2162 (`unity_unusable_column_mapping_metadata_refuses_only_its_column`)
- Issue: The needles are `["'value'", "Name", "delta.columnMapping.physicalName"]`. The third needle contains `Name`, and the refusal text in `delta_schema.rs` always lists both annotation keys, so the mode clause passes under any mode, `Id` included.
- Fix: In `unity_unusable_column_mapping_metadata_refuses_only_its_column`, replace the needle `"Name"` with `"Name-mode"`, the text `unusable_column_mapping` renders from `{mode:?}-mode`.

#### [SHRINKABLE] `create_catalog_copy_virtual_schema` re-declares harness provisioning and depends on a hidden call order
- Location: lines 141-153 (`create_catalog_copy_virtual_schema`)
- Issue: The function copies the DROP-then-CREATE DDL that `common::e2e_harness::prepare_create_virtual_schema` already builds. `specs/testing.md` § Shared harness says no binary re-declares provisioning logic. The function also relies, without stating it, on `create_unity_virtual_schema` having created `CONN_NAME` first.
- Fix: Replace the body of `create_catalog_copy_virtual_schema` with `create_virtual_schema_with_password(conn, &VsProps::new(COPY_VS_NAME, "unity.delta_catalog_copy").with_catalog_kind("UNITY_CATALOG").with_catalog_conn_name(CONN_NAME), UNITY_CATALOG_URI_INTERNAL, &local_stack_connection_password());`. Import `VsProps` and `create_virtual_schema_with_password` from `common::e2e_harness`, and keep the doc comment.

### scripts/unity/seed.sh

#### [OUTDATED_COMMENT] The payload list omits the `properties` member
- Location: line 189 (comment above `replace_table`)
- Issue: The comment lists the registration payload as "(table_type, data_source_format, storage_location, columns[])". `registration()` also sends `properties`, which the Delta declaration reads.
- Fix: In `scripts/unity/seed.sh`, change the parenthetical on line 189 to "(table_type, data_source_format, storage_location, columns[], properties)".

#### [REDUNDANT_COMMENT] The 6-line header paragraph restates `registration()`
- Location: lines 8-13
- Issue: AGENTS.md limits a comment to 1 or 2 lines of non-obvious why. Only the first clause is a why. The rest describes what `registration()` and `CATALOG_COPIES` show: `type_json` is the StructField JSON, `partition_index` follows `partitionColumns`, `properties` is `configuration`.
- Fix: Replace lines 8-13 with these two lines: `# The engine declares Delta columns from the Unity Catalog entry, so each fixture is registered from its log's latest metaData;` and `# delta_catalog_copy holds deliberately wrong copies for the declaration-check and refusal tests.`

### docs/catalogs.md

#### [OUTDATED_COMMENT] The footer-cost paragraph cites the superseded #412 and describes a change
- Location: lines 640 and 646-647
- Issue: Line 647 says "there is no footer-statistics pruning (#412)". Decision [12] records #412 as closed and superseded because no pushdown reads a footer, so the reference points readers at work that will not happen. Line 640 says the planning cost "no longer grows", which describes the change instead of the current behavior. Line 646 also runs past the paragraph's wrap width.
- Fix: In `docs/catalogs.md`, change "so its cost no longer grows with the kept files' footers" to "so its cost does not grow with the kept files' footers". Change "there is no footer-statistics pruning (#412)" to "there is no footer-statistics pruning, because planning reads no footer". Rewrap the paragraph to the width of the surrounding lines.

## Expert fixes
[none]
