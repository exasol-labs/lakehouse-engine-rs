# Plan: add-unity-parquet-table-routing

## Summary

Unity Catalog tables that report `data_source_format` `PARQUET` become listable and queryable, planned by a new Unity Parquet format reader that takes its schema and partition columns from the catalog. Issue #409, work unit 3 of the "Support for non-Iceberg tables" milestone, builds on the direct-storage catalog kind (#407) and its Hive partitioning (#408). For every format, the scan binds an identity-bound column case-insensitively and refuses a data-file type outside identity and the supported widening set.

## Design

### Context

The Unity Catalog client admits only `DELTA` base tables. A `PARQUET` external table is skipped as `SkipReason::NotDeltaBaseTable` at listing, and `load_table` refuses it at plan time. The engine already owns a Parquet reader for `DIRECT_STORAGE`, but that reader derives its schema by folding footers. For a Unity table the catalog is the schema authority (interview Q1). Unity Catalog declares partition columns through `partition_index` and discovers their values from the Hive-style directory layout (interview Q2). The data files never agreed to the catalog's names and types. The requester chose loud, Spark-compatible failure on that drift (review round 1).

- **Goals**
  - Admit `MANAGED` and `EXTERNAL` tables with `data_source_format` `PARQUET` at listing and at single-table load.
  - Plan such a table from the catalog's declared columns and partition columns, with the file list from the shared directory seam.
  - Resolve its storage exactly as a Unity Delta table does, vended or static.
  - For every format, bind an identity-bound column across a letter-case difference and refuse a physical type outside identity and the supported widening set.
- **Non-Goals**
  - Other Unity formats (`CSV`, `JSON`, `AVRO`, `ORC`, `TEXT`, `DELTASHARING`) and Unity Iceberg (UniForm) planning.
  - Databricks partition metadata logging.
  - Plan-time file pruning on non-string partition columns or on column statistics.
  - A plan-time footer read to compare file schemas with the catalog. The scan checks names and types per file instead.

### Decision

#### Architecture

```
createVirtualSchema                         pushdown (plan time)
  UnityCatalogSession::list_tables            TableScanResolver (RequestSession::Unity)
    admission: base table AND                   load_table -> CatalogTable{format, partition_columns,
    DELTA|PARQUET, tagged per format                          columns[type_json], vending key}
  shared listing pipeline (unchanged)         format_reader(ScanSource::Unity{session, table})
                                                match table.format
                                                  Delta   -> DeltaFormatReader ---------+
                                                  Parquet -> UnityParquetFormatReader --+
                                                  Iceberg -> refused                    |
                                                                                        v
                                              UnityTableStorage (location check, READ vend or
                                                static backend, redaction)
UnityParquetFormatReader
  type_json -> delta_kernel StructField (nullable) -> build_delta_table_schema(None mapping)
  partition_columns (catalog order) --> parquet_directory::list_parquet_files
                                         (shared listing, fold-matched values, no footer,
                                          keep predicate over string partition columns)
  -> ResolvedScan{files, effective_storage, logical_schema, table_root, partition_columns}

scan, per file, every format: FieldIdExprAdapterFactory
  bind: field-id | declared physical name | identity (exact, then uppercase fold)
  admit: widen() owner | timestamp unit/zone rule | text rendering, else refuse
  -> DefaultPhysicalExprAdapter cast (admitted pairs only)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Catalog as schema authority | `UnityParquetFormatReader` | The catalog declares columns and partition columns. The files supply only the listing and the partition values. |
| One Spark-type classifier | `delta_schema::build_delta_table_schema` | Unity `type_json` and the Delta log share the Spark `StructField` JSON representation, so one classifier types both. |
| Composition over a shared storage component | `format/unity_table_storage.rs` | The vending rule has one owner for every format Unity Catalog hosts. |
| Exhaustive dispatch on the format tag | `format_reader` Unity arm | A new `TableFormat` variant is a compile error at the one dispatch site. |
| Admit, then cast | `FieldIdExprAdapterFactory::create` | One per-file check decides which physical-to-logical pairs reach the cast, for every format and binding key. |

#### Spec compliance

- **Delta protocol.** A Unity Parquet table is not a Delta table: it has no `_delta_log`, so no Delta reader requirement (reader features, deletion vectors, column mapping, type widening) applies to it. The plan reuses one Delta artifact, the schema serialization: "Delta uses a subset of Spark SQL's JSON Schema representation to record the schema of a table in the transaction log" (PROTOCOL.md § Schema Serialization Format). Unity Catalog's `type_json` carries that representation per column ("Full data type specification, JSON-serialized.", Databricks Tables API).
- **Iceberg spec.** The Iceberg REST client only sets the new `partition_columns` field to an empty list. The scan-side admission check refuses a data-file type that is neither the field's type nor a valid promotion source. It admits the rows of § Schema Evolution's "Valid primitive type promotions are:" table that `datafusion-scan/type-relaxation` supports: `int` to `long`, `float` to `double`, and `decimal(P, S)` to `decimal(P', S)` if `P' > P`. The case fold never reaches an Iceberg column, because every Iceberg logical field carries a field-id.
- **Delta protocol, scan side.** § Consistency Between Table Metadata and Data Files, a writer requirement, states: "Any data file column that exists in the table schema MUST have the same type (except as allowed by the [Type Widening] table feature, if enabled)." A refused pair is a file that breaks this rule. Neither PROTOCOL.md nor the Iceberg spec states a column-name case rule. The fold follows Spark's default `spark.sql.caseSensitive=false`.
- **Exasol trade-offs.** A `struct`, `array`, or `map` column surfaces as JSON `VARCHAR(2000000)` through the existing nested-type rule, because Exasol has no nested types. A `binary` or `variant` column is refused by name, as for a Delta table. The scan admits a text-rendered file type (binary, time) under any string-tagged column, because Exasol has no such types and both columns carry the `utf8` tag.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Schema from catalog `type_json` through the Delta classifier | Footer fold (direct-storage reader). A new Spark `type_name` table. | The catalog owns the schema. `type_name` carries no nested structure, and a third classifier would drift from the other two. |
| Partition columns from `partition_index`, values from `key=value` paths | Infer partition columns from paths. | Unity Catalog declares them and discovers values from the directory layout (Databricks partition discovery). |
| Every logical field nullable | Keep catalog nullability. | A file missing a column reads NULL. A required declaration fails the scan instead. |
| Prune only on string partition columns | Typed partition comparison. | The shared partition predicate compares strings, and string order is not integer or date order. Typed pruning is a non-goal. |
| `ScanSource::UnityDelta` renamed `Unity`, dispatch on format tag | One `UnityFormatReader` branching internally. | `format_reader` stays the one dispatch site. |
| `UnityTableStorage` shared by both Unity readers | Duplicate the vending code. | Two copies of the no-fallback rule can drift. |
| Refuse a physical type outside identity, the widening set, and text rendering, for every format | Cast every arrow-castable pair and document the drift outcomes. | Requester choice: loud, Spark-compatible failure. A cast truncates `double` 1.7 to `int` 1. |
| Fold letter case for identity-bound fields only | Exact case everywhere. Fold every name step. | Spark resolves Parquet columns case-insensitively by default. Field-id and declared-physical-name bindings keep their exact recorded rules. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| unity-parquet-table-planning | NEW | `vs-adapter/unity-parquet-table-planning/spec.md` |
| unity-catalog-e2e-harness-parquet-queries | NEW | `unity-e2e/unity-catalog-e2e-harness-parquet-queries/spec.md` |
| unity-catalog-client | CHANGED | `vs-adapter/unity-catalog-client/spec.md` |
| unity-catalog-create-virtual-schema | CHANGED | `vs-adapter/unity-catalog-create-virtual-schema/spec.md` |
| catalog-crate-public-surface-extensions | CHANGED | `vs-adapter/catalog-crate-public-surface-extensions/spec.md` |
| parquet-directory-seam | CHANGED | `vs-adapter/parquet-directory-seam/spec.md` |
| delta-table-planning | CHANGED | `vs-adapter/delta-table-planning/spec.md` |
| pushdown-format-neutral-resolution | CHANGED | `vs-adapter/pushdown-format-neutral-resolution/spec.md` |
| scan-execution-field-id-projection | CHANGED | `datafusion-scan/scan-execution-field-id-projection/spec.md` |
| type-relaxation | CHANGED | `datafusion-scan/type-relaxation/spec.md` |
| direct-storage-e2e | CHANGED | `e2e-harness/direct-storage-e2e/spec.md` |

## Impact

- After a `REFRESH` or a new `CREATE VIRTUAL SCHEMA`, a Unity virtual schema lists every `PARQUET` base table in its namespace. Before this change, the adapter skipped each such table with a warning.
- A query on a Unity Parquet table returns rows. Only a predicate on a `string` partition column reduces the files read.
- A Parquet data file without the `.parquet` suffix is not read. `docs/catalogs.md` states the rule.
- **Breaking, every format:** a query that reads a column whose data-file type is outside identity, the supported widening set, and text rendering now fails, naming the table's storage location, the column, and both types. Before this change, the scan cast the value, which could truncate or narrow it.
- **Breaking, direct storage `MERGE_SCHEMA = 'FALSE'`:** a query that reads a column whose type in some file is wider than the sampled declaration now fails, even when every value fits. Setting `MERGE_SCHEMA = 'TRUE'` restores the read for every pair the fold can widen, because the fold then declares the wider type.
- A column without a binding key (Delta `none` column mapping, direct storage, Unity Parquet) now binds a file column whose name differs only in letter case. Before this change, it read NULL.
- The skip warning text and the Delta error texts are unchanged. No virtual-schema property is added.

## Implementation Tasks

### A. Catalog admission and neutral metadata

- [ ] 1.1 In `crates/lakehouse-catalog/src/unity/client.rs`, add `type_json: Option<String>` and `partition_index: Option<u32>` to the crate-private `ColumnInfo`. Add `type_json: Option<String>` to `ColumnSourceType::Unity` in `crates/lakehouse-catalog/src/client.rs`. Fill it in `neutral_column`.
- [ ] 1.2 Add `partition_columns: Vec<String>` to `CatalogTable`. Fill it in `neutral_table` with the names of columns that carry a `partition_index`, ordered by it. Set `Vec::new()` at `client.rs:253` (Iceberg) and `crates/lakehouse-engine/src/adapter/direct_storage.rs:107`. Update every construction site in this census:
  - `CatalogTable` literals in tests: `lakehouse-catalog/src/client_tests.rs:25,42`, `lakehouse-catalog/tests/catalog_public_surface.rs:182,259`, `lakehouse-engine/src/adapter/adapter_tests.rs:1632`, `lakehouse-engine/src/adapter/catalog_client_tests.rs:48,125`, `lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs:43`, `lakehouse-engine/src/adapter/pushdown/format/format_tests.rs:38`.
  - `ColumnSourceType::Unity` sites: the destructuring pattern in `lakehouse-engine/src/types/mapping.rs:563` (add `..`), plus test literals in `lakehouse-catalog/src/client_tests.rs` (2), `lakehouse-catalog/src/unity/client_tests.rs` (1), `lakehouse-catalog/tests/catalog_public_surface.rs` (1), `lakehouse-engine/src/adapter/catalog_client_tests.rs` (2), `lakehouse-engine/src/types/mapping_tests.rs` (10).
- [ ] 1.3 Replace `delta_base_skip_reason` with one admission function that returns the admitted `TableFormat` or a `SkipReason::NotDeltaBaseTable`. It admits `MANAGED` or `EXTERNAL` with exact uppercase `DELTA` or `PARQUET`. `list_tables` tags each admitted table with the returned format. `neutral_table_format` maps `PARQUET` to `TableFormat::Parquet`.
- [ ] 1.4 Update the catalog tests. In `unity/client_tests.rs`:
  - Rename the `delta_base_skip_reason_*` units to `admission_*` and add a Parquet case and a lowercase `parquet` case.
  - Rename `list_tables_tags_every_admitted_table_delta_and_keeps_the_skip_filter`, `skips_view_non_delta_and_other_type_with_reason`, and `load_table_returns_format_tag_vending_key_and_ordered_columns` to the names in Scenario Coverage.
  - Add `list_tables_admits_a_parquet_base_table_with_its_partition_columns` and `load_table_maps_the_uppercase_parquet_format_to_the_parquet_tag`.
  - In `load_table_refuses_an_absent_or_unrecognized_data_source_format`, replace `PARQUET` with `CSV`, `JSON`, and `DELTASHARING`.
  - Extend `catalog_client_trait_and_neutral_types_are_reachable` to construct and observe both new fields. Do not extend `raw_unity_wire_fields_do_not_appear_in_the_neutral_types`.
  - Assert an empty partition-column list in `iceberg_client_tags_every_table_iceberg_with_no_vending_key` (`lakehouse-catalog/src/client_tests.rs`) and in `hive_partitioning_reaches_the_seam_on_enumeration` (`lakehouse-engine/src/adapter/direct_storage_tests.rs`).
- [ ] 1.5 In `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs`, add `lists_a_parquet_base_table_with_every_declared_column`. Rename `excludes_view_non_delta_and_other_type_entries` to `excludes_view_unplannable_format_and_other_type_entries` and add a `CSV` entry.

### B. Unity Parquet reader, seam listing answer, and dispatch

- [ ] 2.1 In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, add `list_parquet_files(store, prefix, partition_columns: &[String], keep: &PartitionKeepPredicate) -> Result<Vec<ParquetFile>, UdfError>`. Reuse `list_data_files`, `data_file_segments`, `parse_partition_segments`, and `decode_partition_value`. Fill every declared column by the uppercase fold, keyed by the caller's spelling. Set `footer` to `None`. Apply `keep` before returning. Add `listing_answer_fills_caller_declared_partition_columns_and_reads_no_footer` to `parquet_directory_tests.rs`.
- [ ] 2.2 Extract `UnityTableStorage` into `crates/lakehouse-engine/src/adapter/pushdown/format/unity_table_storage.rs`. It holds `READ_OPERATION`, `checked_table_root`, `effective_storage`, `table_name`, and `redacted`. `DeltaFormatReader` composes it with unchanged behavior and error texts. Move `redacted_masks_every_effective_storage_secret_in_a_raised_error` to `unity_table_storage_tests.rs`. Give `ensure_table_has_a_mappable_column` a table-kind label parameter, keep the Delta text byte-identical, and make it `pub(super)`. Make `parquet_format_reader::file_entry` `pub(super)`. [expert]
- [ ] 2.3 Add `UnityParquetFormatReader` in `crates/lakehouse-engine/src/adapter/pushdown/format/unity_parquet_format_reader.rs`, per `vs-adapter/unity-parquet-table-planning`:
  - Resolve storage through `UnityTableStorage` and build the table-root store with `build_table_root_store`.
  - Parse each column's `type_json` with `serde_json` into a `delta_kernel` `StructField`, name it by the catalog column name, and force it nullable. Refuse an absent or unparseable `type_json` by name. A Spark writer emits `name`, `type`, `nullable`, and `metadata` at every depth, and `StructField` requires all four.
  - Classify through `build_delta_table_schema(.., ColumnMappingMode::None, table.partition_columns)`. Apply the mappable-column guard. Fail the plan when a partition column is refused.
  - Build the keep predicate from `PartitionPredicate::from_filter` over the string partition columns only.
  - List through `list_parquet_files`, map each file through `file_entry`, and return a `ResolvedScan` with an empty name mapping. Redact every error against the effective storage. [expert]
- [ ] 2.4 Rename `ScanSource::UnityDelta` to `ScanSource::Unity`. Make its `format_reader` arm match `table.format` exhaustively: Delta selects `DeltaFormatReader`, Parquet selects `UnityParquetFormatReader`, and Iceberg returns a `UdfError` naming the table and format. Rename sites: `format/mod.rs` (2), `pushdown/scan_resolution.rs:171`, `format/format_tests.rs:66,109`, and `crates/lakehouse-engine/tests/e2e_unity_test.rs:14,388` (feature `unity-e2e`).
- [ ] 2.5 Give the loopback endpoint in `crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs` a generic prefix listing. Add `unity_parquet_format_reader_tests.rs` with the tests in Scenario Coverage. `a_case_drifted_file_column_binds_to_its_catalog_column` and `a_file_type_outside_the_widening_set_is_refused_naming_the_column` feed the reader's `ResolvedScan` logical fields into `FieldIdExprAdapterFactory::create` against a drifted physical schema. The refusal test then asserts the error from rewriting a `Column` that references the refused field. In `format_tests.rs`, rename `format_reader_refuses_a_non_delta_table_under_the_unity_source` to `format_reader_refuses_an_iceberg_table_under_the_unity_source` and add the Parquet selection test.
- [ ] 2.6 Update the Unity section of `docs/catalogs.md` (lines 215-225) with these rules:
  - Parquet base tables are listed.
  - Partition columns come from the catalog, and values come from `key=value` paths.
  - A table with Databricks partition metadata logging enabled is read from its directories. The reader does not consult the partitions that log registers.
  - Only string partition columns prune files, and no footer is read at plan time.
  - A data file needs the `.parquet` suffix.
  - A data-file column binds to the catalog column whose name it matches ignoring letter case. A data-file type outside identity and the widening set fails every query that reads the column. The error names the column.

### C. Unity Parquet E2E coverage

- [ ] 3.1 Add `feature = "unity-e2e"` to the `raw_parquet` gates in `crates/lakehouse-engine/tests/common/mod.rs` and `tests/common/raw_parquet.rs`. In `e2e_unity_test.rs` `setup()`, before `create_unity_virtual_schema`, write the three `sales_parquet` files under `s3://warehouse/unity_parquet/sales_parquet/`. Register the table through the Unity Catalog REST API with `DELETE` then `POST /tables`, each column carrying `type_json` and `partition_index`. Add `SALES_PARQUET` to `EXPECTED_TABLES`.
- [ ] 3.2 Generalize `resolve_delta_scan` into `resolve_unity_scan`. Add `unity_parquet_table_is_listed_with_its_declared_columns`, `unity_parquet_table_returns_its_rows_and_partition_values`, and `unity_parquet_planning_agrees_under_vended_and_static_credentials`.
- [ ] 3.3 Bring up the stack with `make unity-up` and run `make test-e2e-unity`. Every test MUST pass.

### D. Scan-side case fold and type admission

- [ ] 4.1 In `crates/lakehouse-engine/src/scan/field_id_projection.rs`, extend the identity step of `claim_logical`. After the exact-name match, match an identity-bound logical field (no field-id, no declared physical name) by the uppercase fold. Record an ambiguous fold, in either direction, in `ColumnBinding`, and fail it in `FieldIdExprAdapterFactory::create` naming the logical column and every candidate. Keep this failure in `create`, unlike the type refusal of task 4.2, because its scenario fails the query whether or not it reads the column. This keeps the 14 `bind_columns` calls in `field_id_projection_tests.rs` compiling. The field-id, declared-physical-name, name-mapping, and physical-name steps stay exact.
- [ ] 4.2 In `FieldIdExprAdapterFactory::create`, check each bound column without a nested descriptor before building the delegate. Admit the pair when `types::widening::widen(physical, logical)` returns the logical type, when the timestamp unit and zone rule holds, or when a string-tagged logical field meets a string encoding or a primitive type that `needs_json_fallback` flags. Judge an unadmitted dictionary by its value type. Record every other pair as refused. Do not fail in `create`. DataFusion hands `create` the whole table file schema (`datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:589`), not only the columns a query reads. Key a per-file map by refused logical column index, holding the column name and both types. Store it on `FieldIdExprAdapter` beside `absent_default_by_index`. In `FieldIdExprAdapter::rewrite`, check each `Column` before delegating. If it references a refused index, return a `DataFusionError` naming the table root, the column, and both types. Add the table root to the factory at its three literals: `scan/positional_deletes.rs:952` (from `self.table_root`) and `field_id_projection_tests.rs:87,840`. [expert]
- [ ] 4.3 Add the tests in Scenario Coverage to `field_id_projection_tests.rs` and `type_relaxation_tests.rs`. In `a_refused_column_fails_only_a_rewrite_that_references_it`, create an adapter over a file with one refused column. Assert that a rewrite of another column succeeds. Assert that a rewrite of the refused column fails. Every existing unit test MUST pass unchanged: `casts_on_type_divergence_by_field_id` reads supported row 1, and `a_nested_physical_column_with_no_descriptor_fails_the_cast_rather_than_rendering` still gets an error naming `addr`, now from `rewrite`.
- [ ] 4.4 In `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, rename `merge_schema_false_declares_the_narrow_sampled_type_and_reads_the_fitting_projection` to `merge_schema_false_declares_the_narrow_sampled_type_and_refuses_a_wider_file_column`. Assert that `SELECT ID` returns both files' four rows and that `SELECT ID, PRICE` fails naming `PRICE`. Keep `stale_declaration_decides_the_emitted_width` unchanged. In the direct-storage section of `docs/catalogs.md`, update the `MERGE_SCHEMA` row and the mixed-timestamp-units limitation:
  - Under `'FALSE'`, a file whose column type is wider than the sampled type fails every query that reads the column.
  - The `'FALSE'` workaround for mixed timestamp units reads a file only when its unit equals the sampled unit or is coarser.
- [ ] 4.5 Run `cargo test`, then `docker compose up -d` and `make test-e2e`. If an existing test fails on a refused pair, stop and report the test and the pair. Do not widen the admitted set to make the test pass.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Catalog admission and neutral metadata | 1.1-1.5 | None | spec deltas `vs-adapter/unity-catalog-client`, `vs-adapter/unity-catalog-create-virtual-schema`, `vs-adapter/catalog-crate-public-surface-extensions`, plus `crates/lakehouse-catalog/src/unity/client.rs`, `crates/lakehouse-catalog/src/client.rs`, `crates/lakehouse-engine/src/types/mapping.rs`, `crates/lakehouse-engine/src/adapter/direct_storage.rs`, the test files in tasks 1.2 and 1.4, `crates/lakehouse-catalog/src/unity/client_tests.rs`, `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` |
| D: Scan-side case fold and type admission | 4.1-4.5 | None | spec deltas `datafusion-scan/scan-execution-field-id-projection`, `datafusion-scan/type-relaxation`, `e2e-harness/direct-storage-e2e`, plus `crates/lakehouse-engine/src/scan/field_id_projection.rs`, `crates/lakehouse-engine/src/scan/positional_deletes.rs`, `crates/lakehouse-engine/src/types/widening.rs` (read only), `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs`, `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs`, `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, the direct-storage section of `docs/catalogs.md` |
| B: Unity Parquet reader and dispatch | 2.1-2.6 | A (reads `CatalogTable.partition_columns` and `type_json`), D (task 2.5 tests the adapter's fold and refusal) | spec deltas `vs-adapter/unity-parquet-table-planning`, `vs-adapter/parquet-directory-seam`, `vs-adapter/delta-table-planning`, `vs-adapter/pushdown-format-neutral-resolution`, plus `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, `crates/lakehouse-engine/src/adapter/pushdown/format/`, `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs`, `crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs`, the Unity section of `docs/catalogs.md` |
| C: Unity Parquet E2E | 3.1-3.3 | B (shares `tests/e2e_unity_test.rs`, needs the reader) | spec delta `unity-e2e/unity-catalog-e2e-harness-parquet-queries`, plus `crates/lakehouse-engine/tests/e2e_unity_test.rs`, `crates/lakehouse-engine/tests/common/mod.rs`, `crates/lakehouse-engine/tests/common/raw_parquet.rs` |

A and D share no file and run in parallel. B runs after both, and C runs after B. B consumes A's types and D's adapter rules, and B and C both edit `tests/e2e_unity_test.rs`. B edits only the Unity section of `docs/catalogs.md`, after D edits the direct-storage section.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `delta_base_skip_reason` in `crates/lakehouse-catalog/src/unity/client.rs` | Replaced by the admission function of task 1.3 |
| Functions, constant | `checked_table_root`, `effective_storage`, `table_name`, `redacted`, `READ_OPERATION` in `delta_format_reader.rs` | Moved to `unity_table_storage.rs` |
| Test | `redacted_masks_every_effective_storage_secret_in_a_raised_error` in `delta_format_reader_tests.rs` | Moved to `unity_table_storage_tests.rs` |
| Function | `resolve_delta_scan` in `tests/e2e_unity_test.rs` | Replaced by `resolve_unity_scan` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A Unity Parquet table lists its files through the shared directory seam and reads no footer | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/unity_parquet_format_reader_tests.rs` | `files_are_listed_through_the_seam_and_no_footer_is_read` |
| The logical schema is the catalog's declared column list | Integration | same | `logical_schema_is_the_catalog_column_list_classified_by_the_spark_type_classifier`, `a_column_without_a_readable_type_json_is_refused_by_name`, `a_table_whose_every_column_is_refused_is_refused_as_a_whole`, `a_case_drifted_file_column_binds_to_its_catalog_column`, `a_file_type_outside_the_widening_set_is_refused_naming_the_column` |
| Partition columns come from the catalog and partition values from the file paths | Integration | same | `partition_columns_come_from_partition_index_and_values_from_paths`, `a_refused_partition_column_fails_the_plan` |
| A predicate on a string partition column prunes files and no other predicate does | Integration | same | `only_string_partition_columns_prune_files` |
| Storage is resolved through the table's own catalog exactly as for a Delta table | Integration | same | `vending_without_a_vending_key_errors_and_never_falls_back_to_static`, `empty_storage_location_errors_identically_under_both_credential_modes` |
| The listing answer serves a caller that declares its own partition columns | Integration | `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs` | `listing_answer_fills_caller_declared_partition_columns_and_reads_no_footer` |
| The format reader is selected at one site and refuses a mismatched pairing | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/format_tests.rs` | `format_reader_refuses_an_iceberg_table_under_the_unity_source`, `format_reader_selects_the_delta_reader_for_a_delta_table_without_contacting_the_catalog`, `format_reader_selects_the_unity_parquet_reader_for_a_parquet_table_without_contacting_the_catalog` |
| A Unity Catalog table's identity survives the round trip from the involved table | Integration | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `unity_table_identity_round_trips_through_the_recorded_identifier`, `a_recorded_identifier_recovers_its_namespace_segments_and_table_name` |
| A table the reader cannot plan fails the query loud at plan time | Unit | `format_tests.rs`, `delta_format_reader_tests.rs` | `format_reader_refuses_an_iceberg_table_under_the_unity_source`, `a_table_whose_every_column_is_refused_is_refused_as_a_whole` |
| The Unity Catalog session is reached only through the shared catalog-client trait | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-catalog/src/unity/client_tests.rs` | `catalog_client_trait_and_neutral_types_are_reachable`, `load_table_returns_format_tag_vending_key_partition_columns_and_ordered_columns` |
| The client lists tables in a configured catalog and schema | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `lists_tables_in_catalog_schema`, `list_tables_tags_each_admitted_table_by_its_own_format` |
| The client admits a Parquet base table and reports its partition columns | Integration | same | `list_tables_admits_a_parquet_base_table_with_its_partition_columns` |
| The client routes a view, an unplannable format, and any other table type into the skipped set with a reason | Integration | same | `skips_view_unplannable_format_and_other_type_with_reason`, `admission_*` units |
| The client retrieves a table's metadata including its columns | Integration | same | `loads_table_metadata_with_columns`, `load_table_returns_format_tag_vending_key_partition_columns_and_ordered_columns` |
| The single-table load refuses a data source format the crate cannot name | Integration | same | `load_table_refuses_an_absent_or_unrecognized_data_source_format`, `load_table_maps_the_uppercase_parquet_format_to_the_parquet_tag` |
| Create virtual schema enumerates every table in the configured Unity Catalog namespace | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `enumerates_unity_namespace_tables`, `lists_a_parquet_base_table_with_every_declared_column` |
| Create virtual schema excludes every entry that is not a plannable base table and warns per exclusion | Integration | same | `excludes_view_unplannable_format_and_other_type_entries` |
| The neutral table's partition columns and the Unity column's type descriptor extend the crate's public surface through an explicit reviewed edit | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-catalog/src/client_tests.rs`, `crates/lakehouse-engine/src/adapter/direct_storage_tests.rs`, `format_tests.rs` | `catalog_client_trait_and_neutral_types_are_reachable`, `iceberg_client_tags_every_table_iceberg_with_no_vending_key`, `hive_partitioning_reaches_the_seam_on_enumeration`, `format_reader_selects_the_unity_parquet_reader_for_a_parquet_table_without_contacting_the_catalog` |
| A Unity Parquet table appears in the createVirtualSchema listing | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_parquet_table_is_listed_with_its_declared_columns` |
| A Unity Parquet table returns its rows and partition values end to end | Integration (E2E) | same | `unity_parquet_table_returns_its_rows_and_partition_values`, `unity_suite_fails_when_stack_unavailable` |
| A Unity Parquet table's scan resolves identically under vended and static credentials | Integration (E2E) | same | `unity_parquet_planning_agrees_under_vended_and_static_credentials` |
| An identity-bound field binds a file column whose name differs only in letter case | Integration | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `identity_bound_field_reads_a_case_folded_file_column_rows`, `an_exact_name_match_wins_over_a_case_folded_one`, `an_ambiguous_case_folded_match_fails_naming_every_candidate`, `field_id_and_declared_physical_name_bindings_stay_case_exact` |
| A narrow physical column binds to the current wider logical type and is cast per file | Integration | `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` | `a_narrow_physical_column_is_cast_to_the_current_logical_type_per_file`, `a_narrower_logical_type_than_the_file_is_refused_not_narrowed` |
| A physical type outside identity and the supported set is refused before any cast | Integration | same, `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `a_physical_type_outside_identity_and_the_supported_set_is_refused_before_any_cast`, `timestamp_unit_and_zone_variants_are_admitted_unless_an_instant_shifts`, `string_encodings_and_json_fallback_types_are_admitted_under_a_string_declaration`, `a_dictionary_column_is_judged_by_its_value_type`, `the_refusal_holds_under_every_binding_key`, `a_refused_column_fails_only_a_rewrite_that_references_it` |
| MERGE_SCHEMA FALSE declares the sampled footer's type on the refresh and the scan path | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `merge_schema_false_declares_the_narrow_sampled_type_and_refuses_a_wider_file_column`, `stale_declaration_decides_the_emitted_width` |

### Manual Testing

Run these commands after `make cross-udf-build`, `make unity-up`, and one `make test-e2e-unity` run, which registers `sales_parquet` and creates `UNITY_DELTA_E2E_VS`. The `type-relaxation` row also needs one `make test-e2e` run on the base stack, which creates `DIRECT_LAKEHOUSE_NARROW`. `$DSN` is `exasol://sys:exasol@localhost:28563?validateservercertificate=0`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| unity-catalog-client | `curl -s "http://localhost:${LH_UNITY_PORT:-18080}/api/2.1/unity-catalog/tables/unity.delta_e2e.sales_parquet"` | JSON with `"data_source_format":"PARQUET"`, `"table_type":"EXTERNAL"`, and `partition_index` 0 on `year` and 1 on `region` |
| unity-catalog-create-virtual-schema | `exapump sql "SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA='UNITY_DELTA_E2E_VS' AND COLUMN_TABLE='SALES_PARQUET' ORDER BY COLUMN_ORDINAL_POSITION" -d "$DSN"` | `ID DECIMAL(20,0)`, `AMOUNT DOUBLE`, `YEAR DECIMAL(10,0)`, `REGION VARCHAR(2000000)` |
| unity-parquet-table-planning | `exapump sql "SELECT * FROM UNITY_DELTA_E2E_VS.SALES_PARQUET ORDER BY ID" -d "$DSN"` | Four rows, each with the `YEAR` and `REGION` of its directory |
| parquet-directory-seam | `exapump sql "SELECT COUNT(*) FROM UNITY_DELTA_E2E_VS.SALES_PARQUET WHERE REGION = 'eu'" -d "$DSN"` | The count of fixture rows under `region=eu` |
| delta-table-planning | `exapump sql "SELECT COUNT(*) FROM UNITY_DELTA_E2E_VS.BASIC_PARTITIONED" -d "$DSN"` | The Delta fixture's row count, unchanged from before this plan |
| pushdown-format-neutral-resolution | `exapump sql "EXPLAIN VIRTUAL SELECT * FROM UNITY_DELTA_E2E_VS.SALES_PARQUET WHERE REGION = 'eu'" -d "$DSN"` | A pushdown SQL that calls the scan UDF with a spec listing only `region=eu` files |
| catalog-crate-public-surface-extensions | `cargo test -p lakehouse-catalog --test catalog_public_surface` | 0 failures |
| unity-catalog-e2e-harness-parquet-queries | `make test-e2e-unity` | 0 failures. With the stack down, the suite fails rather than skips. |
| scan-execution-field-id-projection | `cargo test -p lakehouse-engine --lib field_id_projection` | 0 failures, including the four case-fold tests |
| type-relaxation | `exapump sql "SELECT ID, PRICE FROM DIRECT_LAKEHOUSE_NARROW.WIDENED" -d "$DSN"` | An error naming `PRICE`, `Float32`, `Float64`, and the `widened/` storage location |
| direct-storage-e2e | `docker compose up -d && make test-e2e` | 0 failures |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `docker compose up -d && make test-e2e` | 0 failures |
| E2E (Unity) | `make unity-up && make test-e2e-unity` | 0 failures |
| Lint | `cargo clippy --all-targets` and `cargo clippy --all-targets --features unity-e2e` | 0 errors or warnings |
| Format | `cargo fmt --check` | No changes |
