# Plan: add-column-source-notes

## Summary

Every catalog kind records each virtual column's declared source shape (a format-neutral `LogicalField`, its partition position, and its refusal reason) in that column's `adapterNotes` at CREATE, REFRESH, and SET, and every pushdown plans from those notes instead of deriving a schema per format reader (#426). The work lands in three phases matching the issue's per-format PRs: notes plus direct storage, Iceberg REST plus Glue Iceberg, and Delta plus Unity Parquet plus Glue Parquet.

## Context

- Only the mapped Exasol `dataType` reaches Exasol today. The source shape (Iceberg `timestamptz`, `decimal(38,10)`, `list<struct<...>>`, a Delta type, a binding key) is lost at CREATE and re-derived at every pushdown, once per format reader, each reader differently.
- The Exasol Virtual Schema protocol persists a per-column `adapterNotes` string in `SYS.EXA_ALL_VIRTUAL_COLUMNS.ADAPTER_NOTES` and returns it in `involvedTables[].columns[].adapterNotes` on every pushdown, single-table and join (verified on Docker Exasol 2025.1.16 by spike branch `spike/426-direct-storage-column-notes`).
- The spike planned direct storage from the notes alone: `EXPLAIN VIRTUAL` planned and pruned after the data files were overwritten with non-Parquet bytes, and the scan UDF was the first to read a footer. Its production delta was +202 / −143 lines.
- The schema-level `adapterNotes` limit is 2,000,000 characters (sqlCode `04000`, measured). The issue reports the table-level limit is lower than its column type suggests, so the per-column limit is unknown until measured.
- CREATE matches partition-key directory segments exactly, while the footer-free listing matches them case-insensitively; the interview fixes one rule for both.
- Unity's `type_json` is the Delta `StructField` JSON, and Unity's table-level `properties` map is where the Delta table property `delta.columnMapping.mode` belongs. The Databricks List tables reference returns `properties` on the list sweep unless `omit_properties` is set. Whether Databricks and OSS Unity Catalog carry the column-mapping metadata and the mode is unverified. The Unity client deserializes no `properties` today, and `scripts/unity/seed.sh` registers each Delta fixture with empty column metadata and no properties, because the pushdown reads the schema from the log.
- #425 moves `TABLE_MAP` into table-level `adapterNotes` and edits the same listing and `involvedTables` code. #412 (footer-statistics pruning) is closed as superseded.
- The architecture changes (column notes owned by vs-adapter, the declaration built at CREATE from what each catalog client returns, table properties on the neutral table, the Delta declaration check at pushdown) are in `architecture.md`.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| Column Source Notes | NEW | `vs-adapter/column-source-notes/spec.md` |
| Refresh And Set Properties | CHANGED | `vs-adapter/refresh-and-set-properties/spec.md` |
| Direct-Storage Table Planning | CHANGED | `direct-storage/direct-storage-table-planning/spec.md` |
| Direct-Storage Hive Partitioning | CHANGED | `direct-storage/direct-storage-hive-partitioning/spec.md` |
| Parquet Directory Seam | CHANGED | `direct-storage/parquet-directory-seam/spec.md` |
| Parquet Directory Seam: File Listing | CHANGED | `direct-storage/parquet-directory-seam-file-listing/spec.md` |
| Direct-Storage Virtual-Schema Properties | CHANGED | `direct-storage/direct-storage-properties/spec.md` |
| Direct-Storage Table Discovery | CHANGED | `direct-storage/direct-storage-table-discovery/spec.md` |
| Catalog Crate Public Surface Extensions | CHANGED | `catalog/catalog-crate-public-surface-extensions/spec.md` |
| Pushdown Module Structure | CHANGED | `pushdown/pushdown-module-structure/spec.md` |
| Format-Neutral Pushdown Resolution | CHANGED | `file-planning/pushdown-format-neutral-resolution/spec.md` |
| Pushdown File Resolution | CHANGED | `file-planning/pushdown-planning-file-resolution/spec.md` |
| Pushdown File Pruning | CHANGED | `file-planning/pushdown-file-pruning/spec.md` |
| Glue Table Planning | CHANGED | `glue/glue-table-planning/spec.md` |
| Unity Catalog Parquet Table Planning | CHANGED | `unity-catalog/unity-parquet-table-planning/spec.md` |
| Unity Catalog Create Virtual Schema | CHANGED | `unity-catalog/unity-catalog-create-virtual-schema/spec.md` |
| Unity Catalog Native REST Client | CHANGED | `unity-catalog/unity-catalog-client/spec.md` |
| Delta Table Planning | CHANGED | `delta/delta-table-planning/spec.md` |
| Delta Plan-Time File Pruning | CHANGED | `delta/delta-file-pruning/spec.md` |
| Delta Schema Type Mapping | CHANGED | `delta/delta-type-mapping/spec.md` |

## Impact

- **Breaking:** a virtual schema created before this change carries no column notes. Every pushdown on it fails, naming the table, the column, and `ALTER VIRTUAL SCHEMA ... REFRESH`, until it is refreshed or recreated. No migration path exists.
- Every kind plans against the schema declared at the last CREATE, REFRESH, or SET. A source column added later stays invisible until `REFRESH`, and a type widened at the source fails the query at scan time, naming the column and both types.
- Direct storage: a pushdown reads no Parquet footer, so planning cost no longer grows with the kept files' footers. `MERGE_SCHEMA` and `HIVE_PARTITIONING` changes, and new files that change a column's type or add a partition key, take effect at the next `REFRESH` or `SET`.
- **Breaking:** two spellings of one partition key (such as `year=` and `Year=`) now fail CREATE under both merge modes and fail every query over a direct-storage or Unity Parquet table whose files carry both. Before, CREATE failed only for the files the merge mode read, and the Unity Parquet listing filled the key from either spelling silently.
- Iceberg: a column renamed at the source keeps reading its values under its declared name until `REFRESH`, where it failed before. A column dropped at the source reads the values older data files store until `REFRESH`, where it failed before. A filter on a renamed column prunes by its declared field id.
- Unity Catalog: `createVirtualSchema`, `refresh`, and `setProperties` still read catalog metadata only, with no added request, credential, or storage access. A Delta table's notes come from Unity's `type_json` and the table's `properties`, so its catalog entry must carry the Delta schema's column metadata and `delta.columnMapping.mode` as the Delta log holds them. Databricks is expected to (task 3.1 checks), and an OSS Unity Catalog registrant must send them.
- **Breaking (Unity Catalog):** a Delta table whose catalog entry disagrees with its log on the column-mapping mode or the partition columns now fails every query, naming the disagreement, where the reader trusted the log before. A Delta column whose column-mapping annotation is unusable is refused alone, where every query on the table failed before. A table the reader-feature gate refuses is still listed and still fails at query time.
- Unity Delta, Unity Parquet, and Glue Parquet: a catalog column change takes effect at the next `REFRESH`. A Delta column renamed or dropped at the source under `name` or `id` mode behaves like the Iceberg case above until `REFRESH`.
- A column whose note exceeds the measured per-column limit fails CREATE, REFRESH, and SET naming the table and the column.
- Iceberg and Delta: a column declared non-nullable that the source later allows NULL in fails every query on its table, naming the column and `REFRESH`, until `REFRESH`. A dropped column declared non-nullable with no `initial-default` fails a query that reads a file written after the drop, with the required-absent error, until `REFRESH`.
- Specs: the recorded specs keep describing the pre-change behavior until `/speq:record` runs with PR 3 (§ Delivery).

## Requirements

| Requirement | Details |
|-------------|---------|
| No footer read at direct-storage pushdown | `declared_columns` plumbing and `absent_declared_fields` deleted (issue acceptance 1) |
| No reader derives a schema at pushdown | Every kind reads `logical_schema`, partition columns, and `refused_columns` from the notes (issue acceptance 2) |
| No one-arm parameter | No resolver or reader signature carries a parameter read by one format arm only, after task 3.6 (issue acceptance 3) |
| Net production lines go down | Each phase reports its net production line delta, and each delta MUST be negative (issue acceptance 4, decision [13]) |
| Spec check | Iceberg § Column Projection, § Schema Evolution, § Scan Planning and Delta PROTOCOL.md § Column Mapping and § Type Widening are quoted in the deltas; the Iceberg and Delta dropped-column behaviors and a Delta type change the declaration does not record are scoped exceptions |

## Dependencies

- #425 edits `build_listing_virtual_tables` and the `involvedTables` extraction; whichever change merges second rebases (decision [14]).
- Task 3.1 needs a Databricks workspace that can create a Unity Catalog Delta table with column mapping, and a token for it, supplied by a human. No such workspace is configured in `test.env`. If none is available, task 3.1 cannot pass its gate and Phase 3's Delta tasks wait. Its OSS half runs against the local Unity Catalog container (`make unity-up`).
- Open question: who supplies the Databricks workspace and token for task 3.1. No `(#TBD)` follow-up remains.
- DuckDB CLI for task 1.2.

## Delivery

- The plan ships as three PRs, one per phase. PR 1 implements group A (tasks 1.1 to 1.11), PR 2 implements group B (tasks 2.1 to 2.5), and PR 3 implements groups C, D, and E (tasks 3.1 to 3.9). Each implementation run is scoped to its PR's groups, and each PR leaves the later tasks unchecked.
- `/speq:record` runs once, with the PR that completes Phase 3. Until then the recorded specs describe the pre-change behavior of direct storage, Iceberg, Unity, and Glue, although PRs 1 and 2 have changed the direct-storage and Iceberg code (§ Impact).
- If task 3.1's gate fails, PR 3 holds tasks 3.2 and 3.3 and its own line count. Tasks 3.4 to 3.8 wait, and task 3.6 waits too, because it changes the Delta reader's `resolve_scan`. The plan returns for a human revision before any recording.

## Implementation Tasks

### Phase 1: Column notes and direct storage

- [ ] 1.1 Measure live, through exapump against the local Docker Exasol 2025.x and 8.29.x images, the largest per-column `adapterNotes` length Exasol persists on `createVirtualSchema` and on `refresh` (a minimal Lua adapter script that declares one column with a note of N characters, binary-searched on N), and whether one table's notes have a combined bound below the per-column limit times its column count. Record the per-column value in the Background of `vs-adapter/column-source-notes` and in decision [5]. If a combined bound exists, stop and revise the plan.
- [ ] 1.2 Write a hive-partitioned Parquet directory with DuckDB that mixes `Year=` and `year=` directories (`COPY ... (FORMAT PARQUET, PARTITION_BY (...))` plus a renamed copy), read it with `read_parquet(..., hive_partitioning = true)`, and record in decision [6] which column names DuckDB writes and whether it merges, splits, or rejects the two spellings.
- [ ] 1.3 Add `crates/lakehouse-engine/src/adapter/column_notes.rs` with `column_notes_tests.rs`: the note type (`sourceType`, `partition` position, `refused`), the encoder with the per-column limit check from 1.1, and the parse of one table's notes into a declared schema (logical fields without refused columns, partition columns ordered by position, refused columns, the whole-table refusal) with the missing-or-unreadable-note error. Move `RefusedColumn`, `binary_refusal`, `binary_cause`, `without_refused_columns`, and `ensure_table_has_a_mappable_column` from `pushdown/format/mod.rs` into it, so `column_notes` imports nothing from `adapter::pushdown`. The pushdown façade keeps re-exporting `RefusedColumn` under the same name, so both probes keep their item lists.
- [ ] 1.4 Add the opaque `declaration: Option<String>` field to `CatalogColumn` in `crates/lakehouse-catalog/src/client.rs`, leave it `None` in every catalog-crate constructor, and edit `crates/lakehouse-catalog/tests/catalog_public_surface.rs`.
- [ ] 1.5 Build each direct-storage column's declaration at enumeration in `adapter/direct_storage.rs` from the one fold: move `logical_schema` and `parquet_refusal` out of `pushdown/format/parquet_format_reader.rs`, take partition positions from the seam's partition columns, and keep the Exasol type tag unchanged.
- [ ] 1.6 Make `build_listing_virtual_tables` write each column's `adapterNotes` through the `column_notes` encoder when the column carries a declaration, and fail the statement on an encoder error, redacted like every listing error.
- [ ] 1.7 In `adapter/parquet_directory.rs`: delete the `keep` predicate of `resolve_parquet_directory` and `ParquetFile.footer`; replace the two partition fills with one fill that matches key names under the uppercase fold, keeps values verbatim, and fails on two spellings of one declared key; run its spelling check over every listed file at enumeration under both merge modes, and before the file-keep predicate in `list_parquet_files`.
- [ ] 1.8 Rewire the pushdown for direct storage: one extraction pass over `involvedTables[].columns` in `pushdown/support.rs` yields each column's name, Exasol type, and note for the single-table path and each join leg; `TableScanResolver::resolve` takes the involved table's raw notes; `ScanSource::DirectParquet` drops the directory options and the declared columns and carries the raw notes until task 3.6 moves them to the reader's parameter; `ParquetFormatReader` parses the notes and lists through `list_parquet_files`. Delete the `declared_columns` plumbing, `absent_declared_fields`, `plannable_schema`, and the `DirectoryOptions` of `RequestSession::DirectStorage`.
- [ ] 1.9 Write and update the Phase 1 tests listed under Scenario Coverage, carrying one `/// Scenario:` line per scenario. Port the spike's E2E test. Delete `stale_declaration_decides_the_emitted_width` and every unit test of the deleted `keep` predicate, footer field, and `declared_columns` code, per `specs/testing.md` § Regression guards.
- [ ] 1.10 Update `docs/catalogs.md` (direct storage): `MERGE_SCHEMA`, `HIVE_PARTITIONING`, and file changes take effect at `REFRESH`; the partition-key case rule; the no-migration `REFRESH` requirement.
- [ ] 1.11 Count the net production line delta of Phase 1 (`git diff --numstat <phase base> -- 'crates/*/src/**' ':!*_tests.rs'`), confirm it is negative, and record it for the PR description.

### Phase 2: Iceberg REST and Glue Iceberg

- [ ] 2.1 Widen `ColumnSourceType::Iceberg` to carry the column's whole Iceberg field (`NestedFieldRef`): fill it in `iceberg_catalog_table`, map the Exasol type from its `field_type`, and edit the public-surface probe.
- [ ] 2.2 Add `declare_columns(&CatalogTable)` in `pushdown/format/mod.rs`: decode a column's declaration slot when present, otherwise build an Iceberg column's logical field (field id, name, Arrow tag, nullability, `initial-default`, nested members) and binary refusal from its field, reusing the builders in `format/iceberg.rs`. Call it from `build_listing_virtual_tables` for every table. Swap `build_logical_schema` for `declare_columns` on the pushdown façade, update both probes, and move every test that called `build_logical_schema` to the added item.
- [ ] 2.3 Make the Iceberg reader take its logical schema and refused columns from the notes, reached through `ScanSource::Iceberg` until task 3.6, and delete its pushdown `plannable_schema`. Translate the filter in `adapter/iceberg_predicate.rs` through each column's declared field id to the name the current schema gives that id, and leave a column whose declared id the current schema lacks untranslated. Keep the date-promotion refusal and the name mapping on the current metadata. Fail the query when a declared non-nullable column's field id is optional in the current schema, naming the table, the column, and `REFRESH`. [expert]
- [ ] 2.4 Write and update the Phase 2 tests listed under Scenario Coverage: unit tests in `iceberg_predicate_tests.rs` and `iceberg_tests.rs`, and E2E tests in `tests/e2e_refresh_test.rs` (schema changes through `rest_replace_current_schema`, including a required column made optional and a dropped optional column) and `tests/e2e_glue_test.rs`.
- [ ] 2.5 Count the net production line delta of Phase 2, confirm it is negative, and record it for the PR description.

### Phase 3: Unity Parquet, Glue Parquet, and Delta

- [ ] 3.1 Research gate, live against the local OSS Unity Catalog container (`make unity-up`) and a human-supplied Databricks workspace. On OSS, register the `cdf-column-mapping-name-mode` fixture through `POST /tables` with its log's `metaData.configuration` as `properties` and each column's log `StructField` JSON as `type_json`. On Databricks, create a `name`-mode column-mapped Delta table with a nested struct column and a partition column, and an `id`-mode table if the workspace allows one. Against each catalog, through `GET /tables` without `omit_properties` and `GET /tables/{full_name}`, record in decision [8]: (a) whether `properties` come inline on the list sweep or only from the per-table GET (on Databricks, also whether the mode appears only under `delta_runtime_properties_kvpairs` or with `include_delta_metadata`); (b) whether each top-level and nested field's `type_json` metadata carries `delta.columnMapping.physicalName` and `delta.columnMapping.id`; (c) whether `properties` carries `delta.columnMapping.mode`; (d) whether `partition_index` follows the log's `partitionColumns`; and, without gating, (e) whether `type_json` metadata carries `delta.typeChanges`. Gate: if either catalog fails (a), (b), (c), or (d) on the list sweep, stop tasks 3.4, 3.5, and 3.7 and the Delta part of 3.8, and hand the plan back for a human revision. Add no log-read fallback.
- [ ] 3.2 Extend `declare_columns` to Unity Parquet and Glue Parquet tables: run `catalog_schema` (moved unchanged from the reader) at CREATE, take partition positions from `CatalogTable.partition_columns`, and record column-level refusals in the notes. Make `CatalogParquetFormatReader` take its logical schema, partition columns, and partition types from the notes; the whole-table and refused-partition-column refusals run on the parsed declaration at pushdown.
- [ ] 3.3 Write and update the tests for 3.2 listed under Scenario Coverage (`catalog_parquet_format_reader_tests.rs`, `format_tests.rs`, `tests/e2e_unity_test.rs`, `tests/e2e_glue_test.rs`).
- [ ] 3.4 Declare Unity Delta columns from catalog metadata. In `crates/lakehouse-catalog`: add `properties: BTreeMap<String, String>` to `CatalogTable` (empty from every client except Unity), deserialize `TableInfo.properties` in `unity/client.rs` and copy it verbatim in `neutral_table`, and edit `tests/catalog_public_surface.rs`. In the engine: give `catalog_schema` the column-mapping mode as a parameter, and add the Delta arm of `declare_columns` that parses the mode from `properties["delta.columnMapping.mode"]` (absent means `none`) and classifies the columns' `type_json` with it, taking partition positions from `CatalogTable.partition_columns`. Make `classify_spark_schema` record a malformed or missing column-mapping annotation, a malformed `delta.typeChanges` annotation, and an unrecognized mode value as column refusals instead of errors (decision [17]), so it returns no `Result`. Keep the Exasol type mapping from `type_json` unchanged.
- [ ] 3.5 Make `DeltaFormatReader` take its logical fields, partition columns, and refusals from the notes, and delete `build_delta_table_schema`. After the gate, check the declaration against the snapshot (decision [18]): each non-refused field carries exactly the key `column_mapping_mode()` selects, and the declared partition columns equal `partition_columns()` in order, matched by binding key under `name` or `id` and by name under `none`; fail the query on a disagreement. In the same check, fail the query when a declared non-nullable column is nullable in the snapshot. Translate the filter in `delta_predicate.rs` through each column's declared physical name or field id to the current snapshot's display name, leaving a column whose key the snapshot lacks untranslated, and key each file's partition values by the declared partition columns. [expert]
- [ ] 3.6 Hoist the note parse into `TableScanResolver::resolve` as its one site: `FormatReader::resolve_scan` takes the parsed declared schema and returns only files, effective storage, table root, and name mapping; the resolver assembles `ResolvedScan` with the declared logical schema, partition columns, and refused columns; delete the raw-notes parameter. No `ScanSource` variant carries notes afterwards, so `ScanSource::DirectParquet` holds the store and the table root only. Write the tests this plan maps to `format_tests.rs`, `scan_resolution_tests.rs`, and `pushdown_tests.rs` for the format-neutral resolution and third-source scenarios. [expert]
- [ ] 3.7 Write and update the tests for 3.4 and 3.5 listed under Scenario Coverage. In `scripts/unity/seed.sh`, register every Delta fixture from its log's latest `metaData` action: each column's `type_json` is the log's `StructField` JSON with its metadata, `partition_index` follows `partitionColumns`, and `properties` is `metaData.configuration`, replacing the hand-written advisory columns and the nested-as-`STRING` registration. Register in a second schema, `delta_catalog_copy`, the tables the check and refusal tests need: `cm_name_mode` without the mode property, `basic_partitioned` without `partition_index`, `cm_name_mode` with `value` stripped of `delta.columnMapping.physicalName`, `cm_name_mode` with the mode `label`, `cm_name_mode` with `"nullable": false` in the `type_json` of `id`, and a Delta table whose storage location holds no log. Add an integration test over a temporary copy of `cdf-column-mapping-name-mode` with an appended rename-swap commit, and declaration and check tests over the vendored fixtures. `unity_delta_unsupported_reader_feature_fails_the_query_loud` stays unchanged.
- [ ] 3.8 Update `docs/catalogs.md` (Unity Catalog): Delta and Parquet columns are declared from catalog metadata at `CREATE` and `REFRESH`; a Delta table's catalog entry must carry the log's column metadata in `type_json` and `delta.columnMapping.mode` in `properties`, and a disagreement with the log fails queries; a table the reader-feature gate refuses is listed and fails at query time; catalog changes take effect at `REFRESH`.
- [ ] 3.9 Count the net production line delta of Phase 3 and the cumulative delta of the three phases, confirm both are negative, and record them for the PR description.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Column notes and direct storage | 1.1-1.11 | — | spec deltas `vs-adapter/column-source-notes`, `vs-adapter/refresh-and-set-properties`, `direct-storage/*`, `catalog/catalog-crate-public-surface-extensions` (declaration slot); `crates/lakehouse-engine/src/adapter/{column_notes.rs,mod.rs,direct_storage.rs,parquet_directory.rs}`, `adapter/pushdown/{support.rs,scan_resolution.rs,mod.rs,joins/}`, `adapter/pushdown/format/{mod.rs,parquet_format_reader.rs}`, `crates/lakehouse-catalog/src/client.rs`, `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, `docs/catalogs.md` |
| B: Iceberg declaration and field-id pruning | 2.1-2.5 | A (shares `adapter/mod.rs`, `pushdown/format/mod.rs`, `lakehouse-catalog/src/client.rs`) | spec deltas `file-planning/pushdown-planning-file-resolution`, `file-planning/pushdown-file-pruning`, `glue/glue-table-planning` (Iceberg scenario), `pushdown/pushdown-module-structure` (first scenario), `catalog/catalog-crate-public-surface-extensions` (Iceberg field); `adapter/pushdown/format/iceberg.rs`, `adapter/iceberg_predicate.rs`, `adapter/pushdown/format/mod.rs`, `types/mapping.rs`, `adapter/pushdown_surface_probe_tests.rs`, `tests/pushdown_public_surface.rs`, `tests/e2e_refresh_test.rs`, `tests/e2e_glue_test.rs` |
| C: Catalog-declared Parquet declaration | 3.2-3.3 | B (shares `declare_columns` in `pushdown/format/mod.rs`) | spec deltas `unity-catalog/unity-parquet-table-planning`, `glue/glue-table-planning` (Parquet scenario); `adapter/pushdown/format/{catalog_parquet_format_reader.rs,mod.rs}`, `tests/e2e_unity_test.rs` (Parquet tests), `tests/e2e_glue_test.rs` |
| D: Delta declaration from catalog metadata, declaration check, and key-bound pruning | 3.1, 3.4, 3.5, 3.7, 3.8 | C (shares `declare_columns` and `catalog_schema` in `pushdown/format/`) | spec deltas `unity-catalog/unity-catalog-create-virtual-schema`, `unity-catalog/unity-catalog-client`, `delta/delta-table-planning`, `delta/delta-file-pruning`, `delta/delta-type-mapping`, `catalog/catalog-crate-public-surface-extensions` (table properties); `crates/lakehouse-catalog/src/{client.rs,unity/client.rs}`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `adapter/pushdown/format/{mod.rs,catalog_parquet_format_reader.rs,delta_schema.rs,delta_format_reader.rs,delta_replay.rs,delta_predicate.rs}`, `scripts/unity/seed.sh`, `tests/e2e_unity_test.rs`, `docs/catalogs.md` |
| E: One declaration parse in the resolver | 3.6, 3.9 | D (touches every reader's `resolve_scan`) | spec delta `file-planning/pushdown-format-neutral-resolution`; `adapter/pushdown/scan_resolution.rs`, `adapter/pushdown/format/{mod.rs,iceberg.rs,delta_format_reader.rs,catalog_parquet_format_reader.rs,parquet_format_reader.rs}`, `adapter/pushdown/mod.rs`, `adapter/pushdown/joins/planning.rs` |

The groups run in sequence: each phase is one PR, and groups B to E edit files an earlier group edits.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `adapter/pushdown/format/parquet_format_reader.rs` `absent_declared_fields`, `plannable_schema` | The notes declare every column (Phase 1) |
| Field | `ScanSource::DirectParquet.declared_columns`, `.options`; `ParquetFormatReader.declared_columns`, `.options` | Replaced by the notes (Phase 1) |
| Parameter | `TableScanResolver::resolve` `declared_columns`; `resolve_one_join_side` `declared_columns` | Replaced by the notes (Phase 1), then by the parsed declaration (Phase 3) |
| Field | `RequestSession::DirectStorage.options` | Directory options apply at enumeration only (Phase 1) |
| Parameter | `resolve_parquet_directory` `keep` | Enumeration keeps every file (Phase 1) |
| Field | `ParquetFile.footer` | No pushdown reads a footer; #412 superseded (Phase 1) |
| Function | duplicate partition fill closures in `parquet_directory.rs` | One case-folding fill with the spelling check (Phase 1) |
| Function | separate column-type and column-note extraction in `pushdown/support.rs` | One pass yields name, type, and note (Phase 1) |
| Function | `adapter/pushdown/format/iceberg.rs` `plannable_schema` | The Iceberg declaration moves to CREATE (Phase 2) |
| Façade item | `build_logical_schema` | Replaced by `declare_columns` (Phase 2) |
| Call | `catalog_schema` in `CatalogParquetFormatReader::resolve_scan` | Runs at CREATE (Phase 3) |
| Function | `build_delta_table_schema` and its call in `read_delta_log` | The Delta declaration classifies `type_json` at CREATE through `classify_spark_schema` (Phase 3) |
| Error path | `Result` return of `classify_spark_schema` | Malformed annotations become column refusals (Phase 3, decision [17]) |
| Fields | `ResolvedScan` assembly inside each reader | The resolver assembles the declared parts (Phase 3) |
| Test | `tests/e2e_direct_storage_test.rs` `stale_declaration_decides_the_emitted_width` | Its scenario is removed; `merge_schema_false_declares_the_narrow_sampled_type_and_refuses_a_wider_file_column` keeps the coverage |
| Test | `parquet_directory_tests.rs` keep-predicate and footer-presence tests of the schema answer; `scan_resolution_tests.rs` and `parquet_format_reader_tests.rs` `declared_columns` and plan-time fold tests | Test removed behavior |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Every declared column carries its source shape in its adapterNotes | E2E | `tests/e2e_direct_storage_test.rs`, `tests/e2e_refresh_test.rs`, `tests/e2e_unity_test.rs`, `tests/e2e_glue_test.rs` | `direct_storage_columns_persist_source_notes`, `iceberg_columns_persist_source_notes`, `unity_delta_and_parquet_columns_persist_source_notes`, `glue_columns_persist_source_notes` |
| A partition column's note carries its position among the partition columns | E2E | `tests/e2e_direct_storage_test.rs`, `tests/e2e_unity_test.rs` | `partition_column_notes_carry_their_position`, `unity_parquet_partition_notes_follow_partition_index` |
| A refused column's note records its refusal and the column stays declared | E2E | `tests/e2e_scan_test.rs`, `tests/e2e_direct_storage_test.rs` | `iceberg_binary_column_note_records_the_refusal`, `direct_storage_binary_column_note_records_the_refusal` |
| Pushdown takes the declared schema from the column notes for every kind | E2E + Integration | `tests/e2e_direct_storage_test.rs`, `src/adapter/pushdown/scan_resolution_tests.rs` | `pushdown_plans_from_column_notes_without_reading_a_footer`, `resolver_assembles_the_declared_schema_from_each_tables_notes` |
| A column without a readable note fails the pushdown naming the fix | Unit | `src/adapter/column_notes_tests.rs`, `src/adapter/pushdown/pushdown_tests.rs` | `missing_or_unreadable_note_names_table_column_and_refresh`, `pushdown_with_a_note_less_column_fails_without_fallback` |
| Refresh and set properties rebuild every column note from the source | E2E | `tests/e2e_refresh_test.rs` | `refresh_and_set_properties_rebuild_column_notes_from_the_source` |
| A source change after the last REFRESH plans against the declaration | E2E | `tests/e2e_direct_storage_test.rs` | `source_change_after_refresh_plans_against_the_declaration` |
| A column note longer than the per-column limit fails the statement | E2E + Unit | `tests/e2e_direct_storage_test.rs`, `src/adapter/column_notes_tests.rs` | `over_limit_column_note_fails_create_and_refresh`, `encoder_refuses_a_note_over_the_limit_naming_table_and_column` |
| Refresh re-enumerates the namespace and returns a refresh response | E2E | `tests/e2e_refresh_test.rs` | `refresh_reenumerates_namespace` |
| The format reader is selected at the same one site for a third source | Unit | `src/adapter/pushdown/format/format_tests.rs` | `direct_parquet_source_carries_only_store_and_table_root` |
| A direct-storage table resolves its files and schema through the shared seam | Integration | `src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `direct_storage_scan_takes_its_schema_from_notes_and_files_from_the_listing` |
| A direct-storage pushdown plans without reading a footer | E2E | `tests/e2e_direct_storage_test.rs` | `pushdown_plans_from_column_notes_without_reading_a_footer` |
| A key=value directory segment declares a VARCHAR partition column | E2E | `tests/e2e_direct_storage_test.rs` | `hive_segments_declare_varchar_partition_columns_with_decoded_values` |
| A table's partition columns are the union of its files' keys | E2E | `tests/e2e_direct_storage_test.rs` | `mixed_layout_unions_partition_keys_and_nulls_the_missing_key` |
| Two spellings of one partition key fail the refresh and the query | E2E | `tests/e2e_direct_storage_test.rs` | `two_spellings_of_one_partition_key_fail_refresh_and_query` |
| A partition key matches its declared column across letter case | E2E | `tests/e2e_direct_storage_test.rs` | `partition_key_matches_its_declared_column_across_letter_case` |
| A predicate on partition columns prunes files before their footers are read | E2E | `tests/e2e_direct_storage_test.rs`, `tests/e2e_pruning_test.rs` | `partition_filter_prunes_the_resolved_file_list`, `hive_partition_pruning_keeps_every_file_with_a_matching_row` |
| A declared column absent from every kept file reads NULL | E2E | `tests/e2e_direct_storage_test.rs` | `a_column_only_pruned_files_carry_reads_null` |
| One seam answers the file list and the folded schema for enumeration | Integration | `src/adapter/parquet_directory_tests.rs` | `schema_answer_lists_every_file_and_folds_without_a_keep_predicate` |
| The merge mode selects every footer or exactly one | Integration | `src/adapter/parquet_directory_tests.rs` | `merge_mode_reads_every_footer_or_exactly_the_first` |
| A file-keep predicate narrows the files before any footer is read | Integration | `src/adapter/parquet_directory_tests.rs` | `listing_answer_keep_predicate_narrows_files_without_a_footer_read` |
| The listing answer serves a caller that declares its own partition columns | Integration | `src/adapter/parquet_directory_tests.rs` | `listing_answer_fills_caller_declared_partition_columns_and_reads_no_footer` |
| Two spellings of one declared partition key fail the listing answer | Integration | `src/adapter/parquet_directory_tests.rs` | `listing_answer_fails_on_two_spellings_of_a_declared_key` |
| MERGE_SCHEMA selects one footer or every footer at enumeration | E2E + Integration | `tests/e2e_direct_storage_test.rs`, `src/adapter/direct_storage_tests.rs` | `merge_schema_false_declares_the_narrow_sampled_type_and_refuses_a_wider_file_column`, `merge_schema_selects_footers_at_enumeration_only` |
| HIVE_PARTITIONING decides the declared partition columns at enumeration | E2E | `tests/e2e_direct_storage_test.rs` | `hive_partitioning_false_declares_no_partition_columns` |
| A table's columns and data files come from the one shared directory seam | Integration | `src/adapter/direct_storage_tests.rs` | `columns_carry_tag_and_declaration_from_one_fold` |
| The neutral column's declaration slot extends the crate's public surface through an explicit reviewed edit | Unit (compile-time probe) | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `catalog_column_declaration_slot_is_reachable` |
| The Iceberg column source carries the whole Iceberg field through an explicit reviewed edit | Unit (compile-time probe) | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `iceberg_column_source_carries_the_field` |
| The neutral table's catalog properties extend the crate's public surface through an explicit reviewed edit | Unit (compile-time probe) | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `catalog_table_properties_are_reachable` |
| The column declaration entry point replaces the Iceberg logical-schema builder on the pushdown façade | Unit (compile-time probe) | `src/adapter/pushdown_surface_probe_tests.rs`, `tests/pushdown_public_surface.rs` | probe item lists and counts |
| Every pushdown request shape resolves through the one format-reader seam | Integration + E2E | `src/adapter/pushdown/pushdown_tests.rs`, `src/adapter/pushdown/scan_resolution_tests.rs`, `tests/e2e_unity_test.rs` | `every_request_shape_resolves_through_the_format_reader_seam`, `resolver_parses_notes_once_and_every_reader_reads_the_declaration`, `unity_delta_join_and_aggregate_pushdown_return_correct_rows` |
| Pushdown resolves the file list once and builds a scan-driving query | Unit | `src/adapter/pushdown/support_tests.rs` | `pushdown_resolves_files_once_builds_scan_sql` |
| A column renamed at the source after the last REFRESH reads under its declared name | E2E | `tests/e2e_refresh_test.rs` | `renamed_iceberg_column_reads_under_its_declared_name` |
| A column dropped at the source after the last REFRESH reads what older data files store | E2E | `tests/e2e_refresh_test.rs` | `dropped_iceberg_column_reads_what_older_files_store` |
| A declared required column that the current schema marks optional fails the query | E2E | `tests/e2e_refresh_test.rs` | `iceberg_column_made_optional_after_refresh_fails_until_refresh` |
| A filter column resolves through its declared field id | E2E + Unit | `tests/e2e_refresh_test.rs`, `src/adapter/iceberg_predicate_tests.rs` | `filter_on_rename_swapped_iceberg_columns_prunes_by_declared_field_id`, `filter_column_resolves_through_its_declared_field_id` |
| A Glue Iceberg table is planned from its metadata location by the one Iceberg planner | Integration + E2E | `src/adapter/pushdown/format/iceberg_tests.rs`, `tests/e2e_glue_test.rs` | `a_glue_iceberg_table_is_planned_from_its_metadata_file`, `glue_columns_persist_source_notes` |
| A Glue Parquet table is planned by the shared catalog-declared Parquet reader | Integration | `src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs` | `glue_parquet_table_is_planned_by_the_shared_reader_with_nullable_catalog_columns` |
| The logical schema is the catalog's declared column list | E2E | `tests/e2e_unity_test.rs` | `unity_parquet_all_types_declare_and_return_their_mapped_values` |
| A Unity Parquet column with no usable type descriptor is refused, and nullability always follows the file | Unit | `src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs` | `declaration_refuses_a_column_without_a_usable_type_json_and_forces_nullable` |
| Partition columns come from the catalog and partition values from the file paths | Integration | `src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs` | `partition_columns_come_from_partition_index_and_values_from_paths` |
| The reader never infers partition columns from paths or partition metadata logging, and fails a plan whose partition column is refused | Integration | `src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs` | `refused_partition_column_note_fails_the_plan` |
| Enumeration names tables through the shared fold and stops at catalog metadata | E2E | `tests/e2e_unity_test.rs` | `unity_create_declares_a_delta_table_whose_location_holds_no_log` |
| Create virtual schema declares each Delta column from its type_json and the table's column-mapping mode property | E2E | `tests/e2e_unity_test.rs` | `unity_create_declares_delta_columns_from_catalog_metadata` |
| A Delta column whose column-mapping metadata is unusable is refused in its note and the table stays listed | E2E + Unit | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_schema_tests.rs` | `unity_unusable_column_mapping_metadata_refuses_only_its_column`, `unusable_annotation_or_mode_refuses_the_column` |
| The client carries each table's properties verbatim from the list sweep and the single-table load | E2E | `tests/e2e_unity_test.rs` | `unity_session_carries_table_properties_from_list_and_load` |
| Each logical field carries the binding key its column-mapping mode selects | E2E + Unit | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_schema_tests.rs` | `unity_delta_column_mapped_tables_return_logical_column_values`, `declaration_carries_the_mode_selected_key_from_type_json` |
| A declaration that disagrees with the Delta log fails the query | E2E + Integration | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_format_reader_tests.rs` | `unity_delta_query_fails_when_the_catalog_copy_disagrees_with_the_log`, `declaration_check_refuses_a_mode_or_partition_mismatch` |
| Every recorded Delta type change is validated, and an unsupported one refuses its column | E2E + Unit | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_schema_tests.rs` | `unity_unsupported_type_change_in_type_json_refuses_its_column`, `a_field_carrying_an_unsupported_recorded_type_change_is_refused_naming_both_types`, `malformed_type_change_annotation_refuses_its_column` |
| A declared non-nullable column that the Delta log marks nullable fails the query | E2E + Integration | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_format_reader_tests.rs` | `unity_delta_non_nullable_declaration_of_a_nullable_column_fails`, `declaration_check_refuses_a_non_nullable_declaration_of_a_nullable_field` |
| A filter column resolves through its declared binding key | Integration | `src/adapter/pushdown/format/delta_format_reader_tests.rs` | `filter_resolves_through_the_declared_physical_name_after_a_rename_swap` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Column Source Notes | `exapump sql "SELECT COLUMN_TABLE, COLUMN_NAME, ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_COLUMNS WHERE COLUMN_SCHEMA = 'DIRECT_COLUMN_NOTES_VS' ORDER BY 1, 2"` | One JSON object per column with `sourceType`; `YEAR` carries `"partition":0` |
| Direct-Storage Table Planning | Overwrite the `sales/` fixture files with non-Parquet bytes, then `exapump sql "EXPLAIN VIRTUAL SELECT ID FROM DIRECT_COLUMN_NOTES_VS.SALES WHERE YEAR = '2025'"` and the same `SELECT` | The plan lists `year=2025/p1.parquet` only; the `SELECT` fails with `scan failed` and `Corrupt footer` |
| Direct-Storage Hive Partitioning | Add a `YEAR=2027/` file to a table created under `year=` directories, then `exapump sql "SELECT COUNT(*) FROM <vs>.<table>"` | Error naming `year`, `YEAR`, and a path carrying each |
| Pushdown File Resolution | Rename an Iceberg column through the REST catalog, then `exapump sql "SELECT SCORE FROM <vs>.<table>"` | The pre-rename values; after `ALTER VIRTUAL SCHEMA <vs> REFRESH` the column reads as `RATING` |
| Pushdown File Pruning | After a rename swap, `exapump sql "SELECT COUNT(*) FROM <vs>.<table> WHERE A = 7"` | The same count as before the swap |
| Unity Catalog Create Virtual Schema | `make unity-up`, create a Unity virtual schema over `unity.delta_e2e`, then `exapump sql "SELECT COLUMN_TABLE, COLUMN_NAME, ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_COLUMNS WHERE COLUMN_SCHEMA = '<vs>' AND COLUMN_TABLE = 'CM_NAME_MODE'"` | Each note carries `physical_name`; the statement read no object storage |
| Unity Catalog Parquet Table Planning | `exapump sql "SELECT COLUMN_NAME, ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_COLUMNS WHERE COLUMN_SCHEMA = '<vs>' AND COLUMN_TABLE = '<parquet table>'"` | `year` carries `"partition":0`, `region` `"partition":1` |
| Glue Table Planning | `make test-e2e-glue` | The Glue suite passes, including `glue_columns_persist_source_notes` |
| Delta Table Planning, Delta Plan-Time File Pruning, Delta Schema Type Mapping, Unity Catalog Native REST Client | `make test-e2e-unity` | The Unity suite passes |
| Refresh And Set Properties, Format-Neutral Pushdown Resolution, Parquet Directory Seam, Parquet Directory Seam: File Listing, Direct-Storage Virtual-Schema Properties, Direct-Storage Table Discovery, Catalog Crate Public Surface Extensions, Pushdown Module Structure | `cargo test` then `make test-e2e` | 0 failures |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E (local Docker) | `make test-e2e` | 0 failures |
| E2E (Unity) | `make test-e2e-unity` | 0 failures |
| E2E (Lakekeeper) | `make test-e2e-lakekeeper` | 0 failures |
| E2E (Glue) | `make test-e2e-glue` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Line delta | `git diff --numstat <phase base> -- 'crates/*/src/**' ':!*_tests.rs'` | Net negative per phase and cumulatively |
