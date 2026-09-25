# Decision Log: add-unity-parquet-table-routing

## Interview

**Q:** How should the new reader get its schema, given that the existing `ParquetFormatReader` used for `DIRECT_STORAGE` always folds every kept file's footer to build the schema?
**A:** "This question puzzles me, because the catalog and the format are two different things. For Unity the schema is established by the catalog as always. For direct storage catalog the schema is established as always. The format doesn't establish the schema, or at least it shouldn't if we are doing things right."

**Q:** Should Hive-style partitioning for Unity Parquet tables be inferred from `key=value` path segments the way `DIRECT_STORAGE` does, or does Unity Catalog itself declare partition columns?
**A:** "We should know how does Unity handle this with Spark for example when using UC." Resolved by research: Unity Catalog declares partition columns through each column's `partition_index`, and by default reads partition values by recursively listing the Hive-style directories under the table location (Databricks, *Partition discovery for external tables*). Partition metadata logging is an opt-in Databricks Runtime 13.3+ strategy and is out of scope.

**Q:** Verify the live `data_source_format` wire value before trusting it.
**A:** "Verify now." Verified by the orchestrator against the project's OSS Unity Catalog fixture (`docker-compose.unity.yml`): a `POST /tables` with `table_type` `EXTERNAL`, `data_source_format` `PARQUET`, and columns carrying `partition_index` (`null` for a data column, `0` for the first partition column) is echoed verbatim by `GET /tables/{full_name}` and by `GET /tables?catalog_name=...&schema_name=...`. The value is exactly the uppercase `PARQUET`. No probe object was left behind.

## Design Decisions

### [1] A Unity Catalog Parquet table takes its schema and partition columns from the catalog, and only its file listing from the directory seam

- **Decision:** The Unity Parquet reader builds its logical schema from the catalog's declared columns and its partition columns from each column's `partition_index`. It classifies each column's `type_json` (a Spark `StructField` JSON) through the Delta reader's Spark-type classifier (`build_delta_table_schema`) under no column mapping. It asks the parquet-directory seam only for the file list, filled against those catalog-declared partition columns, and reads no footer.
- **Alternatives:** Reuse the direct-storage `ParquetFormatReader`, which folds footers into the schema: rejected, because the catalog is the schema authority and a footer fold would let a table's data files redefine its declared columns. Classify the Unity `type_name` through a new Spark-name-to-Arrow table: rejected, because `type_name` carries no nested structure, so every `struct`, `array`, and `map` column would be refused, and the table would add a third Spark-type classifier beside the Delta one and the createVirtualSchema one. Infer partition columns from path segments as `DIRECT_STORAGE` does: rejected, because Unity Catalog declares them.
- **Rationale:** A catalog decides a table's schema, and a file format does not (interview Q1). The Delta protocol records its schema in "a subset of Spark SQL's JSON Schema representation" (PROTOCOL.md § Schema Serialization Format), the same representation Unity Catalog's `type_json` carries, so one classifier answers both and a Delta column and a Unity Parquet column of the same Spark type get the same Arrow tag, nested descriptor, or refusal.
- **Consequences:**
  - `ColumnSourceType::Unity` gains `type_json`, and `CatalogTable` gains ordered `partition_columns`, empty for the Iceberg REST and direct-storage clients. The raw `partition_index` stays crate-private.
  - Every logical field is declared nullable, because a raw Parquet directory does not guarantee that each file carries every column.
  - Partition values come from `key=value` directory segments matched by the uppercase fold. A missing segment reads NULL, as `DIRECT_STORAGE` already does.
  - Only `string` partition columns reach the partition-pruning predicate, because it compares values as strings. A filter on any other column narrows rows but not files, since no footer or statistic is read at plan time.
  - A data file without the `.parquet` suffix is not read, a documented trade-off inherited from the seam's data-file rule.
  - Amends `delta-base-filter-inside-unity-catalog-client`: the client-side admission filter now admits `DELTA` and `PARQUET` base tables. Its location inside the client is unchanged. `SkipReason::NotDeltaBaseTable` and its warning text keep their names, because every entry the filter skips is still not a Delta base table and the detail names the disqualifier.
- **Promotes to ADR:** yes

### [2] The Unity scan source selects its reader by format tag, and both Unity readers share one table-storage component

- **Decision:** `ScanSource::UnityDelta` becomes `ScanSource::Unity`. Its arm in `format_reader` matches the loaded table's `TableFormat` exhaustively: Delta selects `DeltaFormatReader`, Parquet selects `UnityParquetFormatReader`, and Iceberg is refused. The storage-location check, the vended-or-static storage decision, and error redaction move out of `DeltaFormatReader` into one `UnityTableStorage` component that both readers compose.
- **Alternatives:** Duplicate the vending logic in the new reader: rejected, because two copies of the no-fallback rule and the `READ` scope can drift, and a drift reads storage with a credential the operator did not select. One `UnityFormatReader` that resolves storage and then branches on format internally: rejected, because it hides a second format dispatch inside a reader, while `format_reader` is the one recorded dispatch site.
- **Rationale:** The vending decision belongs to the Unity Catalog table, not to its file format, so it has one owner shared by every format Unity Catalog hosts. Each reader still owns its whole resolution (`format-reader-engine-owns-whole-resolution`) and delegates only the storage step.
- **Consequences:**
  - Supersedes the naming consequence of `format-dispatch-matches-scansource-not-catalogkind`, which kept the name `UnityDelta` to state a Unity-implies-Delta coupling until a second format appeared. The dispatch decision itself (match `ScanSource`, never `CatalogKind`) is unchanged.
  - `DeltaFormatReader`'s behavior and error texts are unchanged.
- **Promotes to ADR:** yes

### [3] The E2E fixture writes and registers its table in the test binary

- **Decision:** `e2e_unity_test.rs` setup writes the `sales_parquet` files with `tests/common/raw_parquet.rs` and registers the table through the Unity Catalog REST API (`DELETE` then `POST /tables`) before it creates the virtual schema. `scripts/unity/seed.sh` is unchanged.
- **Alternatives:** Register the table in `seed.sh` and write the files from the test: rejected, because the declared columns and the written files would live in two files and could drift. Commit prebuilt Parquet files under `scripts/unity/fixtures/`: rejected, because the seed would mirror them under the Delta prefix and the issue asks to reuse the raw-Parquet writer.
- **Rationale:** One place owns the fixture's data and its catalog registration. The registration is idempotent, and the Unity Catalog server keeps its catalog in memory, so a restart needs a re-run of the setup, which every suite run performs.
- **Promotes to ADR:** no

### [4] The scan binds an identity-bound column across letter case and refuses a physical type it cannot admit, for every format

- **Decision:** Before the cast, the scan admits each bound column per file. It admits a pair that the supported-set owner (`widen`) resolves to the logical type, a timestamp pair that keeps every stored instant, or a text-rendered type under a string tag. Every other pair fails a query that reads the column, naming the table's storage location, the column, and both types. The identity binding matches the exact name first, then the uppercase fold, and fails on an ambiguous fold. Field-id and declared-physical-name bindings stay exact.
- **Alternatives:** Keep the arrow cast and document the drift outcomes: rejected by the requester, because the cast truncates `double` 1.7 to `int` 1 and a case-drifted column reads NULL. Check drift inside the Unity Parquet reader: rejected, because that reader reads no footer and every format shares one binding and cast path. Fold every name step: rejected, because field-id and `name`-mapping bindings follow keys the table's own metadata records.
- **Rationale:** Requester choice after review round 1: "Fail loud, Spark-compatible." Delta PROTOCOL.md § Consistency Between Table Metadata and Data Files states: "Any data file column that exists in the table schema MUST have the same type (except as allowed by the [Type Widening] table feature, if enabled)." Spark resolves Parquet columns case-insensitively by default (`spark.sql.caseSensitive=false`).
- **Consequences:**
  - Supersedes the `datafusion-scan/type-relaxation` narrowing-direction clauses. A logical type narrower than a file's column now fails the query.
  - Direct storage under `MERGE_SCHEMA = 'FALSE'` fails a query that reads a column some file stores wider than the sampled type. The recorded E2E test read `Float64` values under a `Float32` declaration, and its values were exact in `Float32`, which hid the precision loss.
  - A string-tagged column admits every text-rendered physical type. A catalog `string` over a `binary` file column therefore renders text instead of failing, because the scan cannot tell it from a JSON-fallback column: both carry the `utf8` tag.
- **Promotes to ADR:** yes

## Review Findings

### [plan-review] Catalog-to-file name and type drift had no stated outcome

- **Finding:** Round 1 `[COMPLETENESS_GAP]` BLOCKER. The exact-case identity binding and forced nullability turned a case-drifted column into NULL. `DefaultPhysicalExprAdapter` cast every arrow-castable pair. The spec stated neither outcome.
- **Direction change:** The requester chose loud, Spark-compatible failure, recorded as decision [4]. The plan adds deltas on `datafusion-scan/scan-execution-field-id-projection`, `datafusion-scan/type-relaxation`, and `e2e-harness/direct-storage-e2e`, group D, and the matching clauses in `vs-adapter/unity-parquet-table-planning`. The Non-Goal "A footer-based schema check against the catalog" now reads as a plan-time footer read, because the scan checks each file.
- **Promotes to ADR:** no

### [plan-review] Background prose that no scenario step depended on

- **Finding:** Round 1 `[IMPLEMENTATION_LEAKAGE]` BLOCKER. The E2E Background named the writer, the `DELETE` idempotence, and a rationale. The planning Background stated that partition metadata logging is not read. No step depended on either.
- **Direction change:** Both fragments are deleted. Scenario "Partition columns come from the catalog and partition values from the file paths" and task 2.6 now state that the reader does not consult the partitions that metadata logging registers.
- **Promotes to ADR:** no

### [plan-review] The type refusal failed queries that never read the refused column

- **Finding:** Round 2 `[UNSTATED_ASSUMPTION]` BLOCKER. Task 4.2 refused a file in `FieldIdExprAdapterFactory::create`. DataFusion 54.1 hands `create` the whole table file schema, so the refusal failed every query on that file, including three of the plan's own acceptance checks.
- **Direction change:** `create` records each refused pair per file. `FieldIdExprAdapter::rewrite` fails only when a `Column` it rewrites references a refused column. The type-relaxation scenario, the Unity planning scenario, decision [4], and task 2.6 now scope the failure to a query that reads the column. The ambiguous-fold failure stays in `create`. Task 4.3 adds `a_refused_column_fails_only_a_rewrite_that_references_it`.
- **Promotes to ADR:** no
