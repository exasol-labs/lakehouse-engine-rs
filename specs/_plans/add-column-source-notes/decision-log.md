# Decision Log: add-column-source-notes

## Interview

**Q:** What is the scope of the plan for issue #426?
**A:** Full scope. One plan with three phased task groups matching the issue's per-format PRs: (1) notes plus direct storage (the spike), (2) Iceberg REST and Glue Iceberg, (3) Delta, Unity Parquet, and Glue Parquet. The Delta part carries the Unity `type_json` question as a gated early research task.

**Q:** What happens to a virtual schema created before this change, which has no column notes?
**A:** Nothing special. The product is not in use. Missing column notes at pushdown is an unexpected error, and a clear message is fine. No fallback code, no migration path, and no "run REFRESH" special-casing beyond a plain error.

**Q:** What does a column note contain?
**A:** Every catalog kind stores the SAME format-neutral `LogicalField` (Arrow type tag, nested members, Iceberg field id or Delta physical name), plus a partition flag and, for a refused column, the refusal reason. Not `type_json`: `type_json` is only an input at Unity CREATE that gets converted to a `LogicalField`. No per-kind note format.

**Q:** How do partition key names match their columns?
**A:** Case-insensitively on the column name; the path value stays verbatim. Two directory keys that differ only by case (for example `Year` and `year`) are an error, not a silent pick. One rule for both CREATE and the footer-free listing. Research: the Hive metastore stores partition column names lowercased while directories keep the writer's case (SPARK-19359 and its revert); Cloudera states SQL names are case-insensitive while HDFS paths are case-sensitive; AWS advises lowercase S3 prefixes and `MSCK REPAIR` skips camel-case paths; Glue folds database and table identifiers to lowercase; Spark's default `spark.sql.caseSensitive=false` resolves partition columns case-insensitively, the directory casing can win over the schema (SPARK-26990), and only case-sensitive mode rejects `a=1` beside `A=2` (SPARK-26230); Exasol folds identifiers; DuckDB's documentation says nothing, so a live DuckDB check is a plan task.

**Q:** Which live measurements does the plan include?
**A:** As early plan tasks: (a) the largest per-column `adapterNotes` Exasol accepts, measured on Docker Exasol through exapump with wide nested columns, because the table-level limit turned out lower than the column type suggests; the spec states the contract that an over-limit column fails CREATE with a clear error; (b) whether Unity's `type_json` carries `delta.columnMapping.physicalName`, verified live against Databricks; (c) DuckDB's partition-key case behavior.

**Q:** How are declared Iceberg field ids handled when the source renames a column between refreshes?
**A:** Open for the planner: check against the Iceberg table spec's column projection rules and the Delta protocol's column mapping rules, quote the normative sections, and fix each deviation in the plan or record it as a scoped exception.

**Q:** How does this plan relate to other issues?
**A:** #425 (table-level `adapterNotes`, the TABLE_MAP refactor) introduces the per-object notes pattern; cite it as (#425) and note the ordering. #412 is closed as superseded. Planning opens no GitHub issue; anything without an issue is marked `(#TBD)` and listed as an open question.

**Q:** Which acceptance criteria from the issue must the plan carry?
**A:** Direct-storage pushdown performs no footer read for schema purposes, and the `declared_columns` plumbing and `absent_declared_fields` are deleted. No format reader derives `logical_schema` or `refused_columns` at pushdown. No resolver or reader signature carries a parameter read by only one format arm. The net production line count of the touched paths goes down, reported in each PR.

**Q:** (Review of the first revision) Should Unity CREATE read each Delta table's log to declare its columns?
**A:** No, reverse it. Unity CREATE stays catalog-only: no Delta log read, no vend, no new `READ` requirement, and no engine-side listing client wrapping the Unity session. Declare Delta columns from `type_json` and the table-level `properties` map, where `delta.columnMapping.mode` should sit, taken from the existing `GET /tables` sweep. The Unity client does not deserialize `properties` today, so add it. No per-table GET unless task 3.1 proves the sweep omits it. Keep the "stops at catalog metadata" rule of `unity-catalog-create-virtual-schema`, amended only to add `properties`.

**Q:** (Review) What does task 3.1 verify, and what happens when it fails?
**A:** Live, on Databricks and on OSS Unity Catalog: (a) whether `properties` come inline on the list sweep or only from a per-table GET; (b) whether `type_json` field metadata carries `delta.columnMapping.physicalName` and `delta.columnMapping.id`; (c) whether the mode is in `properties`. If the catalog cannot supply every binding key plus the mode, Phase 3's Delta tasks stop and the plan is revised, a human decision. No silent log-read fallback. The plan still specifies the Delta design fully for the success case.

**Q:** (Review) What can CREATE refuse from catalog data alone?
**A:** Re-derive it. The reader-feature gate needs the log's protocol, so it likely cannot run at CREATE: say so, keep the pushdown gate as the authority, and record the trade-off. Drop the SKIPPED_TABLES-on-gate policy and keep the 404-only skip ADR unchanged. For a malformed column-mapping annotation in `type_json` or `properties`, pick a policy consistent with the existing ADR and state it.

**Q:** (Review) Are the partition position and the Iceberg dropped-column behavior accepted?
**A:** Yes, as proposed: `"partition": <0-based position>` instead of a boolean flag, and the Iceberg dropped-column stale-values behavior until `REFRESH` as a recorded scoped exception.

**Q:** (Review) Which follow-ups and open questions remain?
**A:** No log-read follow-up is needed. No Databricks workspace is available yet: task 3.1 needs a human-supplied workspace and token, which stays an open question.

## Design Decisions

### [1] A table's declared schema is decided at CREATE and persisted per column in adapterNotes

- **Decision:** `createVirtualSchema`, `refresh`, and `setProperties` record each virtual column's declared source shape in that column's `adapterNotes`. Every pushdown takes the table's logical schema, partition columns, and refused columns from those notes, and no format reader derives a schema at pushdown.
- **Alternatives:** Keep per-reader derivation at pushdown: rejected, because each reader re-derives the shape differently per query, and the derived shape can differ from the one Exasol declared. Persist the shape in schema-level `adapterNotes`: rejected, because that value grows with every column of every table and is capped at 2,000,000 characters for the whole schema. Re-derive the shape from the Exasol `dataType` at pushdown: rejected, because the Exasol type loses the nested structure, the binding key, and the source type.
- **Rationale:** `/speq:adr-rules` rule-2 criterion 2: a long-lived constraint that binds every future format reader and catalog kind; criterion 1: it changes vs-adapter, pushdown-planner, format-readers, and lakehouse-catalog. Search: `speq decision-log show` holds no ADR on per-column notes. The decision conforms to `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute`: the declared shape is derived at create time, and a pushdown cannot recompute it because it sees only the source's current shape. It conforms to `format-reader-engine-owns-whole-resolution`: each reader still owns its catalog request, credential, and file discovery, and the declaration reaches every reader through one format-neutral parse, so no shared caller pre-fetches per format. It conforms to `vs-refresh-reuses-create-virtual-schema-enumeration`: refresh rebuilds every note through the same enumeration. It conforms to `one-catalog-declared-parquet-reader-and-one-iceberg-planner-serve-glue`: Glue Parquet and Unity Parquet still share one catalog-declared Parquet reader, and Glue Iceberg still uses the one Iceberg planner; only the generalization over a type source moves from that reader to the CREATE-time declaration. It supersedes `unity-parquet-schema-from-catalog-not-footer` through decision [20].
- **Consequences:**
  - The declared schema goes stale exactly when the Exasol `dataType` does; `REFRESH` heals both.
  - A source column added after the last `REFRESH` stays invisible, a widened type fails at scan time, and `MERGE_SCHEMA` and `HIVE_PARTITIONING` take effect at CREATE, REFRESH, or SET only.
  - Pushdown keeps its per-query load only for what is not the schema (snapshot, files, deletes, name mapping, credentials, partitions).
- **Architecture:** § Components, § Data Flow, § Interfaces, § Constraints
- **Promotes to ADR:** yes

### [2] One note format for every catalog kind, with a partition position instead of a flag

- **Decision:** A note is the JSON object string `{"sourceType": <LogicalField in its scan-spec encoding>, "partition": <0-based position>, "refused": "<reason>"}`. `partition` and `refused` are omitted when they do not apply. One module, `adapter/column_notes.rs`, owns the encoding, the size check, the parse into a declared schema, and the missing-note error.
- **Alternatives:** A boolean partition flag (the spike's shape): rejected, because the Unity Parquet spec orders partition columns by `partition_index`, and a Delta table by the log's `partitionColumns`, which Unity Catalog mirrors in `partition_index`; neither is the column order. Storing `type_json` or a Hive type per kind: rejected by the interview.
- **Rationale:** A position is the flag plus the order the recorded specs require, so no partition-ordering rule changes and no golden scan-spec encoding moves. One owning module keeps the format from leaking into the direct-storage client, the listing pipeline, and the pushdown path (`/speq:design-philosophy`, back-door leakage).
- **Promotes to ADR:** no

### [3] An engine-side client hands its declaration through an opaque slot on the neutral column

- **Decision:** `CatalogColumn` gains `declaration: Option<String>`, an engine-encoded declaration that an engine-side client resolved from data files. The direct-storage client (Phase 1) sets it. For every other column the engine derives the declaration from the column's source type in one dispatch, `declare_columns`, at the listing pipeline. `ColumnSourceType::Iceberg` widens from the Iceberg type to the whole Iceberg field, so the dispatch reads the field id, the required flag, and the `initial-default`.
- **Alternatives:** The spike's `adapter_notes` field: rejected, because it names the Exasol channel inside the catalog crate. Replace `ColumnSourceType::Parquet(tag)` with a `Declared(note)` variant: rejected, because the type-mapping home (`types/mapping.rs`, which depends on no other component) would then have to decode the note format `adapter/column_notes.rs` owns to find the Exasol type, or the listing pipeline would special-case that variant before the type mapping. Move `LogicalField` into `lakehouse-catalog`: rejected, because the scan-spec wire format would then live in the catalog crate.
- **Rationale:** The slot is the smallest neutral addition that carries a declaration only the engine can build, and the catalog crate never reads it (ADR `catalog-client-implementor-may-live-in-lakehouse-engine`). The Exasol `dataType` keeps its recorded source per kind. Every note still goes through one encoder with one size check.
- **Consequences:** Two inputs feed the one dispatch: an engine-side declaration, or a source type plus the neutral table metadata. The catalog public-surface probe gains the field, the widened Iceberg payload, and the table properties map of decision [15] (`catalog/catalog-crate-public-surface-extensions`).
- **Promotes to ADR:** no

### [4] A missing or unreadable note is a plain error with no fallback

- **Decision:** A pushdown whose involved table carries a column without a readable note fails naming the virtual table, the column, and `ALTER VIRTUAL SCHEMA ... REFRESH`. No fallback derives the schema, and no migration exists.
- **Alternatives:** Fall back to per-reader derivation for legacy schemas: rejected by the interview, because it keeps the code this issue deletes.
- **Rationale:** The product is not in use, and #425 takes the same no-migration position for table-level notes.
- **Promotes to ADR:** no

### [5] A column note longer than the measured per-column limit fails the statement

- **Decision:** Task 1.1 measures the largest per-column `adapterNotes` Exasol 2025.x and 8.29.x persist and records it in the spec Background and as one constant. The encoder fails `createVirtualSchema`, `refresh`, and `setProperties` for a longer note, naming the table, the column, the length, and the limit.
- **Alternatives:** Truncate or drop the note: rejected, because a pushdown cannot plan from a partial declaration. Drop the nested descriptor of an over-limit column: rejected, because the JSON renderer needs it.
- **Rationale:** The schema-level limit (2,000,000 characters, sqlCode `04000`) is measured; the issue reports the table-level limit is lower than the column type suggests, so the column limit must be measured too. If task 1.1 finds a combined bound on one table's notes below the per-column limit times the column count, the implementer stops and the plan is revised.
- **Promotes to ADR:** no

### [6] Partition key names match case-insensitively; two spellings of one key fail at CREATE and at pushdown

- **Decision:** One fill function in `adapter/parquet_directory.rs` serves the schema-resolving answer and the listing answer: a key name matches its column under the uppercase fold, the value stays verbatim, and a listing in which one declared key appears under two spellings fails naming both spellings and a path carrying each. CREATE checks every listed file under both merge modes, because the check reads no footer. The listing answer checks before its file-keep predicate, so a filter cannot hide the conflict.
- **Alternatives:** Exact matching at CREATE and case-insensitive at pushdown (today): rejected by the interview. Deepest or first spelling wins: rejected by the interview.
- **Rationale:** Interview decision, backed by the metastore and engine research above. The listing answer is shared with the Unity Parquet reader, so the rule applies there too.
- **Consequences:** A Unity Parquet location whose files mix `year=` and `Year=` now fails its queries instead of filling both silently. Under `MERGE_SCHEMA = 'FALSE'`, a second spelling carried only by unsampled files now fails CREATE instead of going undetected. Task 1.2's DuckDB result is recorded here and does not change the rule.
- **Promotes to ADR:** no

### [7] A declared Iceberg field id is the column's identity; pruning resolves filter columns through it

- **Decision:** The Iceberg reader translates each filter column through the field id its note declares to the name the current schema gives that id, and drops a conjunct whose declared id the current schema no longer carries. A column dropped at the source between refreshes is a scoped exception: it reads the values older data files store, and NULL or its declared `initial-default` from later files, until `REFRESH`.
- **Alternatives:** Translate by declared name against the current schema (today's lookup): rejected, because after a rename swap the bounds of another field id prune files that hold matching rows, which violates § Scan Planning ("Data files that match the query filter must be read by the scan"). Refuse a query on a dropped column at pushdown: rejected, because it re-adds a pushdown-time schema check and the spec defines projection by field id for any projection schema.
- **Rationale:** Iceberg § Column Projection: "Columns in Iceberg data files are selected by field id. The table schema's column names and order may change after a data file is written, and projection must be done using field ids." § Schema Evolution: "Renaming an existing field must change the name, but not the field ID" and "The `initial-default` must be set when a field is added and cannot change". `last-column-id` ensures "columns are always assigned an unused ID when evolving schemas", so a declared id never names a later column. The quotes are recorded in `file-planning/pushdown-planning-file-resolution` and `file-planning/pushdown-file-pruning`.
- **Promotes to ADR:** no

### [8] Unity CREATE declares Delta columns from catalog metadata alone, gated by a live research task

- **Decision:** `createVirtualSchema`, `refresh`, and `setProperties` under the Unity Catalog kind stay catalog-only. A Delta table's column notes come from each column's `type_json` (the Delta `StructField` JSON with its column metadata), the `delta.columnMapping.mode` entry of the table's `properties`, and the columns' `partition_index`, all from the existing `GET /tables` sweep. The `declare_columns` dispatch classifies them with the `classify_spark_schema` that Unity Parquet uses, passing the mode where Parquet passes `none`. The Exasol `dataType` stays mapped from `type_json`. Task 3.1 checks live, on Databricks and on OSS Unity Catalog, that the sweep returns `properties` inline, that `type_json` carries `delta.columnMapping.physicalName` and `delta.columnMapping.id` on top-level and nested fields, that `properties` carries the mode, and that `partition_index` follows the log's `partitionColumns`. Gate: if either catalog cannot supply every binding key, the mode, and the partition order on the sweep, Phase 3's Delta tasks stop and the plan is revised by a human. No log-read fallback exists.
- **Alternatives:** Read each Delta table's log at CREATE through an engine-side Unity listing client that sets each column's declaration slot (this plan's first revision, reversed in review): rejected, because it adds one credential vend and one log read per Delta table to every CREATE and REFRESH, needs `READ` on every Delta table's storage where CREATE needed catalog metadata only, breaks the recorded rule that Unity CREATE "stops at catalog metadata", and needed a new skip policy for gated tables beside ADR `namespace-skip-non-iceberg-on-404-only`. A per-table `GET /tables/{full_name}`, with Databricks' `include_delta_metadata`: rejected unless task 3.1 shows the sweep omits `properties`, because it turns one paginated sweep into N+1 requests and changes a recorded rule, so that outcome also stops the Delta tasks for a revision. Read the mode from `type_json`: impossible, because the protocol keeps the mode in a table property.
- **Rationale:** The Delta protocol keeps what the declaration needs in two places Unity Catalog mirrors. PROTOCOL.md § Column Mapping: "The column mapping is governed by the table property `delta.columnMapping.mode`", and "The physical name is stored as part of the column metadata with the key `delta.columnMapping.physicalName`." The Databricks List tables reference returns `properties` on the sweep unless `omit_properties` is set. PROTOCOL.md § Writer Requirements for Column Mapping makes the physical name static and unique across versions, so a key recorded at CREATE stays valid. A catalog copy that disagrees with the log is caught at pushdown by decision [18].
- **Consequences:** Unity CREATE's cost and access requirements are unchanged. An OSS Unity Catalog registrant must send each Delta column's `StructField` metadata and the table's properties, so `scripts/unity/seed.sh` registers every Delta fixture from its log's latest `metaData` action instead of hand-written advisory columns. A Delta table the reader-feature gate refuses stays listed and fails at query time (decision [16]).
- **Promotes to ADR:** no

### [9] Delta pruning resolves filter columns through the declared binding key

- **Decision:** The Delta predicate translator finds a filter column's note by the uppercase fold, then the current snapshot field carrying the note's physical name (`name` mode) or field id (`id` mode), and emits that field's current display name. Under the `none` mode the declared name resolves as today.
- **Alternatives:** Translate by declared name against the current snapshot (today): rejected for the same rename-swap reason as Iceberg; PROTOCOL.md resolves "column level statistics" by physical name.
- **Rationale:** PROTOCOL.md § Reader Requirements for Column Mapping: in `name` mode "Partition values and column level statistics will also be resolved by their physical names". Under `none` the protocol offers no rename or drop without column mapping.
- **Promotes to ADR:** no

### [10] The resolver parses notes once; readers receive the declaration and return only per-query parts

- **Decision:** Phases 1 and 2 pass the involved table's raw notes to `TableScanResolver::resolve`, which hands them to the converted readers through their `ScanSource` variants (`DirectParquet`, then `Iceberg`), and each converted reader parses them. Task 3.6 hoists the parse to one site in the resolver: every reader receives the parsed declared schema, returns only the files, effective storage, table root, and name mapping, and the resolver assembles `ResolvedScan` with the declared logical schema, partition columns, and refused columns.
- **Alternatives:** Parse in each reader permanently: rejected, because four parses of one value is the per-reader derivation this issue removes. Keep `ResolvedScan` fields filled by readers: rejected, because a reader that copies its input to its output is a shallow pass-through.
- **Rationale:** Criterion 3 of the issue: no signature carries a parameter read by one arm only. The transitional state in Phases 1 and 2 violates it and is scheduled for removal by task 3.6.
- **Promotes to ADR:** no

### [11] CREATE-only code leaves the pushdown readers; the façade swaps one item

- **Decision:** The direct-storage declaration (`logical_schema` over the folded Arrow schema, `parquet_refusal`) moves from `parquet_format_reader.rs` to the direct-storage enumeration module. `binary_refusal`, `binary_cause`, `without_refused_columns`, and `ensure_table_has_a_mappable_column` move to `adapter/column_notes.rs`, because they now serve the note encoder and the note parse. `RefusedColumn` moves to `adapter/column_notes.rs` with them, so the column-notes component depends only on the scan spec and no import cycle forms between it and `adapter::pushdown::format`. The pushdown façade keeps re-exporting `RefusedColumn` under the same name, gains `declare_columns`, and loses `build_logical_schema` in Phase 2. It gains no other item, so both probes keep 27 and 17 items (`pushdown/pushdown-module-structure`).
- **Alternatives:** Export each per-format helper on the façade: rejected, because each would freeze a submodule-internal helper.
- **Rationale:** Code that runs only at CREATE belongs with CREATE; one façade entry per CREATE-time dispatch keeps the per-format helpers private (`/speq:design-philosophy`, deep modules).
- **Promotes to ADR:** no

### [12] #412 is superseded; #419 remains open for the CREATE-time footer cost

- **Decision:** The direct-storage specs record footer-statistics pruning (#412) as closed and superseded, because no pushdown reads a footer. The unbounded footer-read cost at CREATE stays an explicit exception citing #419.
- **Alternatives:** none
- **Rationale:** Interview: #412 is closed as superseded.
- **Promotes to ADR:** no

### [13] Every phase reports a negative net production line delta

- **Decision:** Each phase ends with a task that counts the added and deleted lines of the touched production files (`crates/*/src/**`, excluding `*_tests.rs`) against the phase's base commit and records the delta in the PR description. Each phase's delta MUST be negative. Phase 1 reaches it by merging the column-type and column-note extraction into one pass over `involvedTables[].columns`, by one fill function for both partition matchers, and by deleting the `declared_columns`, `absent_declared_fields`, `plannable_schema`, `keep`, `ParquetFile.footer`, and pushdown `DirectoryOptions` code.
- **Alternatives:** Count the issue-level delta only: rejected, because the interview asks for the delta in each PR, and the spike was +59 production lines.
- **Rationale:** Acceptance criterion 4 of the issue.
- **Promotes to ADR:** no

### [14] #425 and this plan touch the same two functions and land in either order

- **Decision:** No dependency on #425 is modeled. Whichever change merges second rebases `build_listing_virtual_tables` and the `involvedTables` extraction in `pushdown/support.rs`.
- **Alternatives:** Block this plan on #425: rejected, because column notes need no table-level notes.
- **Rationale:** Interview: cite #425 and note the ordering.
- **Promotes to ADR:** no

### [15] The neutral table carries the catalog's table properties verbatim

- **Decision:** `CatalogTable` gains `properties: BTreeMap<String, String>`, the table-level properties the catalog reported, copied verbatim. The Unity client deserializes `TableInfo.properties` and fills the field in its one `neutral_table` conversion, so the sweep and the single-table load agree. Every other client leaves it empty. The engine's Delta declaration reads `delta.columnMapping.mode` from it.
- **Alternatives:** A `column_mapping_mode` field on the neutral table: rejected, because the catalog crate would then name and parse a Delta property, format knowledge the one-way dependency keeps in the engine. Repeat the mode in each column's `ColumnSourceType::Unity`: rejected, because the mode is one value per table and per-column copies could disagree. Carry only the `delta.*` keys: rejected, because the filter is an interpretation the crate makes nowhere else.
- **Rationale:** A verbatim map is the neutral shape the crate can supply without understanding it, and the engine owns the one key it reads (`/speq:design-philosophy`, information hiding: the Delta property name lives in one engine module). `metadata_location` (Glue only) and `vended_credential_key` (Unity only) are neutral fields that one client fills.
- **Consequences:** `catalog/catalog-crate-public-surface-extensions` and `unity-catalog/unity-catalog-client` each gain one scenario.
- **Promotes to ADR:** no

### [16] CREATE refuses what catalog data decides; the reader-feature gate stays the authority at pushdown

- **Decision:** CREATE records as column refusals every refusal the catalog metadata decides: an unmappable Delta type, a `delta.typeChanges` entry outside the supported list when `type_json` carries it, and an unusable column-mapping annotation or mode (decision [17]). The reader-feature gate, the protocol condition on the mode property, and the comparison with the log stay at pushdown, because each needs the log. A table the gate refuses is listed and fails at query time, as today. The first revision's policy of skipping a gated table into `SKIPPED_TABLES` is dropped, and ADR `namespace-skip-non-iceberg-on-404-only` stays unchanged.
- **Alternatives:** Skip a gated table at CREATE: impossible without reading the log, which decision [8] rejects. Approximate the gate from `type_json`, such as refusing a `variant` column as a proxy for the `variantType` feature: rejected, because features such as `deletionVectors` and `v2Checkpoint` never show in the schema, so the proxy would be incomplete while looking authoritative.
- **Rationale:** PROTOCOL.md § Reader Requirements: "to read a table, readers must implement and respect all features listed in `readerFeatures`", which sit in the log's `protocol` action. The pushdown reader opens the gated snapshot before planning any file, so no row of a gated table is ever read.
- **Consequences:** Trade-off: a gated Delta table appears in the virtual schema, and every query on it fails naming the feature (`delta/delta-reader-feature-gating`, unchanged). A type change the catalog's copy omits is caught at scan time, a scoped exception recorded in `delta/delta-table-planning`.
- **Promotes to ADR:** no

### [17] An unusable column-mapping annotation refuses its column, not the table or the statement

- **Decision:** Under the `name` or `id` mode, a column whose `type_json` lacks the annotation the mode requires or carries it malformed gets a note whose refusal names the column, the mode, and the problem. A `properties` mode value outside `none`, `id`, and `name` refuses every column of the table, naming the value. CREATE neither fails nor skips the table. `classify_spark_schema` therefore returns a malformed column-mapping or `delta.typeChanges` annotation as a column refusal instead of an error.
- **Alternatives:** Abort CREATE: rejected, because one table's metadata would block the whole virtual schema, and ADR `namespace-skip-non-iceberg-on-404-only` keeps aborts for failures of the catalog call. Skip the table into `SKIPPED_TABLES`: rejected, because that ADR admits a skip only on a definitive not-loadable answer, and a skip hides the reason from the query that would show it. Fail every query on the table (today's pushdown behavior): rejected, because ADR `scope-delta-type-refusal-to-column-not-table` scopes a refusal to its column so the other columns stay queryable.
- **Rationale:** ADR `scope-delta-type-refusal-to-column-not-table` already refuses an unmappable type per column. An unusable binding key makes the column unreadable in the same way, and substituting the ordinal or the logical name would bind a column the writer never wrote. A table whose every column is refused is still refused as a whole by the note parse.
- **Promotes to ADR:** no

### [18] The pushdown Delta reader fails a query whose declaration disagrees with the log

- **Decision:** After the gate, the Delta reader compares the declaration with the snapshot it already opened. Every non-refused declared field must carry exactly the key the snapshot's mode selects (physical name under `name`, field id under `id`, neither under `none`). The declared partition columns must be the log's `partitionColumns` in the log's order, matched by binding key under `name` or `id` and by name under `none`. On any disagreement the query fails naming the table, the part that disagrees, and the fix: correct the catalog entry, then `REFRESH`. The check derives nothing from the log.
- **Alternatives:** No check: rejected, because a catalog copy without the mode would bind the scan by display name to columns the files store under physical names, and a copy without `partition_index` would read a partition column from data files that never hold it; both return NULL silently. Re-derive the declaration from the log on a mismatch: rejected, because it is the fallback the review excluded and the per-reader derivation this issue deletes.
- **Rationale:** PROTOCOL.md § Column Mapping: "The table property should only be honored if the table's protocol has reader and writer versions and/or table features that support the `columnMapping` table feature", a condition only the log shows. The snapshot is already open for the gate and the file list, so the check adds no I/O. A check that refuses is not a derivation, so issue acceptance criterion 2 holds.
- **Consequences:** A table that enables column mapping after the last `REFRESH` fails its queries until `REFRESH`, although its existing columns' physical names equal their display names; the check is conservative there.
- **Promotes to ADR:** no

### [19] A declared non-nullable column that the source now allows NULL in fails the query

- **Decision:** At pushdown, the Iceberg reader looks up each declared non-nullable column in the current schema by its declared field id, and the Delta reader looks it up in the snapshot as decision [18] identifies columns. When the source marks the column nullable, the query fails naming the table, the column, and `ALTER VIRTUAL SCHEMA ... REFRESH`. A dropped column declared non-nullable with no `initial-default` keeps the recorded required-absent error until `REFRESH`.
- **Alternatives:** Declare every Iceberg and Delta column nullable, as Unity Parquet does: rejected, because it drops the recorded `nullable: !required` rule and the required-absent error that rule feeds. Accept the stale flag until `REFRESH`: rejected, because DataFusion 54.1 rewrites `c IS NULL` to `false` and `c IS NOT NULL` to `true` for a non-nullable field (`datafusion-optimizer` `simplify_expressions/expr_simplifier.rs`), so the query would drop matching rows without an error.
- **Rationale:** A note fixes nullability, which the Exasol `dataType` does not carry, so the note adds a drift that `dataType` alone never had. Both readers already load the current schema or snapshot at pushdown, so the check adds no I/O, and a check that refuses is not a derivation (issue acceptance criterion 2).
- **Consequences:** The dropped-column scenarios name an optional column, and a required dropped column fails as `scan-read-path/scan-execution-field-id-projection-absent-fields` records. A column made optional at the source fails every query on its table until `REFRESH`.
- **Promotes to ADR:** no

### [20] The catalog stays the Unity Parquet schema authority, and the declaration classifies `type_json` at CREATE

- **Decision:** A Unity Parquet table's logical schema still comes from the catalog's declared columns, and its partition columns still come from each column's `partition_index`. `createVirtualSchema` classifies each column's `type_json` through the shared Spark-type classifier with no column mapping and records the result in the column notes. The Unity Parquet reader takes the declaration from the notes, and the parquet-directory seam still supplies only the file list, filled against the declared partition columns. No footer is read.
- **Alternatives:** Keep the classification in the reader at pushdown, as the superseded ADR decides: rejected, because it is the per-reader derivation issue #426 removes (decision [1]).
- **Rationale:** `/speq:adr-rules` rule-2 criterion 1: the classification moves between components, from format-readers at pushdown to the CREATE-time declaration. Search: `speq decision-log show` holds `unity-parquet-schema-from-catalog-not-footer`, whose Decision states that the reader classifies each column's `type_json`. This entry supersedes that clause and keeps the rest: the catalog as schema authority, partition columns from `partition_index`, files only from the seam, and every field nullable.
- **Supersedes:** unity-parquet-schema-from-catalog-not-footer
- **Architecture:** § Components
- **Promotes to ADR:** yes

## Review Findings

### [1] [plan-review] The plan did not say how one plan becomes three PRs or when it is recorded

- **Finding:** `/speq:implement` and `/speq:record` act on a whole plan, so the plan's three PRs had no stated scope, and recording after Phase 1 would have recorded Iceberg, Unity, and Delta behavior before its code existed.
- **Direction change:** plan.md gains § Delivery. Each PR implements its phase's groups only. `/speq:record` runs once, with PR 3. A failed task 3.1 gate limits PR 3 to tasks 3.2 and 3.3 and returns the plan for revision before any recording. § Impact states that the recorded specs lag until then.
- **Promotes to ADR:** no

### [2] [plan-review] A note fixes nullability, and a stale non-nullable flag loses rows silently

- **Finding:** DataFusion 54.1 simplifies `IS NULL` on a non-nullable field to `false`, so a declared `nullable: false` that the source later relaxed, or that an OSS Unity copy got wrong, returns wrong rows without an error. A dropped required column also hit the required-absent error that the dropped-column scenario said would not occur.
- **Direction change:** Decision [19]. Both readers fail a query whose declared non-nullable column the source marks nullable, with NEW scenarios in `file-planning/pushdown-planning-file-resolution` and `delta/delta-table-planning`. The dropped-column scenario and the Delta exception now name an optional column and keep the required-absent error for a required one. The column-source-notes Background lists the four dimensions a note can drift on. Tasks 2.3, 2.4, 3.5, and 3.7, § Impact, and § Scenario Coverage carry the change.
- **Promotes to ADR:** no

### [3] [plan-review] The column note format created a component cycle

- **Finding:** vs-adapter owned the note format, while format-readers and pushdown-planner parse notes and vs-adapter depends on both, and `binary_refusal` returns `RefusedColumn` from `pushdown::format`, so `column_notes` and `pushdown::format` would import each other.
- **Direction change:** architecture.md records `column-notes` as its own component that depends only on scan-spec, and vs-adapter, pushdown-planner, and format-readers depend on it. Task 1.3 moves `RefusedColumn` into `adapter/column_notes.rs`, and the pushdown façade keeps re-exporting it under the same name (decision [11]).
- **Promotes to ADR:** no

### [4] [plan-review] Two accepted ADRs on catalog-declared Parquet went unaddressed

- **Finding:** `unity-parquet-schema-from-catalog-not-footer` decides that the Unity Parquet reader classifies `type_json`, and `one-catalog-declared-parquet-reader-and-one-iceberg-planner-serve-glue` generalizes that reader over a type source. After this plan the classification runs at CREATE.
- **Direction change:** Decision [20] supersedes `unity-parquet-schema-from-catalog-not-footer` and keeps the catalog as schema authority, partition columns from `partition_index`, and files only from the seam. Decision [1]'s Rationale states conformance with the Glue ADR: one catalog-declared Parquet reader and one Iceberg planner still serve Glue, and only the type-source generalization moves to the declaration.
- **Promotes to ADR:** no
