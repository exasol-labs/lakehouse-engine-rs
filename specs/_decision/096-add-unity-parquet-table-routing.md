# Decisions: add-unity-parquet-table-routing

## ADR: A Unity Catalog Parquet table takes its schema and partition columns from the catalog, and only its file listing from the directory seam

**ID:** unity-parquet-schema-from-catalog-not-footer
**Plan:** add-unity-parquet-table-routing
**Status:** Accepted

### Context

The Unity Catalog client admitted only `DELTA` tables, so a `PARQUET` external table was skipped at listing and refused at plan time. The engine's existing Parquet reader for `DIRECT_STORAGE` derives its schema from footers. For a Unity table the catalog is the schema authority, as the requester stated. Unity Catalog declares partition columns through each column's `partition_index` and discovers their values by listing Hive-style directories under the table location.

### Decision

The Unity Parquet reader builds its logical schema from the catalog's declared columns and takes partition columns from each column's `partition_index`. It classifies each column's `type_json` through the Delta reader's Spark-type classifier with no column mapping. It asks the parquet-directory seam only for the file list, filled against the catalog-declared partition columns, and reads no footer.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse the direct-storage `ParquetFormatReader` | Rejected: its footer-folded schema would let data files redefine catalog-declared columns |
| Classify the Unity `type_name` through a new Spark-name-to-Arrow table | Rejected: `type_name` has no nested structure, so it refuses every struct, array, and map column, and it adds a third classifier |
| Infer partition columns from `key=value` path segments | Rejected: Unity Catalog already declares them |

### Consequences

`ColumnSourceType::Unity` gains `type_json`, and `CatalogTable` gains ordered `partition_columns`, which are empty for the Iceberg REST and direct-storage clients. Every logical field is nullable, because a raw Parquet directory does not guarantee that every file carries every column. Partition values come from `key=value` segments matched by the uppercase fold, and a missing segment reads NULL. Only `string` partition columns reach the partition-pruning predicate, which compares strings. A data file without the `.parquet` suffix is not read, a trade-off inherited from the seam. The Unity admission filter now admits `DELTA` and `PARQUET` base tables. `SkipReason::NotDeltaBaseTable` and its warning keep their names, because every skipped entry is still not a Delta base table.

## ADR: The Unity scan source selects its reader by format tag, and both Unity readers share one table-storage component

**ID:** unity-scan-source-dispatches-by-format-tag
**Plan:** add-unity-parquet-table-routing
**Status:** Accepted
**Supersedes:** format-dispatch-matches-scansource-not-catalogkind

### Context

A second table format under Unity Catalog means the scan-source dispatch can no longer assume that Unity implies Delta. The storage-location check, the vended-or-static storage decision, and error redaction lived in `DeltaFormatReader`, and a second reader needs the same rules without a drifting copy.

### Decision

`ScanSource::UnityDelta` becomes `ScanSource::Unity`, and its arm in `format_reader` matches the table's `TableFormat` exhaustively. Delta selects `DeltaFormatReader`, Parquet selects `UnityParquetFormatReader`, and Iceberg is refused. The storage-location check, the vended-or-static decision, and error redaction move into one `UnityTableStorage` component that both readers compose.

### Options Considered

| Option | Verdict |
|--------|---------|
| Duplicate the vending logic in the new reader | Rejected: the no-fallback rule and `READ` scope can drift, and drift reads storage with an unselected credential |
| One `UnityFormatReader` that branches on format internally | Rejected: hides a second format dispatch, while `format_reader` is the one recorded dispatch site |

### Consequences

This supersedes the naming consequence of the earlier ADR, which kept the name `UnityDelta` until a second format appeared. The dispatch rule (match `ScanSource`, never `CatalogKind`) is unchanged, as are `DeltaFormatReader`'s behavior and error texts.

## ADR: The scan binds an identity-bound column across letter case and refuses a physical type it cannot admit, for every format

**ID:** scan-case-folds-identity-binding-refuses-unadmitted-types
**Plan:** add-unity-parquet-table-routing
**Status:** Accepted

### Context

Exact-case identity binding with forced nullability turned a case-drifted column into NULL, and `DefaultPhysicalExprAdapter` cast every castable pair, including narrowing casts that truncate values. Delta PROTOCOL.md requires a data file column to have the table schema's type, except as Type Widening allows. Spark resolves Parquet columns case-insensitively by default.

### Decision

Before the cast, the scan admits each bound column per file. It admits a pair that `widen` resolves to the logical type, a timestamp pair that keeps every stored instant, or a text-rendered type under a string tag. Any other pair fails a query that reads the column, naming the storage location, the column, and both types. Identity binding matches the exact name first, then the uppercase fold, and fails on an ambiguous fold. Field-id and declared-physical-name bindings stay exact.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the arrow cast and document the drift | Rejected: the cast truncates `double` 1.7 to `int` 1, and a case-drifted column reads NULL |
| Check drift in the Unity Parquet reader alone | Rejected: that reader reads no footer, and all formats share one binding and cast path |
| Fold every name step, including field-id and name-mapping bindings | Rejected: those bindings follow keys the table metadata records |

### Consequences

This supersedes the narrowing-direction clauses of `datafusion-scan/type-relaxation`: a logical type narrower than a file's physical column now fails the query. Direct storage under `MERGE_SCHEMA = 'FALSE'` fails when a file stores a column wider than the sampled type, where it used to narrow silently. A string-tagged column admits every text-rendered physical type, so a catalog `string` over a `binary` file column renders text, because the scan cannot tell it from a JSON-fallback column.
