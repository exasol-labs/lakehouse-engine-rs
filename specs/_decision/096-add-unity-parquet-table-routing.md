# Decisions: add-unity-parquet-table-routing

## ADR: A Unity Catalog Parquet table takes its schema and partition columns from the catalog, and only its file listing from the directory seam

**ID:** unity-parquet-schema-from-catalog-not-footer
**Plan:** add-unity-parquet-table-routing
**Status:** Accepted

### Context

The Unity Catalog client admitted only `DELTA` base tables. A `PARQUET` external table was skipped
at listing and refused at plan time. The engine already owns a Parquet reader for
`DIRECT_STORAGE`, but that reader derives its schema by folding footers. For a Unity table the
catalog is the schema authority: the requester stated that "the catalog and the format are two
different things... the format doesn't establish the schema". Unity Catalog declares partition
columns through each column's `partition_index` and discovers their values by recursively listing
the Hive-style directories under the table location by default (Databricks, *Partition discovery
for external tables*).

### Decision

The Unity Parquet reader builds its logical schema from the catalog's declared columns and its
partition columns from each column's `partition_index`. It classifies each column's `type_json` (a
Spark `StructField` JSON) through the Delta reader's Spark-type classifier
(`build_delta_table_schema`) under no column mapping. It asks the parquet-directory seam only for
the file list, filled against those catalog-declared partition columns, and reads no footer.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build the schema from the catalog's declared columns and partition columns; ask the seam only for the file list | ✓ Chosen — the catalog is the schema authority, so a footer read cannot redefine its declared columns |
| Reuse the direct-storage `ParquetFormatReader`, which folds footers into the schema | ✗ Rejected — would let a table's data files redefine its catalog-declared columns |
| Classify the Unity `type_name` through a new Spark-name-to-Arrow table | ✗ Rejected — `type_name` carries no nested structure, refusing every struct, array, and map column, and would add a third Spark-type classifier beside the Delta one and the createVirtualSchema one |
| Infer partition columns from `key=value` path segments as `DIRECT_STORAGE` does | ✗ Rejected — Unity Catalog already declares them |

### Consequences

`ColumnSourceType::Unity` gains `type_json`, and `CatalogTable` gains ordered `partition_columns`,
empty for the Iceberg REST and direct-storage clients. Every logical field is declared nullable,
because a raw Parquet directory does not guarantee every file carries every column. Partition
values come from `key=value` directory segments matched by the uppercase fold; a missing segment
reads NULL, as `DIRECT_STORAGE` already does. Only `string` partition columns reach the
partition-pruning predicate, because it compares values as strings. A data file without the
`.parquet` suffix is not read, a documented trade-off inherited from the seam's data-file rule.
Amends the Unity Catalog client's admission filter, which now admits `DELTA` and `PARQUET` base
tables; `SkipReason::NotDeltaBaseTable` and its warning text keep their names, because every entry
the filter skips is still not a Delta base table.

## ADR: The Unity scan source selects its reader by format tag, and both Unity readers share one table-storage component

**ID:** unity-scan-source-dispatches-by-format-tag
**Plan:** add-unity-parquet-table-routing
**Status:** Accepted
**Supersedes:** format-dispatch-matches-scansource-not-catalogkind

### Context

Adding a second table format under the Unity Catalog kind meant the scan-source dispatch could no
longer assume Unity Catalog implies Delta. The storage-location check, the vended-or-static storage
decision, and error redaction lived inside `DeltaFormatReader`, and a second reader needed the same
rules without a second copy that could drift from the first.

### Decision

`ScanSource::UnityDelta` becomes `ScanSource::Unity`. Its arm in `format_reader` matches the loaded
table's `TableFormat` exhaustively: Delta selects `DeltaFormatReader`, Parquet selects
`UnityParquetFormatReader`, and Iceberg is refused. The storage-location check, the vended-or-static
storage decision, and error redaction move out of `DeltaFormatReader` into one `UnityTableStorage`
component that both readers compose.

### Options Considered

| Option | Verdict |
|--------|---------|
| One `UnityTableStorage` component shared by both readers | ✓ Chosen — one owner for the vending decision and the no-fallback rule, for every format Unity Catalog hosts |
| Duplicate the vending logic in the new reader | ✗ Rejected — two copies of the no-fallback rule and the `READ` scope can drift, and a drift reads storage with a credential the operator did not select |
| One `UnityFormatReader` that resolves storage and then branches on format internally | ✗ Rejected — hides a second format dispatch inside a reader, while `format_reader` is the one recorded dispatch site |

### Consequences

Supersedes the naming consequence of `format-dispatch-matches-scansource-not-catalogkind`, which
kept the name `UnityDelta` to state a Unity-implies-Delta coupling until a second format appeared.
The dispatch decision itself (match `ScanSource`, never `CatalogKind`) is unchanged.
`DeltaFormatReader`'s behavior and error texts are unchanged.

## ADR: The scan binds an identity-bound column across letter case and refuses a physical type it cannot admit, for every format

**ID:** scan-case-folds-identity-binding-refuses-unadmitted-types
**Plan:** add-unity-parquet-table-routing
**Status:** Accepted

### Context

An exact-case identity binding and forced nullability turned a case-drifted column into NULL, and
`DefaultPhysicalExprAdapter` cast every arrow-castable pair, including a narrowing cast that could
truncate a value. Delta PROTOCOL.md § Consistency Between Table Metadata and Data Files states:
"Any data file column that exists in the table schema MUST have the same type (except as allowed
by the [Type Widening] table feature, if enabled)." Spark resolves Parquet columns
case-insensitively by default (`spark.sql.caseSensitive=false`).

### Decision

Before the cast, the scan admits each bound column per file. It admits a pair that the
supported-set owner (`widen`) resolves to the logical type, a timestamp pair that keeps every
stored instant, or a text-rendered type under a string tag. Every other pair fails a query that
reads the column, naming the table's storage location, the column, and both types. The identity
binding matches the exact name first, then the uppercase fold, and fails on an ambiguous fold.
Field-id and declared-physical-name bindings stay exact.

### Options Considered

| Option | Verdict |
|--------|---------|
| Admit only the proven-castable set, fold case for identity binding only, refuse everything else loud | ✓ Chosen — requester choice: fail loud, Spark-compatible, rather than truncate or drift silently |
| Keep the arrow cast and document the drift outcomes | ✗ Rejected — the cast truncates `double` 1.7 to `int` 1, and a case-drifted column reads NULL |
| Check drift inside the Unity Parquet reader alone | ✗ Rejected — that reader reads no footer, and every format shares one binding and cast path |
| Fold every name step, including field-id and name-mapping bindings | ✗ Rejected — those bindings follow keys the table's own metadata records |

### Consequences

Supersedes the `datafusion-scan/type-relaxation` narrowing-direction clauses: a logical type
narrower than a file's physical column now fails the query. Direct storage under
`MERGE_SCHEMA = 'FALSE'` now fails a query that reads a column some file stores wider than the
sampled type, rather than silently narrowing it. A string-tagged column admits every text-rendered
physical type, so a catalog `string` over a `binary` file column renders text instead of failing,
because the scan cannot tell it from a JSON-fallback column.
