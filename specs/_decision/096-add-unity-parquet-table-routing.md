# Decisions: add-unity-parquet-table-routing

## ADR: The Unity scan source selects its reader by format tag, and both Unity readers share one table-storage component

**ID:** unity-scan-source-dispatches-by-format-tag
**Plan:** add-unity-parquet-table-routing
**Status:** Accepted

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
