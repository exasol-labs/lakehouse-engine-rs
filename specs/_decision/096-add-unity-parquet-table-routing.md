# Decisions: add-unity-parquet-table-routing

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
