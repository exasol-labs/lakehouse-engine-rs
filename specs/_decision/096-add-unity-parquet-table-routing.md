# Decisions: add-unity-parquet-table-routing

## ADR: The scan binds an identity-bound column across letter case and refuses a physical type it cannot admit, for every format

**ID:** scan-case-folds-identity-binding-refuses-unadmitted-types
**Plan:** add-unity-parquet-table-routing
**Status:** Accepted

### Context

Exact-case column binding turned a case-drifted column into NULL, and the default expression adapter cast every castable pair, including narrowing casts that truncate values. The Delta protocol requires a data file column to have the table schema's type, except as Type Widening allows. Spark resolves Parquet columns case-insensitively by default.

### Decision

For every format, the scan checks each bound column per file before the cast. It admits a type pair that widens to the logical type, a timestamp pair that keeps every stored instant, or a text-rendered type under a string tag. Any other pair fails a query that reads the column, naming the storage location, the column, and both types. Name binding matches the exact name first, then the uppercase fold, and fails on an ambiguous fold. Field-id and declared-physical-name bindings stay exact.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the arrow cast and document the drift | Rejected: the cast truncates a double 1.7 to the integer 1, and a case-drifted column reads NULL |
| Check drift in the Unity Parquet reader alone | Rejected: all formats share one binding and cast path |
| Fold every name step, including field-id and name-mapping bindings | Rejected: those bindings follow keys the table metadata records exactly |

### Consequences

- A logical type narrower than a file's physical column fails the query instead of narrowing silently, including direct storage with schema merging off.
- A string-tagged column admits every text-rendered physical type, so a catalog `string` over a `binary` file column renders text, because the scan cannot tell it from a JSON-fallback column.
