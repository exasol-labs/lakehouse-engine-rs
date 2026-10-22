# Feature: Format-Neutral Pushdown Resolution

Routes every pushdown request through the table-format reader seam. The reader that owns a table's format produces its active file list, logical schema, partition columns, table root, and storage, and the rest of the pipeline names no format. A single-table scan, each leg of a broadcast join, and every aggregate shape therefore reach a Delta table by the same route as an Iceberg table.

## Background

* A `FormatReader` returns a `ResolvedScan`. `format_reader` selects the reader by matching a `ScanSource` exhaustively, and both concrete readers are private to that module.
* A request resolves its catalog session once and reuses it for every table it touches, so a two-leg join performs no more catalog authentication round-trips than a single-table scan.
* Pushdown SQL shape (projection, filter, LIMIT, ORDER BY, aggregate decomposition, join eligibility) is decided from the request's SQL alone. Only file resolution differs per format.
* The generated SQL and the serialized per-shard scan specs are byte-identical to the output before the format-reader seam, except for the `storage` value, which is the tagged wrapper of `storage-access/scan-spec-credential-reference`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A Unity Catalog table's identity survives the round trip from the involved table

* *GIVEN* a pushdown request under the Unity Catalog kind whose involved table's identifier note,
  recorded per `vs-adapter/create-virtual-schema`, holds a three-level Unity Catalog identifier
* *WHEN* the adapter resolves that identifier into the table its format reader plans
* *THEN* the adapter SHALL recover the catalog table identifier's namespace segments and table name
  from the recorded identifier and SHALL load exactly that table from the Unity Catalog
* *AND* the loaded table SHALL be the one whose catalog-reported full name equals the recorded
  identifier, because the Unity Catalog addresses a table by that same dotted full name and the
  segmentation is therefore lossless
* *AND* a recorded identifier that yields no table name SHALL fail with an error naming the
  unresolvable identifier, and MUST NOT resolve a different table
* *AND* a pushdown request whose involved table carries no readable identifier note SHALL fail with the error `file-planning/pushdown-planning-file-resolution` specifies for a missing note, under every catalog kind
<!-- /DELTA:CHANGED -->
