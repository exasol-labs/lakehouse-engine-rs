# Feature: Delta Plan-Time File Pruning

Translates the soundly-translatable nodes of the Exasol WHERE predicate into a
`delta_kernel::expressions::Predicate` handed to the Delta scan builder, so log replay drops files on
partition values and per-file min/max statistics before any Parquet byte is read — while the full
predicate stays applied above the scan as the sole source of row-level correctness.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/delta/delta-file-pruning/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Delta pruning keeps every file with a matching row for every filter shape

* *GIVEN* a virtual schema over a partitioned Delta table whose files differ in partition value, per-file statistics, per-row-group bounds, per-page bounds, and NULL content, including a NULL partition value and a file whose statistics-only column is NULL in every row, and a native Exasol table that holds the same rows
* *WHEN* queries apply `NOT` over `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, `IS NULL`, and `IS NOT NULL` on the partition column, the statistics-only column, and both, with a `LIKE`, a scalar function, or a predicate the DataFusion dialect declines as the untranslatable side
* *THEN* each query SHALL return the native table's rows, and the scanned files SHALL include every file that holds a matching row
* *AND* a `NOT` over a partly translated predicate (`file-planning/pushdown-file-pruning`) MUST NOT prune by its negation, while for a fully translated predicate the scanned files SHALL be exactly the files the kernel's partition and statistics evaluation keeps, so a change that turns pruning off fails
* *AND* a query that scans no file SHALL take the empty-result route, and every other query SHALL apply its predicate in the scan or in the adapter's outer `WHERE`, never drop it
<!-- /DELTA:NEW -->
