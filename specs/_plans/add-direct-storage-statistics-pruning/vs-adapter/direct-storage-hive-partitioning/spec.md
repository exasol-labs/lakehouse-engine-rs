# Feature: Direct-Storage Hive Partitioning

Declares the `key=value` directory segments of a direct-storage table as partition columns, and
prunes the table's files on those values at plan time. A partitioned directory therefore exposes
its keys as columns, and a query that filters on them reads only the matching files.

## Background

* A partition value is directory text, constant for every row of its file. Pruning evaluates it
  directly.
* Apache Iceberg and Delta specification check: NOT implicated. Hive-style directory partitioning
  belongs to neither specification, and this kind implements neither format
  (`vs-adapter/direct-storage-table-planning`).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A predicate on partition columns prunes files before their footers are read

* *GIVEN* the `sales/` table, extended with a third file so its three files carry the `year` values `2026`, `2025`, and `2024`, under `MERGE_SCHEMA` TRUE
* *WHEN* the adapter plans a query over `SALES` carrying `WHERE YEAR = '2026'`, and separately a query carrying `WHERE YEAR > '2025'`
* *THEN* the resolved file list SHALL hold only the `year=2026` file for the equality query, and only the `year=2026` file for the range query too, excluding both `year=2025` and `year=2024`; the footer of a pruned file MUST NOT be read
* *AND* the returned rows SHALL equal the rows of the same query without pruning, because the full predicate still applies above the scan per `vs-adapter/pushdown-declined-filter-self-apply`
* *AND* a file SHALL be pruned before its footer is read only when the predicate cannot evaluate TRUE for any of its rows: a node over partition columns SHALL evaluate on the file's own values under SQL three-valued logic, and every other node SHALL count as possibly TRUE, FALSE, or NULL in this pre-footer pass
* *AND* the evaluated nodes SHALL be `=`, `<>`, `IN`, `IS NULL`, `IS NOT NULL`, `<`, `<=`, `>`, `>=`, and `BETWEEN` of a partition column against non-empty string literals, combined by `AND`, `OR`, and `NOT`, so a function or a non-string literal never prunes a file on its partition values
* *AND* the files this pass keeps SHALL then be pruned from their footers by `vs-adapter/direct-storage-statistics-pruning`, which evaluates the same predicate on the same partition values
* *AND* a range or `BETWEEN` comparison SHALL compare partition values as plain strings in byte/codepoint order, which is DataFusion's `Utf8` comparison order and, as verified live against a running Exasol instance per this project's SQL-capability verification rule, also Exasol's own `VARCHAR` comparison order
* *AND* a filter column SHALL resolve to a partition column by the declaration's uppercase fold
* *AND* a predicate that keeps no file SHALL resolve to zero kept files with zero footers read, and the query SHALL return zero rows without error
<!-- /DELTA:CHANGED -->
