# Feature: Direct-Storage Hive Partitioning

Declares the `key=value` directory segments of a direct-storage table as partition columns, and
prunes the table's files on those values at plan time. A partitioned directory therefore exposes
its keys as columns, and a query that filters on them reads only the matching files.

<!-- DELTA:CHANGED -->
## Background

* A partition value is directory text, constant for every row of its file. Pruning evaluates it
  directly.
* A partition key NAME matches its column under the uppercase fold, while its VALUE stays verbatim.
  Writers keep the case a column was declared with in the directory name, while metastores and
  query engines resolve names case-insensitively: the Hive metastore stores partition column names
  lowercased (SPARK-19359), Spark resolves partition columns case-insensitively under its default
  `spark.sql.caseSensitive=false`, and Exasol folds unquoted identifiers to upper case. Two
  spellings of one key cannot be told apart by name, so they are an error rather than a silent pick.
* Enumeration decides a table's partition columns once and records each one's position in its
  column note (`vs-adapter/column-source-notes`). A pushdown fills and prunes against those recorded
  columns.
* Apache Iceberg and Delta specification check: NOT implicated. Hive-style directory partitioning
  belongs to neither specification, and this kind implements neither format
  (`direct-storage/direct-storage-table-planning`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A key=value directory segment declares a VARCHAR partition column

* *GIVEN* a table `sales/` holding `year=2026/month=09/p1.parquet` and `year=2025/month=__HIVE_DEFAULT_PARTITION__/p2.parquet`, and a table `encoded/` holding `region=a%2Fb/p.parquet`, under a direct-storage virtual schema that leaves `HIVE_PARTITIONING` absent
* *WHEN* the virtual schema is created and each table is queried
* *THEN* `SALES` SHALL declare `YEAR` and `MONTH` as `VARCHAR(2000000)` after its Parquet columns, and each row SHALL carry its own file's values
* *AND* a DIRECTORY segment below the table root (every segment except the file name) SHALL be a partition segment only when it matches `^[^/=]+=[^/]*$`, at any depth and in any order, with the key before the first `=` and the value after it, so any other directory contributes no column and a file name that happens to match the pattern (e.g. `x=1.parquet`) never does
* *AND* the value, but not the key, SHALL be percent-decoded, so `ENCODED.REGION` reads `a/b`, and a value that does not decode to valid UTF-8 SHALL keep its raw text
* *AND* `__HIVE_DEFAULT_PARTITION__` and an empty value SHALL read NULL, because the scan already reads an empty partition value as NULL
* *AND* a key repeated within one path under one spelling SHALL take its deepest value
* *AND* a partition column MUST NOT be typed from its values, because a directory value carries no type and a type inferred from the values seen changes when a new directory appears
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A table's partition columns are the union of its files' keys

* *GIVEN* a table `mixed/` holding `A/p.parquet` and `year=2026/p.parquet`
* *WHEN* the table is declared and queried under `MERGE_SCHEMA` TRUE
* *THEN* `MIXED` SHALL declare `YEAR`, reading `2026` for the second file's rows and NULL for the first file's rows
* *AND* partition columns SHALL be ordered by each key's first appearance in the deterministic listing order, shallow to deep within one path
* *AND* each file's partition-value map SHALL carry EVERY declared key, with no value where its path lacks the segment, because the scan treats a declared key missing from the map as a planning defect
* *AND* under `MERGE_SCHEMA = 'FALSE'` the declared keys SHALL come from the sampled file's path alone, and a key only other files carry SHALL be ignored, so a divergent layout degrades to NULL values and ignored keys and MUST NOT fail a query or return a wrong row; a key that differs from a declared key only in letter case is not ignored but fails per the two-spellings scenario below
* *AND* the user documentation of `MERGE_SCHEMA` SHALL state the precondition that every file shares one partition layout under `'FALSE'`
* *AND* query planning SHALL fill and prune against the partition columns the column notes record, so enumeration decides a table's partition columns once and a later file's new key stays undeclared until `REFRESH`
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Two partition keys that fold to the same name fail the refresh

* *GIVEN* a table whose files sit under both `Year=2026` and `year=2026` directories, under `MERGE_SCHEMA` TRUE
* *WHEN* the virtual schema is created or refreshed
* *THEN* the statement SHALL fail with an error naming both spellings and a file carrying each
* *AND* the comparison SHALL be the declaration's uppercase fold, because both names would become the same Exasol column
* *AND* the adapter MUST NOT rename, drop, or prefer either spelling, because neither key is the file's own stored value and there is no established precedent for choosing between two directory encodings of one logical column
* *AND* under `MERGE_SCHEMA = 'FALSE'` the declared keys SHALL come from the sampled file's own path alone, so this check SHALL cover only that one file's own keys, and a collision carried only by unsampled files SHALL go undetected, per the sampled-file layout precondition the union scenario above states for `'FALSE'`
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Two spellings of one partition key fail the refresh and the query

* *GIVEN* a table whose files sit under both `Year=2026` and `year=2025` directories, once under `MERGE_SCHEMA` TRUE and once under `MERGE_SCHEMA = 'FALSE'`
* *AND* a second table created with every file under `year=` directories, to which a file under a `YEAR=2027` directory is added after creation
* *WHEN* the first virtual schema is created or refreshed, and a query over the second table is planned without a refresh
* *THEN* each statement SHALL fail with an error naming both spellings and the path of a file carrying each
* *AND* the comparison SHALL be the declaration's uppercase fold, because both names would become the same Exasol column
* *AND* the check SHALL cover every listed file under both merge modes, because it reads no footer
* *AND* the adapter MUST NOT rename, drop, or prefer either spelling, because neither key is the file's own stored value and there is no established precedent for choosing between two directory encodings of one logical column
* *AND* a key repeated within one path under two spellings, such as `year=2025/Year=2026/p.parquet`, SHALL fail the same way
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A partition key matches its declared column across letter case

* *GIVEN* a table created with every file under `year=` directories, whose files are then all rewritten under `YEAR=` directories with the same values
* *WHEN* a query carrying `WHERE YEAR = '2026'` runs without a refresh
* *THEN* each row SHALL read its file's `YEAR=` segment value as the declared `year` column, because a key name matches its column under the uppercase fold
* *AND* the filter SHALL prune the files whose `YEAR=` value differs, and the query SHALL return the same rows as before the rewrite
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A predicate on partition columns prunes files before their footers are read

* *GIVEN* the `sales/` table, extended with a third file so its three files carry the `year` values `2026`, `2025`, and `2024`, under `MERGE_SCHEMA` TRUE
* *WHEN* the adapter plans a query over `SALES` carrying `WHERE YEAR = '2026'`, and separately a query carrying `WHERE YEAR > '2025'`
* *THEN* the resolved file list SHALL hold only the `year=2026` file for the equality query, and only the `year=2026` file for the range query too, excluding both `year=2025` and `year=2024`; planning SHALL read no footer of any file, kept or pruned
* *AND* the returned rows SHALL equal the rows of the same query without pruning, because the full predicate still applies above the scan per `pushdown/pushdown-declined-filter-self-apply`
* *AND* a file SHALL be pruned only when the predicate cannot evaluate TRUE for any of its rows: a node over partition columns SHALL evaluate on the file's own values under SQL three-valued logic, and every other node SHALL count as possibly TRUE, FALSE, or NULL
* *AND* the evaluated nodes SHALL be `=`, `<>`, `IN`, `IS NULL`, `IS NOT NULL`, `<`, `<=`, `>`, `>=`, and `BETWEEN` of a partition column against non-empty string literals, combined by `AND`, `OR`, and `NOT`, so a function or a non-string literal never prunes a file
* *AND* a range or `BETWEEN` comparison SHALL compare partition values as plain strings in byte/codepoint order, which is DataFusion's `Utf8` comparison order and, as verified live against a running Exasol instance per this project's SQL-capability verification rule, also Exasol's own `VARCHAR` comparison order
* *AND* a filter column SHALL resolve to a partition column by the declaration's uppercase fold
* *AND* a predicate that keeps no file SHALL resolve to zero kept files, and the query SHALL return zero rows without error
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A declared column absent from every kept file reads NULL

* *GIVEN* the `sales/` table, whose `year=2026` file carries a column `DISCOUNT` that its `year=2025` file lacks, under `MERGE_SCHEMA` TRUE
* *WHEN* an Exasol user selects `DISCOUNT` from `SALES` with `WHERE YEAR = '2025'`
* *THEN* the query SHALL return the `year=2025` file's rows with `DISCOUNT` NULL, and MUST NOT fail
* *AND* the reader SHALL take `DISCOUNT`'s logical field from the column notes enumeration recorded, never from whichever files pruning happens to keep, because plan-time pruning narrows which files are read, not what the table's schema is
* *AND* such a field SHALL carry no binding key, so every scanned file that lacks the column reads it as NULL
<!-- /DELTA:CHANGED -->
