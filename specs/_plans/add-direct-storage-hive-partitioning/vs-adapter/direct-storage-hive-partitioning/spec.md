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

### Scenario: A key=value directory segment declares a VARCHAR partition column

* *GIVEN* a table `sales/` holding `year=2026/month=09/p1.parquet` and `year=2025/month=__HIVE_DEFAULT_PARTITION__/p2.parquet`, and a table `encoded/` holding `region=a%2Fb/p.parquet`, under a direct-storage virtual schema that leaves `HIVE_PARTITIONING` absent
* *WHEN* the virtual schema is created and each table is queried
* *THEN* `SALES` SHALL declare `YEAR` and `MONTH` as `VARCHAR(2000000)` after its Parquet columns, and each row SHALL carry its own file's values
* *AND* a DIRECTORY segment below the table root (every segment except the file name) SHALL be a partition segment only when it matches `^[^/=]+=[^/]*$`, at any depth and in any order, with the key before the first `=` and the value after it, so any other directory contributes no column and a file name that happens to match the pattern (e.g. `x=1.parquet`) never does
* *AND* the value, but not the key, SHALL be percent-decoded, so `ENCODED.REGION` reads `a/b`, and a value that does not decode to valid UTF-8 SHALL keep its raw text
* *AND* `__HIVE_DEFAULT_PARTITION__` and an empty value SHALL read NULL, because the scan already reads an empty partition value as NULL
* *AND* a key repeated within one path SHALL take its deepest value
* *AND* a partition column MUST NOT be typed from its values, because a directory value carries no type and a type inferred from the values seen changes when a new directory appears

### Scenario: A table's partition columns are the union of its files' keys

* *GIVEN* a table `mixed/` holding `A/p.parquet` and `year=2026/p.parquet`
* *WHEN* the table is declared and queried under `MERGE_SCHEMA` TRUE
* *THEN* `MIXED` SHALL declare `YEAR`, reading `2026` for the second file's rows and NULL for the first file's rows
* *AND* partition columns SHALL be ordered by each key's first appearance in the deterministic listing order, shallow to deep within one path
* *AND* each file's partition-value map SHALL carry EVERY declared key, with no value where its path lacks the segment, because the scan treats a declared key missing from the map as a planning defect
* *AND* under `MERGE_SCHEMA = 'FALSE'` the declared keys SHALL come from the sampled file's path alone, and a key only other files carry SHALL be ignored, so a divergent layout degrades to NULL values and ignored keys and MUST NOT fail a query or return a wrong row
* *AND* the user documentation of `MERGE_SCHEMA` SHALL state the precondition that every file shares one partition layout under `'FALSE'`
* *AND* table enumeration and query planning SHALL declare the same partition columns, because both obtain them from `vs-adapter/parquet-directory-seam`

### Scenario: Two partition keys that fold to the same name fail the refresh

* *GIVEN* a table whose files sit under both `Year=2026` and `year=2026` directories, under `MERGE_SCHEMA` TRUE
* *WHEN* the virtual schema is created or refreshed
* *THEN* the statement SHALL fail with an error naming both spellings and a file carrying each
* *AND* the comparison SHALL be the declaration's uppercase fold, because both names would become the same Exasol column
* *AND* the adapter MUST NOT rename, drop, or prefer either spelling, because neither key is the file's own stored value and there is no established precedent for choosing between two directory encodings of one logical column
* *AND* under `MERGE_SCHEMA = 'FALSE'` the declared keys SHALL come from the sampled file's own path alone, so this check SHALL cover only that one file's own keys, and a collision carried only by unsampled files SHALL go undetected, per the sampled-file layout precondition the union scenario above states for `'FALSE'`

### Scenario: A partition key that names a Parquet column overrides it

* *GIVEN* a table whose Parquet files carry a column `K`, every one of them sitting under a `k=` directory segment
* *WHEN* the virtual schema is created or refreshed under `MERGE_SCHEMA` TRUE, and `K` is later queried
* *THEN* the statement SHALL succeed and SHALL declare `K` exactly once, as the partition column, `VARCHAR(2000000)`, appended after the folded columns in the position `vs-adapter/parquet-directory-seam` specifies
* *AND* every row SHALL read `K` from its file's `k=` segment value, and the file's own stored `K` column SHALL NEVER be read, because the fold drops a Parquet column that collides with a declared key rather than adding it
* *AND* the comparison SHALL be the declaration's uppercase fold, matching every other collision check in this feature
* *AND* under `MERGE_SCHEMA = 'FALSE'` the fold reads only the sampled file's own footer, so this check SHALL run against that one file alone: when the sampled file carries `K` and its own path carries the `k=` segment, the same drop-and-override rule applies from that single footer, and no claim is made about any unsampled file

### Scenario: A file missing the colliding key's segment fails the refresh

* *GIVEN* a table whose Parquet files carry a column `K`, where at least one file sits under a `k=` directory segment (so `k` is a declared key) and at least one other file also carries the stored column `K` but its own path holds no `k=` segment
* *WHEN* the virtual schema is created or refreshed under `MERGE_SCHEMA` TRUE
* *THEN* the statement SHALL fail with an error naming `K`, the key `k`, and the path of one file lacking the segment
* *AND* the adapter MUST NOT read that file's `K` as NULL and MUST NOT fall back to its own stored value, because neither answer is the directory value the override rule promises, and the fold's job is to pick exactly one source for the column across the whole table, not a per-file source
* *AND* a file under `k=__HIVE_DEFAULT_PARTITION__/` or `k=` (empty value) carries the segment, so it SHALL read `K` as NULL under the override and MUST NOT fail the statement, because this check tests whether the file's path carries the segment at all, never the value that segment decodes to
* *AND* under `MERGE_SCHEMA = 'FALSE'` this check SHALL apply only to the sampled file itself: when the sampled file's own footer carries `K` and its own path holds no `k=` segment, `CREATE`/`REFRESH` SHALL fail the same way, naming `K`, `k`, and the sampled file's own path. An unsampled file with the identical problem SHALL go undetected, per the sampled-file layout precondition the union scenario above states for `'FALSE'`

### Scenario: HIVE_PARTITIONING = FALSE reads key=value segments as plain directories

* *GIVEN* the `sales/` table and the `k=1` table above, under a virtual schema with `HIVE_PARTITIONING = 'FALSE'`
* *WHEN* the virtual schema is created and `SALES` is queried
* *THEN* no table SHALL declare a partition column, and every file entry SHALL carry an empty partition-value map
* *AND* the `k=1` table SHALL be declared without error, because no key exists to collide
* *AND* no file SHALL be pruned by a partition value

### Scenario: A predicate on partition columns prunes files before their footers are read

* *GIVEN* the `sales/` table, extended with a third file so its three files carry the `year` values `2026`, `2025`, and `2024`, under `MERGE_SCHEMA` TRUE
* *WHEN* the adapter plans a query over `SALES` carrying `WHERE YEAR = '2026'`, and separately a query carrying `WHERE YEAR > '2025'`
* *THEN* the resolved file list SHALL hold only the `year=2026` file for the equality query, and only the `year=2026` file for the range query too, excluding both `year=2025` and `year=2024`; the footer of a pruned file MUST NOT be read
* *AND* the returned rows SHALL equal the rows of the same query without pruning, because the full predicate still applies above the scan per `vs-adapter/pushdown-declined-filter-self-apply`
* *AND* a file SHALL be pruned only when the predicate cannot evaluate TRUE for any of its rows: a node over partition columns SHALL evaluate on the file's own values under SQL three-valued logic, and every other node SHALL count as possibly TRUE, FALSE, or NULL
* *AND* the evaluated nodes SHALL be `=`, `<>`, `IN`, `IS NULL`, `IS NOT NULL`, `<`, `<=`, `>`, `>=`, and `BETWEEN` of a partition column against non-empty string literals, combined by `AND`, `OR`, and `NOT`, so a function or a non-string literal never prunes a file
* *AND* a range or `BETWEEN` comparison SHALL compare partition values as plain strings in byte/codepoint order, which is DataFusion's `Utf8` comparison order and, as verified live against a running Exasol instance per this project's SQL-capability verification rule, also Exasol's own `VARCHAR` comparison order
* *AND* a filter column SHALL resolve to a partition column by the declaration's uppercase fold
* *AND* a predicate that keeps no file SHALL resolve to zero kept files with zero footers read, and the query SHALL return zero rows without error

### Scenario: A declared column absent from every kept file reads NULL

* *GIVEN* the `sales/` table, whose `year=2026` file carries a column `DISCOUNT` that its `year=2025` file lacks, under `MERGE_SCHEMA` TRUE
* *WHEN* an Exasol user selects `DISCOUNT` from `SALES` with `WHERE YEAR = '2025'`
* *THEN* the query SHALL return the `year=2025` file's rows with `DISCOUNT` NULL, and MUST NOT fail
* *AND* the reader SHALL add each Exasol-declared column that neither the kept files' footers nor the partition columns carry as a nullable logical field typed from its declared Exasol type, sourced from the table's schema as Exasol already fixed it at `REFRESH` and echoes per query in the pushdown request's `involvedTables`, never recomputed from whichever files pruning happens to keep, because plan-time pruning narrows which files are read, not what the table's schema is for an absent column
* *AND* such a field SHALL carry no binding key and no nested descriptor, so every scanned file reads it as NULL
