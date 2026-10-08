# Feature: Pushdown File Pruning

Translates the soundly-translatable conjuncts of the Exasol WHERE predicate into an
`iceberg::expr::Predicate` that is applied to the Iceberg table scan at file-resolution
time, so `plan_files` prunes data files on partition values and per-file min/max bounds
before any S3 I/O — while the DataFusion scan keeps applying the full filter as the sole
source of row-level correctness. This delta extends the same resolve-once seam to preserve
each data file's associated positional-delete files and to fail loud on delete mechanisms
the engine cannot apply.

<!-- DELTA:CHANGED -->
## Background

* Apache Iceberg table spec, § "Scan Planning": "Scan predicates are converted to partition
  predicates using an _inclusive projection_: if a scan predicate matches a row, then the partition
  predicate must match that row’s partition." The same section applies scan predicates to
  statistics: "Scan predicates are also used to filter data and delete files using column bounds
  and counts that are stored by field id in manifests." Pruning therefore never skips a file that
  holds a matching row (#466).
* The pruning predicate is sound-not-complete. It keeps every file that could hold a matching row
  and may keep more. A node that cannot be translated soundly imposes no constraint.
* The full predicate is still applied to every row: in the scan, or in the adapter's own outer
  `WHERE` when the DataFusion dialect declines it (`pushdown/pushdown-declined-filter-self-apply`).
  Pruning receives the full filter in both cases. It changes which files are read, never which rows
  are returned.
* "Untranslatable" here means untranslatable to an Iceberg pruning predicate, which costs pruning
  only. A DataFusion-dialect decline is a separate question.
* A translated node is exact when it is true on exactly the rows where the user predicate is true,
  and false on exactly the rows where it is false. On a row where the user predicate is NULL, it may
  be either. A translated node that is not exact is partly translated: it is true on every matching
  row and may be true on more.
* For a fully translated predicate, pruning keeps exactly the files that Iceberg's inclusive
  evaluation of the translated predicate keeps. For a partly translated predicate, the only
  guarantee is that every file holding a matching row is kept. Which other files it keeps is not
  part of this feature's contract.
* Under `AND`, an untranslatable child is dropped, because dropping a conjunct only widens the file
  set. An `AND` that dropped a child, or holds a partly translated child, is partly translated.
* Under `OR`, an untranslatable branch makes the whole `OR` impose no constraint. An `OR` is exact
  only when every branch is exact.
* `NOT` negates only an exact child. Pruning never negates an untranslatable or partly translated
  child, because negating a widened predicate narrows it and would skip files that hold matching
  rows. Such a `NOT` counts as partly translated.
* A `BETWEEN` whose bound does not translate keeps the other bound and is partly translated.
* A comparison, `IN` list, or `BETWEEN` bound translates only when its literal converts to the
  column's Iceberg type without rounding. A literal the type cannot hold exactly imposes no
  constraint: for example `0.7` against a `float` column, or a literal with seven fraction digits
  against a microsecond `timestamp` or `timestamptz` column.
* Delta pruning combines translated nodes by the same rules (`delta/delta-file-pruning`).
  Direct-storage partition pruning evaluates partition values under three-valued logic instead
  (`direct-storage/direct-storage-hive-partitioning`).
* Exasol pre-normalises `>`→`<` and `>=`→`<=`, so only LESS/LESSEQUAL comparison nodes
  reach the adapter.
* A data file's associated positional-delete files (as resolved by `plan_files` per the
  Iceberg sequence-number rules) MUST be preserved into the scan spec, never discarded.
* Delete mechanisms this engine cannot apply (equality deletes, Puffin/v3 deletion
  vectors, and ORC/Avro data or delete files) MUST be detected at plan time and fail the
  request loud.
* See `pushdown/pushdown-planning` for the broader pushdown plan and the scenario that
  covers wiring of the Iceberg predicate alongside the DataFusion filter string.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A NOT over a partly translated predicate keeps every file with a matching row

* *GIVEN* a virtual schema over a partitioned Iceberg table whose files hold rows on both sides of the comparison `k < 30`
* *AND* a query whose WHERE clause is `NOT (k < 30 AND name LIKE 'x%')`, with the conjuncts in either order (#466)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter MUST NOT prune by the negation of the partly translated conjunction, and every file that holds a row the `NOT` matches SHALL be scanned
* *AND* the query SHALL return the rows and the `COUNT(*)` that native Exasol returns for the same predicate over the same rows, and MUST NOT answer with the empty result
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A NOT over a fully translated predicate still prunes

* *GIVEN* the same table and a query whose WHERE clause is `NOT (k >= 10 AND k <= 20)`, where both conjuncts translate
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL prune with the negation of the translated predicate, so a file whose `k` values all lie from 10 to 20, or are all NULL, SHALL NOT be scanned
* *AND* the query SHALL return the rows that native Exasol returns for the same predicate over the same rows
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A literal the column type cannot hold exactly imposes no constraint

* *GIVEN* an Iceberg `float` column compared against `0.7`, which no `float` value equals, and a microsecond `timestamp` or `timestamptz` column compared against a literal with seven fraction digits, including a `timestamptz` literal with the offset `+02:30` or `Z`
* *WHEN* the adapter translates each comparison, alone or under `NOT`
* *THEN* none of these comparisons SHALL impose a pruning constraint
* *AND* a `float` comparison against `0.5`, and a nanosecond `timestamp` or `timestamptz` comparison against a literal with nine fraction digits, SHALL still translate at the column's own precision and prune
* *AND* a `timestamptz` literal with an offset that the column's unit holds exactly, such as `2024-03-01 12:30:00.123456+02:30`, SHALL translate to its UTC instant, `2024-03-01 10:00:00.123456` UTC
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Pruning keeps every file with a matching row for every filter shape

* *GIVEN* a partitioned Iceberg table whose files differ in partition value, per-file bounds, per-row-group bounds, per-page bounds, and NULL content, and a native Exasol table that holds the same rows
* *WHEN* queries apply `NOT` over `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, `IS NULL`, and `IS NOT NULL` on a partition column, a statistics-only column, and both, with a `LIKE`, a scalar function, or a predicate the DataFusion dialect declines as the untranslatable side
* *THEN* each query SHALL return the native table's rows, and the scanned files SHALL include every file that holds a matching row
* *AND* for a fully translated predicate, the scanned files SHALL be exactly the files Iceberg's inclusive evaluation keeps, so a change that turns pruning off fails, while a partly translated predicate SHALL be held only to keeping every file with a matching row
* *AND* a query that scans no file SHALL take the empty-result route, and every other query SHALL apply its predicate in the scan or in the adapter's outer `WHERE`, never drop it
<!-- /DELTA:NEW -->
