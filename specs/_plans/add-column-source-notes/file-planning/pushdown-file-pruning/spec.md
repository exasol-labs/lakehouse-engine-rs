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
* Delta pruning follows the same rules (`delta/delta-file-pruning`). Direct-storage partition pruning evaluates partition values under three-valued logic instead
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
* A filter names an Exasol column, and the table's column notes record which Iceberg field id that
  column declares (`vs-adapter/column-source-notes`). The translation resolves each filter column
  through its declared field id to the field the current schema carries under that id, because
  § Scan Planning states "Data files that match the query filter must be read by the scan" and
  column bounds are "stored by field id in manifests". Resolving by name instead would read another
  column's bounds after a source-side rename between refreshes and skip files that hold matching
  rows. A filter column whose declared field id the current schema no longer carries cannot be
  translated, so it imposes no constraint under the rules above.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A filter column resolves through its declared field id

* *GIVEN* an Iceberg table with columns `a` (field id 1) and `b` (field id 2), whose two data files hold disjoint value ranges of `a` and of `b` such that the file holding `a = 7` holds no `b = 7`, a virtual schema created over it, and the source columns afterwards renamed so that field id 1 is named `b` and field id 2 is named `a`, with no `REFRESH`
* *WHEN* a query carries `WHERE A = 7`
* *THEN* the adapter SHALL translate the filter on `A` against field id 1, the id the column notes declare for `A`, so pruning keeps the file whose field-id-1 bounds admit 7
* *AND* the query SHALL return the rows whose field-id-1 value is 7, the same rows it returns before the rename
* *AND* a filter on a declared column whose field id the current schema no longer carries SHALL prune no file for that conjunct, and the query SHALL still apply the full filter above the scan
<!-- /DELTA:NEW -->
