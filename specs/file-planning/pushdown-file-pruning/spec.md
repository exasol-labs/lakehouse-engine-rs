# Feature: Pushdown File Pruning

Translates the soundly translatable conjuncts of the Exasol WHERE predicate into an `iceberg::expr::Predicate` that the adapter applies at file-resolution time. `plan_files` then prunes data files on partition values and per-file min/max bounds before any S3 I/O. The DataFusion scan still applies the full filter and is the only source of row-level correctness.

## Background

* Iceberg table spec, § "Scan Planning": scan predicates become partition predicates by an inclusive projection, and "Scan predicates are also used to filter data and delete files using column bounds and counts that are stored by field id in manifests." Pruning therefore never skips a file that holds a matching row.
* The pruning predicate is sound but not complete: it keeps every file that could hold a matching row and may keep more. A node that cannot be translated imposes no constraint.
* Pruning receives the full filter, including when the DataFusion dialect declines it and the adapter applies it in its own outer `WHERE` (`pushdown/pushdown-declined-filter-self-apply`). It changes which files are read, never which rows are returned.
* A translated node is exact when it is true on exactly the rows where the user predicate is true and false on exactly the rows where it is false. On a row where the user predicate is NULL, it can be either. A translated node that is not exact is partly translated: it is true on every matching row and can be true on more.
* For a fully translated predicate, pruning keeps exactly the files that Iceberg's inclusive evaluation of the translated predicate keeps. For a partly translated predicate, the only guarantee is that every file holding a matching row is kept.
* Under `AND`, an untranslatable child is dropped, because dropping a conjunct only widens the file set. An `AND` that dropped a child, or holds a partly translated child, is partly translated.
* Under `OR`, an untranslatable branch makes the whole `OR` impose no constraint. An `OR` is exact only when every branch is exact.
* `NOT` negates only an exact child, because negating a widened predicate narrows it and skips files that hold matching rows. Any other `NOT` is partly translated.
* A `BETWEEN` whose bound does not translate keeps the other bound and is partly translated.
* A data file's associated positional-delete files, as resolved by `plan_files`, are preserved into the scan spec.
* Delete mechanisms this engine cannot apply (equality deletes, Puffin/v3 deletion vectors, and ORC or Avro data or delete files) are detected at plan time and fail the request.
* A filter names an Exasol column, and the table's column notes record the Iceberg field id that column declares (`vs-adapter/column-source-notes`). The translation resolves each filter column through that field id to the field the scanned snapshot's schema carries under it, because column bounds are stored by field id in manifests. Resolving by name would read another column's bounds after a source-side rename between refreshes and skip files that hold matching rows. A filter column whose field id the schema no longer carries imposes no constraint.

## Scenarios

### Scenario: Equality on a partition column prunes data files

* *GIVEN* a virtual schema over a partitioned Iceberg table backed by SeaweedFS whose data files are distributed across partition values
* *AND* a query with a `WHERE partition_col = <value>` predicate over a column the adapter can translate
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL set an `iceberg::expr::Predicate` equality term on the table scan before calling `plan_files`
* *AND* the resolved data-file list SHALL contain only files whose partition value matches `<value>`
* *AND* the data files belonging to non-matching partitions SHALL NOT appear in the scan-driving SQL

### Scenario: Range predicate prunes files via per-file min/max bounds

* *GIVEN* a virtual schema over an Iceberg table whose data files carry disjoint per-file min/max column statistics
* *AND* a query with a `WHERE col <= <value>` (or `BETWEEN`) predicate over a column the adapter can translate
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL apply the translated Iceberg predicate so `plan_files` evaluates each file's column bounds
* *AND* a data file whose min/max bounds provably exclude `<value>` SHALL NOT appear in the resolved file list
* *AND* a data file whose bounds overlap `<value>` SHALL remain in the resolved file list

### Scenario: Untranslatable conjunct disables pruning for that conjunct only

* *GIVEN* a query whose WHERE clause is `<translatable predicate> AND <untranslatable predicate>` (for example `col = 5 AND name LIKE 'A%'`)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL emit an Iceberg pruning predicate carrying only the translatable conjunct
* *AND* the adapter SHALL drop the untranslatable conjunct from the pruning predicate
* *AND* the full original predicate SHALL still be applied — in `ScanSpec.filter` for DataFusion when the DataFusion dialect renders it, and otherwise in the adapter's own outer `WHERE` per `pushdown/pushdown-declined-filter-self-apply` — so "untranslatable for Iceberg pruning" never means "unapplied"
* *AND* the query result SHALL be identical to the result without any Iceberg pruning

### Scenario: An untranslatable branch of an OR disables pruning entirely

* *GIVEN* a query whose WHERE clause is `<translatable predicate> OR <untranslatable predicate>` (for example `col = 5 OR name LIKE 'A%'`)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL NOT apply any Iceberg pruning predicate derived from that `OR`, because a row satisfying the untranslatable branch MAY live in any file
* *AND* the resolved file list SHALL equal the unpruned file list for that `OR`
* *AND* the query result SHALL be correct because DataFusion applies the full predicate

### Scenario: Positional-delete files are preserved into the scan spec

* *GIVEN* a virtual schema over an Iceberg merge-on-read table whose resolved `FileScanTask`s carry associated Parquet positional-delete files, at either `write.delete.granularity=file` or `write.delete.granularity=partition`
* *WHEN* Exasol sends the corresponding pushdown request and the adapter resolves the file list once
* *THEN* the adapter MUST NOT discard a data file's associated positional-delete files, and SHALL carry each delete file's path, byte size, and delete content type into the per-shard file entry for the data file(s) it applies to
* *AND* the association between a data file and its delete files SHALL follow the Iceberg sequence-number rules exactly as `plan_files` resolved them, so a delete file is carried for exactly the data files it applies to and no others
* *AND* the adapter SHALL carry the delete-file references as the ONLY delete-related addition to the per-shard files argument — it MUST NOT add a serialized Iceberg schema or a bound predicate to the spec for delete support

### Scenario: An unsupported delete mechanism fails loud at plan time

* *GIVEN* a virtual schema over an Iceberg table whose current snapshot carries a delete mechanism this engine cannot apply — an equality-delete file, a v3 / Puffin deletion vector, or a delete file (or data file) in ORC or Avro format
* *WHEN* Exasol sends the corresponding pushdown request and the adapter inspects the snapshot's delete files at the manifest / `DataFile` level, where the Puffin discriminator and file format are still visible
* *THEN* the adapter SHALL fail the request at plan time with a clean error naming the unsupported delete mechanism, BEFORE building or returning any scan-driving SQL and before fan-out
* *AND* the adapter MUST NOT silently return pre-delete rows and MUST NOT emit scan-driving SQL for that request
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token
* *AND* this plan-time detection SHALL be the authoritative correctness gate; any scan-time check is a secondary backstop only

### Scenario: A NOT over a partly translated predicate keeps every file with a matching row

* *GIVEN* a virtual schema over a partitioned Iceberg table whose files hold rows on both sides of the comparison `k < 30`
* *AND* a query whose WHERE clause is `NOT (k < 30 AND name LIKE 'x%')`, with the conjuncts in either order (#466)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter MUST NOT prune by the negation of the partly translated conjunction, and every file that holds a row the `NOT` matches SHALL be scanned
* *AND* the query SHALL return the rows and the `COUNT(*)` that native Exasol returns for the same predicate over the same rows, and MUST NOT answer with the empty result

### Scenario: A NOT over a fully translated predicate still prunes

* *GIVEN* the same table and a query whose WHERE clause is `NOT (k >= 10 AND k <= 20)`, where both conjuncts translate
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL prune with the negation of the translated predicate, so a file whose `k` values all lie from 10 to 20, or are all NULL, SHALL NOT be scanned
* *AND* the query SHALL return the rows that native Exasol returns for the same predicate over the same rows

### Scenario: Pruning keeps every file with a matching row for every filter shape

* *GIVEN* a partitioned Iceberg table whose files differ in partition value, per-file bounds, per-row-group bounds, per-page bounds, and NULL content, and a native Exasol table that holds the same rows
* *WHEN* queries apply `NOT` over `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, `IS NULL`, and `IS NOT NULL` on a partition column, a statistics-only column, and both, with a `LIKE`, a scalar function, or a predicate the DataFusion dialect declines as the untranslatable side
* *THEN* each query SHALL return the native table's rows, and the scanned files SHALL include every file that holds a matching row
* *AND* for a fully translated predicate, the scanned files SHALL be exactly the files Iceberg's inclusive evaluation keeps, so a change that turns pruning off fails, while a partly translated predicate SHALL be held only to keeping every file with a matching row
* *AND* a query that scans no file SHALL take the empty-result route, and every other query SHALL apply its predicate in the scan or in the adapter's outer `WHERE`, never drop it

### Scenario: A filter column resolves through its declared field id

* *GIVEN* an Iceberg table with columns `a` (field id 1) and `b` (field id 2), whose two data files hold disjoint value ranges of `a` and of `b` such that the file holding `a = 7` holds no `b = 7`, a virtual schema created over it, and the source columns afterwards renamed so that field id 1 is named `b` and field id 2 is named `a`, with no `REFRESH`
* *WHEN* a query carries `WHERE A = 7`
* *THEN* the adapter SHALL translate the filter on `A` against field id 1, the id the column notes declare for `A`, so pruning keeps the file whose field-id-1 bounds admit 7
* *AND* the query SHALL return the rows whose field-id-1 value is 7, the same rows it returns before the rename
* *AND* a filter on a declared column whose field id the scanned snapshot's schema no longer carries SHALL prune no file for that conjunct, and the query SHALL still apply the full filter above the scan
