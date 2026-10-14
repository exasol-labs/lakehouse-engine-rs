# Feature: Delta Table Planning: Declaration Checks

Extends `delta/delta-table-planning` with the checks that fail a query when the declared columns disagree with the Delta log.

This is a sibling of `delta/delta-table-planning`, split out once that feature's scenario count crossed this library's per-spec organization threshold. The parent feature keeps its original scope.

## Background

* The scenarios here share the parent feature's Background.

## Scenarios

### Scenario: A declaration that disagrees with the Delta log fails the query

* *GIVEN* the `name`-mode table `cdf-column-mapping-name-mode` registered in Unity Catalog without
  `delta.columnMapping.mode` in its `properties`, so its notes carry no binding key, and the
  partitioned table `basic_partitioned` registered with no `partition_index` on its partition column
  `letter`
* *WHEN* a `SELECT` runs over each table
* *THEN* the Delta format reader SHALL compare the declaration with the gated snapshot it already
  opens, and each query SHALL fail before any file is planned with an error naming the table, stating
  which part disagrees (the log's column-mapping mode against the declared binding keys, or the log's
  partition columns against the declared ones), and stating that the fix is to give the catalog
  entry the Delta schema's column metadata and properties, then run `ALTER VIRTUAL SCHEMA ...
  REFRESH`
* *AND* the reader MUST NOT re-derive a logical field, a binding key, or a partition column from the
  log, because the check refuses a disagreeing declaration and never repairs it
* *AND* a declaration in which every non-refused field carries exactly the key the snapshot's mode
  selects, and whose partition columns are the log's partition columns in the log's order, identified
  by binding key under `name` or `id` mode and by name under `none`, SHALL plan normally
* *AND* no error SHALL contain a credential value

### Scenario: A declared non-nullable column that the Delta log marks nullable fails the query

* *GIVEN* the `name`-mode table `cdf-column-mapping-name-mode` registered in Unity Catalog with
  `"nullable": false` in the `type_json` of `id`, whose Delta log marks `id` nullable, so the note of
  `id` declares it non-nullable
* *WHEN* `SELECT id FROM <table> WHERE id IS NULL` runs, and separately a query that reads only
  `name`
* *THEN* each query SHALL fail before any file is planned with an error naming the table and the
  column `id`, stating that the declaration marks it non-nullable while the Delta log allows NULL,
  and naming `ALTER VIRTUAL SCHEMA ... REFRESH` as the fix, because a non-nullable declaration lets
  the scan drop rows whose value is NULL
* *AND* the reader SHALL identify each declared column in the snapshot by its binding key under
  `name` or `id` mode and by its name under `none`, as the declaration check does
* *AND* a declared nullable column whose snapshot field is non-nullable SHALL plan normally
* *AND* no error SHALL contain a credential value
