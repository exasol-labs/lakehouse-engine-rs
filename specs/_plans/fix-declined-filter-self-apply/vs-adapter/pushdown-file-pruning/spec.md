# Feature: Pushdown File Pruning

Prunes Iceberg data files at plan time from partition equality and min/max column bounds, keeping
every file whose bounds could match and dropping only files that provably cannot.

## Background

* Pruning conservatism and DataFusion-render declines are two separate mechanisms and this delta
  keeps them separate. "Untranslatable" for pruning means untranslatable to an ICEBERG predicate,
  which costs pruning only. A DataFusion-dialect render decline is the different question owned by
  `vs-adapter/pushdown-declined-filter-self-apply`.
* Pruning stays sound under a render decline: the raw filter tree forwarded to pruning is unchanged,
  and the predicate is still evaluated — in the wrapper's outer `WHERE` instead of in the scan.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Untranslatable conjunct disables pruning for that conjunct only

* *GIVEN* a query whose WHERE clause is `<translatable predicate> AND <untranslatable predicate>` (for example `col = 5 AND name LIKE 'A%'`)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL emit an Iceberg pruning predicate carrying only the translatable conjunct
* *AND* the adapter SHALL drop the untranslatable conjunct from the pruning predicate
* *AND* the full original predicate SHALL still be applied — in `ScanSpec.filter` for DataFusion when the DataFusion dialect renders it, and otherwise in the adapter's own outer `WHERE` per `vs-adapter/pushdown-declined-filter-self-apply` — so "untranslatable for Iceberg pruning" never means "unapplied"
* *AND* the query result SHALL be identical to the result without any Iceberg pruning
<!-- /DELTA:CHANGED -->
