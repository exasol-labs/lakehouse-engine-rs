# Feature: VS Expression Translator, Predicates

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the filter-predicate operator set: comparison, logical connectives, IS NULL / IS NOT NULL, IN over a constant list, BETWEEN, and LIKE.

## Background

* Exasol ignores NULL entries in an IN list under both `IN` and `NOT IN`. DataFusion three-valued logic filters non-matching rows for `NOT IN` when the list contains NULL, so the translator strips NULL entries. A NULL entry is a `literal_null` node or any typed literal carrying a null `value`, and these render to divergent strings, so stripping keys on the node, not the rendered string.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: IS NULL and IS NOT NULL predicates translate correctly

* *GIVEN* a `predicate_is_null` or `predicate_is_not_null` node with an `expression` child
* *WHEN* the adapter renders the node for the scan
* *THEN* `predicate_is_null` SHALL render `(<inner> IS NULL)`, and `predicate_is_not_null` SHALL render `(<inner> IS NOT NULL)`
* *AND* over a bare string column, at any position, `predicate_is_null` SHALL render `("S" IS NULL OR "S" = '')` and `predicate_is_not_null` SHALL render `("S" IS NOT NULL AND "S" <> '')`, per `pushdown-types/pushdown-planning-scan-empty-string`
<!-- /DELTA:CHANGED -->
