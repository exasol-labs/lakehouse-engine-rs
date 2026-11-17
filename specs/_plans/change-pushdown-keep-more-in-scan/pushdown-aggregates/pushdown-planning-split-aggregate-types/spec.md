# Feature: Pushdown Planning: Split-Aggregate Column Types

Every column a split aggregate emits and merges carries the exact type native Exasol gives it. The split aggregate is the single-group or grouped partial/merge plan, where each shard emits partial rows and an Exasol wrapper merges them. When no exact type exists, the adapter routes the request to the qualified wrapper, where Exasol evaluates the expression itself (#476).

<!-- DELTA:CHANGED -->
## Background

* Exasol sends `selectListDataTypes`, one type per select-list item. A nested node, such as the `MAX(UPPER(s))` inside `LENGTH(MAX(UPPER(s)))`, and an unprojected `groupBy` node carry no type, except a `CAST` node, whose `dataType` is its target type.
* A guessed type breaks the merge. A text, date, or timestamp value fails the emit or the merge, a `DOUBLE` partial over an exact expression loses digits, and a key staged as text is read back by the session's NLS format and splits values that are equal in Exasol, such as `-0.0` and `0.0`.
* An expression's exact type comes from its `selectListDataTypes` entry when it is a select-list item. Otherwise the adapter derives the type the Exasol version of the pushdown gives the expression, whether or not the scan computes it (`sql-functions/vs-expression-translator-date-add-fns`). It types a bare column from `involvedTables`, a `CAST` from its `dataType`, a `JSON_VALUE` from its `RETURNING` type or `VARCHAR(2000000)` without one, a predicate as BOOLEAN, a literal from its value (DOUBLE for a DOUBLE literal, `DECIMAL(n,0)` for an integer of n digits, DATE, and `TIMESTAMP(p)` by its fractional digits), an aggregate as native Exasol types it (below), a date-part function as "A date-part function takes its fixed native type" states, and the forms its feature specs list: integer branches in `pushdown-types/pushdown-planning-scan-text-conversion`, date and timestamp forms in `sql-functions/vs-expression-translator-date-add-fns`, and `SECOND` in `sql-functions/vs-expression-translator-second`. For example, a CASE, `COALESCE`, `GREATEST`, or `LEAST` over only dates is a DATE, and a CASE or `COALESCE` over DOUBLEs has no exact type.
* Native Exasol types an aggregate as follows: `COUNT` is `DECIMAL(18,0)`; `AVG`, `STDDEV`, and `VARIANCE` and their variants are `DOUBLE`; `SUM` over `DECIMAL(p,s)` is `DECIMAL(min(p+11, 36), s)` and over `DOUBLE` is `DOUBLE`; `MIN` and `MAX` take the argument's type.
* A constant group key forms one group whatever type carries it, so it is staged as `VARCHAR(2000000)`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A date-part function takes its fixed native type

* *GIVEN* a split aggregate whose unprojected group key, or the argument of a nested `MIN`, `MAX`, or `SUM`, is a one-argument `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` call, or an `EXTRACT` of `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE`, over a DATE or a TIMESTAMP, such as `SELECT COUNT(*) FROM t GROUP BY YEAR(d)` or `SELECT MAX(MONTH(d)) + 1 FROM t`
* *WHEN* the adapter types the key or the aggregate
* *THEN* `YEAR` SHALL take `DECIMAL(4,0)`, and `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` SHALL take `DECIMAL(2,0)`, the types native Exasol 8 and 2025 give them
* *AND* the request SHALL keep the split path, and the result SHALL equal native Exasol
<!-- /DELTA:NEW -->
