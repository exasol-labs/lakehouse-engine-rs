# Feature: Pushdown Planning: Split-Aggregate Column Types

Every column a split aggregate emits and merges carries the exact type native Exasol gives it. The split aggregate is the single-group or grouped partial/merge plan, where each shard emits partial rows and an Exasol wrapper merges them. When no exact type exists, the adapter routes the request to the qualified wrapper, where Exasol evaluates the expression itself (#476).

<!-- DELTA:CHANGED -->
## Background

* Exasol sends `selectListDataTypes`, one type per select-list item. A nested node, such as the `MAX(UPPER(s))` inside `LENGTH(MAX(UPPER(s)))`, and an unprojected `groupBy` node carry no type, except a `CAST` node, whose `dataType` is its target type.
* A guessed type breaks the merge. A text, date, or timestamp value fails the emit or the merge, a `DOUBLE` partial over an exact expression loses digits, and a key staged as text is read back by the session's NLS format and splits values that are equal in Exasol, such as `-0.0` and `0.0`.
* An expression's exact type comes from its `selectListDataTypes` entry when it is a select-list item, else from its own `dataType` (a `CAST`), else from `involvedTables` when it is a bare column. `SECOND(x)` and `EXTRACT(SECOND FROM x)` whose argument `x` has the exact type `TIMESTAMP(p)` have the exact type `DECIMAL(2+p, p)` (`sql-functions/vs-expression-translator-date-fns`).
* Native Exasol types an aggregate as follows: `COUNT` is `DECIMAL(18,0)`; `AVG`, `STDDEV`, and `VARIANCE` and their variants are `DOUBLE`; `SUM` over `DECIMAL(p,s)` is `DECIMAL(min(p+11, 36), s)` and over `DOUBLE` is `DOUBLE`; `MIN` and `MAX` take the argument's type.
* The adapter does not infer the type of any other expression. Exasol's typing rules depend on literal values, the session, and the Exasol version, so an adapter copy of them would drift. The `SECOND` rule depends only on the argument's precision and holds on 8.29.13 and 2025.1.16.
* A constant group key forms one group whatever type carries it, so it is staged as `VARCHAR(2000000)`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A nested aggregate over an expression declines the split

* *GIVEN* a single-group or grouped `pushdown` request whose select list nests `MIN`, `MAX`, or `SUM` over an expression inside a scalar, such as `LENGTH(MAX(UPPER(s)))` or `ROUND(SUM(a * b), 6)`
* *AND* no top-level select-list item is the same aggregate, and the argument has no exact type (Background)
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL route it to the qualified wrapper
* *AND* the result SHALL equal native Exasol over the same rows, including every decimal digit
* *AND* the result SHALL match native Exasol whether every file is pruned, the scanned files hold no matching row, or rows match
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: An unprojected group key without an exact type declines the split

* *GIVEN* a grouped request whose `groupBy` holds an expression that is absent from the select list, has no exact type (Background), and is not a constant, such as `SELECT COUNT(*) FROM t GROUP BY UPPER(s)`
* *WHEN* the adapter types the key
* *THEN* the adapter SHALL route the request to the qualified wrapper
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: An unprojected SECOND group key takes the type of its argument's precision

* *GIVEN* a grouped request whose `groupBy` holds `SECOND(x)` or `EXTRACT(SECOND FROM x)` absent from the select list, where `x` has the exact type `TIMESTAMP(p)`, such as `SELECT COUNT(*) FROM t GROUP BY SECOND(ts)`
* *WHEN* the adapter types the key
* *THEN* the key SHALL take the type `DECIMAL(2+p, p)`
* *AND* the request SHALL keep the split path
<!-- /DELTA:NEW -->
