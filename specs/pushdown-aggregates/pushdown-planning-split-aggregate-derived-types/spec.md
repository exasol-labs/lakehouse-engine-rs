# Feature: Pushdown Planning: Split-Aggregate Derived Column Types

A split aggregate types a date, timestamp, or date-part expression with the exact type the Exasol version of the pushdown gives it. The general typing rules live in `pushdown-aggregates/pushdown-planning-split-aggregate-types`.

## Background

* The Background of `pushdown-aggregates/pushdown-planning-split-aggregate-types` applies: Exasol sends `selectListDataTypes` per select-list item, and a nested aggregate argument or an unprojected group key carries no type, so the adapter derives one.
* A guessed type breaks the merge, so a form with no exact type declines the split.

## Scenarios

### Scenario: A form whose type differs by Exasol version takes the type of the version it runs on

* *GIVEN* a split aggregate whose nested aggregate argument or unprojected group key is `TO_TIMESTAMP`, `ADD_HOURS` or `ADD_MINUTES` over a DATE, `SECOND` over a DATE, a TIMESTAMP literal with other than 3 fractional digits, or a CASE or `GREATEST` over a DATE and a TIMESTAMP
* *WHEN* the adapter plans the request on Exasol 8 or Exasol 2025
* *THEN* the argument or key SHALL take the type that version gives it (`sql-functions/vs-expression-translator-date-add-fns`, `sql-functions/vs-expression-translator-second`)
* *AND* the request SHALL keep the split path
* *AND* the result SHALL equal native Exasol over the same rows, including sub-millisecond and pre-1970 values

### Scenario: An unprojected SECOND group key takes the type of its argument's precision

* *GIVEN* a grouped request whose `groupBy` holds `SECOND(x)` or `EXTRACT(SECOND FROM x)` absent from the select list, where `x` has the exact type `TIMESTAMP(p)` in the typed scan pass, such as `SELECT COUNT(*) FROM t GROUP BY SECOND(ts)` or `GROUP BY SECOND(COALESCE(updated_at, created_at))`
* *WHEN* the adapter types the key
* *THEN* the key SHALL take the type `DECIMAL(2+p, p)`
* *AND* the request SHALL keep the split path

### Scenario: A date-part function takes its fixed native type

* *GIVEN* a split aggregate whose unprojected group key, or the argument of a nested `MIN`, `MAX`, or `SUM`, is a one-argument `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` call, or an `EXTRACT` of `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE`, over a DATE or a TIMESTAMP, such as `SELECT COUNT(*) FROM t GROUP BY YEAR(d)` or `SELECT MAX(MONTH(d)) + 1 FROM t`
* *WHEN* the adapter types the key or the aggregate
* *THEN* `YEAR` SHALL take `DECIMAL(4,0)`, and `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` SHALL take `DECIMAL(2,0)`, the types native Exasol 8 and 2025 give them
* *AND* the request SHALL keep the split path, and the result SHALL equal native Exasol
