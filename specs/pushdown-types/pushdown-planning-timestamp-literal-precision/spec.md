# Feature: Pushdown Planning, Timestamp Literal Precision

A pushed-down filter that compares a DATE or TIMESTAMP operand with a literal finer than the operand's precision returns the rows native Exasol returns. So does an expression that compares or unifies a DATE with a TIMESTAMP.

## Background

* Exasol compares a `TIMESTAMP(p)` value with a timestamp literal at the literal's full precision. A bare `TIMESTAMP` declaration holds 3 fractional digits.
* DataFusion truncates the literal to the unit of the TIMESTAMP it compares with (apache/datafusion#22142). The upstream fix, apache/datafusion#22906, is not in a released DataFusion version.
* DataFusion converts a DATE and a TIMESTAMP of any unit to a nanosecond timestamp when it compares or unifies them. A nanosecond timestamp spans only 1677-09-21 to 2262-04-11.
* File pruning converts a timestamp literal at the column's unit, which is nanoseconds only for an Iceberg `timestamp_ns` or `timestamptz_ns` column.
* On Exasol 8.x every timestamp column is declared at milliseconds, so a native table cannot hold a finer source value. A filter there compares at the source unit. This is the named Exasol 8.x target-type limitation, and this feature does not cover that engine arm.

## Scenarios

### Scenario: A timestamp filter compares at full precision

* *GIVEN* an operand whose Exasol type the typed pass derives as `TIMESTAMP(p)`, and a timestamp literal with more than `p` significant fractional digits
* *AND* the operand is a column declared with the `p` fractional digits of its source unit, a CAST, a date addition, `DATE_TRUNC`, `NULLIF`, or a CASE, `COALESCE`, `GREATEST`, or `LEAST`
* *WHEN* a filter compares the operand with the literal through `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, or `IN`, with the literal on either side, under `NOT`, or inside `OR`
* *THEN* the query SHALL return the rows native Exasol returns for the same values
* *AND* the typed pass SHALL rewrite each such comparison in the tree it renders for DataFusion: an ordering compares against the literal floored to `p` digits, an equality becomes `operand <> operand`, and an inequality becomes `operand = operand`, so a NULL operand stays NULL
* *AND* a DATE operand SHALL compare with the literal as written, at the unit that holds the literal
* *AND* the DataFusion dialect SHALL render a literal with more than 6 significant fractional digits at nanosecond precision
* *AND* file pruning SHALL derive no bound from a literal with more significant fractional digits than the column's unit holds

### Scenario: A DATE meets a TIMESTAMP at the timestamp's unit

* *GIVEN* a DATE and a `TIMESTAMP(p)` operand, typed by the typed pass, that an expression compares through `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`, or a simple CASE, or brings to one type through a CASE, `COALESCE`, `GREATEST`, `LEAST`, or `NULLIF`
* *AND* a value lies inside 0001-01-01 to 9999-12-31 but outside 1677-09-21 to 2262-04-11
* *WHEN* the scan computes the expression in a filter, the select list, or a broadcast join condition
* *THEN* the query SHALL return the rows native Exasol returns
* *AND* the typed pass SHALL cast each DATE to the TIMESTAMP at the unit holding the most fractional digits the operands hold (`sql-functions/vs-expression-translator-date-add-fns`), except in a branch form that Exasol 8 brings to a DATE, where it casts each TIMESTAMP to DATE (`sql-functions/vs-expression-translator-date-add-fns`)
