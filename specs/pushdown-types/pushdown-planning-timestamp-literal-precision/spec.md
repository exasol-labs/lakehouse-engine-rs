# Feature: Pushdown Planning, Timestamp Literal Precision

A pushed-down filter that compares a timestamp column with a literal finer than the column's declared precision returns the rows native Exasol returns.

## Background

* Exasol compares a `TIMESTAMP(p)` column with a timestamp literal at the literal's full precision. A bare `TIMESTAMP` declaration holds 3 fractional digits.
* DataFusion truncates the literal to the column's unit before it compares (apache/datafusion#22142). The upstream fix, apache/datafusion#22906, is not in a released DataFusion version.
* Iceberg and Delta file pruning parse a timestamp literal into microseconds.
* On Exasol 8.x every timestamp column is declared at milliseconds, so a native table cannot hold a finer source value. A filter there compares at the source unit. This is the named Exasol 8.x target-type limitation, and this feature does not cover that engine arm.

## Scenarios

### Scenario: A timestamp filter compares at full precision

* *GIVEN* a timestamp column declared with the `p` fractional digits of its source unit, and a timestamp literal with more than `p` significant fractional digits
* *WHEN* a filter compares the column with the literal through `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, or `IN`, with the literal on either side, under `NOT`, or inside `OR`
* *THEN* the query SHALL return the rows native Exasol returns for the same values
* *AND* the adapter SHALL rewrite each such comparison in the tree it renders for DataFusion: an ordering compares against the literal floored to `p` digits, an equality becomes `column <> column`, and an inequality becomes `column = column`, so a NULL column stays NULL
* *AND* the DataFusion dialect SHALL render a literal with more than 6 significant fractional digits at nanosecond precision
* *AND* Iceberg and Delta file pruning SHALL derive no bound from a literal with more than 6 significant fractional digits
