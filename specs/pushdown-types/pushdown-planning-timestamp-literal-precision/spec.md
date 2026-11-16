# Feature: Pushdown Planning, Timestamp Literal Precision

A pushed-down filter that compares a DATE or TIMESTAMP operand with a literal finer than the operand's precision returns the rows native Exasol returns.

## Background

* Exasol compares a `TIMESTAMP(p)` value with a timestamp literal at the literal's full precision. A bare `TIMESTAMP` declaration holds 3 fractional digits.
* DataFusion truncates the literal to the unit of the TIMESTAMP it compares with (apache/datafusion#22142). It compares a DATE with a timestamp in nanoseconds and truncates nothing. The upstream fix, apache/datafusion#22906, is not in a released DataFusion version.
* File pruning converts a timestamp literal at the column's unit, which is nanoseconds only for an Iceberg `timestamp_ns` or `timestamptz_ns` column.
* On Exasol 8.x every timestamp column is declared at milliseconds, so a native table cannot hold a finer source value. A filter there compares at the source unit. This is the named Exasol 8.x target-type limitation, and this feature does not cover that engine arm.

## Scenarios

### Scenario: A timestamp filter compares at full precision

* *GIVEN* an operand whose Exasol type the typed pass derives as `TIMESTAMP(p)`, and a timestamp literal with more than `p` significant fractional digits
* *AND* the operand is a column declared with the `p` fractional digits of its source unit, a CAST, a date addition, `DATE_TRUNC`, `NULLIF`, or a CASE, `COALESCE`, `GREATEST`, or `LEAST`
* *WHEN* a filter compares the operand with the literal through `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, or `IN`, with the literal on either side, under `NOT`, or inside `OR`
* *THEN* the query SHALL return the rows native Exasol returns for the same values
* *AND* the typed pass SHALL rewrite each such comparison in the tree it renders for DataFusion: an ordering compares against the literal floored to `p` digits, an equality becomes `operand <> operand`, and an inequality becomes `operand = operand`, so a NULL operand stays NULL
* *AND* a DATE operand SHALL compare with the literal as written, because DataFusion converts both to nanoseconds
* *AND* a DATE outside 1677-09-21 to 2262-04-11 compared with a TIMESTAMP SHALL fail in the scan, which `docs/semantic-differences.md` lists
* *AND* the DataFusion dialect SHALL render a literal with more than 6 significant fractional digits at nanosecond precision
* *AND* file pruning SHALL derive no bound from a literal with more significant fractional digits than the column's unit holds
