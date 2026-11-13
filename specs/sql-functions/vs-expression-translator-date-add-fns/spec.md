# Feature: VS Expression Translator, Date-Addition Functions

The scan computes Exasol's `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES` over a DATE or TIMESTAMP argument and returns what native Exasol returns, except for a date before 1582-10-15, where the scan follows the proleptic Gregorian calendar and Exasol the Julian calendar, and for a result over a `TIMESTAMP(7)` to `TIMESTAMP(9)` outside 1677-09-21 to 2262-04-11, which fails in the scan. Exasol computes every other form of these functions itself.

## Background

* Exasol returns a DATE from `ADD_DAYS` and `ADD_WEEKS` over a DATE, a `TIMESTAMP(0)` from `ADD_HOURS` and `ADD_MINUTES` over a DATE, and a `TIMESTAMP(p)` from all four over a `TIMESTAMP(p)`.
* Exasol rounds a fractional count half away from zero before it adds the count, and fails a result outside its DATE and TIMESTAMP range of 0001-01-01 to 9999-12-31.
* The scan declares each computed output column with the type Exasol states in `selectListDataTypes` and casts the computed value to it (`datafusion-scan/scan-execution-value-conversion`).
* The scan holds a `TIMESTAMP(7)` to `TIMESTAMP(9)` value as an Arrow nanosecond timestamp, whose range spans 1677-09-21 00:12:43.145224192 to 2262-04-11 23:47:16.854775807. A microsecond timestamp drops its digits below a microsecond, and spans Exasol's whole range.
* A declined expression takes its path's fallback (`pushdown-types/pushdown-planning-scan-text-conversion`), so Exasol computes it.

## Scenarios

### Scenario: The typed pass tells a DATE from a TIMESTAMP

* *GIVEN* a scan expression that contains a DATE or `TIMESTAMP(p)` column, a DATE or TIMESTAMP literal, a CAST to DATE or `TIMESTAMP(p)`, an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` call, a `DATE_TRUNC`, `TO_DATE`, or `TO_TIMESTAMP` call, or a `MIN`, `MAX`, CASE, `COALESCE`, `GREATEST`, `LEAST`, or `NULLIF` over one of them
* *WHEN* `render_scan_expression` types the expression
* *THEN* the typed pass SHALL derive DATE for a DATE column, a DATE literal, a CAST to DATE, `TO_DATE`, and `ADD_DAYS` or `ADD_WEEKS` over a DATE
* *AND* it SHALL derive `TIMESTAMP(p)` for a `TIMESTAMP(p)` column, a TIMESTAMP literal that spells `p` fractional-second digits, trailing zeros included, a CAST to `TIMESTAMP(p)`, and any of the four functions over a `TIMESTAMP(p)`, and `TIMESTAMP(0)` for `ADD_HOURS` or `ADD_MINUTES` over a DATE
* *AND* `MIN`, `MAX`, and `DATE_TRUNC` SHALL keep the type of their DATE or TIMESTAMP argument, and `NULLIF` the type of its first argument
* *AND* a CASE, `COALESCE`, `GREATEST`, or `LEAST` SHALL take the type Exasol gives it: DATE over dates and `TIMESTAMP(max(p, q))` over a `TIMESTAMP(p)` and a `TIMESTAMP(q)`, NULL branches aside
* *AND* one over a DATE and a TIMESTAMP SHALL get no DATE or TIMESTAMP type, because Exasol 2025 gives it the TIMESTAMP's type and Exasol 8 the first branch's type
* *AND* the translator SHALL cast each non-NULL branch of such a TIMESTAMP union to the coarsest of `TIMESTAMP(0)`, `TIMESTAMP(3)`, `TIMESTAMP(6)`, and `TIMESTAMP(9)` that holds `p` digits, because DataFusion brings the branches to the finest unit among them
* *AND* `TO_TIMESTAMP`, which Exasol 2025 types `TIMESTAMP(9)` and Exasol 8 `TIMESTAMP(3)`, and a `TIMESTAMP WITH LOCAL TIME ZONE` value SHALL get no DATE or TIMESTAMP type
* *AND* a DATE or TIMESTAMP value converted to text SHALL still decline, because Exasol formats both by session settings (`pushdown-types/pushdown-planning-scan-text-conversion`)

### Scenario: ADD_DAYS and ADD_WEEKS over a DATE return a DATE from the scan

* *GIVEN* a `function_scalar` node named `ADD_DAYS` or `ADD_WEEKS` whose first argument has the DATE class and whose second argument is a number
* *WHEN* `render_scan_expression` processes the node
* *THEN* the translator SHALL render `vs_add_days(CAST(<arg_sql> AS DATE), <count_sql>)` or `vs_add_weeks(CAST(<arg_sql> AS DATE), <count_sql>)`, which call functions that the scan session registers, so the scan function receives an Arrow `Date32` whatever Arrow type holds the argument
* *AND* the scan SHALL return the date plus the rounded count of days, or plus seven days per rounded count of weeks, as a DATE
* *AND* a query that uses the function in a select list, a WHERE filter, a GROUP BY key, or an aggregate argument SHALL return the rows native Exasol returns
* *AND* the adapter SHALL advertise `FN_ADD_DAYS` and `FN_ADD_WEEKS`

### Scenario: ADD_HOURS and ADD_MINUTES over a DATE return a timestamp from the scan

* *GIVEN* a `function_scalar` node named `ADD_HOURS` or `ADD_MINUTES` whose first argument has the DATE class and whose second argument is a number
* *WHEN* `render_scan_expression` processes the node
* *THEN* the translator SHALL render `vs_add_hours(CAST(<arg_sql> AS DATE), <count_sql>)` or `vs_add_minutes(CAST(<arg_sql> AS DATE), <count_sql>)`
* *AND* the scan SHALL return midnight of the date plus the rounded count of hours or minutes, as a timestamp without fractional seconds, which equals Exasol's `TIMESTAMP(0)` result
* *AND* a query that uses the function in a select list, a WHERE filter, a GROUP BY key, or an aggregate argument SHALL return the rows native Exasol returns
* *AND* the adapter SHALL advertise `FN_ADD_HOURS` and `FN_ADD_MINUTES`

### Scenario: The four functions over a TIMESTAMP keep its fractional seconds in the scan

* *GIVEN* an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` node whose first argument has the TIMESTAMP class
* *WHEN* `render_scan_expression` processes the node
* *THEN* the translator SHALL render the function's `vs_add_*` call with the first argument as `CAST(<arg_sql> AS TIMESTAMP(6))` for a precision from 0 to 6 and `CAST(<arg_sql> AS TIMESTAMP(9))` for a precision from 7 to 9, so the scan function receives a microsecond or nanosecond timestamp without a time zone, which holds every fractional-second digit of the argument
* *AND* the scan SHALL return a microsecond timestamp over a microsecond argument and a nanosecond timestamp over a nanosecond argument
* *AND* the scan SHALL return the timestamp plus the rounded count of days, weeks, hours, or minutes, with every fractional-second digit of the argument kept
* *AND* a result that crosses a month end, a leap day, or a year end SHALL be the date and time native Exasol returns
* *AND* a query that uses the function in a select list, a WHERE filter, a GROUP BY key, or an aggregate argument SHALL return the rows native Exasol returns

### Scenario: The scan rounds a fractional count as Exasol does

* *GIVEN* a count that is a DECIMAL with a scale above 0, a DOUBLE, a BOOLEAN, or NULL
* *WHEN* the scan computes one of the four functions
* *THEN* the scan SHALL round an exact count half away from zero, so 2.5 adds 3, -2.5 adds -3, and 2.49 adds 2
* *AND* the scan SHALL round a DOUBLE count `x` to `sign(x) * floor(|x| + 0.5)` in double arithmetic, as Exasol does, so 0.49999999999999994 adds 1
* *AND* `ADD_WEEKS` SHALL round the count of weeks before it converts weeks to days
* *AND* a BOOLEAN count SHALL add 1 for TRUE and 0 for FALSE, as Exasol converts it, and a NULL count or a NULL first argument SHALL return NULL
* *AND* a NaN or infinite DOUBLE count SHALL fail the query with an error whose message starts with `data exception - numeric value out of range`, recorded in the session's failure slot

### Scenario: A result outside Exasol's datetime range fails the query

* *GIVEN* a row whose result lies after 9999-12-31, or after 9999-12-31 23:59:59.999999 for a timestamp, or before 0001-01-01
* *WHEN* the scan computes the function in a select list, a WHERE filter, or an aggregate argument
* *THEN* the query SHALL fail with an error whose message starts with `data exception - datetime field overflow` for a result after the range and `data exception - datetime field underflow` for a result before it, as native Exasol's error does
* *AND* the scan SHALL record the error in the session's failure slot, so the message survives the Parquet row filter
* *AND* an `ADD_DAYS` or `ADD_WEEKS` count outside -999,999,999 to 999,999,999 SHALL fail the query with an error whose message starts with `data exception - numeric value out of range`, where the scan checks a DOUBLE count before rounding and an exact count after rounding, as native Exasol does
* *AND* an `ADD_HOURS` or `ADD_MINUTES` count that is not a literal and whose nanoseconds exceed 128 bits (about 4.7e25 hours or 2.8e27 minutes) SHALL fail the query with an error whose message starts with `data exception - numeric value out of range`, as native Exasol's does, and a smaller count whose result lies outside the range SHALL fail as an overflow or underflow
* *AND* on Exasol 8, which the scan reads from the database version in its UDF context, the limit SHALL instead be a rounded count whose milliseconds exceed 999,999,999,999,999,999 (277,777,777,777 hours or 16,666,666,666,666 minutes), as native Exasol 8's is
* *AND* a NULL argument SHALL return NULL whatever the count, as native Exasol does
* *AND* the scan MUST NOT return a row, a NULL, or a clamped value in place of the failure

### Scenario: A result outside the nanosecond range fails the query

* *GIVEN* an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` call over a `TIMESTAMP(7)` to `TIMESTAMP(9)` argument, whose result for a row lies after 2262-04-11 23:47:16.854775807 or before 1677-09-21 00:12:43.145224192 but inside Exasol's range
* *WHEN* the scan computes the call
* *THEN* the query SHALL fail with an error whose message starts with `data exception - datetime field overflow` for a result after the nanosecond range and `data exception - datetime field underflow` for a result before it, and names the `TIMESTAMP(7)` to `TIMESTAMP(9)` limit
* *AND* the scan MUST NOT return a row, a NULL, or a wrapped value in place of the failure
* *AND* `docs/semantic-differences.md` SHALL list this difference from native Exasol, which returns the timestamp, together with a comparison of such a call or a `TIMESTAMP(9)` column with a timestamp literal outside the nanosecond range, such as a 9999-12-31 sentinel, which fails in the scan

### Scenario: A comparison with a TIMESTAMP literal finer than the call returns native rows

* *GIVEN* a comparison, `BETWEEN`, or `IN` of an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` call with a TIMESTAMP literal of more significant fractional digits than the call's precision, with the literal on either side
* *WHEN* the scan computes the filter
* *THEN* the query SHALL return the rows native Exasol returns
* *AND* the typed pass SHALL rewrite each such comparison as it rewrites a comparison with a column (`pushdown-types/pushdown-planning-timestamp-literal-precision`), with the precision of the call's Exasol type, 0 over a DATE, because DataFusion truncates the literal to the scan result's unit before it compares
* *AND* a literal with no more significant digits than that precision SHALL compare as written
* *AND* a call that the typed pass declines SHALL take its path's fallback, which applies the comparison as Exasol received it, without the rewrite

### Scenario: A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan

* *GIVEN* an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` call that the scan computes, whose argument or result lies before 1582-10-15
* *WHEN* the scan computes the call
* *THEN* the scan SHALL add the count in the proleptic Gregorian calendar, which extends the Gregorian calendar to dates before 1582-10-15, so an argument of 1500-03-01 with a count of -1 day returns 1500-02-28
* *AND* the scan MUST NOT fail the query or return NULL for such a value
* *AND* `docs/semantic-differences.md` SHALL list this difference from native Exasol, which uses the Julian calendar before 1582-10-15 and returns 1500-02-29 for the same call

### Scenario: A form the scan cannot match runs in Exasol

* *GIVEN* an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` node whose first argument is a `TIMESTAMP WITH LOCAL TIME ZONE` value, text, a NULL literal, or an expression without a derived DATE or TIMESTAMP type, such as `TO_TIMESTAMP` or a CASE over a DATE and a TIMESTAMP, or whose count is text, a DATE, or a TIMESTAMP, or whose count is a numeric literal with a magnitude above 1e10
* *WHEN* `render_scan_expression` processes the node
* *THEN* `render_scan_expression` SHALL return `Err`, and the expression SHALL take its path's fallback, so Exasol computes the function
* *AND* the query SHALL return the rows native Exasol returns
* *AND* a text argument or count SHALL decline, because Exasol reads text by the session's NLS settings
* *AND* a numeric literal count with a magnitude above 1e10 SHALL decline, because Exasol rejects a constant `ADD_HOURS` or `ADD_MINUTES` count whose nanoseconds exceed 36 digits before it reads a row
* *AND* `docs/capabilities.md` SHALL list the argument forms that run in Exasol
* *AND* the adapter SHALL still advertise the four capabilities, because the fallback computes each declined form in Exasol SQL

### Scenario: The four functions render verbatim in the Exasol dialect

* *GIVEN* a `function_scalar` node named `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` with two arguments
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg_sql>, <count_sql>)` with the function's uppercased Exasol name, both arguments rendered in the Exasol dialect, and the argument order preserved
* *AND* the fragment MUST NOT contain `vs_add_`, because Exasol has no such function
