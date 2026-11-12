# Feature: VS Expression Translator, Date-Addition Functions

The scan computes Exasol's `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES` over a DATE or `TIMESTAMP(0)` to `TIMESTAMP(6)` argument and returns what native Exasol returns, except for a date before 1582-10-15, where the scan follows the proleptic Gregorian calendar and Exasol the Julian calendar. Exasol computes every other form of these functions itself.

## Background

* Exasol returns a DATE from `ADD_DAYS` and `ADD_WEEKS` over a DATE, a `TIMESTAMP(0)` from `ADD_HOURS` and `ADD_MINUTES` over a DATE, and a `TIMESTAMP(p)` from all four over a `TIMESTAMP(p)`.
* Exasol rounds a fractional count half away from zero before it adds the count, and fails a result outside its DATE and TIMESTAMP range of 0001-01-01 to 9999-12-31.
* The scan declares each computed output column with the type Exasol states in `selectListDataTypes` and casts the computed value to it (`datafusion-scan/scan-execution-value-conversion`).
* The scan holds a `TIMESTAMP(7)` to `TIMESTAMP(9)` value as an Arrow nanosecond timestamp, whose range ends at 2262-04-11. A microsecond timestamp drops its digits below a microsecond.
* A declined expression takes its path's fallback (`pushdown-types/pushdown-planning-scan-text-conversion`), so Exasol computes it.

## Scenarios

### Scenario: The typed pass tells a DATE from a TIMESTAMP

* *GIVEN* a scan expression that contains a DATE or `TIMESTAMP(p)` column, a DATE or TIMESTAMP literal, a CAST to DATE or `TIMESTAMP(p)`, an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` call, or a `MIN`, `MAX`, or CASE over one of them
* *WHEN* `render_scan_expression` types the expression
* *THEN* the typed pass SHALL give a DATE class to a DATE column, a DATE literal, a CAST to DATE, and `ADD_DAYS` or `ADD_WEEKS` over a DATE
* *AND* it SHALL give a TIMESTAMP class of precision `p` to a `TIMESTAMP(p)` column, a TIMESTAMP literal with `p` fractional-second digits, a CAST to `TIMESTAMP(p)`, and any of the four functions over a `TIMESTAMP(p)`, and a TIMESTAMP class of precision 0 to `ADD_HOURS` or `ADD_MINUTES` over a DATE
* *AND* `MIN` and `MAX` SHALL keep the DATE or TIMESTAMP class of their argument, and a CASE whose other branches are NULL SHALL keep the class of its one non-NULL branch, so the four functions over them run in the scan
* *AND* a CASE over two date values and a `TIMESTAMP WITH LOCAL TIME ZONE` value SHALL keep the class of a type the pass does not derive
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

* *GIVEN* an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` node whose first argument has the TIMESTAMP class with a precision from 0 to 6
* *WHEN* `render_scan_expression` processes the node
* *THEN* the translator SHALL render the function's `vs_add_*` call with the first argument as `CAST(<arg_sql> AS TIMESTAMP(6))`, so the scan function receives a microsecond timestamp without a time zone, which holds every fractional-second digit of a `TIMESTAMP(0)` to `TIMESTAMP(6)` value
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
* *AND* a NULL argument SHALL return NULL whatever the count, as native Exasol does
* *AND* the scan MUST NOT return a row, a NULL, or a clamped value in place of the failure

### Scenario: A comparison with a TIMESTAMP literal finer than a microsecond returns native rows

* *GIVEN* a comparison, `BETWEEN`, or `IN` of an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` call with a TIMESTAMP literal of more than 6 significant fractional digits, with the literal on either side
* *WHEN* the scan computes the filter
* *THEN* the query SHALL return the rows native Exasol returns
* *AND* the adapter SHALL rewrite each such comparison in the tree it renders for DataFusion as it rewrites a comparison with a column of 6 fractional digits (`pushdown-types/pushdown-planning-timestamp-literal-precision`), because the scan's result holds whole microseconds and DataFusion truncates the literal to microseconds before it compares
* *AND* a call that the typed pass declines SHALL take its path's fallback, which applies the comparison as Exasol received it, without the rewrite

### Scenario: A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan

* *GIVEN* an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` call that the scan computes, whose argument or result lies before 1582-10-15
* *WHEN* the scan computes the call
* *THEN* the scan SHALL add the count in the proleptic Gregorian calendar, which extends the Gregorian calendar to dates before 1582-10-15, so an argument of 1500-03-01 with a count of -1 day returns 1500-02-28
* *AND* the scan MUST NOT fail the query or return NULL for such a value
* *AND* `docs/semantic-differences.md` SHALL list this difference from native Exasol, which uses the Julian calendar before 1582-10-15 and returns 1500-02-29 for the same call

### Scenario: A form the scan cannot match runs in Exasol

* *GIVEN* an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` node whose first argument is a `TIMESTAMP(7)` to `TIMESTAMP(9)` value, a `TIMESTAMP WITH LOCAL TIME ZONE` value, text, a NULL literal, or an expression without a DATE or TIMESTAMP class, such as a CASE over two dates, `DATE_TRUNC`, `TO_DATE`, or `TO_TIMESTAMP`, or whose count is text, a DATE, or a TIMESTAMP, or whose count is a numeric literal with a magnitude above 1e10
* *WHEN* `render_scan_expression` processes the node
* *THEN* `render_scan_expression` SHALL return `Err`, and the expression SHALL take its path's fallback, so Exasol computes the function
* *AND* the query SHALL return the rows native Exasol returns
* *AND* a `TIMESTAMP(7)` to `TIMESTAMP(9)` argument SHALL decline, because the scan can neither keep its nanosecond digits in microseconds nor return a nanosecond result after 2262-04-11, which native Exasol returns, and a text argument or count SHALL decline, because Exasol reads text by the session's NLS settings
* *AND* a numeric literal count with a magnitude above 1e10 SHALL decline, because Exasol rejects a constant `ADD_HOURS` or `ADD_MINUTES` count whose nanoseconds exceed 36 digits before it reads a row
* *AND* `docs/semantic-differences.md` SHALL list the argument forms that run in Exasol
* *AND* the adapter SHALL still advertise the four capabilities, because the fallback computes each declined form in Exasol SQL

### Scenario: The four functions render verbatim in the Exasol dialect

* *GIVEN* a `function_scalar` node named `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` with two arguments
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg_sql>, <count_sql>)` with the function's uppercased Exasol name, both arguments rendered in the Exasol dialect, and the argument order preserved
* *AND* the fragment MUST NOT contain `vs_add_`, because Exasol has no such function
