# Feature: VS Expression Translator, Date-Difference Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the Exasol date-difference (`*_BETWEEN`) functions. It also lists the date-arithmetic functions that have no faithful DataFusion rendering and fall through to Exasol.

## Background

* The functions are arms of the same recursive walker behind the public entry points of `crates/vs-expression`: `render_scan_expression` and `render_scan_filter` for DataFusion, and `render_expression_exasol` and `render_exasol_fragment` for Exasol.
* `sql-functions/vs-expression-translator-date-fns` covers the field-shortcut date functions, `EXTRACT`, `DATE_TRUNC`, `TO_DATE`/`TO_TIMESTAMP`, `WEEK` and the now-family. `sql-functions/vs-expression-translator-date-add-fns` covers `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES`. All three features share the Exasol-dialect verbatim rule and the `SCALAR_FUNCTIONS` declaration table described in `sql-comprehension/vs-expression-translator`.
* The adapter translates only functions whose DataFusion rendering executes and matches Exasol, verified against Exasol 2025.1.3. `DAYS_BETWEEN` renders as `DATE − DATE`, which yields an `Int64` day count. `HOURS_BETWEEN`, `MINUTES_BETWEEN` and `SECONDS_BETWEEN` render as fractional differences of `date_part('epoch', …)` seconds.
* The untranslated date functions diverge from DataFusion: `ADD_YEARS` and `ADD_MONTHS` move a month-end date to the target month's end (`ADD_YEARS(DATE '2001-02-28', 3)` is `2004-02-29`) where Arrow keeps the day of month, `ADD_SECONDS` has no established fractional-count parity, `MONTHS_BETWEEN` and `YEARS_BETWEEN` return Oracle-style fractions, `DAYOFWEEK` follows `NLS_FIRST_DAY_OF_WEEK`, `CONVERT_TZ` follows `TIME_ZONE_BEHAVIOR`, `POSIX_TIME` has no rendering, and `LAST_DAY` is not an Exasol function.

## Scenarios

### Scenario: DAYS_BETWEEN translates to a whole-day date difference

* *GIVEN* a VS expression node of type `function_scalar` named `DAYS_BETWEEN` with two datetime arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the first argument's date minus the second argument's date as a whole-day count (canonical form `(CAST(<arg1_sql> AS DATE) - CAST(<arg2_sql> AS DATE))`), using only the date part of a timestamp argument, matching Exasol
* *AND* the result SHALL be negative when the first argument is earlier than the second, matching Exasol's `DAYS_BETWEEN(timestamp1, timestamp2)` sign convention (first minus second)
* *AND* `render_expression_exasol` SHALL return `DAYS_BETWEEN(<arg1_sql>, <arg2_sql>)`, so the sign convention and the date-part-only rule come from Exasol itself rather than from the `DATE − DATE` rewrite
* *AND* the adapter SHALL advertise `FN_DAYS_BETWEEN`, because this rendering matches Exasol's own `DAYS_BETWEEN`

### Scenario: HOURS_BETWEEN, MINUTES_BETWEEN, and SECONDS_BETWEEN translate to epoch-second differences

* *GIVEN* a VS expression node of type `function_scalar` named `HOURS_BETWEEN`, `MINUTES_BETWEEN`, or `SECONDS_BETWEEN` with two timestamp arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the difference of the two arguments' epoch seconds (`date_part('epoch', <arg1_sql>) - date_part('epoch', <arg2_sql>)`), divided by `3600` for `HOURS_BETWEEN`, by `60` for `MINUTES_BETWEEN`, and undivided for `SECONDS_BETWEEN`
* *AND* the result SHALL retain the fractional difference between the two timestamps and SHALL be negative when the first argument is earlier than the second, matching Exasol (first minus second)
* *AND* the adapter SHALL advertise `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, and `FN_SECONDS_BETWEEN`, because each rendering matches Exasol's own function

### Scenario: Date-difference functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named `HOURS_BETWEEN`, `MINUTES_BETWEEN`, or `SECONDS_BETWEEN` with two timestamp arguments
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg1_sql>, <arg2_sql>)` using the function's own uppercased Exasol name, with both arguments rendered recursively in the Exasol dialect and argument order preserved
* *AND* the rendered fragment MUST NOT contain `date_part` and MUST NOT contain the `/ 3600` or `/ 60` unit divisor, because Exasol's own function already returns the requested unit
* *AND* the DataFusion-dialect rendering of the same node MUST remain byte-identical to the preceding scenario, so the epoch-arithmetic path the node-local scan depends on is unchanged

### Scenario: Unsupported date functions fall through as unsupported nodes

* *GIVEN* a VS expression node of type `function_scalar` named with a date-function name that no feature translates: `ADD_YEARS`, `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `CONVERT_TZ`, `POSIX_TIME`, or `LAST_DAY`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return an error naming the unsupported function
* *AND* `render_scan_expression` SHALL return an error for the same node without panicking
* *AND* the adapter SHALL omit the function from the scan spec and let Exasol post-process it, because each named function diverges from its DataFusion equivalent (see Background)
* *AND* the capabilities `FN_ADD_YEARS`, `FN_ADD_SECONDS`, `FN_ADD_MONTHS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, and `FN_CONVERT_TZ` MUST NOT be advertised, and no `FN_LAST_DAY` capability SHALL exist to advertise, because Exasol has no `LAST_DAY` function (`function or script LAST_DAY not found`, SQL code 42000) and its `ScalarFunctionCapability` enum has no `LAST_DAY` member
* *AND* the Exasol-dialect verbatim rule SHALL cover none of these names, because the adapter advertises one capability set for both dialects, and a name advertised for the Exasol wrapper would also be pushed at the DataFusion scan
