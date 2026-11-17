# Feature: VS Expression Translator, Date-Difference Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the Exasol date-difference (`*_BETWEEN`) functions. It also lists the date-arithmetic functions that have no faithful DataFusion rendering and fall through to Exasol.

<!-- DELTA:CHANGED -->
## Background

* `sql-functions/vs-expression-translator-date-fns` covers the field-shortcut date functions, `EXTRACT`, `DATE_TRUNC`, `TO_DATE`/`TO_TIMESTAMP` and `WEEK`. `sql-functions/vs-expression-translator-date-add-fns` covers `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES`. All three features share the Exasol-dialect verbatim rule and the `SCALAR_FUNCTIONS` declaration table described in `sql-comprehension/vs-expression-translator`.
* The adapter translates only functions whose DataFusion rendering executes and matches Exasol, verified against Exasol 2025.1.3. `DAYS_BETWEEN` renders as `DATE − DATE`, which yields an `Int64` day count. `HOURS_BETWEEN`, `MINUTES_BETWEEN` and `SECONDS_BETWEEN` render as fractional differences of `date_part('epoch', …)` seconds.
* The untranslated date functions diverge from DataFusion: `ADD_YEARS` and `ADD_MONTHS` move a month-end date to the target month's end (`ADD_YEARS(DATE '2001-02-28', 3)` is `2004-02-29`) where Arrow keeps the day of month, `ADD_SECONDS` has no established fractional-count parity, `MONTHS_BETWEEN` and `YEARS_BETWEEN` return Oracle-style fractions, `DAYOFWEEK` follows `NLS_FIRST_DAY_OF_WEEK`, `CONVERT_TZ` follows `TIME_ZONE_BEHAVIOR`, `POSIX_TIME` has no rendering, and `LAST_DAY` is not an Exasol function.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: DAYS_BETWEEN translates to a whole-day date difference

* *GIVEN* a VS expression node of type `function_scalar` named `DAYS_BETWEEN` with two datetime arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the first argument's date minus the second argument's date as a whole-day count (canonical form `(CAST(<arg1_sql> AS DATE) - CAST(<arg2_sql> AS DATE))`), using only the date part of a timestamp argument, matching Exasol
* *AND* the result SHALL be negative when the first argument is earlier than the second, matching Exasol's `DAYS_BETWEEN(timestamp1, timestamp2)` sign convention (first minus second)
* *AND* `render_expression_exasol` SHALL return `DAYS_BETWEEN(<arg1_sql>, <arg2_sql>)`, so the sign convention and the date-part-only rule come from Exasol itself rather than from the `DATE − DATE` rewrite
* *AND* the adapter SHALL advertise `FN_DAYS_BETWEEN`, because this rendering matches Exasol's own `DAYS_BETWEEN`
