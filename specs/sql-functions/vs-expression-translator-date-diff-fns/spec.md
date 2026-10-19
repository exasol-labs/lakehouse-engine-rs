# Feature: VS Expression Translator, Date-Difference Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the Exasol date-difference (`*_BETWEEN`) functions. It also lists the date-arithmetic functions that have no faithful DataFusion rendering and fall through to Exasol.

## Background

* The functions are arms of the same recursive walker behind the six public entry points of `crates/vs-expression`: `render_expression`, `render_expression_safe` and `render_df_filter_safe` for DataFusion, and `render_expression_exasol`, `render_expression_exasol_safe` and `render_df_filter_exasol_safe` for Exasol.
* `sql-functions/vs-expression-translator-date-fns` covers the field-shortcut date functions, `EXTRACT`, `DATE_TRUNC`, `TO_DATE`/`TO_TIMESTAMP`, `WEEK` and the now-family. Both features share the Exasol-dialect verbatim rule and the `TRANSLATED_SCALAR_FNS` declaration mechanism described in `sql-comprehension/vs-expression-translator`.
* The adapter translates only functions whose DataFusion rendering executes and matches Exasol, verified against Exasol 2025.1.3. `DAYS_BETWEEN` renders as `DATE − DATE`, which yields an `Int64` day count. `HOURS_BETWEEN`, `MINUTES_BETWEEN` and `SECONDS_BETWEEN` render as fractional differences of `date_part('epoch', …)` seconds.
* These functions are not translated, and each falls through so Exasol post-processes it:
  * `ADD_HOURS`, `ADD_MINUTES`: the integer-microsecond rendering matches Exasol for a TIMESTAMP argument. For a DATE argument it yields `Timestamp(Microsecond)`, mapped to `TIMESTAMP(3)`, while Exasol infers `TIMESTAMP(0)` and rejects the pushdown with `Data type mismatch ... Expected TIMESTAMP(0), but got TIMESTAMP(3)`. The translator has no argument type, so one rendering cannot serve both.
  * `ADD_DAYS`, `ADD_WEEKS`: Exasol returns a DATE for a DATE argument and a TIMESTAMP for a TIMESTAMP argument. The type-blind translator cannot reproduce both, and every execution-safe DataFusion rendering widens a DATE result to TIMESTAMP.
  * `ADD_YEARS`, `ADD_MONTHS`: Exasol maps a month-end date to the last day of the target month (`ADD_YEARS(DATE '2001-02-28', 3)` is `2004-02-29`). Arrow interval addition keeps the day of month and yields `2004-02-28`. `ADD_YEARS` also has an input-type-dependent return type.
  * `ADD_SECONDS`: the count is fractional and truncated to the argument's fractional-seconds precision, and DataFusion parity is not established.
  * `MONTHS_BETWEEN`, `YEARS_BETWEEN`: Exasol returns Oracle-style fractional results, and DataFusion has no native equivalent.
  * `DAYOFWEEK`: the numbering depends on the `NLS_FIRST_DAY_OF_WEEK` session parameter, which the VS cannot observe.
  * `CONVERT_TZ`: the result depends on the `TIME_ZONE_BEHAVIOR` session value and on Exasol-specific shift options. Iceberg `timestamptz` maps to plain Exasol `TIMESTAMP`, so no per-value zone survives to convert.
  * `POSIX_TIME`: not translated.
  * `LAST_DAY`: not an Exasol function. Exasol returns `function or script LAST_DAY not found` (SQL code 42000), and its `ScalarFunctionCapability` enum has no such member.
* A function in this unsupported set falls through in both dialects. The Exasol-dialect verbatim rule covers only names this feature translates, because the adapter advertises one capability set for both dialects, and a name advertised for the Exasol wrapper would also be pushed at the DataFusion scan.

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

* *GIVEN* a VS expression node of type `function_scalar` named with a date-function name this feature does not translate: `ADD_HOURS`, `ADD_MINUTES`, `ADD_DAYS`, `ADD_WEEKS`, `ADD_YEARS`, `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `CONVERT_TZ`, `POSIX_TIME`, or `LAST_DAY`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the unsupported function
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking, and `render_expression_exasol` / `render_expression_exasol_safe` SHALL fall through identically, an error in raising mode, `None` in the safe variant, so the Exasol dialect's verbatim rule never widens the translated set
* *AND* the adapter SHALL omit the function from the scan spec and let Exasol post-process it, because each named function either has an execution, parity, session-state, or input-type-dependent-return divergence from its DataFusion equivalent or (`LAST_DAY`) is not an Exasol function at all (see Background)
* *AND* the capabilities `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_ADD_DAYS`, `FN_ADD_WEEKS`, `FN_ADD_YEARS`, `FN_ADD_SECONDS`, `FN_ADD_MONTHS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, and `FN_CONVERT_TZ` MUST NOT be advertised, and no `FN_LAST_DAY` capability SHALL exist to advertise (Exasol has no `LAST_DAY` function and its `ScalarFunctionCapability` enum has no `LAST_DAY` member; `ADD_HOURS`/`ADD_MINUTES` are in this set because their end-to-end parity test found a DATE-argument `TIMESTAMP(0)` vs. `TIMESTAMP(3)` divergence, see Background)
