# Feature: VS Expression Translator — Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the
Exasol date/time scalar functions that DataFusion 54 can evaluate, so date-valued filters,
select-list expressions, and group keys push down to the node-local DataFusion scan instead of
being post-processed in Exasol. Kept as a separate feature so the scalar-ops spec stays focused on
arithmetic, string, and conditional functions.

## Background

* This feature shares the three public entry points of `crates/vs-expression`
  (`render_expression`, `render_expression_safe`, `render_df_filter_safe`); the date functions are
  additional arms inside the same recursive walker.
* Exasol sends EXTRACT as node type `function_scalar_extract` with the field in a `toExtract`
  property (not as `function_scalar` + `dateTimeField`). DataFusion 54 renders EXTRACT via
  `date_part('<FIELD>', expr)` — not `EXTRACT(<FIELD> FROM expr)` — because DataFusion 54's default
  feature set does not include an EXTRACT ExprPlanner. Both forms are semantically equivalent;
  the translator uses `date_part` to match DataFusion's execution path.
* Only date/time functions whose DataFusion semantics match Exasol's are translated. Functions
  that depend on Exasol session state or whose DataFusion equivalent diverges in result are left
  unsupported (the node returns an error in raising mode, `None` in the safe variants), so the
  adapter omits them and Exasol post-processes them as a correctness backstop.
* `WEEK` is translated because Exasol `WEEK` and DataFusion 54 `date_part('week', …)` are both
  ISO-8601.

<!-- DELTA:CHANGED -->
* Date arithmetic and date-difference functions are translated for the subset whose DataFusion 54
  rendering matches Exasol's documented semantics, verified per function against the Exasol
  built-in function reference and the DataFusion 54 function surface (issue #107). The translated
  set is:
  * `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, `ADD_MINUTES` — fixed-length interval addition. Exasol
    rounds the count to a whole number before adding; DataFusion 54 scales an integer count by a
    unit `INTERVAL` (verified `Interval × integer` coercion) and returns the input date/timestamp
    type unchanged.
  * `ADD_YEARS` — year-interval addition. Arrow's month arithmetic clamps a nonexistent target day
    to the last valid day (`2000-02-29` + 1 year → `2001-02-28`), matching Exasol's clamping rule.
  * `DAYS_BETWEEN` — whole-day date difference. Exasol uses only the date part of a timestamp;
    computed as `DATE − DATE`, assumed to yield an `Int64` day count in DataFusion 54 — gated by
    the `DAYS_BETWEEN` E2E parity test, which withdraws the arm if this assumption doesn't hold.
  * `HOURS_BETWEEN`, `MINUTES_BETWEEN`, `SECONDS_BETWEEN` — fractional differences over full
    timestamps, computed from `date_part('epoch', …)` second differences.
* The following remain unsupported and fall through so Exasol post-processes them, each for a
  named reason (issue #107 permits advertising only the subset with verified parity):
  * `ADD_SECONDS` — the count is fractional (nanosecond resolution) and truncated to the first
    argument's fractional-seconds precision; DataFusion 54's `Float × INTERVAL` scaling is
    unverified and the epoch round-trip (`to_timestamp`) normalizes to nanoseconds and attaches
    the session time zone, so parity is not established.
  * `ADD_MONTHS` — Exasol returns the last day of the target month when the input is a month-end
    date; DataFusion 54 / Arrow interval-month addition does not preserve month-end, so the
    result diverges and a faithful rewrite requires fragile conditional composition.
  * `MONTHS_BETWEEN`, `YEARS_BETWEEN` — Exasol returns Oracle-style fractional results
    (day-fraction over 31, integer only when the day components match or both are month-ends);
    DataFusion 54 has no native equivalent and the composed form is high-risk.
  * `DAYOFWEEK` — the numbering depends on the `NLS_FIRST_DAY_OF_WEEK` session parameter (default
    Sunday). `date_part('dow', …) + 1` matches only the default; the VS cannot observe the session
    parameter, so parity is not guaranteed under a non-default session.
  * `CONVERT_TZ` — the result depends on the `TIME_ZONE_BEHAVIOR` session value (and
    `SESSIONTIMEZONE` for the local-time-zone input type) and on Exasol-specific invalid/ambiguous
    shift options; DataFusion 54 has no single `(naive, from_tz, to_tz) → naive` function. The
    project maps Iceberg `timestamptz` (Iceberg spec: "a time of day with a timezone", stored as
    UTC) to plain Exasol `TIMESTAMP`, so no per-value zone survives to convert.
  * `POSIX_TIME` — out of scope for issue #107; left unsupported as before.
  * `LAST_DAY` — not an Exasol function. Issue #107 listed it in error. Verified against live
    Exasol 2025.1.3 (`SELECT LAST_DAY(DATE '2020-02-15')` returns `function or script LAST_DAY not
    found`, SQL code 42000) and the Exasol `ScalarFunctionCapability` enum, which has no
    `LAST_DAY` member. No `FN_LAST_DAY` capability exists to advertise and the Exasol compiler
    never emits a `function_scalar` named `LAST_DAY`, so the name is fall-through-only.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: EXTRACT translates to the DataFusion date_part call

* *GIVEN* a VS expression node of type `function_scalar_extract` carrying a `toExtract` field with a datetime field name (e.g. `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`) and an `arguments` array with a single source datetime expression
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <source_sql>)` with the field name single-quoted and the source rendered recursively
* *AND* the rendered field name MUST be one DataFusion's `date_part` function recognises

### Scenario: Field-shortcut date functions translate to date_part of the matching field

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `SECOND` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <arg_sql>)` where `<FIELD>` is the function's own name, single-quoted
* *AND* the argument SHALL be rendered recursively

### Scenario: DATE_TRUNC translates to the DataFusion date_trunc call

* *GIVEN* a VS expression node of type `function_scalar` named `DATE_TRUNC` with a precision/unit literal argument and a source datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_trunc(<unit_sql>, <source_sql>)` with both arguments rendered recursively
* *AND* the unit literal MUST be passed through as a string argument DataFusion's `date_trunc` accepts (e.g. `'year'`, `'month'`, `'day'`, `'hour'`)

### Scenario: CURRENT_DATE and CURRENT_TIMESTAMP translate to DataFusion now-family calls

* *GIVEN* a VS expression node of type `function_scalar` named `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `SYSDATE`, or `SYSTIMESTAMP` with no datetime-dependent arguments
* *WHEN* `render_expression` processes the node
* *THEN* `CURRENT_DATE`/`SYSDATE` SHALL render as `current_date()` and `CURRENT_TIMESTAMP`/`SYSTIMESTAMP` SHALL render as `now()`
* *AND* the translator MUST NOT depend on any Exasol session state to render these nodes

### Scenario: TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls

* *GIVEN* a VS expression node of type `function_scalar` named `TO_DATE` or `TO_TIMESTAMP` with a string source argument and an optional format argument
* *WHEN* `render_expression` processes the node
* *THEN* `TO_DATE` SHALL render as `to_date(<args>)` and `TO_TIMESTAMP` SHALL render as `to_timestamp(<args>)` over the recursively rendered arguments
* *AND* when a format argument is present it SHALL be forwarded as the corresponding DataFusion format argument

### Scenario: WEEK translates to the DataFusion date_part('week') ISO-8601 call

* *GIVEN* a VS expression node of type `function_scalar` named `WEEK` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('week', <arg_sql>)` with the argument rendered recursively
* *AND* the rendered call SHALL yield the ISO-8601 week number (1-53, weeks beginning Monday, week 1 containing the year's first Thursday) matching Exasol `WEEK`, including at year boundaries
* *AND* `FN_WEEK` (advertised per `vs-adapter/pushdown-planning-capability-extensions`) SHALL be advertised only while this ISO-8601 parity holds; if a year-boundary case diverges, the `WEEK` arm SHALL be withdrawn and `FN_WEEK` left unadvertised

<!-- DELTA:NEW -->
### Scenario: ADD_DAYS, ADD_WEEKS, ADD_HOURS, and ADD_MINUTES translate to rounded integer interval addition

* *GIVEN* a VS expression node of type `function_scalar` named `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` with a datetime first argument and a numeric count second argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the datetime argument plus the count — rounded to a whole number — scaled by the matching unit interval, using `INTERVAL '1 day'` for `ADD_DAYS`, `INTERVAL '7 day'` for `ADD_WEEKS`, `INTERVAL '1 hour'` for `ADD_HOURS`, and `INTERVAL '1 minute'` for `ADD_MINUTES`, with both arguments rendered recursively (canonical form `(<datetime_sql> + CAST(ROUND(<count_sql>) AS BIGINT) * <unit_interval>)`)
* *AND* the count SHALL be rounded to a whole number before scaling, matching Exasol's rule that the count's decimals are rounded before adding
* *AND* a node with other than two arguments SHALL raise in raising mode and return `None` in the safe variants
* *AND* `FN_ADD_DAYS`, `FN_ADD_WEEKS`, `FN_ADD_HOURS`, and `FN_ADD_MINUTES` SHALL each be advertised only while an end-to-end parity test confirms the rendered expression matches Exasol for that function; if one diverges, that arm SHALL be withdrawn and its capability left unadvertised

### Scenario: ADD_YEARS translates to year-interval addition with leap-year clamping

* *GIVEN* a VS expression node of type `function_scalar` named `ADD_YEARS` with a datetime first argument and a numeric count second argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the datetime argument plus the count — rounded to a whole number — scaled by `INTERVAL '1 year'` (canonical form `(<datetime_sql> + CAST(ROUND(<count_sql>) AS BIGINT) * INTERVAL '1 year')`)
* *AND* the rendered call SHALL clamp a nonexistent target day to the last valid day of the target month (`ADD_YEARS(DATE '2000-02-29', 1)` yields `2001-02-28`), matching Exasol
* *AND* `FN_ADD_YEARS` SHALL be advertised only while an end-to-end parity test confirms this clamping matches Exasol; if it diverges, the `ADD_YEARS` arm SHALL be withdrawn and `FN_ADD_YEARS` left unadvertised

### Scenario: DAYS_BETWEEN translates to a whole-day date difference

* *GIVEN* a VS expression node of type `function_scalar` named `DAYS_BETWEEN` with two datetime arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the first argument's date minus the second argument's date as a whole-day count (canonical form `(CAST(<arg1_sql> AS DATE) - CAST(<arg2_sql> AS DATE))`), using only the date part of a timestamp argument, matching Exasol
* *AND* the result SHALL be negative when the first argument is earlier than the second, matching Exasol's `DAYS_BETWEEN(timestamp1, timestamp2)` sign convention (first minus second)
* *AND* `FN_DAYS_BETWEEN` SHALL be advertised only while an end-to-end parity test confirms the rendered expression matches Exasol; if it diverges, the arm SHALL be withdrawn and the capability left unadvertised

### Scenario: HOURS_BETWEEN, MINUTES_BETWEEN, and SECONDS_BETWEEN translate to epoch-second differences

* *GIVEN* a VS expression node of type `function_scalar` named `HOURS_BETWEEN`, `MINUTES_BETWEEN`, or `SECONDS_BETWEEN` with two timestamp arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the difference of the two arguments' epoch seconds (`date_part('epoch', <arg1_sql>) - date_part('epoch', <arg2_sql>)`), divided by `3600` for `HOURS_BETWEEN`, by `60` for `MINUTES_BETWEEN`, and undivided for `SECONDS_BETWEEN`
* *AND* the result SHALL retain the fractional difference between the two timestamps and SHALL be negative when the first argument is earlier than the second, matching Exasol (first minus second)
* *AND* `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, and `FN_SECONDS_BETWEEN` SHALL each be advertised only while an end-to-end parity test confirms the rendered expression matches Exasol for that function; if one diverges, that arm SHALL be withdrawn and its capability left unadvertised
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Unsupported date functions fall through as unsupported nodes

* *GIVEN* a VS expression node of type `function_scalar` named with a date-function name this feature does not translate — `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `CONVERT_TZ`, `POSIX_TIME`, or `LAST_DAY`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the unsupported function
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the function from the scan spec and let Exasol post-process it, because each named function either has a parity or session-state divergence from its DataFusion 54 equivalent or (`LAST_DAY`) is not an Exasol function at all (see Background)
* *AND* the capabilities `FN_ADD_SECONDS`, `FN_ADD_MONTHS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, and `FN_CONVERT_TZ` MUST NOT be advertised, and no `FN_LAST_DAY` capability SHALL exist to advertise (Exasol has no `LAST_DAY` function and its `ScalarFunctionCapability` enum has no `LAST_DAY` member)
<!-- /DELTA:CHANGED -->
