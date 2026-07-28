# Feature: VS Expression Translator — Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the
Exasol date/time scalar functions that DataFusion 54 can evaluate, so date-valued filters,
select-list expressions, and group keys push down to the node-local DataFusion scan instead of
being post-processed in Exasol. Kept as a separate feature so the scalar-ops spec stays focused on
arithmetic, string, and conditional functions.

## Background

* This feature shares the six public entry points of `crates/vs-expression` — the DataFusion trio
  (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) and the Exasol trio
  (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`);
  the date functions are additional arms inside the same recursive walker.

<!-- DELTA:NEW -->
* **Exasol-dialect rendering is verbatim.** The Exasol dialect exists because the rendered fragment
  becomes wrapper SQL text parsed and evaluated by Exasol's own core engine, not by DataFusion.
  Every date/time function in this feature is an Exasol function that Exasol's compiler itself
  emitted, so the Exasol dialect renders the original name, the original argument order, and the
  original argument count — no name mapping, no re-shaping. This makes Exasol-dialect parity
  structural rather than tested: Exasol evaluates the same call it sent, so no divergence is
  possible. Only the DataFusion dialect needs the `date_part`/epoch-arithmetic translations below
  (issue #209). Verified on live Exasol 2025.1.x: `DATE_PART` is not an Exasol function
  (`function or script DATE_PART not found`, SQL code 42000), so every `date_part`-based rendering
  is a hard compilation error in Exasol-dialect wrapper SQL.
* The dialect split is a rendering-time concern only. It changes no capability advertisement: every
  function named below stays advertised, because the DataFusion dialect still governs what the
  node-local scan can evaluate.
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
* Exasol sends EXTRACT as node type `function_scalar_extract` with the field in a `toExtract`
  property (not as `function_scalar` + `dateTimeField`). DataFusion 54 renders EXTRACT via
  `date_part('<FIELD>', expr)` — not `EXTRACT(<FIELD> FROM expr)` — because DataFusion 54's default
  feature set does not include an EXTRACT ExprPlanner. Both forms are semantically equivalent;
  the DataFusion dialect uses `date_part` to match DataFusion's execution path, while the Exasol
  dialect renders Exasol's own `EXTRACT(<FIELD> FROM expr)` form.
* Only date/time functions whose DataFusion semantics match Exasol's are translated. Functions
  that depend on Exasol session state or whose DataFusion equivalent diverges in result are left
  unsupported (the node returns an error in raising mode, `None` in the safe variants), so the
  adapter omits them and Exasol post-processes them as a correctness backstop. This parity gate
  governs the DataFusion dialect, which decides what the node-local scan evaluates; it does not
  govern the Exasol dialect, whose verbatim rendering cannot diverge.
* `WEEK` is translated because Exasol `WEEK` and DataFusion 54 `date_part('week', …)` are both
  ISO-8601. The Exasol dialect renders `WEEK(…)` and inherits Exasol's own week numbering directly.
* Date-difference functions are translated for the subset whose DataFusion
  54.0.0 rendering both executes and matches Exasol's documented semantics, verified per function
  against the Exasol built-in function reference (live Exasol 2025.1.3) and the DataFusion 54.0.0
  function surface — confirmed against the pinned-tag source and by executing each rendering through
  DataFusion 54.0.0 (issue #107). The DataFusion-dialect translated set is:
  * `DAYS_BETWEEN` — whole-day date difference. Exasol uses only the date part of a timestamp;
    computed as `DATE − DATE`, which yields an `Int64` day count in DataFusion 54.0.0 (confirmed:
    `is_date_minus_date` in `datafusion/expr-common/src/type_coercion/binary.rs` returns
    `ret: Int64`, and an executed `CAST(<a> AS DATE) - CAST(<b> AS DATE)` returns an `Int64`).
  * `HOURS_BETWEEN`, `MINUTES_BETWEEN`, `SECONDS_BETWEEN` — fractional differences over full
    timestamps, computed from `date_part('epoch', …)` (`Float64` seconds) differences.
<!-- /DELTA:CHANGED -->
* The following remain unsupported and fall through so Exasol post-processes them, each for a
  named reason (issue #107 permits advertising only the subset with verified parity):
  * `ADD_HOURS`, `ADD_MINUTES` — withdrawn after end-to-end parity testing (the parity gate this
    feature imposes on every advertised arm). The integer-microsecond rendering executes and matches
    Exasol for a TIMESTAMP argument (`ADD_HOURS(<ts>, 1.5)` → +2 hours, round-half-away-from-zero
    confirmed), but it diverges for a DATE argument: the rendering always yields a
    `Timestamp(Microsecond)` (mapped to `TIMESTAMP(3)`), whereas Exasol infers `TIMESTAMP(0)` for
    `ADD_HOURS(<date>, n)`, so live Exasol 2025.1.3 rejects the pushdown with `Data type mismatch in
    column number 1 ... Expected TIMESTAMP(0), but got TIMESTAMP(3)`. This is the same
    input-type-dependent-return class as `ADD_DAYS`/`ADD_WEEKS` below (here at the fractional-seconds
    precision level rather than DATE-vs-TIMESTAMP): the type-blind string translator has no argument
    type and cannot vary the result precision, and no single execution-safe DataFusion 54.0.0
    rendering matches Exasol for both DATE and TIMESTAMP arguments. Deferred rather than shipping a
    capability that fails on DATE columns; a future type-aware translator could revisit this.
  * `ADD_DAYS`, `ADD_WEEKS` — Exasol's return type depends on the argument type: a DATE argument
    yields a DATE, a TIMESTAMP argument yields a TIMESTAMP preserving time-of-day (verified on live
    Exasol 2025.1.3). The translator renders SQL from the pushdown expression tree with no argument
    type information (column nodes carry only a name), so a single rendering cannot reproduce both
    return types; every execution-safe DataFusion 54.0.0 rendering routes through a `TIMESTAMP` and
    would widen a DATE result. (`<x> + <n> * INTERVAL '1 day'` — which would preserve the argument
    type — is rejected at plan time by arrow-rs#9030.) Deferred rather than shipping a type-widening
    rendering; a future type-aware translator could revisit this.
  * `ADD_YEARS` — Exasol applies month-end stickiness that no execution-safe DataFusion 54.0.0
    rendering reproduces, the same divergence class as `ADD_MONTHS`. The leap-day clamp alone
    (`ADD_YEARS(DATE '2000-02-29', 1)` → `2001-02-28`) IS reproducible and execution-safe: a
    year-interval builds without the broken runtime multiply via
    `arrow_cast(<months_int>, 'Interval(YearMonth)')` (Arrow 58 allows `Int32 → Interval(YearMonth)`),
    and `Date`/`Timestamp` + `Interval(YearMonth)` addition executes and clamps an overflow day to the
    last valid day. That path does NOT reproduce Exasol's month-end stickiness: `ADD_YEARS(DATE
    '2001-02-28', 3)` returns `2004-02-29` on live Exasol 2025.1.3 (a last-day-of-month argument maps
    to the last day of the target month), whereas Arrow's month arithmetic keeps the day-of-month and
    yields `2004-02-28`. Epoch-second arithmetic (a fixed `365.25`-day year) is not calendar-correct,
    and the return type is input-type-dependent like `ADD_DAYS`. Deferred on the same defer-honestly
    precedent as `ADD_MONTHS`.
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
  * A function in this unsupported set falls through in BOTH dialects. The Exasol dialect's verbatim
    rule applies only to names this feature already translates; it MUST NOT be read as a licence to
    render an untranslated name just because Exasol would accept it, because the adapter advertises
    one capability set for both dialects and a name advertised for the Exasol wrapper would also be
    pushed at the DataFusion scan.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: EXTRACT translates to the DataFusion date_part call

* *GIVEN* a VS expression node of type `function_scalar_extract` carrying a `toExtract` field with a datetime field name (e.g. `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`) and an `arguments` array with a single source datetime expression
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <source_sql>)` with the field name single-quoted and the source rendered recursively
* *AND* the rendered field name MUST be one DataFusion's `date_part` function recognises
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: EXTRACT renders Exasol's EXTRACT FROM form in the Exasol dialect

* *GIVEN* the same `function_scalar_extract` node as the preceding scenario
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `EXTRACT(<FIELD> FROM <source_sql>)` with the field name rendered as a bare keyword — NOT single-quoted — and the source rendered recursively in the Exasol dialect
* *AND* the rendered fragment MUST NOT contain `date_part`, because Exasol has no `DATE_PART` function and the fragment is parsed by Exasol's own core engine (`function or script DATE_PART not found`, SQL code 42000)
* *AND* the DataFusion-dialect rendering of the same node MUST remain byte-identical to the preceding scenario
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Field-shortcut date functions translate to date_part of the matching field

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `SECOND` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <arg_sql>)` where `<FIELD>` is the function's own name, single-quoted
* *AND* the argument SHALL be rendered recursively
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Field-shortcut date functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `SECOND` with a single datetime argument
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg_sql>)` using the function's own uppercased Exasol name, with the argument rendered recursively in the Exasol dialect
* *AND* the rendered fragment MUST NOT contain `date_part`
* *AND* the DataFusion-dialect rendering of the same node MUST remain byte-identical to the preceding scenario
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: DATE_TRUNC translates to the DataFusion date_trunc call

* *GIVEN* a VS expression node of type `function_scalar` named `DATE_TRUNC` with a precision/unit literal argument and a source datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_trunc(<unit_sql>, <source_sql>)` with both arguments rendered recursively
* *AND* the unit literal MUST be passed through as a string argument DataFusion's `date_trunc` accepts (e.g. `'year'`, `'month'`, `'day'`, `'hour'`)
* *AND* `render_expression_exasol` SHALL return `DATE_TRUNC(<unit_sql>, <source_sql>)` — Exasol's own PostgreSQL-compatible `DATE_TRUNC` takes the same argument order, so the unit literal Exasol sent is forwarded unchanged and Exasol applies its own `NLS_FIRST_DAY_OF_WEEK` for the `'week'` unit
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: CURRENT_DATE and CURRENT_TIMESTAMP translate to DataFusion now-family calls

* *GIVEN* a VS expression node of type `function_scalar` named `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `SYSDATE`, or `SYSTIMESTAMP` with no datetime-dependent arguments
* *WHEN* `render_expression` processes the node
* *THEN* `CURRENT_DATE`/`SYSDATE` SHALL render as `current_date()` and `CURRENT_TIMESTAMP`/`SYSTIMESTAMP` SHALL render as `now()`
* *AND* the translator MUST NOT depend on any Exasol session state to render these nodes
* *AND* `render_expression_exasol` SHALL render each of the four names as its own bare Exasol keyword — `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` — with no parentheses and no collapsing of one name onto another, so Exasol applies its own session-time-vs-database-time distinction instead of the translator silently mapping `SYSDATE` onto `CURRENT_DATE`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls

* *GIVEN* a VS expression node of type `function_scalar` named `TO_DATE` or `TO_TIMESTAMP` with a string source argument and an optional format argument
* *WHEN* `render_expression` processes the node
* *THEN* `TO_DATE` SHALL render as `to_date(<args>)` and `TO_TIMESTAMP` SHALL render as `to_timestamp(<args>)` over the recursively rendered arguments
* *AND* when a format argument is present it SHALL be forwarded as the corresponding DataFusion format argument
* *AND* `render_expression_exasol` SHALL render `TO_DATE(<args>)` / `TO_TIMESTAMP(<args>)`, forwarding the format argument unchanged, so Exasol interprets the format model it sent rather than the translator forwarding an Exasol format string to a DataFusion parser
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: WEEK translates to the DataFusion date_part('week') ISO-8601 call

* *GIVEN* a VS expression node of type `function_scalar` named `WEEK` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('week', <arg_sql>)` with the argument rendered recursively
* *AND* the rendered call SHALL yield the ISO-8601 week number (1-53, weeks beginning Monday, week 1 containing the year's first Thursday) matching Exasol `WEEK`, including at year boundaries
* *AND* `render_expression_exasol` SHALL return `WEEK(<arg_sql>)`, so Exasol computes its own week number and the ISO-8601 equivalence above is not relied upon on that path
* *AND* `FN_WEEK` (advertised per `vs-adapter/pushdown-planning-capability-extensions`) SHALL be advertised only while the DataFusion-dialect ISO-8601 parity holds; if a year-boundary case diverges, the DataFusion-dialect `WEEK` rendering SHALL be withdrawn and `FN_WEEK` left unadvertised
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: DAYS_BETWEEN translates to a whole-day date difference

* *GIVEN* a VS expression node of type `function_scalar` named `DAYS_BETWEEN` with two datetime arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the first argument's date minus the second argument's date as a whole-day count (canonical form `(CAST(<arg1_sql> AS DATE) - CAST(<arg2_sql> AS DATE))`), using only the date part of a timestamp argument, matching Exasol
* *AND* the result SHALL be negative when the first argument is earlier than the second, matching Exasol's `DAYS_BETWEEN(timestamp1, timestamp2)` sign convention (first minus second)
* *AND* `render_expression_exasol` SHALL return `DAYS_BETWEEN(<arg1_sql>, <arg2_sql>)`, so the sign convention and the date-part-only rule come from Exasol itself rather than from the `DATE − DATE` rewrite
* *AND* `FN_DAYS_BETWEEN` SHALL be advertised only while an end-to-end parity test confirms the DataFusion-dialect rendering matches Exasol; if it diverges, the DataFusion-dialect rendering SHALL be withdrawn and the capability left unadvertised
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: HOURS_BETWEEN, MINUTES_BETWEEN, and SECONDS_BETWEEN translate to epoch-second differences

* *GIVEN* a VS expression node of type `function_scalar` named `HOURS_BETWEEN`, `MINUTES_BETWEEN`, or `SECONDS_BETWEEN` with two timestamp arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the difference of the two arguments' epoch seconds (`date_part('epoch', <arg1_sql>) - date_part('epoch', <arg2_sql>)`), divided by `3600` for `HOURS_BETWEEN`, by `60` for `MINUTES_BETWEEN`, and undivided for `SECONDS_BETWEEN`
* *AND* the result SHALL retain the fractional difference between the two timestamps and SHALL be negative when the first argument is earlier than the second, matching Exasol (first minus second)
* *AND* `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, and `FN_SECONDS_BETWEEN` SHALL each be advertised only while an end-to-end parity test confirms the DataFusion-dialect rendering matches Exasol for that function; if one diverges, that DataFusion-dialect rendering SHALL be withdrawn and its capability left unadvertised
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Date-difference functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named `HOURS_BETWEEN`, `MINUTES_BETWEEN`, or `SECONDS_BETWEEN` with two timestamp arguments
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg1_sql>, <arg2_sql>)` using the function's own uppercased Exasol name, with both arguments rendered recursively in the Exasol dialect and argument order preserved
* *AND* the rendered fragment MUST NOT contain `date_part` and MUST NOT contain the `/ 3600` or `/ 60` unit divisor, because Exasol's own function already returns the requested unit
* *AND* the DataFusion-dialect rendering of the same node MUST remain byte-identical to the preceding scenario, so the epoch-arithmetic path the node-local scan depends on is unchanged
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Unsupported date functions fall through as unsupported nodes

* *GIVEN* a VS expression node of type `function_scalar` named with a date-function name this feature does not translate — `ADD_HOURS`, `ADD_MINUTES`, `ADD_DAYS`, `ADD_WEEKS`, `ADD_YEARS`, `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `CONVERT_TZ`, `POSIX_TIME`, or `LAST_DAY`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the unsupported function
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking, and `render_expression_exasol` / `render_expression_exasol_safe` SHALL fall through identically — an error in raising mode, `None` in the safe variant — so the Exasol dialect's verbatim rule never widens the translated set
* *AND* the adapter SHALL omit the function from the scan spec and let Exasol post-process it, because each named function either has an execution, parity, session-state, or input-type-dependent-return divergence from its DataFusion 54.0.0 equivalent or (`LAST_DAY`) is not an Exasol function at all (see Background)
* *AND* the capabilities `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_ADD_DAYS`, `FN_ADD_WEEKS`, `FN_ADD_YEARS`, `FN_ADD_SECONDS`, `FN_ADD_MONTHS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, and `FN_CONVERT_TZ` MUST NOT be advertised, and no `FN_LAST_DAY` capability SHALL exist to advertise (Exasol has no `LAST_DAY` function and its `ScalarFunctionCapability` enum has no `LAST_DAY` member; `ADD_HOURS`/`ADD_MINUTES` are in this set because their end-to-end parity test found a DATE-argument `TIMESTAMP(0)` vs. `TIMESTAMP(3)` divergence — see Background)
<!-- /DELTA:CHANGED -->
