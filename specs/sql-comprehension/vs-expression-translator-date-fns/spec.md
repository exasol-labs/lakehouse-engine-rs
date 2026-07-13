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
  ISO-8601. The date-arithmetic (`ADD_*`), date-difference (`*_BETWEEN`), month/year arithmetic
  (`ADD_MONTHS`, `ADD_YEARS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`), `DAYOFWEEK`, `LAST_DAY`, and
  `CONVERT_TZ` functions are not: DataFusion 54 lacks these builtins, its variable×INTERVAL scaling
  is unverified, its `date_part('dow')` numbers Sunday as 0, and `CONVERT_TZ` is session-timezone
  dependent.

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

### Scenario: Unsupported date functions fall through as unsupported nodes

* *GIVEN* a VS expression node of type `function_scalar` named with an Exasol date function this feature does not translate — the date-arithmetic functions (`ADD_DAYS`, `ADD_HOURS`, `ADD_MINUTES`, `ADD_SECONDS`, `ADD_WEEKS`, `ADD_MONTHS`, `ADD_YEARS`), the date-difference functions (`DAYS_BETWEEN`, `HOURS_BETWEEN`, `MINUTES_BETWEEN`, `SECONDS_BETWEEN`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`), or the other date scalars (`DAYOFWEEK`, `LAST_DAY`, `CONVERT_TZ`, `POSIX_TIME`)
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the unsupported function
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the function from the scan spec and let Exasol post-process it, because their DataFusion 54 equivalents diverge from Exasol (see Background)
