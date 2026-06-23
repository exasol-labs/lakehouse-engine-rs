# Feature: VS Expression Translator — Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the
Exasol date/time scalar functions that DataFusion 54 can evaluate, so date-valued filters,
select-list expressions, and group keys push down to the node-local DataFusion scan instead of
being post-processed in Exasol. Kept as a separate feature so the scalar-ops spec stays focused on
arithmetic, string, and conditional functions.

## Background

* This feature shares the three public entry points of `crates/vs-expression`
  (`render_expression`, `render_expression_safe`, `render_df_filter_safe`); the date functions are
  additional `function_scalar` name arms inside the same recursive walker.
* Only date/time functions whose DataFusion semantics match Exasol's are translated. Functions
  that depend on Exasol session state or whose DataFusion equivalent diverges in result are left
  unsupported (the node returns an error in raising mode, `None` in the safe variants), so the
  adapter omits them and Exasol post-processes them as a correctness backstop.
* `EXTRACT(<field> FROM <source>)` and `date_part('<field>', <source>)` are interchangeable in
  DataFusion; the translator MAY use either, but MUST produce an integer-valued result matching
  Exasol's field semantics.

## Scenarios

### Scenario: EXTRACT translates to the DataFusion EXTRACT form

* *GIVEN* a VS expression node of type `function_scalar` named `EXTRACT` carrying a datetime field name (e.g. `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`) and a source datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `EXTRACT(<FIELD> FROM <source_sql>)` with the source rendered recursively
* *AND* the rendered field name MUST be one DataFusion recognises for the EXTRACT form

### Scenario: Field-shortcut date functions translate to EXTRACT of the matching field

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `SECOND` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `EXTRACT(<FIELD> FROM <arg_sql>)` where `<FIELD>` is the function's own name
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

### Scenario: Unsupported date functions fall through as unsupported nodes

* *GIVEN* a VS expression node of type `function_scalar` named with an Exasol date function this feature does not translate (e.g. `ADD_DAYS`, `DAYS_BETWEEN`, `CONVERT_TZ`, `POSIX_TIME`)
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the unsupported function
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL therefore omit the function from the scan spec and let Exasol post-process it
