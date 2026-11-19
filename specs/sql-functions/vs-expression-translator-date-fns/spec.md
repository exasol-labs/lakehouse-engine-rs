# Feature: VS Expression Translator, Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the Exasol date and time scalar functions that DataFusion can evaluate, so date-valued filters, select-list expressions and group keys run in the node-local scan. `sql-functions/vs-expression-translator-date-diff-fns` covers the `*_BETWEEN` family and the date-arithmetic functions that stay unsupported.

## Background

* The Exasol dialect renders wrapper SQL text that Exasol's own engine parses. It renders the original name, argument order and argument count of every date or time function, because Exasol's compiler emitted them. `EXTRACT` is the exception, because Exasol sends it as its own node type. Only the DataFusion dialect uses the `date_part` and epoch-arithmetic translations. `DATE_PART` is not an Exasol function (`function or script DATE_PART not found`, SQL code 42000), so a `date_part` rendering in Exasol-dialect wrapper SQL fails to compile.
* No clock function reaches the scan or the wrapper SQL: the pushed SQL holds a date or timestamp literal in its place (`pushdown/pushdown-planning-clock-functions`).
* Exasol sends EXTRACT as node type `function_scalar_extract` with the field in a `toExtract` property. For every field except `SECOND`, the DataFusion dialect renders `date_part('<FIELD>', expr)`, because the DataFusion dependency's default feature set has no EXTRACT planner. The Exasol dialect renders `EXTRACT(<FIELD> FROM expr)`.
* The DataFusion dialect translates only functions whose DataFusion semantics match Exasol's. A node it refuses returns an error. An omitted node is post-processed by Exasol only while its capability is unadvertised. For an advertised capability the caller applies the node itself: see `pushdown-capabilities/pushdown-planning-capability-extensions` and `pushdown/pushdown-declined-filter-self-apply`.
* A refused arity is the same case as a refused name. The Exasol dialect renders a declared verbatim-call name at any arity, while the DataFusion dialect computes only the arities it supports. `SECOND` is the exception: the scan computes only its one-argument call. A call with an arity the DataFusion dialect does not compute declines in the DataFusion dialect and renders in the Exasol dialect, and the caller self-applies it.
* `WEEK` is translated because Exasol `WEEK` and DataFusion `date_part('week', …)` are both ISO-8601. The Exasol dialect renders `WEEK(…)` and inherits Exasol's week numbering.
* Exasol reads the fields of a `TIMESTAMP WITH LOCAL TIME ZONE` value in the session time zone, which the scan UDF does not receive.

## Scenarios

### Scenario: A refused argument count declines for DataFusion and renders for Exasol

* *GIVEN* a `function_scalar` node whose name is a declared verbatim-call date/time function and whose argument count exceeds what the DataFusion dialect computes, for example `SECOND(<datetime>, <precision>)`, whose Exasol signature takes an optional precision and whose typed scan pass computes only the one-argument call
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error, because no DataFusion arm expresses that call
* *AND* the Exasol dialect SHALL render the call verbatim, the name, argument order, and argument count Exasol sent, because Exasol's own compiler emitted it
* *AND* the caller SHALL treat that asymmetry as a decline to self-apply, not as an omission, because the function's capability is advertised and Exasol therefore delegated the call

### Scenario: EXTRACT translates to the DataFusion date_part call

* *GIVEN* a VS expression node of type `function_scalar_extract` carrying a `toExtract` field of `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE` and an `arguments` array with a single source datetime expression
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <source_sql>)` with the field name single-quoted and the source rendered recursively
* *AND* the rendered field name MUST be one DataFusion's `date_part` function recognises

### Scenario: EXTRACT renders Exasol's EXTRACT FROM form in the Exasol dialect

* *GIVEN* a `function_scalar_extract` node whose `toExtract` field is a datetime field name, `SECOND` included
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `EXTRACT(<FIELD> FROM <source_sql>)` with the field name rendered as a bare keyword, NOT single-quoted, and the source rendered recursively in the Exasol dialect
* *AND* the rendered fragment MUST NOT contain `date_part`, because Exasol has no `DATE_PART` function and the fragment is parsed by Exasol's own core engine (`function or script DATE_PART not found`, SQL code 42000)
* *AND* the DataFusion-dialect rendering of the same node SHALL follow the preceding scenario, or `The untyped DataFusion walker declines SECOND` in `sql-functions/vs-expression-translator-second` for a `SECOND` field

### Scenario: Field-shortcut date functions translate to date_part of the matching field

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <arg_sql>)` where `<FIELD>` is the function's own name, single-quoted
* *AND* the argument SHALL be rendered recursively

### Scenario: Field-shortcut date functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `SECOND` with a single datetime argument
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg_sql>)` using the function's own uppercased Exasol name, with the argument rendered recursively in the Exasol dialect
* *AND* the rendered fragment MUST NOT contain `date_part`
* *AND* the DataFusion-dialect rendering of the same node SHALL follow the preceding scenario, or `The untyped DataFusion walker declines SECOND` in `sql-functions/vs-expression-translator-second` for `SECOND`

### Scenario: DATE_TRUNC translates to the DataFusion date_trunc call

* *GIVEN* a VS expression node of type `function_scalar` named `DATE_TRUNC` with a precision/unit literal argument and a source datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_trunc(<unit_sql>, <source_sql>)` with both arguments rendered recursively
* *AND* the unit literal MUST be passed through as a string argument DataFusion's `date_trunc` accepts (e.g. `'year'`, `'month'`, `'day'`, `'hour'`)
* *AND* over a DATE the translator SHALL return `CAST(date_trunc(<unit_sql>, <source_sql>) AS DATE)`, because Exasol returns a DATE and DataFusion a timestamp
* *AND* the scan SHALL fail on a source outside 1677-09-21 to 2262-04-11 that is a DATE, or a TIMESTAMP truncated to `week`, `month`, `quarter`, or `year`, because DataFusion converts it to nanoseconds, and `docs/semantic-differences.md` SHALL list this difference
* *AND* a TIMESTAMP truncated to `second`, `minute`, `hour`, or `day` SHALL return native Exasol's value across Exasol's whole range
* *AND* `render_expression_exasol` SHALL return `DATE_TRUNC(<unit_sql>, <source_sql>)`, Exasol's own PostgreSQL-compatible `DATE_TRUNC` takes the same argument order, so the unit literal Exasol sent is forwarded unchanged and Exasol applies its own `NLS_FIRST_DAY_OF_WEEK` for the `'week'` unit

### Scenario: TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls

* *GIVEN* a VS expression node of type `function_scalar` named `TO_DATE` or `TO_TIMESTAMP` with a string source argument and an optional format argument
* *WHEN* `render_expression` processes the node
* *THEN* with one argument the DataFusion dialect SHALL decline the node, because Exasol parses the source by the session's `NLS_DATE_FORMAT` or `NLS_TIMESTAMP_FORMAT` (`pushdown-types/pushdown-planning-scan-text-conversion`)
* *AND* with a format argument the DataFusion dialect SHALL translate the format or decline the node, as `sql-functions/vs-expression-translator-datetime-format` specifies, and MUST NOT forward an Exasol format model to a DataFusion parser
* *AND* `render_expression_exasol` SHALL render `TO_DATE(<args>)` / `TO_TIMESTAMP(<args>)`, forwarding the format argument unchanged, so Exasol interprets the format model it sent

### Scenario: WEEK translates to the DataFusion date_part('week') ISO-8601 call

* *GIVEN* a VS expression node of type `function_scalar` named `WEEK` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('week', <arg_sql>)` with the argument rendered recursively
* *AND* the rendered call SHALL yield the ISO-8601 week number (1-53, weeks beginning Monday, week 1 containing the year's first Thursday) matching Exasol `WEEK`, including at year boundaries
* *AND* `render_expression_exasol` SHALL return `WEEK(<arg_sql>)`, so Exasol computes its own week number and the ISO-8601 equivalence above is not relied upon on that path
* *AND* `FN_WEEK` (advertised per `pushdown-capabilities/pushdown-planning-capability-extensions`) SHALL be advertised only while the DataFusion-dialect ISO-8601 parity holds; if a year-boundary case diverges, the DataFusion-dialect `WEEK` rendering SHALL be withdrawn and `FN_WEEK` left unadvertised
