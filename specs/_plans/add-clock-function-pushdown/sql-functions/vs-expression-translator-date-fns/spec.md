# Feature: VS Expression Translator, Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the Exasol date and time scalar functions that DataFusion can evaluate, so date-valued filters, select-list expressions and group keys run in the node-local scan. `sql-functions/vs-expression-translator-date-diff-fns` covers the `*_BETWEEN` family and the date-arithmetic functions that stay unsupported.

<!-- DELTA:CHANGED -->
## Background

* The Exasol dialect renders wrapper SQL text that Exasol's own engine parses. It renders the original name, argument order and argument count of every date or time function, because Exasol's compiler emitted them. `EXTRACT` is the exception, because Exasol sends it as its own node type. Only the DataFusion dialect uses the `date_part` and epoch-arithmetic translations. `DATE_PART` is not an Exasol function (`function or script DATE_PART not found`, SQL code 42000), so a `date_part` rendering in Exasol-dialect wrapper SQL fails to compile.
* No clock function reaches the scan or the wrapper SQL: the pushed SQL holds a date or timestamp literal in its place (`pushdown/pushdown-planning-clock-functions`).
* Exasol sends EXTRACT as node type `function_scalar_extract` with the field in a `toExtract` property. For every field except `SECOND`, the DataFusion dialect renders `date_part('<FIELD>', expr)`, because the DataFusion dependency's default feature set has no EXTRACT planner. The Exasol dialect renders `EXTRACT(<FIELD> FROM expr)`.
* The DataFusion dialect translates only functions whose DataFusion semantics match Exasol's. A node it refuses returns an error. An omitted node is post-processed by Exasol only while its capability is unadvertised. For an advertised capability the caller applies the node itself: see `pushdown-capabilities/pushdown-planning-capability-extensions` and `pushdown/pushdown-declined-filter-self-apply`.
* A refused arity is the same case as a refused name. The Exasol dialect renders a declared verbatim-call name at any arity, while the DataFusion dialect computes only the arities it supports. `SECOND` is the exception: the scan computes only its one-argument call. A call with an arity the DataFusion dialect does not compute declines in the DataFusion dialect and renders in the Exasol dialect, and the caller self-applies it.
* `WEEK` is translated because Exasol `WEEK` and DataFusion `date_part('week', …)` are both ISO-8601. The Exasol dialect renders `WEEK(…)` and inherits Exasol's week numbering.
* Exasol reads the fields of a `TIMESTAMP WITH LOCAL TIME ZONE` value in the session time zone, which the scan UDF does not receive.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The now-family is not translated

* *GIVEN* the adapter's advertised capability set and a clock function call
* *WHEN* Exasol plans a query that uses the call
* *THEN* this behavior SHALL be removed; the pushed SQL holds a literal in place of every clock function, per `pushdown/pushdown-planning-clock-functions`
<!-- /DELTA:REMOVED -->
