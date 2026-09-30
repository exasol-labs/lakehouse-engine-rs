# Feature: VS Expression Translator — Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the
Exasol date/time scalar functions that DataFusion 54 can evaluate, so date-valued filters,
select-list expressions, and group keys push down to the node-local DataFusion scan instead of
being post-processed in Exasol. Kept as a separate feature so the scalar-ops spec stays focused on
arithmetic, string, and conditional functions. The date-difference (`*_BETWEEN`) family and the
divergent date-arithmetic functions issue #107 examined but declined to translate are covered by
the sibling feature `sql-comprehension/vs-expression-translator-date-diff-fns`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/sql-comprehension/vs-expression-translator-date-fns/spec.md`.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: DATE_TRUNC translates to the DataFusion date_trunc call

* *GIVEN* a VS expression node of type `function_scalar` named `DATE_TRUNC` with a precision/unit literal argument and a source datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_trunc(<unit_sql>, <source_sql>)` with both arguments rendered recursively
* *AND* the unit literal MUST be passed through as a string argument DataFusion's `date_trunc` accepts (e.g. `'year'`, `'month'`, `'day'`, `'hour'`)
* *AND* `render_expression_exasol` SHALL return `DATE_TRUNC(<unit_sql>, <source_sql>)` — Exasol's own PostgreSQL-compatible `DATE_TRUNC` takes the same argument order, so the unit literal Exasol sent is forwarded unchanged and Exasol applies its own `NLS_FIRST_DAY_OF_WEEK` for the `'week'` unit
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: DATE_TRUNC renders as the date-trunc UDF for the seven calendar units

* *GIVEN* a `function_scalar` node named `DATE_TRUNC` whose first argument is a `literal_string` naming `year`, `quarter`, `month`, `day`, `hour`, `minute`, or `second` in any letter case, and whose second argument is a source datetime expression
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `<date-trunc-fn>(<source_sql>, '<format>')`, where `<format>` is the vocabulary token of the same unit (`YYYY`, `Q`, `MM`, `DD`, `HH`, `MI`, `SS` respectively, see `sql-comprehension/vs-expression-translator-datetime-trunc`)
* *AND* `<date-trunc-fn>` SHALL be exported from `crates/vs-expression` as one public constant that the rendering reads and the scan crate registers, and the sweep's banned-token list SHALL gain it, because Exasol has no function of that name
* *AND* the rendering MUST NOT emit DataFusion's `date_trunc`, which converts a `Date32` to a nanosecond timestamp limited to the years 1677 to 2262, while `9999-12-31` is a common Exasol "no end date" value
* *AND* `render_expression_exasol` SHALL return `DATE_TRUNC(<unit_sql>, <source_sql>)`, forwarding the unit literal Exasol sent unchanged
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: DATE_TRUNC declines week, every other unit, and a non-literal unit in the DataFusion dialect

* *GIVEN* a `DATE_TRUNC` node whose first argument is `'week'`, a token outside the seven units of the preceding scenario (for example `'decade'`, `'century'`, `'millennium'`, `'milliseconds'`, `'microseconds'`, or `' month'`), or a node that is not a `literal_string`
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error in raising mode and `None` in the safe variants
* *AND* the Exasol dialect SHALL render `DATE_TRUNC(<unit_sql>, <source_sql>)` verbatim, so Exasol applies its own session `NLS_FIRST_DAY_OF_WEEK` to `'week'`
* *AND* the `'week'` decline SHALL rest on the scan's missing session context: DataFusion's `date_trunc('week', …)` always truncates to Monday, and through the local virtual schema it returned `2024-01-01` for `DATE '2024-01-01'` against Exasol's `2023-12-31` at `NLS_FIRST_DAY_OF_WEEK = 7`
<!-- /DELTA:NEW -->
