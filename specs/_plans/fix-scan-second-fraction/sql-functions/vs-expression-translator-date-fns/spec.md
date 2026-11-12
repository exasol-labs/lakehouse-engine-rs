# Feature: VS Expression Translator, Date and Time Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the Exasol date and time scalar functions that DataFusion can evaluate, so date-valued filters, select-list expressions and group keys run in the node-local scan. `sql-functions/vs-expression-translator-date-diff-fns` covers the `*_BETWEEN` family and the date-arithmetic functions that stay unsupported.

<!-- DELTA:CHANGED -->
## Background

* The date functions are arms of the same recursive walker behind the public entry points of `crates/vs-expression`: `render_scan_expression` and `render_scan_filter` for DataFusion, and `render_expression_exasol`, `render_expression_exasol_safe` and `render_df_filter_exasol_safe` for Exasol.
* The Exasol dialect renders wrapper SQL text that Exasol's own engine parses. It renders the original name, argument order and argument count of every date or time function, because Exasol's compiler emitted them. Each such name is declared `VerbatimCall` in the crate's declaration of translated names (see `sql-comprehension/vs-expression-translator`), and the declaration's own branch renders it ahead of every per-name arm. `EXTRACT` is the exception: it is the node type `function_scalar_extract` and branches on dialect inside its own arm. Only the DataFusion dialect uses the `date_part` and epoch-arithmetic translations. `DATE_PART` is not an Exasol function (`function or script DATE_PART not found`, SQL code 42000), so a `date_part` rendering in Exasol-dialect wrapper SQL fails to compile.
* The now-family `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP` and `SYSTIMESTAMP` is translated in neither dialect, and `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE` and `FN_SYSTIMESTAMP` are not advertised: see `pushdown-capabilities/pushdown-planning-capability-extensions`. Exasol reads the instant in the session time zone (`CURRENT_TIMESTAMP`) or the database time zone (`SYSTIMESTAMP`), and `CURRENT_DATE` and `SYSDATE` are `TO_DATE` of each. The scan UDF receives neither zone and has no statement anchor, so it can only read its container clock in UTC, once per shard. Exasol's now-family is statement-constant. Exasol never delegates an unadvertised capability, so withdrawal hands the four names back to the engine that owns the clock.
* Exasol sends EXTRACT as node type `function_scalar_extract` with the field in a `toExtract` property. For every field except `SECOND`, the DataFusion dialect renders `date_part('<FIELD>', expr)`, because the DataFusion dependency's default feature set has no EXTRACT planner. The Exasol dialect renders `EXTRACT(<FIELD> FROM expr)`.
* The DataFusion dialect translates only functions whose DataFusion semantics match Exasol's. A node it refuses returns an error in raising mode and `None` in the safe variants. An omitted node is post-processed by Exasol only while its capability is unadvertised. For an advertised capability the caller applies the node itself: see `pushdown-capabilities/pushdown-planning-capability-extensions` and `pushdown/pushdown-declined-filter-self-apply`.
* A refused arity is the same case as a refused name. The Exasol dialect renders a declared verbatim-call name at any arity, while the DataFusion dialect checks arity in each name's arm. `SECOND` is the exception: the typed scan pass computes only its one-argument call, and its DataFusion arm declines every call. A call with an arity the DataFusion dialect does not compute declines in the DataFusion dialect and renders in the Exasol dialect, and the caller self-applies it.
* `WEEK` is translated because Exasol `WEEK` and DataFusion `date_part('week', …)` are both ISO-8601. The Exasol dialect renders `WEEK(…)` and inherits Exasol's week numbering.
* Exasol types `SECOND(x)` and `EXTRACT(SECOND FROM x)` over a `TIMESTAMP(p)` argument as `DECIMAL(2+p, p)`: the seconds with every fractional digit the argument holds. This was measured on 2025.1.16 for p from 0 to 9, and on 8.29.13, whose only timestamp precision is 3.
* `SECOND` over a DATE is 0. Exasol 2025.1.16 types it `DECIMAL(2,0)`, and 8.29.13 types it `DECIMAL(5,3)`. `EXTRACT(SECOND FROM <date>)` fails on both versions with `Feature not supported: extract(SECOND, date) not supported` (SQL state 0A000).
* Exasol cuts the fractional digits beyond p when it stores a value at precision p, for example `CAST(ts AS TIMESTAMP(3))` of `59.999999` is `59.999`. DataFusion's `date_part('second', …)` returns whole seconds only.
* Exasol reads the fields of a `TIMESTAMP WITH LOCAL TIME ZONE` value in the session time zone, which the scan UDF does not receive.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A refused argument count declines for DataFusion and renders for Exasol

* *GIVEN* a `function_scalar` node whose name is a declared verbatim-call date/time function and whose argument count exceeds what the DataFusion dialect computes, for example `SECOND(<datetime>, <precision>)`, whose Exasol signature takes an optional precision and whose typed scan pass computes only the one-argument call
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error in raising mode and `None` in the safe variants, because no DataFusion arm expresses that call
* *AND* the Exasol dialect SHALL render the call verbatim, the name, argument order, and argument count Exasol sent, because Exasol's own compiler emitted it
* *AND* the caller SHALL treat that asymmetry as a decline to self-apply, not as an omission, because the function's capability is advertised and Exasol therefore delegated the call
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: EXTRACT translates to the DataFusion date_part call

* *GIVEN* a VS expression node of type `function_scalar_extract` carrying a `toExtract` field of `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE` and an `arguments` array with a single source datetime expression
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <source_sql>)` with the field name single-quoted and the source rendered recursively
* *AND* the rendered field name MUST be one DataFusion's `date_part` function recognises
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: EXTRACT renders Exasol's EXTRACT FROM form in the Exasol dialect

* *GIVEN* a `function_scalar_extract` node whose `toExtract` field is a datetime field name, `SECOND` included
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `EXTRACT(<FIELD> FROM <source_sql>)` with the field name rendered as a bare keyword, NOT single-quoted, and the source rendered recursively in the Exasol dialect
* *AND* the rendered fragment MUST NOT contain `date_part`, because Exasol has no `DATE_PART` function and the fragment is parsed by Exasol's own core engine (`function or script DATE_PART not found`, SQL code 42000)
* *AND* the DataFusion-dialect rendering of the same node SHALL follow the preceding scenario, or `The untyped DataFusion walker declines SECOND` for a `SECOND` field
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Field-shortcut date functions translate to date_part of the matching field

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE` with a single datetime argument
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `date_part('<FIELD>', <arg_sql>)` where `<FIELD>` is the function's own name, single-quoted
* *AND* the argument SHALL be rendered recursively
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Field-shortcut date functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `SECOND` with a single datetime argument
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg_sql>)` using the function's own uppercased Exasol name, with the argument rendered recursively in the Exasol dialect
* *AND* the rendered fragment MUST NOT contain `date_part`
* *AND* the DataFusion-dialect rendering of the same node SHALL follow the preceding scenario, or `The untyped DataFusion walker declines SECOND` for `SECOND`
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The untyped DataFusion walker declines SECOND

* *GIVEN* a `function_scalar` node named `SECOND`, or a `function_scalar_extract` node whose `toExtract` field is `SECOND`
* *WHEN* `render_expression` processes the node
* *THEN* it SHALL decline the node, because only the typed pass knows the argument's precision
* *AND* `render_scan_expression` SHALL render a one-argument `SECOND(x)` or `EXTRACT(SECOND FROM x)` per `SECOND keeps the fraction of its argument's declared precision`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: SECOND keeps the fraction of its argument's declared precision

* *GIVEN* `SECOND(x)` or `EXTRACT(SECOND FROM x)` whose argument `x` is a column or a `CAST` with the declared type `TIMESTAMP(p)`
* *WHEN* `render_scan_expression` renders it
* *THEN* the fragment SHALL compute the seconds of `x` with their first p fractional digits, and SHALL carry the type `DECIMAL(2+p, p)`
* *AND* it SHALL read `date_part('second', …)` when p is 0, `date_part('microsecond', …)` when p is 1 to 6, and `date_part('nanosecond', …)` when p is 7 to 9
* *AND* it SHALL cut the digits beyond p, never round them, and SHALL scale with exact decimal arithmetic, never through `DOUBLE`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: SECOND over a DATE computes 0 without a stated type

* *GIVEN* `SECOND(x)` whose argument `x` is a column or a `CAST` with the declared type `DATE`
* *WHEN* `render_scan_expression` renders it
* *THEN* the fragment SHALL compute 0 for a non-NULL `x` and NULL for a NULL `x`
* *AND* the fragment SHALL carry no exact type, because the type Exasol gives it differs by version (Background)
* *AND* a conversion of it to text SHALL run in Exasol, which writes the value in the type it gives it (`pushdown-types/pushdown-planning-scan-text-conversion`)
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: SECOND over any other argument, or EXTRACT(SECOND) over a DATE, runs in Exasol

* *GIVEN* `SECOND(x)` or `EXTRACT(SECOND FROM x)` whose argument `x` has neither the declared type `TIMESTAMP(p)` nor `DATE`, such as `SECOND(DATE_TRUNC('minute', ts))`, `SECOND` of a `VARCHAR` column, or `SECOND(CAST(ts AS TIMESTAMP(6) WITH LOCAL TIME ZONE))`, or `EXTRACT(SECOND FROM x)` over a DATE `x`
* *WHEN* `render_scan_expression` renders it
* *THEN* the DataFusion dialect SHALL decline the node, so Exasol computes the function in the wrapper
* *AND* `EXTRACT(SECOND FROM <date>)` SHALL therefore fail with Exasol's own `Feature not supported` error, as it does over a native table
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: SECOND and EXTRACT(SECOND) return native results through the virtual schema

* *GIVEN* a virtual table whose `TIMESTAMP(6)` and `TIMESTAMP(9)` columns hold values with fractional seconds
* *WHEN* a query applies `SECOND(ts)` or `EXTRACT(SECOND FROM ts)` in the select list, a WHERE filter, a projected or unprojected GROUP BY key, or an aggregate argument
* *THEN* the query SHALL return the values, rows, and column types the same query returns over a native copy of the table
* *AND* the pushdown SQL SHALL show that the scan computes the function
* *AND* `SECOND(ts, n)` SHALL keep returning native results, computed by Exasol
<!-- /DELTA:NEW -->
