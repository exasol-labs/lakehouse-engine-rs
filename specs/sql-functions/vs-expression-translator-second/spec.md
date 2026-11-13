# Feature: VS Expression Translator, SECOND

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with `SECOND` and `EXTRACT(SECOND FROM x)`, which the typed scan pass computes with the fractional digits of the argument's precision. `sql-functions/vs-expression-translator-date-fns` covers the other date and time functions.

## Background

* Exasol types `SECOND(x)` and `EXTRACT(SECOND FROM x)` over a `TIMESTAMP(p)` argument as `DECIMAL(2+p, p)`: the seconds with every fractional digit the argument holds. This was measured on 2025.1.16 for p from 0 to 9, and on 8.29.13, whose only timestamp precision is 3.
* `SECOND` over a DATE is 0. Exasol 2025.1.16 types it `DECIMAL(2,0)`, and 8.29.13 types it `DECIMAL(5,3)`. `EXTRACT(SECOND FROM <date>)` fails on both versions with `Feature not supported: extract(SECOND, date) not supported` (SQL state 0A000).
* Exasol cuts the fractional digits beyond p when it stores a value at precision p, for example `CAST(ts AS TIMESTAMP(3))` of `59.999999` is `59.999`. DataFusion's `date_part('second', …)` returns whole seconds only.

## Scenarios

### Scenario: The untyped DataFusion walker declines SECOND

* *GIVEN* a `function_scalar` node named `SECOND`, or a `function_scalar_extract` node whose `toExtract` field is `SECOND`
* *WHEN* `render_expression` processes the node
* *THEN* it SHALL decline the node, because only the typed pass knows the argument's precision
* *AND* `render_scan_expression` SHALL render a one-argument `SECOND(x)` or `EXTRACT(SECOND FROM x)` per `SECOND keeps the fraction of its argument's declared precision`

### Scenario: SECOND keeps the fraction of its argument's declared precision

* *GIVEN* `SECOND(x)` or `EXTRACT(SECOND FROM x)` whose argument `x` is a column with the declared type `TIMESTAMP(p)`, or a `CAST` to `TIMESTAMP(p)` with p of 0, 3, 6, or 9, the only CAST precisions the DataFusion dialect renders
* *WHEN* `render_scan_expression` renders it
* *THEN* the fragment SHALL compute the seconds of `x` with their first p fractional digits, and SHALL carry the type `DECIMAL(2+p, p)`
* *AND* it SHALL read `date_part('second', …)` when p is 0, `date_part('microsecond', …)` when p is 1 to 6, and `date_part('nanosecond', …)` when p is 7 to 9
* *AND* it SHALL cut the digits beyond p, never round them, and SHALL scale with exact decimal arithmetic, never through `DOUBLE`

### Scenario: SECOND over a DATE computes 0 without a stated type

* *GIVEN* `SECOND(x)` whose argument `x` is a column or a `CAST` with the declared type `DATE`
* *WHEN* `render_scan_expression` renders it
* *THEN* the fragment SHALL compute 0 for a non-NULL `x` and NULL for a NULL `x`
* *AND* the fragment SHALL carry no exact type, because the type Exasol gives it differs by version (Background)
* *AND* a conversion of it to text SHALL run in Exasol, which writes the value in the type it gives it (`pushdown-types/pushdown-planning-scan-text-conversion`)

### Scenario: SECOND over any other argument, or EXTRACT(SECOND) over a DATE, runs in Exasol

* *GIVEN* `SECOND(x)` or `EXTRACT(SECOND FROM x)` whose argument `x` has neither the declared type `TIMESTAMP(p)` nor `DATE`, such as `SECOND(DATE_TRUNC('minute', ts))`, `SECOND` of a `VARCHAR` column, `SECOND(CAST(ts AS TIMESTAMP(7)))` (a CAST precision the DataFusion dialect cannot render), or `SECOND(CAST(ts AS TIMESTAMP(6) WITH LOCAL TIME ZONE))`, or `EXTRACT(SECOND FROM x)` over a DATE `x`
* *WHEN* `render_scan_expression` renders it
* *THEN* the DataFusion dialect SHALL decline the node, so Exasol computes the function in the wrapper
* *AND* `EXTRACT(SECOND FROM <date>)` SHALL therefore fail with Exasol's own `Feature not supported` error, as it does over a native table

### Scenario: SECOND and EXTRACT(SECOND) return native results through the virtual schema

* *GIVEN* a virtual table whose `TIMESTAMP(6)` and `TIMESTAMP(9)` columns hold values with fractional seconds
* *WHEN* a query applies `SECOND(ts)` or `EXTRACT(SECOND FROM ts)` in the select list, a WHERE filter, a projected or unprojected GROUP BY key, or an aggregate argument
* *THEN* the query SHALL return the values, rows, and column types the same query returns over a native copy of the table
* *AND* the pushdown SQL SHALL show that the scan computes the function
* *AND* `SECOND(ts, n)` SHALL keep returning native results, computed by Exasol
