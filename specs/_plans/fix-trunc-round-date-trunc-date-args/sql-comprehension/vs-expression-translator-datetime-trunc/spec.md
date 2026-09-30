# Feature: VS Expression Translator — Date/Time TRUNC and ROUND

Renders Exasol `TRUNC` and `ROUND` for the node-local scan for every argument type, numeric
included. It declines every form whose result depends on state the render cannot check, so Exasol
evaluates that form itself (#201). The date/time results the called UDFs compute are specified in
`datafusion-scan/scan-execution-datetime-trunc`. `DATE_TRUNC`'s rendering is specified in
`sql-comprehension/vs-expression-translator-date-fns`.

## Background

* In the DataFusion dialect, `TRUNC` and `ROUND` render as calls to the truncation UDF
  `<trunc-fn>` and the rounding UDF `<round-fn>` that #431 introduces (its example names:
  `exa_trunc`, `exa_round`). Each UDF chooses its behavior from its first argument's Arrow type,
  which DataFusion knows at planning time. The translator therefore stays type-blind: it never
  knows whether the first argument is a number, a DATE, or a TIMESTAMP.
* Exasol's date/time format vocabulary is declared once in `crates/vs-expression`. The
  DataFusion-dialect decline and the scan UDFs' format parsing both read that one declaration.
  Exasol matches a token ASCII case-insensitively and does not trim whitespace. Measured on the
  Docker Exasol container (`exasol/docker-db:2025.1.16`): `'mm'`, `'Mm'`, and `'iw'` are accepted,
  while `' MM'`, `'MM '`, and `'XX'` fail with `data exception - unsupported format in date trunc`
  (SQL state `22769`).

| Tokens | Unit |
|---|---|
| `CC`, `SCC` | century |
| `YYYY`, `SYYYY`, `YEAR`, `SYEAR`, `YYY`, `YY`, `Y` | year |
| `IYYY`, `IYY`, `IY`, `I` | ISO year |
| `Q` | quarter |
| `MONTH`, `MON`, `MM`, `RM` | month |
| `WW` | week of year |
| `IW` | ISO week |
| `W` | week of month |
| `DDD`, `DD`, `J` | day |
| `HH`, `HH12`, `HH24` | hour |
| `MI` | minute |
| `SS` | second |
| `D`, `DAY`, `DY` | first day of the session week |

* `D`, `DAY`, and `DY` depend on the session parameter `NLS_FIRST_DAY_OF_WEEK`, which no pushdown
  request carries. Measured: `TRUNC(DATE '2024-05-15', 'D')` is `2024-05-12` at
  `NLS_FIRST_DAY_OF_WEEK = 7` and `2024-05-13` at `1`. `IW` and `WW` do not change.
* Exasol constant-folds the second argument before it builds the pushdown request. Measured with
  `EXPLAIN VIRTUAL`: `'M' || 'M'` arrives as `'MM'`, `1+1` as `2`, and `-1` as the numeric literal
  `-1`. A second argument that arrives as a non-literal node is therefore row-dependent.
* Exasol casts a string second argument of numeric `TRUNC` to a number: `TRUNC(1.2345, '2')` is
  `1.23`.

## Scenarios

### Scenario: Date/time TRUNC and ROUND render as the truncation and rounding UDFs in the DataFusion dialect

* *GIVEN* a `function_scalar` node named `TRUNC` or `ROUND` with one argument, with a numeric literal second argument, or with a second argument that is a `literal_string` naming a vocabulary token other than `D`, `DAY`, or `DY`, in any letter case
* *WHEN* `render_expression` processes the node
* *THEN* `TRUNC` SHALL render as `<trunc-fn>(<arg_sql>)` or `<trunc-fn>(<arg_sql>, <format_sql>)`, and `ROUND` as the same shape over `<round-fn>`, with the second argument's literal forwarded unchanged
* *AND* the rendering MUST NOT emit DataFusion's built-in `trunc(` or `round(`, which reject a `Date32` or `Timestamp` argument at planning time (`No function matches the given name and argument types 'trunc(Date32, Utf8)'`, reproduced through the local virtual schema)
* *AND* every token the translator forwards SHALL be one the scan UDFs parse, because both read the one vocabulary declaration

### Scenario: A session-week, unknown, or non-literal format declines in the DataFusion dialect and renders verbatim in the Exasol dialect

* *GIVEN* a `function_scalar` node named `TRUNC` or `ROUND` whose second argument is a `literal_string` naming `D`, `DAY`, or `DY`, a `literal_string` outside the vocabulary (for example `'XX'`, `' MM'`, or `'2'`), or a node that is neither a string literal nor a numeric literal (a column, an expression, or `literal_null`)
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error in raising mode and `None` in the safe variants
* *AND* the Exasol dialect SHALL render `TRUNC(...)` or `ROUND(...)` verbatim, as `sql-comprehension/vs-expression-translator-scalar-fns` specifies
* *AND* the non-literal decline SHALL cover the numeric form as well, because the translator cannot tell a digits column from a format column: `TRUNC(x, n_col)` declines and Exasol evaluates it
