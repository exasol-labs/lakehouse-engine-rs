# Feature: VS Expression Translator — TRUNC, ROUND, and DATE_TRUNC

The VS pushes Exasol's `TRUNC` and `ROUND` into the node-local DataFusion scan and returns the
values native Exasol returns for DECIMAL, integer, DOUBLE, DATE, and TIMESTAMP arguments. The
values `DATE_TRUNC` returns are specified here too, and its rendering in
`sql-functions/vs-expression-translator-date-fns`. A form the scan cannot reproduce falls back
to native Exasol evaluation instead of returning a different value (#431, #201).

## Background

* The adapter advertises `FN_TRUNC`, `FN_ROUND`, and `FN_DATE_TRUNC`. Exasol delegates an
  advertised capability fully and never re-checks the value, so the scan's value is the value
  the user sees.
* A pushdown position is any place a pushed expression can sit: the select list, a `WHERE`
  predicate, a `GROUP BY` key, an aggregate argument, or a broadcast join's projection, leg
  filter, or join condition.
* The translator does not know the Arrow type of a column. The DataFusion dialect therefore
  renders `TRUNC` and `ROUND` as calls to `exa_trunc` and `exa_round`, two functions the scan
  session registers (`datafusion-scan/scan-execution-expression-pushdown`). Each function selects
  its computation from its argument types when DataFusion plans the query. `crates/vs-expression`
  owns the function names and the accepted format vocabulary.
* DataFusion's own functions differ from Exasol. Measured on DataFusion 54.1: `trunc` converts a
  DECIMAL to DOUBLE. Neither `trunc` nor `round` accepts an Exasol format argument on a DATE or TIMESTAMP. DataFusion 54.1 `date_trunc`
  has no `decade`, `century`, or `millennium` unit, spells its sub-second units in the singular
  where Exasol accepts only the plural, and converts a DATE to a nanosecond TIMESTAMP. The scan
  session therefore also registers `exa_date_trunc` for `DATE_TRUNC`.
* The emit boundary casts every scan value to the Exasol type the adapter declared for its
  position, and the cast is strict: a value that does not fit fails the query and never becomes
  NULL.
* Exasol's DECIMAL holds at most 36 digits. Native `ROUND` checks its result against its declared
  result type, so a rounded value that needs more than 36 digits fails with SQL state `22003`.
  Native `TRUNC` never grows a value.
* Exasol constant-folds a `TRUNC` or `ROUND` whose arguments are all literals before it builds the
  pushdown request, so only an expression over a column reaches the scan.
* Normative source types (Iceberg spec, "Primitive Types"; Delta protocol, "Primitive Types"):
  * Iceberg `decimal(P,S)`: "Fixed-point decimal; precision P, scale S", with "Scale is fixed,
    precision must be 38 or less". Delta `decimal`: "The precision and scale can be up to 38."
    `scan-types/type-mapping` exposes a decimal wider than 36 digits as `VARCHAR`, a named
    trade-off forced by Exasol's 36-digit limit.
  * Iceberg `double`: "64-bit IEEE 754 floating point". Iceberg `float`: "32-bit IEEE 754 floating
    point". Delta: "8-byte double-precision floating-point numbers" and "4-byte single-precision
    floating-point numbers".
  * Iceberg `date`: "Calendar date without timezone or time", stored as "days from 1970-01-01".
    Delta `date`: "A calendar date, represented as a year-month-day triple without a timezone."
    Neither source names a calendar for dates before 1582.
  * Iceberg `timestamp`: "Timestamp, microsecond precision, without timezone", which "represent a
    date and time of day regardless of zone". Iceberg `timestamptz`: "values are stored as UTC".
    Iceberg v3 adds nanosecond variants of both. Delta `timestamp` is "Microsecond precision
    timestamp elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC".
    `scan-types/type-mapping` emits a zoned timestamp as plain `TIMESTAMP` on its UTC wall clock,
    a named trade-off forced by Exasol's UDF `EMITS` type limit.
* The scan reads a DATE as a day count and emits the year-month-day label of that day, and Exasol
  stores that label. Exasol's calendar is Julian before 1582-10-15 and Gregorian from that day on
  (measured: `DATE '1582-10-04'` plus one day is `1582-10-15`, and `DATE '0001-01-01'` is a
  Saturday). The scan's DATE type is proleptic Gregorian, which has no February 29 in the years
  100, 200, 300, 500, and every later century year up to 1500 that is not divisible by 400.
* Exasol accepts these format spellings, case-insensitively and with no surrounding spaces. Each
  row is one format group, and the first spelling is the group's canonical token. `TRUNC` returns
  the period start in the fourth column. `ROUND` returns the next period start when the condition
  in the fifth column holds, and the `TRUNC` result otherwise. For the three week groups, the next
  period start is the `TRUNC` result plus 7 days:

  | Group | Spellings | Applies to | `TRUNC` returns | `ROUND` moves to the next period start when |
  |-------|-----------|------------|-----------------|---------------------------------------------|
  | Century | `CC`, `SCC` | DATE, TIMESTAMP | January 1 of the century's first year (`2001`, `2101`) | the year of the century is 51 or more |
  | Year | `YYYY`, `SYYY`, `YEAR`, `SYEAR`, `YYY`, `YY`, `Y` | DATE, TIMESTAMP | January 1 | the month is July or later |
  | ISO year | `IYYY`, `IYY`, `IY`, `I` | DATE, TIMESTAMP | the Monday that starts the ISO year containing the value | not applicable: `ROUND` returns the start of the ISO year numbered like the value's calendar year when the month is before July, and of the next ISO year otherwise |
  | Quarter | `Q` | DATE, TIMESTAMP | the first day of the quarter | the value is in the quarter's second month on day 16 or later, or in its third month |
  | Month | `MM`, `MONTH`, `MON`, `RM` | DATE, TIMESTAMP | the first day of the month | the day of the month is 16 or later |
  | Week of year | `WW` | DATE, TIMESTAMP | the latest day on or before the value whose day of the year is 1, 8, 15, and so on | the value is 3 days 12 hours or more after the `TRUNC` result |
  | ISO week | `IW` | DATE, TIMESTAMP | the Monday on or before the value | the value is 3 days 12 hours or more after the `TRUNC` result |
  | Week of month | `W` | DATE, TIMESTAMP | the latest day on or before the value whose day of the month is 1, 8, 15, 22, or 29 | the value is 3 days 12 hours or more after the `TRUNC` result |
  | Day (the default when no format is given) | `DD`, `DDD`, `J` | DATE, TIMESTAMP | midnight of the day | the hour is 12 or later |
  | Hour | `HH`, `HH24`, `HH12` | TIMESTAMP | the start of the hour | the minute is 30 or later |
  | Minute | `MI` | TIMESTAMP | the start of the minute | the second is 30 or later |
  | Second | `SS` | TIMESTAMP | the start of the second | the fraction of the second is 0.5 or more |

* The last `WW` period of a year and the last `W` period of a month hold at most 3 days, so `ROUND`
  to `WW` or `W` never moves to the next year's or the next month's first day.
* The formats `D`, `DAY`, and `DY` start a week on the session's `NLS_FIRST_DAY_OF_WEEK`. The scan
  never receives that session parameter. `DATE_TRUNC('week', ...)` depends on the same parameter.
* A failure raised inside the scan UDF reaches the user as `F-UDF-CL-RUST-9001` with SQL state
  `22002`, where native Exasol reports `22003` (numeric value out of range), `22008` (datetime
  field overflow or underflow), `22769` (unsupported format in date trunc), or `22770` (unsupported
  format in date round). The message text carries Exasol's own phrase. This is a named trade-off,
  recorded in `docs/semantic-differences.md`.

## Scenarios

### Scenario: TRUNC and ROUND render as calls to the scan's Exasol functions in the DataFusion dialect

* *GIVEN* a VS expression node of type `function_scalar` named `TRUNC` or `ROUND` with one argument, or with two arguments whose second argument is a numeric literal (`literal_exactnumeric` or `literal_double`), the `NEG` of a numeric literal, a `CAST` to a numeric type, or a `literal_string` whose value is a spelling from the format vocabulary
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render `TRUNC` as `exa_trunc(<arg1_sql>[, <arg2_sql>])` and `ROUND` as `exa_round(<arg1_sql>[, <arg2_sql>])`, reading both names from the constants `crates/vs-expression` exports
* *AND* a format literal SHALL render as its group's canonical token in single quotes, so `TRUNC("DT", 'month')` and `TRUNC("DT", 'RM')` both render `exa_trunc("DT", 'MM')`
* *AND* a numeric second argument SHALL render recursively and unchanged, including the `CAST(<column> AS DECIMAL(9,0))` that Exasol wraps around a column used as the scale
* *AND* the translator MUST NOT render DataFusion's own `trunc` or `round` for either name
* *AND* `render_expression_exasol` SHALL render both names verbatim, byte-identical to the rendering before this feature
* *AND* `LOG` SHALL keep its current rendering in both dialects

### Scenario: A TRUNC, ROUND, or DATE_TRUNC form the scan cannot reproduce falls back to native Exasol on every pushdown path

* *GIVEN* a `TRUNC` or `ROUND` node whose second argument is `D`, `DAY`, or `DY` in any letter case, a string literal outside the format vocabulary (including one with surrounding spaces), a column, a `literal_null`, or any other expression that is not a numeric literal, the `NEG` of one, or a `CAST` to a numeric type, or a node with more than two arguments
* *AND* alternatively a `DATE_TRUNC` node whose unit is `week`, a unit spelling outside the accepted list, or a non-literal unit
* *WHEN* `render_expression_safe` or `render_df_filter_safe` processes the node
* *THEN* the translator SHALL return `None`, and `render_expression` SHALL return an error naming the function
* *AND* the adapter SHALL let native Exasol evaluate the node: a `WHERE` conjunct is applied by the adapter-written wrapper, a select-list item routes the request to the qualified single-table wrapper, a `GROUP BY` key routes to the grouped wrapper fallback, an aggregate argument falls back to row scanning, and a broadcast join's leg filter or join condition routes the join to the unaccelerated N-scan wrapper
* *AND* in each of these positions the query SHALL return the rows native Exasol returns under `NLS_FIRST_DAY_OF_WEEK = 7`
* *AND* the pushdown SQL MUST NOT contain an `exa_` call for the declined node
* *AND* `render_expression_exasol` SHALL still render the declined node verbatim, so the wrapper SQL evaluates it

### Scenario: TRUNC and ROUND on DECIMAL and integer arguments compute exactly

* *GIVEN* a column of type `DECIMAL(p,s)` with `p` at most 36, or an integer column, which the scan treats as `DECIMAL(P,0)` with `P` from `scan-types/type-mapping`
* *AND* a scale argument `n` that is absent (meaning 0), a numeric literal, or a per-row numeric expression, of integer, DOUBLE, or DECIMAL type
* *WHEN* a query pushes `TRUNC(x[, n])` or `ROUND(x[, n])` to the scan in any pushdown position
* *THEN* `TRUNC` SHALL drop every digit after position `n` toward zero, and `ROUND` SHALL round at position `n` half away from zero, both in exact decimal arithmetic and never through a DOUBLE
* *AND* a non-integer `n` SHALL first be rounded half away from zero, so `TRUNC(12.345, 1.7)` returns `12.34` and `ROUND(12.345, 1.5)` returns `12.35`
* *AND* an `n` at or above the scale SHALL return the value unchanged, and an `n` below the number of integer digits SHALL return 0, so `TRUNC(12.345, -40)` returns 0
* *AND* a NULL value or a NULL `n` SHALL yield NULL
* *AND* `ROUND(99.99, 1)` on `DECIMAL(4,2)` SHALL return `100.0`, `ROUND(99.99)` SHALL return `100`, and `TRUNC(99.99, 1)` SHALL return `99.9`
* *AND* every value the query returns MUST equal the value native Exasol returns for the same stored value, in the declared type native Exasol declares

### Scenario: ROUND on DECIMAL fails where native Exasol overflows

* *GIVEN* a `DECIMAL(p,s)` column and a `ROUND(x, n)` whose rounded value needs more than 36 digits at the result scale `r`, where `r` is the clamp of `n` to the range 0 to `s` for a literal `n`, and `r` is `s` for a per-row `n`
* *WHEN* a query pushes the `ROUND` to the scan in any pushdown position
* *THEN* the query SHALL fail with a message that contains `numeric value out of range`
* *AND* `ROUND` of the 36-digit value `999999999999999999999999999999999999` as `DECIMAL(36,0)` with `n = -1` MUST fail
* *AND* `ROUND(9999999999999999999999999999999999.99, 1)` on `DECIMAL(36,2)` with a literal `n` SHALL return `10000000000000000000000000000000000.0`
* *AND* the same value with a per-row `n` that equals 1 MUST fail, because native Exasol then judges the result at scale 2
* *AND* a value whose rounded result fits in 36 digits MUST NOT fail
* *AND* `TRUNC` MUST NOT fail for any DECIMAL value or any `n`
* *AND* the failure SHALL reach the user with SQL state `22002` where native Exasol reports `22003`

### Scenario: TRUNC and ROUND on DOUBLE reproduce native Exasol's floating-point steps

* *GIVEN* a DOUBLE or FLOAT column, or a DECIMAL column wider than 36 digits, which Exasol sees as `VARCHAR` and converts to DOUBLE inside `TRUNC` and `ROUND` (measured: `TRUNC(<varchar>, 1)` and `ROUND(<varchar>, 1)` declare `DOUBLE`)
* *WHEN* a query pushes `TRUNC(x[, n])` or `ROUND(x[, n])` to the scan in any pushdown position
* *THEN* the scan SHALL compute `p = 10^|n|` as the correctly rounded DOUBLE, then `y = x * p` for `n >= 0` or `y = x / p` for `n < 0`, apply the step to `y`, and undo the scaling with the inverse operation
* *AND* the `TRUNC` step SHALL drop the fraction of `y` toward zero, so `TRUNC(2.675E0, 2)` returns `2.67`, `TRUNC(1.005E0, 2)` returns `1.0`, and `TRUNC(0.285E0, 2)` returns `0.28`
* *AND* the `ROUND` step SHALL return `sign(y) * floor(|y| + 0.5 + c)` in DOUBLE arithmetic, with a constant `c` of at least `5.0E-14` and below `5.1E-14`, so a value just below the half rounds away from zero as native Exasol rounds it: `ROUND(0.49999999999999994E0)` returns 1, `ROUND(-0.49999999999999994E0)` returns -1, `ROUND(2.49999999999999E0)` returns 3, `ROUND(2.4999999999999E0)` returns 2, `ROUND(1234567.4999999998E0)` returns 1234567, `ROUND(1000.4999999999999E0)` returns 1000, `ROUND(-2.5E0)` returns -3, `ROUND(2.675E0, 2)` returns `2.68`, and `ROUND(1.005E0, 2)` returns `1.01`
* *AND* a non-finite `p`, `y`, or result SHALL fail the query with a message that contains `numeric value out of range`: `TRUNC(2.675E0, 308)`, `TRUNC(2.675E0, 400)`, `TRUNC(2.675E0, -320)`, and `TRUNC(1.5E300, 10)` MUST fail, while `TRUNC(2.675E0, -308)` SHALL return 0 and `ROUND(1.5E300, -300)` SHALL return `2E300`
* *AND* the scale argument SHALL follow the rules of the DECIMAL scenario: a non-integer `n` rounds half away from zero, and a NULL yields NULL
* *AND* a FLOAT value SHALL be widened to its exact DOUBLE value before the step
* *AND* every value the query returns MUST equal the value native Exasol returns for the same stored value

### Scenario: TRUNC and ROUND on DATE and TIMESTAMP follow native Exasol's period rules for every supported format

* *GIVEN* a DATE column or a TIMESTAMP column of any precision, with or without a zone, and a format that is absent (meaning the day) or a spelling from the format vocabulary
* *WHEN* a query pushes `TRUNC(x[, fmt])` or `ROUND(x[, fmt])` to the scan in any pushdown position
* *THEN* `TRUNC` SHALL return the period start the format table names, and `ROUND` SHALL return the next period start when the table's `ROUND` condition holds and the `TRUNC` result otherwise, except for the ISO year, whose `ROUND` result the table states directly
* *AND* the scan SHALL return these values, as native Exasol does:

  | Value | Function and format | Result |
  |-------|---------------------|--------|
  | `DATE '2024-05-15'` | `TRUNC` to `CC`, `YYYY`, `Q`, `MM` | `2001-01-01`, `2024-01-01`, `2024-04-01`, `2024-05-01` |
  | `DATE '2024-05-15'` | `TRUNC` to `WW`, `IW`, `W`, `DD` | `2024-05-13`, `2024-05-13`, `2024-05-15`, `2024-05-15` |
  | `TIMESTAMP '2024-05-15 10:37:42.500'` | `TRUNC` to `HH`, `MI`, `SS` | `10:00:00`, `10:37:00`, `10:37:42` |
  | `TIMESTAMP '2024-05-15 10:37:42.500'` | `ROUND` to `HH`, `MI`, `SS` | `11:00:00`, `10:38:00`, `10:37:43` |
  | `DATE '2024-05-15'`, `DATE '2024-05-16'` | `ROUND` to `Q` | `2024-04-01`, `2024-07-01` |
  | `DATE '2024-08-15'`, `DATE '2024-08-16'` | `ROUND` to `Q` | `2024-07-01`, `2024-10-01` |
  | `DATE '2024-06-30'`, `DATE '2024-07-01'` | `ROUND` to `YYYY` | `2024-01-01`, `2025-01-01` |
  | `DATE '2024-06-30'`, `DATE '2024-07-01'` | `ROUND` to `IYYY` | `2024-01-01`, `2024-12-30` |
  | `DATE '2050-12-31'`, `DATE '2051-01-01'` | `ROUND` to `CC` | `2001-01-01`, `2101-01-01` |
  | `DATE '2024-01-15'`, `DATE '2024-01-16'` | `ROUND` to `MM` | `2024-01-01`, `2024-02-01` |
  | `DATE '2020-12-31'` | `ROUND` to `WW` | `2020-12-30` |
  | `DATE '2024-05-31'`, `DATE '2024-06-30'` | `ROUND` to `W` | `2024-05-29`, `2024-06-29` |
  | `TIMESTAMP '2024-05-16 11:59:59.999'`, `TIMESTAMP '2024-05-16 12:00:00.000'` (a Thursday) | `ROUND` to `IW` | `2024-05-13`, `2024-05-20` |
  | `TIMESTAMP '2024-02-29 23:59:59.999'` | `ROUND` to `DD`, `HH`, `MI`, `SS` | `2024-03-01 00:00:00` for each |

* *AND* a NULL value SHALL yield NULL
* *AND* every value the query returns MUST equal the value native Exasol returns for the same stored value

### Scenario: TRUNC and ROUND on DATE and TIMESTAMP keep the argument's type and reject a time format on a DATE

* *GIVEN* a DATE column or a TIMESTAMP column of any precision, with or without a zone
* *WHEN* a query pushes `TRUNC` or `ROUND` with a format to the scan in any pushdown position
* *THEN* the result SHALL keep the argument's type: a DATE yields a DATE, and a TIMESTAMP yields a TIMESTAMP of the same precision and zone
* *AND* a zoned TIMESTAMP SHALL be computed on its UTC wall clock, the value `scan-types/type-mapping` emits
* *AND* a DATE MUST NOT be converted to a nanosecond TIMESTAMP, whose range (years 1677 to 2262) excludes dates the DATE type holds
* *AND* an hour, minute, or second format on a DATE SHALL fail the query with a message that contains `unsupported format`, as native Exasol does

### Scenario: DATE_TRUNC on DATE and TIMESTAMP matches native Exasol for every supported unit

* *GIVEN* a DATE or TIMESTAMP column and a `DATE_TRUNC` unit from the accepted list: `microseconds`, `milliseconds`, `second`, `minute`, `hour`, `day`, `month`, `quarter`, `year`, `decade`, `century`, or `millennium`
* *WHEN* a query pushes `DATE_TRUNC(<unit>, x)` to the scan in any pushdown position
* *THEN* the scan SHALL return the start of the unit's period that contains the value, in the argument's type
* *AND* a unit smaller than a day SHALL return a DATE argument unchanged, because native `DATE_TRUNC` accepts every unit on a DATE while native `TRUNC` rejects a time format on one
* *AND* `decade` SHALL return `2020-01-01` for `2024-05-15`, `century` and `millennium` SHALL return `2001-01-01` for it, `century` SHALL return `1901-01-01` for `2000-12-31`, and `millennium` SHALL return `9001-01-01` for `9999-12-31`
* *AND* `decade`, `century`, and `millennium` SHALL return `0001-01-01` for `0005-05-05` instead of a year before 1, and `decade` SHALL return `0010-01-01` for a date in the year 0010
* *AND* `DATE_TRUNC` MUST NOT fail for any DATE or TIMESTAMP value
* *AND* every value the query returns MUST equal the value native Exasol returns for the same stored value

### Scenario: Week and ISO-year formats follow Exasol's calendar before 1582-10-15

* *GIVEN* a DATE or TIMESTAMP value whose label lies before `1582-10-15`, and the format `WW`, `IW`, `W`, or the ISO year
* *WHEN* a query pushes `TRUNC` or `ROUND` with that format to the scan
* *THEN* the scan SHALL count days and weekdays in Exasol's calendar and SHALL return the label native Exasol returns
* *AND* `TRUNC` SHALL return `0100-03-04` for `WW`, `0100-03-02` for `IW`, and `0100-03-01` for `W` from `0100-03-05`
* *AND* from `1582-10-15`, `TRUNC` SHALL return `1582-10-01` for `WW`, `IW`, and `W`, and `ROUND` to `IW` SHALL return `1582-10-18`
* *AND* from `1600-03-05`, `TRUNC` SHALL return `1600-03-04` for `WW`, `1600-02-28` for `IW`, and `1600-03-01` for `W`, and `ROUND` to `IW` SHALL return `1600-03-06`
* *AND* from `0001-01-08`, `TRUNC` SHALL return `0001-01-08` for `WW`, `0001-01-03` for `IW`, and `0001-01-08` for `W`, and `ROUND` to `IW` SHALL return `0001-01-10`
* *AND* the century, year, quarter, month, day, and time formats, and every `DATE_TRUNC` unit, SHALL depend on the label alone and SHALL NOT change at 1582-10-15
* *AND* a result label that exists in Exasol's calendar but not in the proleptic Gregorian calendar, such as `0100-02-29`, SHALL fail the query rather than return a different date, a scoped exception recorded in `docs/semantic-differences.md`

### Scenario: Date and timestamp range edges fail where native Exasol fails

* *GIVEN* a DATE or TIMESTAMP column that holds `0001-01-01`, `9999-12-31`, or `9999-12-31 23:59:59.999`
* *WHEN* a query pushes `TRUNC` or `ROUND` over that value to the scan
* *THEN* `TRUNC(DATE '9999-12-31')` SHALL return `9901-01-01` for the century, `9999-01-01` for the year, `9999-01-04` for the ISO year, `9999-10-01` for the quarter, `9999-12-01` for the month, `9999-12-31` for `WW`, `9999-12-27` for `IW`, `9999-12-29` for `W`, and `9999-12-31` for the day
* *AND* `ROUND(DATE '9999-12-31')` MUST fail with a message that contains `datetime field overflow` for the century, year, ISO year, quarter, month, and `IW`, and SHALL return `9999-12-31` for `WW` and the day and `9999-12-29` for `W`
* *AND* `ROUND(TIMESTAMP '9999-12-31 23:59:59.999')` MUST fail with the same message for the day, hour, minute, and second
* *AND* `TRUNC(DATE '0001-01-01')` MUST fail for the ISO year with a message that contains `invalid date value` and for `IW` with a message that contains `datetime field underflow`, and SHALL return `0001-01-01` for the century, year, quarter, month, `WW`, `W`, and the day
* *AND* `ROUND(DATE '0001-01-01')` SHALL return `0001-01-03` for the ISO year and `IW`
* *AND* every outcome MUST match native Exasol: the query fails where native Exasol fails, with SQL state `22002` in place of native Exasol's state, and returns native Exasol's value where it succeeds
