# Feature: DataFusion Scan Execution — Exasol Text Conversion Function

Registers `exa_to_varchar`, the scan session function the DataFusion dialect calls wherever Exasol
converts a value to text (`sql-comprehension/vs-expression-translator-string-conversion`). The
function picks the conversion from the argument's Arrow type and returns the text Exasol produces
for the same value under the default session settings (issues #227 and #223).

## Background

* Exasol's text forms under the default session settings, captured live on the Docker Exasol
  container with `CAST(<value> AS VARCHAR(40))`:

| Exasol type | Text rule | Captured examples |
|-------------|-----------|-------------------|
| integer (`DECIMAL(p,0)`) | plain digits | `1234567`, `-7` |
| DECIMAL | trailing scale zeros removed, no bare decimal point | `-0.50` → `-0.5`, `100.10` → `100.1`, `5.00` → `5`, `0.00001` → `0.00001` |
| DOUBLE | at most 15 significant digits, fixed notation for a decimal exponent from -4 to 14, otherwise a bare lowercase `e` exponent | `0.333333333333333`, `237.186666666667`, `100000000000000`, `1e15`, `1e20`, `0.0001`, `1e-5`, `1.234e-5`, `1.23456789012346e15`, `-0.0` → `0` |
| BOOLEAN | `TRUE` / `FALSE` | `TRUE` |
| DATE | `YYYY-MM-DD` | `2024-05-15` |
| TIMESTAMP | `YYYY-MM-DD HH24:MI:SS.FF6`, always six fraction digits, a longer fraction truncated | `2024-05-15 10:00:00.000000` for `TIMESTAMP(0)`, `...00.123000` for `TIMESTAMP(3)`, `.9999999` → `.999999` |

* Exasol's DOUBLE text is close to C's `%.15g` but not identical. Captured values that differ:
  `CAST(1e-20 AS DOUBLE)` → `9.99999999999999e-21`, `CAST(1e23 AS DOUBLE)` → `9.99999999999999e22`,
  and `CAST(1e-16 AS DOUBLE)` → `9.99999999999999e-17`, where `%.15g` rounds to a power of ten, and
  `999999999999999.5` → `1000000000000000`, where `%.15g` prints `1e+15`. A live parity corpus
  therefore gates the implementation.
* Exasol's DOUBLE admits no NaN and no infinite value. The raw scan emits a stored NaN as NULL
  (#246), and an infinite value fails the query at emit
  (`sql-comprehension/vs-expression-translator-float-div`).
* The pushdown request carries no session setting. DECIMAL and DOUBLE text follow
  `NLS_NUMERIC_CHARACTERS` (captured: `',.'` turns `0.5` into `0,5` for both), DATE text follows
  `NLS_DATE_FORMAT`, and TIMESTAMP text follows `NLS_TIMESTAMP_FORMAT`. Integer and BOOLEAN text
  depend on no session setting (captured under `',.'`: `1234567`, `TRUE`). The function reproduces
  the defaults only: the tracked exception #216.
* DataFusion parses a numeric literal with a fractional part, an exponent, or an integer beyond the
  `u64` range as `Float64`, because the scan session leaves
  `datafusion.sql_parser.parse_float_as_decimal` at `false` (`scan::session_config_for_spec`).
  Exasol types the same literal DECIMAL, so `c_acctbal * 1.5` is DECIMAL in Exasol and `Float64` in
  DataFusion. Exasol's `/` is DECIMAL for some operand pairs and DOUBLE for others (captured over
  table columns: `i / 2` is `DECIMAL(19,1)`, `d / 2` with `d` `DECIMAL(12,2)` is `DECIMAL(13,3)`,
  `i / 3` is `DOUBLE`). DataFusion computes every division as `Float64`, so a DECIMAL-typed
  division belongs to the exception below.
* Two open issues change the value before it reaches this function. #201: DataFusion `date_trunc`
  returns `Timestamp(ns)` for a DATE, so `CAST(DATE_TRUNC('month', o_orderdate) AS VARCHAR(40))`
  yields `1996-01-01 00:00:00.000000` where Exasol yields `1996-01-01`. The fix returns `Date32`,
  and this function needs no change for it. #431: `ROUND` and `TRUNC` over a DECIMAL run as
  `Float64`, so their text follows the DOUBLE rule. Both are tracked text exceptions.
* A column whose Arrow type has no Exasol mapping is declared `VARCHAR(2000000)` and emitted as
  text through the scan's JSON-fallback path (`datafusion-scan/type-mapping`): a `Decimal128` with
  `p > 36` or `s > 36`, a nested type, `Binary`, `Time32`, `Time64`, `Float16`. Exasol treats that
  column as the VARCHAR it receives.
* Iceberg and Delta spec check: neither spec defines a query text form for a value, so Exasol's
  conversion is the reference and no deviation exists to fix or track. The Iceberg table spec's
  Primitive Types table defines `boolean` as "True or false", `double` as "64-bit IEEE 754 floating
  point", `timestamp` as "Timestamp, microsecond precision, without timezone", `date` as "Calendar
  date without timezone or time", and `decimal(P,S)` as "Fixed-point decimal; precision P, scale S"
  with "Scale is fixed, precision must be 38 or less". The Delta protocol's `§ Primitive Types`
  defines `boolean` as "`true` or `false`", `double` as "8-byte double-precision floating-point
  numbers", `date` as "A calendar date, represented as a year-month-day triple without a timezone",
  and `decimal` with "The precision and scale can be up to 38".
* Deliberate Exasol target-type trade-off: Exasol's DECIMAL stops at 36 digits, so a 37- or
  38-digit decimal, which both specs allow, converts as JSON-fallback text with every scale digit,
  not as a trimmed DECIMAL.
* A NULL literal reaches the function as the Arrow `Null` type.
* `crates/vs-expression` names the function through the exported constant `EXA_TO_VARCHAR_FN` and
  does not implement it, the same split `CHECKED_FLOAT_DIV_FN` uses
  (`sql-comprehension/vs-expression-translator-float-div`). The constant is the only link between
  the renderer and this registration.
* `build_session_context` is the one production session builder, and all three run paths take
  their session from it (`datafusion-scan/scan-execution-expression-pushdown`).

## Scenarios

### Scenario: The scan session registers exa_to_varchar for every scan spec

* *GIVEN* a scan session built by `build_session_context`
* *WHEN* the session is constructed, before any spec-derived SQL is planned
* *THEN* the session SHALL register a scalar function under the exact name `crates/vs-expression` exports as `EXA_TO_VARCHAR_FN`, read from that constant rather than restated as a literal
* *AND* the registration SHALL happen for every scan spec, and a spec whose SQL does not call the function SHALL be unaffected in its generated SQL, plan shape, and result
* *AND* the function SHALL be declared `Immutable` and SHALL convert one whole Arrow array per call

### Scenario: A string argument passes through and the call simplifies away

* *GIVEN* `exa_to_varchar` over a `Utf8`, `LargeUtf8`, or `Utf8View` argument, for example `upper(exa_to_varchar("C_VARCHAR"))`
* *WHEN* DataFusion plans the query
* *THEN* the function's return field SHALL carry the argument's own data type and nullability, and its `ScalarUDFImpl::simplify` SHALL replace the call with the argument
* *AND* the optimized logical plan SHALL contain no `exa_to_varchar` call, so it equals the optimized plan of the same query rendered without the wrapper, apart from output column names
* *AND* a non-string argument SHALL NOT simplify away

### Scenario: An integer argument converts to plain digits

* *GIVEN* `exa_to_varchar` over an `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, or `UInt64` array
* *WHEN* it evaluates a batch
* *THEN* each value SHALL render as plain decimal digits with a leading `-` for a negative value (`123`, `-7`, `i64::MIN`, `i64::MAX`), and a NULL value SHALL yield NULL

### Scenario: A DECIMAL argument converts with trailing scale zeros removed

* *GIVEN* `exa_to_varchar` over a `Decimal128(p, s)` array with `p <= 36` and `s <= 36`
* *WHEN* it evaluates a batch
* *THEN* each value SHALL render with its trailing fractional zeros removed, and with no decimal point when no fractional digit remains: `-0.50` → `-0.5`, `100.10` → `100.1`, `5.00` → `5`, `0.00` → `0`, `0.05` → `0.05`
* *AND* a value below one SHALL keep its leading zero (`0.5`), a scale-0 value SHALL render as its digits, and a NULL value SHALL yield NULL

### Scenario: A DOUBLE argument converts to Exasol's DOUBLE text

* *GIVEN* `exa_to_varchar` over a `Float64` array, or a `Float32` array whose values widen to `f64` as the scan emits them
* *WHEN* it evaluates a batch
* *THEN* each finite value SHALL render per the Background's DOUBLE rule, with no `+` sign and no zero padding in the exponent: `0.5`, `-0.5`, `5`, `0.333333333333333`, `123456789012346`, `1e15`, `1e20`, `1e-5`, `1.234e-5`, `1.23456789012346e15`, and `-0.0` → `0`
* *AND* a NaN SHALL yield NULL, the value the raw scan emits for it, and an infinite value SHALL fail the query with an error naming `exa_to_varchar`
* *AND* a NULL value SHALL yield NULL

### Scenario: DOUBLE text matches native Exasol on a live parity corpus

* *GIVEN* a corpus of DOUBLE values that includes every captured example of the Background, for example `1e-20` → `9.99999999999999e-21`, `1e23` → `9.99999999999999e22`, `999999999999999.5` → `1000000000000000`, and `1.7976931348623157e308` → `1.79769313486232e308`
* *WHEN* each value reaches `exa_to_varchar` through a pushed query over the virtual schema, and reaches native Exasol evaluation in the same session on the Docker Exasol container
* *THEN* the two texts SHALL be byte-identical for every corpus value
* *AND* the test SHALL compare against the native in-session oracle, and SHALL fail, not skip, without a database

### Scenario: A BOOLEAN argument converts to TRUE or FALSE

* *GIVEN* `exa_to_varchar` over a `Boolean` array, for example the predicate of `CONCAT(c_acctbal > 0, '')`, the literal of `UPPER(TRUE)`, or the column of `CAST(c_bool AS VARCHAR(5))`
* *WHEN* it evaluates a batch
* *THEN* `true` SHALL render as `TRUE`, `false` as `FALSE`, and a NULL value SHALL yield NULL, not the text `NULL` and not `FALSE`

### Scenario: A DATE argument converts to ISO date text

* *GIVEN* `exa_to_varchar` over a `Date32` array
* *WHEN* it evaluates a batch
* *THEN* each value SHALL render as `YYYY-MM-DD`, for example `2024-05-15`, and a NULL value SHALL yield NULL

### Scenario: A TIMESTAMP argument converts to Exasol's default timestamp text

* *GIVEN* `exa_to_varchar` over a `Timestamp(unit, tz)` array of any unit
* *WHEN* it evaluates a batch
* *THEN* each value SHALL render as `YYYY-MM-DD HH24:MI:SS.FF6` with exactly six fraction digits, for example `2024-05-15 10:00:00.000000` for a whole second and `2024-05-15 10:00:00.123000` for a millisecond value
* *AND* a sub-microsecond fraction SHALL be truncated, not rounded, and a time zone SHALL be ignored, so the text shows the wall-clock value the scan emits for the column
* *AND* a NULL value SHALL yield NULL

### Scenario: A NULL-typed argument converts to NULL text

* *GIVEN* `exa_to_varchar` over an argument of the Arrow `Null` type, for example the NULL literal of `CONCAT(c_name, NULL)`
* *WHEN* DataFusion plans and evaluates the call
* *THEN* the function SHALL return `Utf8` and a NULL for every row, and SHALL NOT raise an error

### Scenario: A JSON-fallback type converts to the text the scan emits for it

* *GIVEN* `exa_to_varchar` over an argument whose Arrow type the scan emits through its JSON-fallback VARCHAR path, for example `Decimal128(38, 4)`, a nested list, `Binary`, or `Time64`
* *WHEN* it evaluates a batch
* *THEN* each value SHALL render as the same text the scan's projection emits for a bare column of that type, so `123.4500` in a `Decimal128(38, 4)` column converts to `123.4500`, not `123.45`
* *AND* the function SHALL reuse the scan's own JSON-fallback conversion, with the cast options of DataFusion's SQL `CAST`, rather than a second implementation of it

### Scenario: Session-dependent text follows the default session settings

* *GIVEN* a session whose `NLS_NUMERIC_CHARACTERS`, `NLS_DATE_FORMAT`, or `NLS_TIMESTAMP_FORMAT` differs from its default (`.,`, `YYYY-MM-DD`, `YYYY-MM-DD HH24:MI:SS.FF6`)
* *WHEN* a pushed query converts a DECIMAL, DOUBLE, DATE, or TIMESTAMP value to text
* *THEN* the function SHALL produce the default-setting text, and the result MAY diverge from native Exasol evaluation, for example `0.5` where native Exasol returns `0,5` under `NLS_NUMERIC_CHARACTERS = ',.'`, because the pushdown request carries no session setting: the tracked exception #216
* *AND* integer and BOOLEAN text SHALL match native Exasol under every session setting

### Scenario: A value DataFusion computes as Float64 converts with the DOUBLE rule

* *GIVEN* a string-converted argument that DataFusion evaluates as `Float64`, either where Exasol evaluates DECIMAL, for example `CAST(c_acctbal * 1.5 AS VARCHAR(40))`, or where Exasol evaluates DOUBLE, for example `CAST(ROUND(c_acctbal / 3, 2) AS VARCHAR(40))`
* *WHEN* DataFusion plans and evaluates the query
* *THEN* planning SHALL succeed and the value SHALL convert with the DOUBLE rule, so `711.56 * 1.5` yields `1067.34` and `ROUND(711.56 / 3, 2)` yields `237.19`, equal to native Exasol
* *AND* where Exasol evaluates DECIMAL, the text SHALL equal Exasol's DECIMAL text for a value of at most 15 significant digits whose magnitude is at least `1e-4` and below `1e15`, and MAY diverge outside that range, for example `1.00 * 0.00001` or `CAST(1.00 AS DECIMAL(12,2)) / 100000`, which native Exasol returns as `0.00001` and the DOUBLE rule renders as `1e-5`: the tracked exception #TBD, which excludes `ROUND` and `TRUNC` (#431)
