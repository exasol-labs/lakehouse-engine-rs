# Feature: DataFusion Scan Execution — Exasol Text Conversion Function

Registers `exa_to_varchar`, the scan session function the DataFusion dialect calls wherever Exasol
converts a value to text (`sql-comprehension/vs-expression-translator-string-conversion`). The
function picks the conversion from the argument's Arrow type, so a string function or a string CAST
over an integer, a decimal, or a date returns Exasol's text (issue #227).

## Background

* Exasol's text form, checked live on Exasol (issue #227): an integer renders as plain digits
  (`123`); a DECIMAL drops trailing scale zeros (`-0.50` → `-0.5`, `100.10` → `100.1`, `5.00` →
  `5`); a DATE renders as `YYYY-MM-DD` under the default `NLS_DATE_FORMAT`.
* Exasol's text for DOUBLE (`1E20`), BOOLEAN (`TRUE`), and TIMESTAMP
  (`2024-05-15 10:00:00.000000`, set by the session's `NLS_TIMESTAMP_FORMAT`) differs from
  DataFusion's and depends on session state the scan does not receive. The function does not
  reproduce these three.
* The adapter declines a bare DOUBLE, BOOLEAN, or TIMESTAMP column argument before it reaches the
  scan (`vs-adapter/pushdown-planning-string-fn-type-coercion`). A computed argument of those types
  has no adapter-visible type, so it reaches this function. That residue is the tracked exception
  #223.
* A column whose Arrow type has no Exasol mapping is declared `VARCHAR(2000000)` and emitted as
  text through the scan's JSON-fallback path (`datafusion-scan/type-mapping`): a `Decimal128` with
  `p > 36` or `s > 36`, a nested type, `Binary`, `Time32`, `Time64`. Exasol treats that column as
  the VARCHAR it receives.
* Deliberate Exasol target-type trade-off: the Iceberg table spec allows `decimal(P,S)` with
  "precision must be 38 or less", and the Delta protocol's `§ Primitive Types` allows a `decimal`
  whose "precision and scale can be up to 38". Exasol's DECIMAL stops at 36, so a 37- or 38-digit
  decimal converts as JSON-fallback text with every scale digit, not as a trimmed DECIMAL.
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

### Scenario: String and integer arguments convert to Exasol text

* *GIVEN* `exa_to_varchar` over a `Utf8`, `LargeUtf8`, or `Utf8View` array, or over an `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, or `UInt64` array
* *WHEN* it evaluates a batch
* *THEN* a string value SHALL be returned unchanged
* *AND* an integer value SHALL render as plain decimal digits, with a leading `-` for a negative value (`123`, `-7`)
* *AND* a NULL value SHALL yield NULL

### Scenario: A DECIMAL argument converts with trailing scale zeros removed

* *GIVEN* `exa_to_varchar` over a `Decimal128(p, s)` array with `p <= 36` and `s <= 36`
* *WHEN* it evaluates a batch
* *THEN* each value SHALL render with its trailing fractional zeros removed, and with no decimal point when no fractional digit remains: `-0.50` → `-0.5`, `100.10` → `100.1`, `5.00` → `5`, `0.00` → `0`, `0.05` → `0.05`
* *AND* a value below one SHALL keep its leading zero (`0.5`), and a scale-0 value SHALL render as its digits
* *AND* a NULL value SHALL yield NULL

### Scenario: A DATE argument converts to ISO date text

* *GIVEN* `exa_to_varchar` over a `Date32` array
* *WHEN* it evaluates a batch
* *THEN* each value SHALL render as `YYYY-MM-DD`, for example `2024-05-15`, and a NULL value SHALL yield NULL
* *AND* under a session whose `NLS_DATE_FORMAT` differs from `YYYY-MM-DD` the result MAY diverge from native Exasol evaluation, because the pushdown request carries no session format — the tracked exception #216

### Scenario: A NULL-typed argument converts to NULL text

* *GIVEN* `exa_to_varchar` over an argument of the Arrow `Null` type, for example the NULL literal of `CONCAT(c_name, NULL)`
* *WHEN* DataFusion plans and evaluates the call
* *THEN* the function SHALL return `Utf8` and a NULL for every row, and SHALL NOT raise an error

### Scenario: A JSON-fallback type converts to the text the scan emits for it

* *GIVEN* `exa_to_varchar` over an argument whose Arrow type the scan emits through its JSON-fallback VARCHAR path, for example `Decimal128(38, 4)`, a nested list, `Binary`, or `Time64`
* *WHEN* it evaluates a batch
* *THEN* each value SHALL render as the same text the scan's projection emits for a bare column of that type, so `123.4500` in a `Decimal128(38, 4)` column converts to `123.4500`, not `123.45`
* *AND* the function SHALL reuse the scan's own JSON-fallback conversion rather than a second implementation of it

### Scenario: A DOUBLE, BOOLEAN, or TIMESTAMP argument fails at planning time

* *GIVEN* a query calling `exa_to_varchar` over a `Float16`, `Float32`, `Float64`, `Boolean`, or `Timestamp(_, _)` argument, reachable through a computed argument such as `UPPER(c_double * 2)`
* *WHEN* DataFusion plans the query
* *THEN* planning SHALL fail, before any row is read, with an error that names `exa_to_varchar` and the rejected Arrow type
* *AND* the function MUST NOT return DataFusion's own text for such a value, because that text differs from Exasol's and would be a silently wrong result
* *AND* this failure SHALL be recorded as the tracked exception #223, not a silent gap
