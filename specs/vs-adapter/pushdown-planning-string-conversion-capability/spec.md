# Feature: Pushdown Planning — String Function and Type-Conversion Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with `FN_CAST`/`FN_NEG` and
`FN_SUBSTR`/`FN_LEFT` capability advertisements, plus the deliberately absent regexp scalar
functions. Split from `vs-adapter/pushdown-planning-capability-extensions`.

## Background

* A scalar-function capability is advertised only once a `crates/vs-expression` arm renders
  it and the DataFusion 54 result matches Exasol. `FN_CAST`, `FN_NEG`, `FN_SUBSTR`, and
  `FN_LEFT` meet this bar; the regexp scalar functions do not and stay unadvertised.
* Iceberg spec compliance: checked, not engaged. This feature changes only which scalar capabilities the adapter advertises, touching no manifest, snapshot, field-id, delete, or type mapping. No normative requirement applies.
* Exasol delegates an advertised capability fully and never re-applies it. A capability the
  scan UDF cannot plan fails the query with no fallback (issue #187).

## Scenarios

### Scenario: Conversion and unary-negation capabilities are advertised so CAST and unary-minus expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_CAST` and `FN_NEG`, each backed by a `crates/vs-expression` translator arm (the CAST arm over its faithful target-type set and the unary-negation arm), so no advertised capability is one the translator would decline for a shape Exasol expects it to handle
* *AND* a CAST to an unsupported target type SHALL fall back — the adapter omits the CAST and Exasol evaluates it — rather than producing an incorrect result
* *AND* `FN_TO_CHAR`, `FN_TO_NUMBER`, and `FN_DIV` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised, so advertising `FN_CAST` and `FN_NEG` introduces no additional join or cross-join capability

### Scenario: Regexp scalar function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`
* *AND* Exasol SHALL post-process regexp scalar functions because no faithful translation exists at DataFusion 54 / `regex` 1.12 (PCRE dialect mismatch, missing `regexp_substr`, argument-shape gaps — see issue #106 and `sql-comprehension/vs-expression-translator-scalar-fns`)
* *AND* the pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement SHALL remain unchanged

### Scenario: Advertised FN_SUBSTR and FN_LEFT return rows instead of failing the query

* *GIVEN* the adapter advertises `FN_SUBSTR` and `FN_LEFT`
* *WHEN* a query selects `SUBSTR(<col>, <start>, <len>)` and `LEFT(<col>, <len>)` from the virtual schema
* *THEN* the query SHALL return correct substring values
* *AND* the query SHALL NOT fail with `F-UDF-CL-RUST-9001` / `Substring could not be planned` (issue #187)
* *AND* the pushdown SQL SHALL contain `substr(`, proving the scan UDF evaluated the expression
