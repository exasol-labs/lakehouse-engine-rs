# Feature: Pushdown Planning, String Function and Type-Conversion Capability Extensions

The adapter advertises `FN_CAST`, `FN_NEG`, `FN_SUBSTR`, and `FN_LEFT`.

## Background

* A scalar-function capability is advertised only when a `crates/vs-expression` arm renders it and the DataFusion result matches Exasol.
* Exasol delegates an advertised capability fully and never re-applies it, so a capability the scan UDF cannot plan fails the query with no fallback.

## Scenarios

### Scenario: Conversion and unary-negation capabilities are advertised so CAST and unary-minus expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_CAST` and `FN_NEG`, each backed by a `crates/vs-expression` translator arm (the CAST arm over its faithful target-type set and the unary-negation arm), so no advertised capability is one the translator would decline for a shape Exasol expects it to handle
* *AND* a CAST to an unsupported target type SHALL fall back, the adapter routes the request to the qualified single-table wrapper, which renders the CAST as Exasol SQL, rather than producing an incorrect result
* *AND* `FN_TO_CHAR`, `FN_TO_NUMBER`, and `FN_DIV` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `pushdown-joins/pushdown-planning-join`) SHALL be advertised, so advertising `FN_CAST` and `FN_NEG` introduces no additional join or cross-join capability

### Scenario: Advertised FN_SUBSTR and FN_LEFT return rows instead of failing the query

* *GIVEN* the adapter advertises `FN_SUBSTR` and `FN_LEFT`
* *WHEN* a query selects `SUBSTR(<col>, <start>, <len>)` and `LEFT(<col>, <len>)` from the virtual schema
* *THEN* the query SHALL return correct substring values
* *AND* the query SHALL NOT fail with `F-UDF-CL-RUST-9001` / `Substring could not be planned`
* *AND* the pushdown SQL SHALL contain `substr(`, proving the scan UDF evaluated the expression
