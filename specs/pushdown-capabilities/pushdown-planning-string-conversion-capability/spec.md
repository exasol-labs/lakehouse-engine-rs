# Feature: Pushdown Planning, String Function and Type-Conversion Capability Extensions

The adapter advertises `FN_CAST`, `FN_NEG`, `FN_SUBSTR`, and `FN_LEFT`.

## Background

* A scalar-function capability is advertised only when a `crates/vs-expression` arm renders it and the DataFusion result matches Exasol.
* Exasol delegates an advertised capability fully and never re-applies it, so a capability the scan UDF cannot plan fails the query with no fallback.

## Scenarios

### Scenario: Conversion and unary-negation capabilities are advertised so CAST and unary-minus expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_CAST` and `FN_NEG`
* *AND* a CAST to a target type the scan does not compute SHALL route to the qualified single-table wrapper, which computes the CAST in Exasol SQL and returns the native result
* *AND* `FN_TO_CHAR` and `FN_TO_NUMBER` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` SHALL be advertised

### Scenario: Advertised FN_SUBSTR and FN_LEFT return rows instead of failing the query

* *GIVEN* the adapter advertises `FN_SUBSTR` and `FN_LEFT`
* *WHEN* a query selects `SUBSTR(<col>, <start>, <len>)` and `LEFT(<col>, <len>)` from the virtual schema
* *THEN* the query SHALL return correct substring values
* *AND* the query SHALL NOT fail with `F-UDF-CL-RUST-9001` / `Substring could not be planned`
* *AND* the pushdown SQL SHALL contain `substr(`, proving the scan UDF evaluated the expression
