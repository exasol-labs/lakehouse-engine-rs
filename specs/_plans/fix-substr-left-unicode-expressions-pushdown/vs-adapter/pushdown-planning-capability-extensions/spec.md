# Feature: Pushdown Planning — Capability Extensions

Extends `vs-adapter/pushdown-planning` with capability advertisements for scalar and
type-conversion functions.

## Background

<!-- DELTA:NEW -->
* Exasol delegates an advertised capability fully and never re-applies it. A capability the scan UDF cannot plan fails the query with no fallback (issue #187).
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Advertised FN_SUBSTR and FN_LEFT return rows instead of failing the query

* *GIVEN* the adapter advertises `FN_SUBSTR` and `FN_LEFT`
* *WHEN* a query selects `SUBSTR(<col>, <start>, <len>)` and `LEFT(<col>, <len>)` from the virtual schema
* *THEN* the query SHALL return correct substring values
* *AND* the query SHALL NOT fail with `F-UDF-CL-RUST-9001` / `Substring could not be planned` (issue #187)
* *AND* the pushdown SQL SHALL contain `substr(`, proving the scan UDF evaluated the expression
<!-- /DELTA:NEW -->
