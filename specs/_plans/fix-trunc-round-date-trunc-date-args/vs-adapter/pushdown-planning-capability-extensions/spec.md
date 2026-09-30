# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with `getCapabilities`-level
advertisements for arithmetic operator scalar functions and ISO week, plus the deliberately
absent bitwise operator functions. Each advertised capability is gated on a
`crates/vs-expression` translator arm. Sibling features:
`pushdown-planning-order-by-capability` (sort keys),
`pushdown-planning-string-conversion-capability` (CAST/NEG, regexp absence, SUBSTR/LEFT),
`pushdown-planning-selectlist-expressions`, `pushdown-planning-aggregate-extensions`,
`pushdown-planning-literal-projection`,
`pushdown-planning-capability-extensions-credential-reference`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-planning-capability-extensions/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A pushed date/time TRUNC, ROUND, or DATE_TRUNC returns native Exasol's result on every pushdown path

* *GIVEN* `FN_TRUNC`, `FN_ROUND`, and `FN_DATE_TRUNC` stay advertised, and a virtual-schema table with a DATE column and a TIMESTAMP column
* *WHEN* a query applies `TRUNC`, `ROUND`, or `DATE_TRUNC` with a format or unit the DataFusion dialect renders, in a `WHERE` predicate, a select-list item (one-argument `TRUNC` included), a `GROUP BY` key, or an aggregate argument
* *THEN* each query SHALL succeed and return the rows native Exasol returns for the same data
* *AND* `EXPLAIN VIRTUAL` SHALL show the call inside the scan spec as `<trunc-fn>`, `<round-fn>`, or `<date-trunc-fn>`, not as an expression of the adapter's wrapper SQL
* *AND* these four positions SHALL cover the four shapes #201 reports failing with `F-UDF-CL-RUST-9001` (SQL state `22002`), each reproduced through the local virtual schema before this change
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A session-week or declined date/time truncation falls back to Exasol on every pushdown path

* *GIVEN* a query that applies `TRUNC` or `ROUND` with `D`, `DAY`, or `DY`, or `DATE_TRUNC` with `'week'` or another unit the DataFusion dialect declines
* *WHEN* the query places that call in a `WHERE` predicate, a select-list item, a `GROUP BY` key, or an aggregate argument
* *THEN* a declined `WHERE` predicate SHALL be applied in the adapter's own outer `WHERE` (`vs-adapter/pushdown-declined-filter-self-apply`), and a declined select-list item, group key, or aggregate argument SHALL route the request to the qualified single-table wrapper
* *AND* the query SHALL return native Exasol's rows under the session's own `NLS_FIRST_DAY_OF_WEEK`, both at `7` and at `1`
* *AND* no fallback path SHALL drop the call, because Exasol never re-applies a delegated capability
<!-- /DELTA:NEW -->
