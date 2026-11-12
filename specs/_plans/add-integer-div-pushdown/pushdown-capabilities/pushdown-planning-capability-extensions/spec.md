# Feature: Pushdown Planning, Capability Extensions

The adapter advertises the arithmetic operator scalar functions and ISO week in `getCapabilities`, and leaves the bitwise operator functions and the now-family date/time functions unadvertised. It also routes a select-list item the scan cannot emit to the qualified single-table wrapper.

<!-- DELTA:CHANGED -->
## Background

* Exasol delegates an advertised capability fully and never re-checks it. It evaluates an unadvertised capability itself over the rows the scan returns.
* The scan UDF receives no session time zone, clock, or statement anchor.
* A scan UDF cannot emit a `TIMESTAMP WITH LOCAL TIME ZONE`, `INTERVAL`, `GEOMETRY`, or `HASHTYPE` column.
* Exasol bit functions operate on unsigned 64-bit integers, DataFusion's integer operators are signed, and Iceberg has no unsigned integer type.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: Arithmetic operator scalar-function capabilities are advertised so arithmetic expression trees are pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise the arithmetic binary-operator scalar-function capabilities for `+`, `-`, `*`, and `/` (the Exasol capability names `FN_ADD`, `FN_SUB`, `FN_MULT`, and `FN_FLOAT_DIV`), verified against the Exasol Virtual Schema capability vocabulary
* *AND* each advertised arithmetic operator SHALL be backed by a `crates/vs-expression` translator arm that renders it (so an advertised operator is never one the translator would decline), keeping the capability set coherent with the translator the same way GROUP BY tuple advertisement is gated on its backing path
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `pushdown-joins/pushdown-planning-join`) SHALL be advertised, advertising arithmetic operators MUST NOT introduce any additional join or cross-join capability
