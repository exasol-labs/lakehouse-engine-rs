# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the newly advertised capabilities: scalar select-list expression pushdown, HAVING clause pushdown, and decomposable statistical aggregate pushdown via sufficient statistics. Each extends the translator or aggregate planner with a shard-associative partial/merge path.

## Background

* Filter, select-list, group-key, and HAVING expressions are all rendered by the shared `crates/vs-expression` translator; an untranslatable expression is omitted/falls back rather than producing an incorrect result.
* An advertised capability is always backed by a translator arm or planner path that can serve it, so the advertised set stays coherent with what the adapter can actually push down.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Arithmetic operator scalar-function capabilities are advertised so arithmetic expression trees are pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise the arithmetic binary-operator scalar-function capabilities for `+`, `-`, `*`, and `/` (the Exasol capability names `FN_ADD`, `FN_SUB`, `FN_MULT`, and `FN_FLOAT_DIV`), verified against the Exasol Virtual Schema capability vocabulary
* *AND* each advertised arithmetic operator SHALL be backed by a `crates/vs-expression` translator arm that renders it (so an advertised operator is never one the translator would decline), keeping the capability set coherent with the translator the same way GROUP BY tuple advertisement is gated on its backing path
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised — advertising arithmetic operators MUST NOT introduce any additional join or cross-join capability
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `ORDER_BY_COLUMN` so Exasol pushes column sort keys (with direction and NULL placement) and the accompanying `LIMIT` into the `pushdown` request, enabling the ordered-top-N partial/merge path in `vs-adapter/pushdown-planning-topn`
* *AND* `ORDER_BY_EXPRESSION` SHALL remain absent, so Exasol never pushes an expression sort key the adapter has no bounded-sort path for
* *AND* `LIMIT_WITH_OFFSET` SHALL remain absent, so Exasol never pushes an OFFSET and the ordered-top-N path needs no offset handling
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised — advertising `ORDER_BY_COLUMN` MUST NOT introduce any additional join or cross-join capability
<!-- /DELTA:CHANGED -->
