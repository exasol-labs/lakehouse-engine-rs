# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the newly advertised
capabilities: scalar select-list expression pushdown, HAVING clause pushdown, and
decomposable statistical aggregate pushdown via sufficient statistics. Each extends the
translator or aggregate planner with a shard-associative partial/merge path.

## Background

* Filter, select-list, group-key, and HAVING expressions are all rendered by the shared
  `crates/vs-expression` translator; an untranslatable expression is omitted/falls back
  rather than producing an incorrect result.
* An aggregate is pushed down only when it decomposes into a shard-associative
  partial/merge plan; otherwise the adapter falls back to row scanning.
* Credentials MUST NOT appear in any returned SQL or error message.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `ORDER_BY_COLUMN` so Exasol pushes column sort keys (with direction and NULL placement) and the accompanying `LIMIT` into the `pushdown` request, enabling the ordered-top-N partial/merge path in `vs-adapter/pushdown-planning-topn`
* *AND* `ORDER_BY_EXPRESSION` SHALL remain absent, so Exasol never pushes an expression sort key the adapter has no bounded-sort path for
* *AND* `LIMIT_WITH_OFFSET` SHALL remain absent, so Exasol never pushes an OFFSET and the ordered-top-N path needs no offset handling
* *AND* join and Cartesian-product capabilities SHALL remain absent (advertising `ORDER_BY_COLUMN` MUST NOT introduce any join capability)

### Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe

* *GIVEN* the adapter advertises `ORDER_BY_COLUMN` and Exasol pushes an `order_by` in a `pushdown` request that the adapter cannot serve as an ordered top-N (no accompanying `LIMIT`, a sort key that is not a bare projected column, or a request that also carries aggregates / group keys / a `having`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL fall back to the pre-existing scan plan for that shape without pushing a per-shard row limit ahead of the ordering, and MUST NOT emit a scan spec that would compute a different result than single-node evaluation
* *AND* the adapter SHALL rely on Exasol to apply the `ORDER BY` it retains over the returned rows, exactly as it already retains a `LIMIT` and a `HAVING` it pushed as a correctness backstop
<!-- /DELTA:NEW -->
