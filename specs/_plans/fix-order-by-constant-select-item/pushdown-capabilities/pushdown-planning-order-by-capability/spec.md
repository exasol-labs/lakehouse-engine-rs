# Feature: Pushdown Planning, Ordered Sort-Key Capability

The adapter advertises `ORDER_BY_COLUMN`, `ORDER_BY_EXPRESSION`, and `LIMIT_WITH_OFFSET`. Each is backed by a correctness-safe rendering path for every ordered shape the adapter can reach.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/pushdown-capabilities/pushdown-planning-order-by-capability/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A constant sort key does not become a select-list position

* *GIVEN* a row-scan, grouped-aggregate, or join query whose `ORDER BY` names a constant, either a select-list position that points to a constant item (`SELECT id, 1 FROM t ORDER BY 2 DESC, 1 LIMIT 3`) or a constant expression (`SELECT id FROM t ORDER BY 1+0 DESC, id LIMIT 3`), which Exasol pushes as a literal sort key
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL leave every constant sort key that Exasol pushes out of the ordering it renders, except the one constant that an `ORDER BY` of constants only keeps for a non-zero `OFFSET`, because Exasol reads a bare integer in `ORDER BY` as a select-list position and a constant does not change the order
* *AND* the query SHALL return the rows of the same query on a single node in the same order, ids 1, 2, 3 for the `LIMIT 3` queries above, and ids 1 to 5 in that order for `SELECT id, 1 FROM t WHERE id <= 5 ORDER BY 2 DESC, 1`
* *AND* a constant larger than the select-list item count (`SELECT id, 5 FROM t ORDER BY 2 DESC, 1 LIMIT 3`) SHALL return those rows instead of failing with `Too large value 5 as select list column reference in ORDER BY`
* *AND* a sort key that calls a function, such as `ABS(score)` in `SELECT id, 1 FROM t ORDER BY 2 DESC, ABS(score) LIMIT 3`, SHALL keep its place in the ordering
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: An ORDER BY of constants only keeps its OFFSET window

* *GIVEN* a row-scan, grouped-aggregate, or join query whose every `ORDER BY` key is a constant and that carries `LIMIT n OFFSET m` with a non-zero `m` (`SELECT id, 1 FROM t ORDER BY 2 LIMIT 3 OFFSET 18` over a 20-row table)
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL order by one constant that Exasol does not read as a select-list position, because Exasol rejects an `OFFSET` without an `ORDER BY`
* *AND* the query SHALL return the row count of the same query on a single node, 2 rows for the query above
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe

* *GIVEN* the adapter advertises `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`, and Exasol pushes an `order_by` in a `pushdown` request that the adapter cannot serve as an ordered top-N (no accompanying `LIMIT`, a non-constant sort key that is not a bare projected column, such as an expression or aggregate sort key, or a request that also carries aggregates / group keys / a `having`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL fall back to the unoptimized declined path for that shape, carrying neither a per-shard row limit nor per-shard sort keys ahead of the ordering, and MUST NOT emit a scan spec that would compute a different result than single-node evaluation
* *AND* the adapter SHALL render the ordering ITSELF, as a self-contained global `ORDER BY` (plus the request's full retained window, `LIMIT n`, and `OFFSET m` when non-zero) wrapping the unbounded fan-out, and SHALL NOT rely on Exasol re-applying an `ORDER BY` it retains, once a sort-key capability is advertised Exasol delegates the pushed `orderBy` and does not re-sort the returned rows, and once `LIMIT_WITH_OFFSET` is advertised it re-applies neither bound of the window either, so rendering the `LIMIT` alone returns the wrong window
* *AND* that wrapper SHALL preserve the derived projection's pre-extension column count and order, emitting any column the ordering needs but the projection lacks as a hidden scan column, so a declined `ORDER BY` never becomes an Exasol column-count rejection nor a reference to a column the scan does not emit
* *AND* every reachable ordered shape SHALL be rendered faithfully, because Exasol states both sort flags on every `orderBy` element and the Exasol dialect renders every non-constant sort key Exasol sends, so the result is never silently unordered and never carries a column the client did not select
<!-- /DELTA:CHANGED -->
