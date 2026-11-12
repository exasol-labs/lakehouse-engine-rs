# Feature: Pushdown Planning, Ordered Sort-Key Capability

The adapter advertises `ORDER_BY_COLUMN`, `ORDER_BY_EXPRESSION`, and `LIMIT_WITH_OFFSET`. Each is backed by a correctness-safe rendering path for every ordered shape the adapter can reach.

<!-- DELTA:CHANGED -->
## Background

* Exasol delegates an advertised ordering and window and does not re-sort or re-window the returned rows. An advertised capability without a backing path returns wrong rows, so each advertisement and its rendering paths land together.
* Without `ORDER_BY_EXPRESSION`, Exasol appends an expression sort key absent from the client's select list to the pushed `selectList` as an extra result column named `HIDDEN_COL_n`. The pushed `selectList` is then byte-identical to a query that selects the key, so the adapter cannot tell the two apart. With the capability, Exasol pushes a structured `orderBy` element and leaves the `selectList` alone.
* Without `LIMIT_WITH_OFFSET`, Exasol pushes the `ORDER BY` without any `limit`, applies `LIMIT n` itself after the pushed ordering, and ignores the offset, so `LIMIT 12 OFFSET 3` returns ranks 1 to 12. With the capability, Exasol pushes `"limit":{"numElements":12,"offset":3}`.
* Exasol's grammar constrains an OFFSET. `OFFSET` without `LIMIT` fails, `LIMIT ... OFFSET` without `ORDER BY` fails, and `OFFSET` in an ungrouped aggregated select fails (`sqlCode 42000`). A wrapper SELECT that renders no `ORDER BY` therefore never renders an `OFFSET`, and the single-group aggregate merge and the lone-`COUNT(DISTINCT)` wrapper never receive one.
* A non-zero `limit.offset` always arrives with a non-empty `orderBy`, because Exasol withholds `limit` when it cannot delegate the ordering. Exasol never pushes `offset: 0`. It pushes no `orderBy` on an ungrouped aggregate request, because a one-row result admits one ordering.
* The single-group aggregate merge SELECT takes the request's raw `limit` as its own input. The per-shard partial scan never reads `limit`.
* The row-scan declined path withholds the per-shard `LIMIT` and re-applies the request's `LIMIT` on its outer wrapper after the global `ORDER BY`. A per-shard limit would truncate rows before the global ordering.
* The derived projection is the projection-item list the adapter builds from the select list, one item per select-list item. Exasol validates the returned column count positionally against the original `selectList`, so a projection widened to the full base row fails with `sqlCode 04000`. A sort-key column absent from the derived projection is appended as a hidden scan column after every existing item, and the declined wrapper names the pre-extension items explicitly by their EMITS identifiers instead of `SELECT *`.
* A request whose derived projection is already widened to the full base row routes to the qualified single-table wrapper before the declined-`ORDER BY` path runs.
* A select-list item's quoted EMITS identifier is the source-column name for a bare column and `_LH_PROJ_{index}` for a rendered expression. The per-shard EMITS clause and the outer wrapper's column list use the same seam.
* Column Exasol types come from `involvedTables[0].columns`. Exasol declares no type for a sort-key expression, so an expression sort key is rendered over columns whose types are known.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `ORDER_BY_COLUMN` so Exasol pushes column sort keys (with direction and NULL placement) and the accompanying `LIMIT` into the `pushdown` request, enabling the ordered-top-N partial/merge path in `pushdown-capabilities/pushdown-planning-topn`
* *AND* the response SHALL advertise `ORDER_BY_EXPRESSION`, so Exasol pushes an expression or aggregate sort key as a structured `orderBy` element instead of silently appending it to the `selectList` as an extra result column
* *AND* `ORDER_BY_EXPRESSION` SHALL be backed by a correctness-safe rendering path for every ordered shape the adapter can reach, the declined row-scan wrapper and the grouped merge (`pushdown-capabilities/pushdown-planning-topn`, `pushdown-grouped-agg/pushdown-planning-grouped-agg`), the qualified single-table wrapper, and the N-scan join wrapper, because Exasol delegates a pushed ordering and does not re-sort the returned rows
* *AND* an expression sort key SHALL NOT make a request eligible for the bounded per-shard top-N: the per-shard sort key stays a bare column, so the scan-spec wire shape and the scan UDF are unchanged by this advertisement
* *AND* the response SHALL advertise `LIMIT_WITH_OFFSET`, because with the capability unadvertised Exasol pushes the ordering without the limit, applies the `LIMIT` itself, and ignores the offset, returning ranks 1..n instead of (m+1)..(m+n)
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `pushdown-joins/pushdown-planning-join`) SHALL be advertised, advertising the ORDER BY capabilities MUST NOT introduce any additional join or cross-join capability
<!-- /DELTA:CHANGED -->
