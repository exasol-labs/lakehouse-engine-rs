# Feature: Pushdown Planning, Ordered Sort-Key Capability

The adapter advertises `ORDER_BY_COLUMN`, `ORDER_BY_EXPRESSION`, and `LIMIT_WITH_OFFSET`. Each is backed by a correctness-safe rendering path for every ordered shape the adapter can reach.

## Background

* Exasol delegates an advertised ordering and window and does not re-sort or re-window the returned rows. An advertised capability without a backing path returns wrong rows, so each advertisement and its rendering paths land together.
* Without `ORDER_BY_EXPRESSION`, Exasol appends an expression sort key absent from the client's select list to the pushed `selectList` as an extra result column named `HIDDEN_COL_n`. The pushed `selectList` is then byte-identical to a query that selects the key, so the adapter cannot tell the two apart. With the capability, Exasol pushes a structured `orderBy` element and leaves the `selectList` alone.
* Without `LIMIT_WITH_OFFSET`, Exasol strips the offset from the pushed request and applies none itself, so `LIMIT 12 OFFSET 3` returns ranks 1 to 12. With the capability, Exasol pushes `"limit":{"numElements":12,"offset":3}`.
* Exasol's grammar constrains an OFFSET. `OFFSET` without `LIMIT` fails, `LIMIT ... OFFSET` without `ORDER BY` fails, and `OFFSET` in an ungrouped aggregated select fails (`sqlCode 42000`). A wrapper SELECT that renders no `ORDER BY` therefore never renders an `OFFSET`, and the single-group aggregate merge and the lone-`COUNT(DISTINCT)` wrapper never receive one.
* A non-zero `limit.offset` always arrives with a non-empty `orderBy`, because Exasol withholds `limit` when it cannot delegate the ordering. Exasol never pushes `offset: 0`. It pushes no `orderBy` on an ungrouped aggregate request, because a one-row result admits one ordering.
* The single-group aggregate merge SELECT takes the request's raw `limit` as its own input. The per-shard partial scan never reads `limit`.
* The row-scan declined path withholds the per-shard `LIMIT` and re-applies the request's `LIMIT` on its outer wrapper after the global `ORDER BY`. A per-shard limit would truncate rows before the global ordering.
* The derived projection is the projection-item list the adapter builds from the select list, one item per select-list item. Exasol validates the returned column count positionally against the original `selectList`, so a projection widened to the full base row fails with `sqlCode 04000`. A sort-key column absent from the derived projection is appended as a hidden scan column after every existing item, and the declined wrapper names the pre-extension items explicitly by their EMITS identifiers instead of `SELECT *`.
* A request whose derived projection is already widened to the full base row routes to the qualified single-table wrapper before the declined-`ORDER BY` path runs.
* A select-list item's quoted EMITS identifier is the source-column name for a bare column and `_LH_PROJ_{index}` for a rendered expression. The per-shard EMITS clause and the outer wrapper's column list use the same seam.
* Column Exasol types come from `involvedTables[0].columns`. Exasol declares no type for a sort-key expression, so an expression sort key is rendered over columns whose types are known.

## Scenarios

### Scenario: ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `ORDER_BY_COLUMN` so Exasol pushes column sort keys (with direction and NULL placement) and the accompanying `LIMIT` into the `pushdown` request, enabling the ordered-top-N partial/merge path in `pushdown-capabilities/pushdown-planning-topn`
* *AND* the response SHALL advertise `ORDER_BY_EXPRESSION`, so Exasol pushes an expression or aggregate sort key as a structured `orderBy` element instead of silently appending it to the `selectList` as an extra result column
* *AND* `ORDER_BY_EXPRESSION` SHALL be backed by a correctness-safe rendering path for every ordered shape the adapter can reach, the declined row-scan wrapper and the grouped merge (`pushdown-capabilities/pushdown-planning-topn`, `pushdown-grouped-agg/pushdown-planning-grouped-agg`), the qualified single-table wrapper, and the N-scan join wrapper, because Exasol delegates a pushed ordering and does not re-sort the returned rows
* *AND* an expression sort key SHALL NOT make a request eligible for the bounded per-shard top-N: the per-shard sort key stays a bare column, so the scan-spec wire shape and the scan UDF are unchanged by this advertisement
* *AND* the response SHALL advertise `LIMIT_WITH_OFFSET`, because with the capability unadvertised Exasol strips the offset from the request AND applies none itself, returning ranks 1..n instead of (m+1)..(m+n)
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `pushdown-joins/pushdown-planning-join`) SHALL be advertised, advertising the ORDER BY capabilities MUST NOT introduce any additional join or cross-join capability

### Scenario: ORDER BY on a column outside the derived projection emits the sort key as a hidden scan column

* *GIVEN* a row-scan `pushdown` request that the adapter cannot serve as a bounded top-N, carrying an `orderBy` whose parsed bare-column sort key is NOT a bare-column item of the adapter's derived projection, a different column entirely (`SELECT score FROM t WHERE id = 1 ORDER BY id`), a column referenced only inside a projected expression (`SELECT id || '-' || name FROM t WHERE id <= 3 ORDER BY id`), or a literal-only select list (`SELECT 1 FROM t ORDER BY name LIMIT 5`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL append each such sort-key column, resolved by name from `involvedTables[0].columns`, to the per-shard scan's projection and its declared EMITS list AFTER every item the derivation already produced, so every pre-existing item keeps its position and its unchanged EMITS identifier
* *AND* the adapter MUST NOT widen the derived projection to the full base row, because the returned query would then carry one column per base-table column where Exasol positionally expects one per select-list item, which Exasol rejects with `sqlCode 04000`
* *AND* the declined-`ORDER BY` wrapper SHALL name the derived projection's pre-extension items EXPLICITLY by their EMITS identifiers rather than using `SELECT *`, so each appended sort-key column is visible to the outer `ORDER BY` but absent from the returned result, and the returned column count and order EQUAL the derived projection's pre-extension column count and order
* *AND* the returned result SHALL equal the same query evaluated over all matching rows on a single node, in the requested sort-key order, direction, and NULL placement, EXCEPT for a sort key whose column requires the JSON-fallback VARCHAR cast, which orders on the emitted JSON string rather than the native value

### Scenario: A sort key absent from the select list adds no synthetic result column

* *GIVEN* the adapter advertises `ORDER_BY_EXPRESSION` and a query whose `ORDER BY` sorts on an expression or aggregate that is NOT an item of the client's select list, `SELECT id, c_price FROM t WHERE id <= 5 ORDER BY ABS(c_price) DESC` (row scan), or `SELECT c_bool, COUNT(*) FROM t GROUP BY c_bool ORDER BY SUM(c_price) DESC` (grouped)
* *WHEN* Exasol sends the `pushdown` request and runs the returned SQL
* *THEN* the pushed `selectList` SHALL carry exactly the client's select-list items, and the sort key SHALL arrive as a structured `orderBy` element
* *AND* the client-visible result SHALL carry exactly the client's select-list columns, with NO synthetic `HIDDEN_COL_n` column, for one sort key and for two or more
* *AND* a query whose sort key IS genuinely also a select-list item SHALL return the same columns in the requested ordering
* *AND* the adapter MUST NOT attempt to separate an appended sort key from a genuinely selected one by inspecting the `selectList`, because the two shapes are byte-identical on the wire and no such test can be correct

### Scenario: A pushed ORDER BY over a single-row aggregate result keeps the request LIMIT

* *GIVEN* a request whose result is exactly one row, an ungrouped single-group aggregate (`SELECT COUNT(*) FROM t ORDER BY COUNT(*) LIMIT 0`) or a lone `COUNT(DISTINCT)`, carrying a request `LIMIT`, for which advertising `ORDER_BY_EXPRESSION` now lets Exasol push an `orderBy` over an aggregate expression
* *WHEN* the adapter builds the scan-driving SQL for that request
* *THEN* the single-group aggregate merge SELECT SHALL render the request's `LIMIT` on ITSELF, the outer `SELECT <merge items> FROM (<fan-out>) LIMIT n`, so a pushed `LIMIT 0` returns ZERO rows rather than the one aggregate row, and SHALL do so for EVERY single-group aggregate request carrying a `LIMIT`, with or without a pushed `orderBy`
* *AND* the value the merge SELECT renders SHALL be the request's RAW `limit`, supplied to the merge builder as its own input, because an ungrouped aggregate request pushes NO `orderBy` at all, so no ordering-based withholding applies to it
* *AND* the lone-`COUNT(DISTINCT)` wrapper SHALL likewise receive the request's `LIMIT` on its outer `SELECT COUNT(DISTINCT "V") FROM (<fan-out>)`, and MUST NOT have it withheld on the grounds that an `ORDER BY` the adapter did not render is present
* *AND* the per-shard scan spec SHALL carry NO `LIMIT` for either shape, so the outer SELECT is the ONLY place a limit is applied and no shard's aggregate input or local distinct set is truncated
* *AND* the adapter MAY leave that pushed `orderBy` unrendered for these two shapes ONLY, because a one-row result admits exactly one ordering, so no ordering the adapter omits is observable
* *AND* the anti-wrong-truncation invariant SHALL remain intact for every multi-row shape, and specifically the row-scan declined path SHALL still withhold the per-shard `LIMIT` entirely and re-apply the request's `LIMIT` only on its own outer wrapper AFTER the global `ORDER BY`, so no rule here moves a limit ahead of an ordering the adapter did not itself render

### Scenario: Hidden sort-key columns are appended at most once and never invented

* *GIVEN* a declined-`ORDER BY` row-scan `pushdown` request whose `orderBy` names a column already present as a bare-column item of the derived projection, or names the same column in more than one sort key, or names a column absent from `involvedTables[0].columns`, or consists only of elements that yield no renderable sort key
* *WHEN* the adapter appends hidden sort-key columns and builds the wrapper
* *THEN* a sort-key column already present as a bare-column item of the derived projection SHALL NOT be appended, and a column named by two or more sort keys SHALL be appended at most once, because a repeated EMITS identifier is a duplicate-column error
* *AND* a column referenced by an EXPRESSION sort key SHALL obey the same at-most-once rule across every sort key and every pre-existing bare-column projection item, so two sort expressions over the same base column append it once
* *AND* a sort-key column that cannot be resolved from `involvedTables[0].columns` SHALL be left unresolved, neither appended nor otherwise special-cased, preserving the existing shape for this defensive case, which is unreachable in practice because every pushed sort key names a real table column
* *AND* a NON-EMPTY `orderBy` that yields ZERO renderable sort keys SHALL return a `User` decline naming the unrenderable `ORDER BY`, and MUST NOT return the unwrapped scan-driving SQL as if no ordering had been pushed, because Exasol delegates a pushed `orderBy` and does not re-sort, so returning it unchanged is the silent-wrong-order outcome
* *AND* an ABSENT or EMPTY `orderBy` SHALL still return the unwrapped scan-driving SQL unchanged, emitting neither a wrapper nor an `ORDER BY` clause, because no ordering was pushed and none is owed
* *AND* when the derived projection has no item to name explicitly the adapter SHALL leave the wrapper's `SELECT *` in place, because an empty explicit select list is not valid SQL

### Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe

* *GIVEN* the adapter advertises `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`, and Exasol pushes an `order_by` in a `pushdown` request that the adapter cannot serve as an ordered top-N (no accompanying `LIMIT`, a sort key that is not a bare projected column, an expression or aggregate sort key, or a request that also carries aggregates / group keys / a `having`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL fall back to the unoptimized declined path for that shape, carrying neither a per-shard row limit nor per-shard sort keys ahead of the ordering, and MUST NOT emit a scan spec that would compute a different result than single-node evaluation
* *AND* the adapter SHALL render the ordering ITSELF, as a self-contained global `ORDER BY` (plus the request's full retained window, `LIMIT n`, and `OFFSET m` when non-zero) wrapping the unbounded fan-out, and SHALL NOT rely on Exasol re-applying an `ORDER BY` it retains, once a sort-key capability is advertised Exasol delegates the pushed `orderBy` and does not re-sort the returned rows, and once `LIMIT_WITH_OFFSET` is advertised it re-applies neither bound of the window either, so rendering the `LIMIT` alone returns the wrong window
* *AND* that wrapper SHALL preserve the derived projection's pre-extension column count and order, emitting any column the ordering needs but the projection lacks as a hidden scan column, so a declined `ORDER BY` never becomes an Exasol column-count rejection nor a reference to a column the scan does not emit
* *AND* for EVERY reachable ordered shape the outcome SHALL be exactly one of two: the ordering is rendered faithfully, or the pushdown declines with a `User` error naming the unrenderable key, never a result that is both silently unordered and successful, and never a result carrying a column the client did not select

### Scenario: LIMIT_WITH_OFFSET is advertised only together with offset rendering on every wrapper that renders a final window

* *GIVEN* the adapter advertises `LIMIT_WITH_OFFSET`, so Exasol pushes `limit.offset` and applies neither the LIMIT nor the OFFSET itself
* *WHEN* the adapter builds the scan-driving SQL for any request carrying a non-zero `limit.offset`
* *THEN* EVERY wrapper SELECT that a non-zero offset can REACH SHALL render that offset alongside its `LIMIT`, through ONE shared limit-and-offset rendering seam rather than a per-wrapper string splice, so no reachable ordered shape can drop the offset: the declined row-scan wrapper (`pushdown-capabilities/pushdown-planning-topn`), the grouped merge (`pushdown-grouped-agg/pushdown-planning-grouped-agg`), and the qualified single-table and N-scan join wrapper (`pushdown-joins/pushdown-planning-join-fallback`)
* *AND* every render site an offset CANNOT reach SHALL be left without offset rendering, collapse arithmetic, or a failure branch: the matched bounded top-N's row-scan SQL (unreachable because a non-zero offset declines the bounded path, which then withholds the limit, `pushdown-capabilities/pushdown-planning-topn`), the single-group aggregate merge, and the lone-`COUNT(DISTINCT)` wrapper (both unreachable because Exasol rejects an `OFFSET` in an ungrouped aggregated select before the adapter is consulted, `pushdown-aggregates/pushdown-planning-single-group-agg`, `pushdown-aggregates/pushdown-planning-count-distinct`)
* *AND* a query whose ordering Exasol cannot delegate, such as `SELECT id FROM t ORDER BY HASH_MD5(id) LIMIT 5 OFFSET 2`, SHALL return the same window evaluated on a single node
* *AND* the shared seam SHALL render no `OFFSET` clause when the offset is zero or absent, so advertising the capability changes no plan that carries no offset
* *AND* NO offset value SHALL be carried into any per-shard scan spec, because a per-shard OFFSET would skip a different row set on every shard and cannot compose into a global window; the scan-spec wire shape and the scan UDF SHALL be unchanged by this advertisement
* *AND* the returned result SHALL equal the same `ORDER BY … LIMIT n OFFSET m` evaluated over all matching rows on a single node, for a plain row scan, a declined-sort-key row scan, a grouped aggregate, and a qualified-wrapper shape

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages
