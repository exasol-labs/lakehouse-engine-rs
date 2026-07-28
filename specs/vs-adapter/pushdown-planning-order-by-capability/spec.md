# Feature: Pushdown Planning — Ordered Sort-Key Capability

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the getCapabilities-level
advertisement of ordered-sort-key capabilities — `ORDER_BY_COLUMN` (bare column sort keys)
and `ORDER_BY_EXPRESSION` (expression or aggregate sort keys, issue #198) — each gated on a
correctness-safe rendering path across every ordered shape the adapter can reach. Per-path
rendering mechanics live in the sibling pushdown-planning features:
`vs-adapter/pushdown-planning-topn` (declined row-scan wrapper and the matched bounded
top-N), `vs-adapter/pushdown-planning-grouped-agg` (grouped merge `ORDER BY`),
`vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback` (unresolvable grouped
`ORDER BY`), and `vs-adapter/pushdown-planning-join-fallback` (the qualified single-table
and N-scan join wrapper).

## Background

* **Why `ORDER_BY_EXPRESSION` must be advertised at all.** While it is unadvertised and the
  client's `ORDER BY` sorts on an expression or aggregate absent from the client's select list,
  Exasol silently APPENDS that sort key to the `selectList` it pushes and names the resulting
  extra result column `HIDDEN_COL_n` (issue #198). Verified on the wire against the local Docker
  stack: `SELECT id, c_price FROM t ORDER BY ABS(c_price)` (2 client columns) and `SELECT id,
  c_price, ABS(c_price) AS a FROM t ORDER BY ABS(c_price)` (3 client columns, the key genuinely
  selected) push a BYTE-IDENTICAL `selectList` and yield identical adapter-generated SQL — the
  only difference is the client-facing column name Exasol picks server-side. No disambiguating
  field exists anywhere in the payload, so no `selectList`-side detection can separate the
  leaking shape from the correct one. Advertising the capability is the only mechanism that
  removes the ambiguity: Exasol then pushes a structured `orderBy` element and leaves the
  `selectList` alone.
* **Why the advertisement and its backing paths are inseparable.** Advertising
  `ORDER_BY_EXPRESSION` makes Exasol DELEGATE the ordering: it stops re-sorting the returned
  rows. Verified live by advertising the capability with no backing path — the row-scan repro
  returned rows in raw file order with no error (a silent wrong-order regression, strictly worse
  than the leak) and the grouped repro hard-errored on the pre-existing unresolvable-ORDER-BY
  decline. So the capability MUST NOT be advertised in any commit that does not already carry a
  correctness-safe backing path for every reachable ordered shape.
* Exasol's re-apply behavior for a declined pushed clause varies by shape, which is why the
  scenarios below are careful to state exactly what each capability's fallback does and does
  not rely on Exasol to restore. Live precedent under `add-topn-pushdown` B5/B6 (issues #225 /
  #189): an `orderBy` pushed TOGETHER with a `limit` is fully delegated — Exasol re-applies
  neither, so the withheld-limit fallback returned wrong, unsorted, unbounded rows and the
  adapter now renders a self-contained global `ORDER BY … LIMIT` (`topn.rs` lines 444-449,
  `mod.rs` lines 690-694). An `orderBy` pushed WITHOUT a `limit` behaves differently: Exasol
  keeps its own top-level `ORDER BY` and re-sorts the returned rows (`tests/e2e_scan_test.rs`
  lines 1133-1138).
* **The advertisement retires the recorded structural-unreachability argument, and exposes a
  separate missing-`LIMIT` defect on the single-group aggregate merge.** It was previously argued
  that the `effective_limit` drop site never executes for the single-group aggregate, because
  that shape's output has no bare column to sort on and only `ORDER_BY_COLUMN` was advertised.
  Advertising `ORDER_BY_EXPRESSION` retires that argument: Exasol can now push an `orderBy` over
  an aggregate expression for an ungrouped aggregate and for a lone `COUNT(DISTINCT)`.
* **The single-group aggregate merge SELECT renders NO `LIMIT` at all**, independently of any
  `orderBy`. Verified in source: the aggregate branch of the scan-driving SQL builder takes no
  limit argument and emits `SELECT <merge items> FROM (<fan-out>)` unconditionally, so a pushed
  `LIMIT 0` returns the one aggregate row. The `effective_limit` binding is therefore NOT the
  drop site to change — it is inert on this sub-path in both directions: the per-shard partial
  scan never reads the scan spec's `limit` field (the partial-aggregate scan path consumes
  `aggregates` only), so the withheld value truncates nothing, and the value never reaches the
  emitted SQL either. The fix site is the outer merge SELECT. The lone-`COUNT(DISTINCT)`
  wrapper already renders its caller-supplied limit correctly on its outer
  `SELECT COUNT(DISTINCT "V") FROM (<fan-out>)`, so only its caller-side withholding is at
  issue there.
* **`effective_limit` serves two sub-paths with opposite requirements, so it stays unchanged.**
  It is computed once, before the aggregate/row-scan split, and `None` is LOAD-BEARING for
  correctness on the row-scan declined path: that path re-applies the real limit on its own
  outer wrapper AFTER the global `ORDER BY`, and a limit left in the per-shard blob is exactly
  the wrong-truncation defect `fix-225` removed. Every rule below therefore leaves the
  per-shard `LIMIT` withheld on the row-scan declined path and changes only where the outer
  SQL renders it.
* A declined `ORDER BY` no longer widens the derived projection to the full base row.
  Instead, each sort-key column absent from the adapter's DERIVED PROJECTION — the
  projection-item list the adapter builds from the select list, normally one item per
  select-list item but separately widened to the full base row when a select-list item is
  untranslatable, an unknown/aggregate node, or carries an EMITS-incompatible declared type
  (see `vs-adapter/pushdown-planning-literal-projection`'s "Projected constant whose declared
  EMITS type Exasol rejects declines to the full base row") — is appended, resolved by name
  from `involvedTables[0].columns`, AFTER every item the derivation already produced, as a
  hidden scan column. The declined wrapper then names the derived projection's pre-extension
  items EXPLICITLY by their EMITS identifiers instead of `SELECT *`, so the returned column
  count and order equal the derived projection's pre-extension shape. Exasol validates a
  returned pushdown query's column count POSITIONALLY against the original `selectList` and
  never re-projects a declined pushdown, so a widened full-base-row projection is rejected
  with `sqlCode 04000` whenever the base row's column count differs from the select list's.
  A request whose derived projection the separate full-base-row fallback above has ALREADY
  widened never reaches the hidden-sort-key-column rule: the dispatcher routes it to the
  qualified single-table wrapper first, before the declined-`ORDER BY` path runs.
* A select-list item's quoted EMITS identifier is produced by ONE seam: the real
  source-column name for a bare column, the positional synthetic `_LH_PROJ_{index}` for a
  rendered expression. The per-shard EMITS clause and any outer wrapper's explicit column
  list render through that same seam, so they agree positionally by construction.
* Column Exasol types are read from `involvedTables[0].columns`. A sort key MAY be any
  expression node once `ORDER_BY_EXPRESSION` is advertised — a scalar-function, arithmetic, or
  CAST node as well as a bare `column`. Exasol declares result types only for `selectList`
  items (`selectListDataTypes`); it declares NO type for a sort-key expression. Every rule
  below therefore renders an expression sort key over columns whose declared types are already
  known, and never derives a type for an expression.
* Credentials MUST NOT appear in any returned SQL or error message.
* Iceberg spec compliance: checked, not engaged. Verified against the Apache Iceberg table
  spec (https://iceberg.apache.org/spec/) rather than from memory: the normative sections
  that could bear on this change are the ones governing what a reader must resolve —
  schema/field-id resolution ("Schemas and Data Types", "Column Projection") and scan
  planning ("Scan Planning", manifest/partition filtering). This feature touches none of them.
  It changes which sort-key capabilities the adapter advertises and the Exasol-side SQL shape
  it returns; no manifest read, snapshot resolution, field-id projection, delete application, or
  type mapping is added or altered, and the hidden columns it emits are ordinary base columns
  already resolved through the existing projection path. No normative requirement applies, so
  there is no deviation to fix and none to track.

## Scenarios

### Scenario: ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `ORDER_BY_COLUMN` so Exasol pushes column sort keys (with direction and NULL placement) and the accompanying `LIMIT` into the `pushdown` request, enabling the ordered-top-N partial/merge path in `vs-adapter/pushdown-planning-topn`
* *AND* the response SHALL advertise `ORDER_BY_EXPRESSION`, so Exasol pushes an expression or aggregate sort key as a structured `orderBy` element instead of silently appending it to the `selectList` as an extra result column (issue #198)
* *AND* `ORDER_BY_EXPRESSION` SHALL be backed, at the SAME commit that advertises it, by a correctness-safe rendering path for every ordered shape the adapter can reach — the declined row-scan wrapper and the grouped merge (`vs-adapter/pushdown-planning-topn`, `vs-adapter/pushdown-planning-grouped-agg`), the qualified single-table wrapper, and the N-scan join wrapper — because Exasol delegates a pushed ordering and does not re-sort the returned rows
* *AND* an expression sort key SHALL NOT make a request eligible for the bounded per-shard top-N: the per-shard sort key stays a bare column, so the scan-spec wire shape and the scan UDF are unchanged by this advertisement
* *AND* `LIMIT_WITH_OFFSET` SHALL remain absent, so Exasol never pushes an OFFSET and no ordered path needs offset handling
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised — advertising the ORDER BY capabilities MUST NOT introduce any additional join or cross-join capability

### Scenario: ORDER BY on a column outside the derived projection emits the sort key as a hidden scan column

* *GIVEN* a row-scan `pushdown` request that the adapter cannot serve as a bounded top-N, carrying an `orderBy` whose parsed bare-column sort key is NOT a bare-column item of the adapter's derived projection — a different column entirely (`SELECT score FROM t WHERE id = 1 ORDER BY id`), a column referenced only inside a projected expression (`SELECT id || '-' || name FROM t WHERE id <= 3 ORDER BY id`), or a literal-only select list (`SELECT 1 FROM t ORDER BY name LIMIT 5`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL append each such sort-key column, resolved by name from `involvedTables[0].columns`, to the per-shard scan's projection and its declared EMITS list AFTER every item the derivation already produced, so every pre-existing item keeps its position and its unchanged EMITS identifier
* *AND* the adapter MUST NOT widen the derived projection to the full base row, because the returned query would then carry one column per base-table column where Exasol positionally expects one per select-list item, which Exasol rejects with `sqlCode 04000`
* *AND* the declined-`ORDER BY` wrapper SHALL name the derived projection's pre-extension items EXPLICITLY by their EMITS identifiers rather than using `SELECT *`, so each appended sort-key column is visible to the outer `ORDER BY` but absent from the returned result, and the returned column count and order EQUAL the derived projection's pre-extension column count and order
* *AND* the returned result SHALL equal the same query evaluated over all matching rows on a single node, in the requested sort-key order, direction, and NULL placement, EXCEPT for a sort key whose column requires the JSON-fallback VARCHAR cast — which orders on the emitted JSON string rather than the native value, pre-existing behaviour on this declined path that this scenario does not change, tracked as an accurately-scoped exception, `(#233)`

### Scenario: A sort key absent from the select list no longer leaks a synthetic result column

* *GIVEN* the adapter advertises `ORDER_BY_EXPRESSION` and a query whose `ORDER BY` sorts on an expression or aggregate that is NOT an item of the client's select list — `SELECT id, c_price FROM t WHERE id <= 5 ORDER BY ABS(c_price) DESC` (row scan), or `SELECT c_bool, COUNT(*) FROM t GROUP BY c_bool ORDER BY SUM(c_price) DESC` (grouped)
* *WHEN* Exasol sends the `pushdown` request and runs the returned SQL
* *THEN* the pushed `selectList` SHALL carry exactly the client's select-list items, and the sort key SHALL arrive as a structured `orderBy` element
* *AND* the client-visible result SHALL carry exactly the client's select-list columns, with NO synthetic `HIDDEN_COL_n` column, for one sort key and for two or more
* *AND* a query whose sort key IS genuinely also a select-list item SHALL be unaffected, returning the same columns and the same ordering as before this delta
* *AND* the adapter MUST NOT attempt to separate an appended sort key from a genuinely selected one by inspecting the `selectList`, because the two shapes are byte-identical on the wire and no such test can be correct

### Scenario: A pushed ORDER BY over a single-row aggregate result keeps the request LIMIT

* *GIVEN* a request whose result is exactly one row — an ungrouped single-group aggregate (`SELECT COUNT(*) FROM t ORDER BY COUNT(*) LIMIT 0`) or a lone `COUNT(DISTINCT)` — carrying a request `LIMIT`, for which advertising `ORDER_BY_EXPRESSION` now lets Exasol push an `orderBy` over an aggregate expression
* *WHEN* the adapter builds the scan-driving SQL for that request
* *THEN* the single-group aggregate merge SELECT SHALL render the request's `LIMIT` on ITSELF — the outer `SELECT <merge items> FROM (<fan-out>) LIMIT n` — so a pushed `LIMIT 0` returns ZERO rows rather than the one aggregate row, and SHALL do so for EVERY single-group aggregate request carrying a `LIMIT`, with or without a pushed `orderBy`, because that merge SELECT renders no `LIMIT` today in either case
* *AND* the value the merge SELECT renders SHALL be the request's RAW `limit`, supplied to the merge builder as its own input, NOT the withholding-adjusted binding the row-scan paths share: for this shape that shared binding is always absent (an `orderBy` is present and no bounded top-N matched), so a rule that only changed the render site while reading the shared binding would render nothing at all
* *AND* the lone-`COUNT(DISTINCT)` wrapper SHALL likewise receive the request's `LIMIT` on its outer `SELECT COUNT(DISTINCT "V") FROM (<fan-out>)`, and MUST NOT have it withheld on the grounds that an `ORDER BY` the adapter did not render is present
* *AND* the per-shard scan spec SHALL carry NO `LIMIT` for either shape, so the outer SELECT is the ONLY place a limit is applied and no shard's aggregate input or local distinct set is truncated
* *AND* the adapter MAY leave that pushed `orderBy` unrendered for these two shapes ONLY, because a one-row result admits exactly one ordering, so no ordering the adapter omits is observable
* *AND* the anti-wrong-truncation invariant SHALL remain intact for every multi-row shape, and specifically the row-scan declined path SHALL still withhold the per-shard `LIMIT` entirely and re-apply the request's `LIMIT` only on its own outer wrapper AFTER the global `ORDER BY`, so no rule here moves a limit ahead of an ordering the adapter did not itself render

### Scenario: Hidden sort-key columns are appended at most once and never invented

* *GIVEN* a declined-`ORDER BY` row-scan `pushdown` request whose `orderBy` names a column already present as a bare-column item of the derived projection, or names the same column in more than one sort key, or names a column absent from `involvedTables[0].columns`, or consists only of elements that yield no renderable sort key
* *WHEN* the adapter appends hidden sort-key columns and builds the wrapper
* *THEN* a sort-key column already present as a bare-column item of the derived projection SHALL NOT be appended, and a column named by two or more sort keys SHALL be appended at most once, because a repeated EMITS identifier is a duplicate-column error
* *AND* a column referenced by an EXPRESSION sort key SHALL obey the same at-most-once rule across every sort key and every pre-existing bare-column projection item, so two sort expressions over the same base column append it once
* *AND* a sort-key column that cannot be resolved from `involvedTables[0].columns` SHALL be left unresolved — neither appended nor otherwise special-cased — preserving the existing shape for this defensive case, which is unreachable in practice because every pushed sort key names a real table column
* *AND* a NON-EMPTY `orderBy` that yields ZERO renderable sort keys SHALL return a `User` decline naming the unrenderable `ORDER BY`, and MUST NOT return the unwrapped scan-driving SQL as if no ordering had been pushed — this REPLACES the earlier rule that returned it unchanged, because Exasol delegates a pushed `orderBy` and does not re-sort, so returning unchanged is the silent-wrong-order outcome, and because declining also removes the invalid-SQL risk that rule existed to avoid (a bare `ORDER BY` with no elements)
* *AND* an ABSENT or EMPTY `orderBy` SHALL still return the unwrapped scan-driving SQL unchanged, emitting neither a wrapper nor an `ORDER BY` clause, because no ordering was pushed and none is owed
* *AND* when the derived projection has no item to name explicitly the adapter SHALL leave the wrapper's `SELECT *` in place, because an empty explicit select list is not valid SQL

### Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe

* *GIVEN* the adapter advertises `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`, and Exasol pushes an `order_by` in a `pushdown` request that the adapter cannot serve as an ordered top-N (no accompanying `LIMIT`, a sort key that is not a bare projected column, an expression or aggregate sort key, or a request that also carries aggregates / group keys / a `having`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL fall back to the unoptimized declined path for that shape, carrying neither a per-shard row limit nor per-shard sort keys ahead of the ordering, and MUST NOT emit a scan spec that would compute a different result than single-node evaluation
* *AND* the adapter SHALL render the ordering ITSELF, as a self-contained global `ORDER BY` (plus the request's `LIMIT`, if any) wrapping the unbounded fan-out, and SHALL NOT rely on Exasol re-applying an `ORDER BY` it retains — once a sort-key capability is advertised Exasol delegates the pushed `orderBy` and does not re-sort the returned rows
* *AND* that wrapper SHALL preserve the derived projection's pre-extension column count and order, emitting any column the ordering needs but the projection lacks as a hidden scan column, so a declined `ORDER BY` never becomes an Exasol column-count rejection nor a reference to a column the scan does not emit
* *AND* for EVERY reachable ordered shape the outcome SHALL be exactly one of two: the ordering is rendered faithfully, or the pushdown declines with a `User` error naming the unrenderable key — never a result that is both silently unordered and successful, and never a result carrying a column the client did not select
