# Feature: Pushdown Planning — Ordered Top-N

Pushes an ordered top-N query (single table, no GROUP BY, no aggregate, no non-zero OFFSET)
down as a decomposed partial/merge top-N: each shard computes its own bounded local top-N
over only its assigned files and emits at most `n` rows, and Exasol merges the per-shard
rows with a final `ORDER BY … LIMIT n`. The merge `ORDER BY … LIMIT n` attaches DIRECTLY
to the outer ungrouped scalar scan select over the nested distributor subquery (or the
from-less single-shard select), with no `SELECT * FROM (...)` wrapper. This replaces the
raw-emit-the-whole-table-then-sort-in-Exasol path for the common "top-N by a sort key"
query, so only `≤ (shard_count × n)` rows cross the UDF boundary instead of every
matching row. Every ordered shape the adapter cannot bound that way — including an
expression or aggregate sort key, and any non-zero OFFSET — takes the declined path, which
renders the retained ordering AND the retained final window itself.

## Background

* The per-shard bounded local top-N is carried once in the shard-invariant common spec; the merge `ORDER BY … LIMIT n` renders through the same order-by seam as the per-shard sort, so direction and NULL placement always agree.
* This path applies only when the `pushdown` request carries an `order_by` list AND a
  `limit` (with `numElements`), targets a single involved table, and carries NO `having`,
  NO `aggregationType`/aggregate select items, and NO group keys. Any other shape declines
  to the pre-existing behavior.
* **A `limit` MAY carry a non-zero OFFSET (issue #191).** This REPLACES the recorded bullet
  claiming "a `limit` never carries an OFFSET … the adapter builds no offset-handling path",
  which rested on `LIMIT_WITH_OFFSET` staying unadvertised. That premise was disproven live:
  while the capability is unadvertised Exasol strips the offset from the request AND applies
  no offset itself, so `ORDER BY score DESC LIMIT 12 OFFSET 3` returned ranks 1-12 instead of
  ranks 4-15. The capability is now advertised (`vs-adapter/pushdown-planning-order-by-capability`)
  and `limit.offset` arrives on the request, so the adapter owns the offset.
* **A non-zero offset declines the bounded path; the declined wrapper renders the window.**
  A per-shard `LIMIT n OFFSET m` is not composable: each shard would skip its OWN first `m`
  rows, so the union of the per-shard windows is not the global window. The bounded path
  therefore declines any non-zero offset outright and the declined wrapper — which already
  renders a self-contained global `ORDER BY` over an unbounded fan-out — renders
  `LIMIT n OFFSET m` on itself, where the window is exact.
* **The bounded path's offset guard MUST test for a non-zero offset, not for the key's
  presence.** Exasol normalises a zero offset away — `LIMIT 5 OFFSET 0` pushes
  `{"numElements":5}` with NO `offset` key, byte-identical to a bare `LIMIT 5` (verified live
  with the capability advertised) — so a presence test would behave identically on Exasol
  `2025.2.1`. The non-zero test is nonetheless the required form, because it encodes the actual
  predicate and stays correct on any future build that DOES attach `offset: 0`, where a
  presence test would decline EVERY ordered LIMIT query into the unbounded declined path: a
  silent cluster-wide loss of top-N pushdown, not a correctness bug and therefore invisible to
  a result-value assertion. The existing scenario asserting the matched bounded shape is the
  regression canary for this.
* **A non-zero offset never arrives without a non-empty `orderBy`.** Established live across
  11 offset-carrying shapes (see the offset-implies-ordering invariant in
  `vs-adapter/pushdown-planning-order-by-capability`). This is what makes the bounded path's
  row-scan SQL unreachable with an offset: the offset declines the bounded path, and the
  anti-wrong-truncation rule below then withholds the limit — but that rule is itself gated on
  a pushed `orderBy`, so the two steps are a CHAIN, not independent guards. The invariant is
  the link that closes it.
* **The offset is never carried per shard.** `ScanSpec` has no offset field and gains none:
  every offset the adapter honours is applied by an Exasol-side wrapper SELECT over the
  complete fan-out, so no shard's row set is skipped ahead of the global ordering.
* A sort key is MATCHED only when its `order_by` entry is a bare column reference that is
  also one of the query's projected output columns. Exasol NOW pushes expression sort keys
  too (`ORDER_BY_EXPRESSION` is advertised, see `vs-adapter/pushdown-planning-order-by-capability`),
  and every one of them declines to the unbounded declined path: an expression sort key does
  not parse as a bare column, so it never matches the bounded shape. The matched bounded
  top-N path itself — eligibility gates, per-shard bounded sort, and merge rendering — is
  unchanged; the scan-spec wire shape and the scan UDF are untouched by expression-key support.
* The declined path is the unoptimized correctness restoration for an ordered shape the
  adapter cannot bound. It renders the ordering itself because Exasol delegates a pushed
  `orderBy` and no longer re-sorts the returned rows.
* Exasol declares a result type for each `selectList` item (`selectListDataTypes`) but NO type
  for a sort-key expression. The declined path therefore emits the sort expression's
  REFERENCED BASE COLUMNS as hidden scan columns — each carrying its declared type from
  `involvedTables[0].columns` — and has Exasol evaluate the sort expression over them. No
  Exasol result type is ever derived for an expression the adapter was not given one for.
* The Exasol-dialect renderer (`crates/vs-expression`, the twin of the DataFusion-dialect
  renderer, differing only in CAST target rendering) is the one seam that translates a sort
  expression for an Exasol-evaluated clause. It is already the renderer the qualified
  single-table wrapper and the N-scan join wrapper use for their own clauses.
* Each `order_by` entry carries a direction (`isAscending`) and a NULL placement
  (`nullsLast`); the per-shard bounded sort and the Exasol-side merge sort MUST use the
  identical key order, direction, and NULL placement, or the merged top-N can differ from
  single-node evaluation.
* The correctness of the per-shard bound is the standard distributed top-N argument: a row
  in the global top-N cannot be outranked by `n` or more rows across all shards, so it
  cannot be outranked by `n` rows within its OWN shard, so it survives that shard's local
  `ORDER BY … LIMIT n` cut — hence the global top-N is a subset of the union of the
  per-shard local top-Ns, and the outer `ORDER BY … LIMIT n` over that union is exact.
* Direction and NULL placement for every `ORDER BY` element the adapter emits — per-shard
  sort, declined wrapper, grouped merge, join wrapper — render through ONE shared seam, so
  they cannot drift. An expression sort key reuses that seam with the rendered expression in
  place of a quoted column identifier.
* Credentials MUST NOT appear in any returned SQL string or error message.
* A declined ordered shape may cause the adapter to emit extra hidden sort-key columns
  from the per-shard scan (see `vs-adapter/pushdown-planning-order-by-capability`).
  Those columns exist only to make the declined wrapper's outer `ORDER BY` resolvable;
  they are not a top-N optimization and are never visible in the returned result.
* Shapes whose sort key falls outside the derived projection now take the declined path
  rather than matching a bounded top-N over a widened projection. Those shapes never
  returned a usable result before (the widened match returned more columns than the select
  list, which Exasol rejects), so the declined path trades an unbounded per-shard scan for
  a correct answer; a bounded variant for them is possible future work, not a regression.

## Scenarios

### Scenario: Ordered top-N over a projected column is pushed down as per-shard bounded sort plus Exasol merge

* *GIVEN* a single-table pushdown request with no group keys, no aggregate select items, and no non-zero `limit.offset`, whose `order_by` entries are all bare column references present in the projection and whose `limit` carries `numElements`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL carry the sort key list (each with its column, direction, and NULL placement) and the row limit `n` into the shard-invariant common scan spec spliced once as the scalar scan UDF's first argument, so every row-scan shard invocation computes the SAME bounded local top-N
* *AND* the generated scan-driving SQL SHALL attach the outer merge `ORDER BY <keys> LIMIT n` — using the identical key order, direction, and NULL placement as the per-shard sort — DIRECTLY to the outer ungrouped scalar scan select over the distributor subquery (or the from-less single-shard scalar select), never inside a `SELECT * FROM (...)` wrapper
* *AND* a request carrying `limit.offset` EQUAL TO ZERO SHALL still match this bounded path and SHALL produce byte-identical SQL to a request carrying no `offset` key at all, so advertising `LIMIT_WITH_OFFSET` cannot silently disable top-N pushdown for every ordered LIMIT query (issue #191)
* *AND* the merged result SHALL equal the same `ORDER BY … LIMIT n` evaluated over all matching rows on a single node

### Scenario: Per-shard row limit is emitted only alongside the matching per-shard sort

* *GIVEN* any `pushdown` request carrying both a `limit` and an `order_by` the adapter has NOT matched as an ordered-top-N shape
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter MUST NOT push the row limit into a per-shard scan spec without also pushing the matching per-shard `ORDER BY`, because a bare per-shard `LIMIT` ahead of a global sort would let each shard return an arbitrary (not top-ranked) subset and silently truncate the true top-N
* *AND* the adapter SHALL instead withhold the per-shard limit (leaving row selection to the Exasol-side ordering) so the returned result cannot be wrongly truncated

### Scenario: Ordered top-N preserves descending and NULL ordering

* *GIVEN* an ordered-top-N `pushdown` request whose `order_by` entry requests descending order and a specific NULL placement (`nullsLast` true or false)
* *WHEN* the adapter builds the per-shard sort and the Exasol-side merge sort
* *THEN* both the per-shard `ORDER BY` and the merge `ORDER BY` SHALL render the requested direction (`ASC`/`DESC`) and the requested NULL placement (`NULLS FIRST`/`NULLS LAST`) identically
* *AND* the merged top-N SHALL equal single-node evaluation for the descending / NULL-placement case, not only the ascending / NULLs-default case

### Scenario: Unsupported ordered-query shapes decline the ordered-top-N path

* *GIVEN* a `pushdown` request that carries an `order_by` but does NOT match the ordered-top-N shape — because it has no `limit`, or a NON-ZERO `limit.offset`, or a sort key that is not a bare projected column, or an expression or aggregate sort key, or it carries group keys / aggregate select items / a `having`, or it involves more than one table
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL NOT emit a per-shard bounded top-N for that request
* *AND* the top-N eligibility check SHALL be evaluated against the adapter's derived projection as it stands BEFORE any declined-path sort-key extension, so a hidden sort-key column appended for the declined wrapper MUST NOT make an otherwise-ineligible shape eligible — the matched path renders the derived projection as the FINAL visible EMITS with no wrapping select, so a hidden column reaching it would leak into the result and break the returned column count — and a shape that already matched MUST NOT be disturbed
* *AND* an expression or aggregate sort key SHALL decline the bounded path unconditionally, because the per-shard sort key carried in the scan spec is a bare column; this delta therefore changes neither the scan-spec wire shape nor the scan UDF
* *AND* a NON-ZERO `limit.offset` SHALL decline the bounded path unconditionally, because a per-shard `LIMIT n OFFSET m` skips each shard's OWN first `m` rows and the union of those per-shard windows is not the global window (issue #191)
* *AND* the declining of a non-zero offset SHALL leave the bounded path's own row-scan SQL builder — the one that attaches the merge `ORDER BY … LIMIT n` directly to the fan-out — carrying NEITHER bound, because the request's limit is withheld once an `orderBy` is present and the shape did not match; that builder SHALL therefore gain NO offset rendering, and the unreachability SHALL be pinned by a test driven by the reachable request shape (a non-zero offset TOGETHER with a non-empty `orderBy`), never by a fixture omitting the `orderBy`, which Exasol cannot produce
* *AND* the adapter SHALL fall back to the declined scan plan for that shape without wrongly truncating, misordering, or mis-windowing the result, rendering the retained ordering itself as a self-contained global `ORDER BY` plus the request's full retained window (`LIMIT n`, and `OFFSET m` when the offset is non-zero) rather than relying on an Exasol-side backstop sort or backstop window that no longer runs once the sort-key and offset capabilities are advertised
* *AND* the adapter MUST NOT emit a scan spec that would compute a different result than single-node evaluation

### Scenario: A declined ordered row scan renders the request's OFFSET on its own wrapper

* *GIVEN* a single-table row-scan `pushdown` request carrying a non-empty `orderBy`, a `limit` with `numElements` = `n`, and a non-zero `limit.offset` = `m` — whether the bounded path declined because of the offset alone (`SELECT id, score FROM t ORDER BY score DESC LIMIT 12 OFFSET 3`, the issue #191 repro) or additionally because the sort key is absent from the select list or is an expression
* *WHEN* the adapter builds the declined-`ORDER BY` wrapper SQL
* *THEN* the wrapper SELECT SHALL render `ORDER BY <keys> LIMIT n OFFSET m`, in that clause order, over the unbounded fan-out — the ONLY place either bound is applied
* *AND* the per-shard scan spec SHALL carry NEITHER the row limit NOR any offset, so the fan-out delivers every matching row to the wrapper and the window is computed over the complete globally-sorted row set
* *AND* the wrapper SHALL keep naming only the pre-extension visible projection items, so adding the offset changes neither the returned column count nor the column order and leaks no hidden sort-key column
* *AND* the returned result SHALL equal the same `ORDER BY … LIMIT n OFFSET m` evaluated over all matching rows on a single node — for the seeded 20-row two-file `events` fixture, `ORDER BY score DESC LIMIT 12 OFFSET 3` SHALL return ids 17 down to 6 (ranks 4-15), NOT ids 20 down to 9
* *AND* the correct window SHALL be returned across the shard boundary, not only within a single shard, because the offset is applied after the per-shard results are merged

### Scenario: A declined ORDER BY on an expression emits the expression's referenced columns as hidden scan columns

* *GIVEN* a single-table row-scan `pushdown` request whose `orderBy` carries an expression sort key — a scalar-function, arithmetic, or CAST node rather than a bare `column` node — sorting on a value absent from the client's select list (`SELECT id, c_price FROM t WHERE id <= 5 ORDER BY ABS(c_price) DESC`, issue #198)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL append every base column the sort expression references, resolved by name from `involvedTables[0].columns` with that column's declared Exasol type, to the per-shard scan's projection and its declared EMITS list AFTER every item the derivation already produced — the same append-only hidden-column mechanism an unprojected bare-column sort key already uses, so every pre-existing item keeps its position and its EMITS identifier
* *AND* the adapter SHALL render the wrapper's outer `ORDER BY` element as the sort expression translated to the EXASOL dialect over those emitted column identifiers, so Exasol evaluates the sort expression itself and no result type has to be derived for an expression Exasol declared none for
* *AND* each hidden column SHALL be appended AT MOST ONCE across all sort keys and all pre-existing bare-column projection items, because a repeated EMITS identifier is a duplicate-column error
* *AND* the wrapper SHALL name only the pre-extension visible items, so the returned column count and order EQUAL the derived projection's and no hidden column reaches the client
* *AND* an `orderBy` carrying two or more expression sort keys SHALL render EVERY key, in the pushed order, each with its own direction and NULL placement, and SHALL leak no column for any of them (issue #198)
* *AND* the returned result SHALL equal the same query evaluated over all matching rows on a single node, in the requested sort-key order, direction, and NULL placement, EXCEPT for a referenced column requiring the JSON-fallback VARCHAR cast — which orders on the emitted JSON string rather than the native value, the pre-existing declined-path behaviour on this path, tracked as an accurately-scoped exception, `(#233)`

### Scenario: An unrenderable expression sort key declines hard rather than silently dropping the ordering

* *GIVEN* a `pushdown` request carrying a NON-EMPTY `orderBy` with at least one element the adapter cannot render — an expression sort key the Exasol-dialect expression renderer cannot render, or an element that omits its direction or NULL-placement flag
* *WHEN* the adapter builds the scan-driving SQL on any path that renders a pushed ordering — the declined row-scan wrapper, the qualified single-table wrapper, or the N-scan join wrapper
* *THEN* the adapter SHALL return a `User` decline naming the unrenderable `ORDER BY` key, and SHALL NOT claim a native re-plan
* *AND* the adapter MUST NOT drop the sort key and return rows successfully in an arbitrary order, because Exasol delegates a pushed `orderBy` and does not re-sort the returned rows once a sort-key capability is advertised
* *AND* a non-empty `orderBy` whose elements ALL fail to render SHALL take that SAME decline, and MUST NOT be treated as "no ordering was pushed" and returned as unwrapped scan-driving SQL — the two cases are one rule, not two, because dropping every key is the same silent-wrong-order outcome as dropping one (see the reconciled zero-parsed-keys clause in `vs-adapter/pushdown-planning-order-by-capability`)
* *AND* an ABSENT or EMPTY `orderBy` SHALL remain outside this scenario entirely: no ordering was pushed, so the adapter emits no wrapper and no `ORDER BY`
* *AND* the decline SHALL be the ONLY alternative to a faithfully rendered ordering, so no reachable ordered shape can return a result that is both successful and silently unordered
