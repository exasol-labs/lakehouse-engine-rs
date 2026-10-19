# Feature: Pushdown Planning, Ordered Top-N

The adapter pushes an ordered top-N query (single table, no GROUP BY, no aggregate, no non-zero OFFSET) as a per-shard bounded top-N. Each shard emits at most `n` rows, and Exasol merges them with a final `ORDER BY … LIMIT n`. Every other ordered shape takes the declined path, which renders the ordering and the final window itself.

## Background

* The per-shard bounded top-N is carried once in the shard-invariant common spec. The merge `ORDER BY … LIMIT n` renders through the same order-by seam as the per-shard sort, so direction and NULL placement agree.
* The bounded path applies only to a request with an `order_by` list and a `limit` with `numElements`, a single involved table, and no `having`, no aggregates, and no group keys.
* A sort key is matched only when its `order_by` entry is a bare column reference that is also a projected output column. An expression or aggregate sort key never matches, because the per-shard sort key in the scan spec is a bare column.
* Each `order_by` entry carries a direction (`isAscending`) and a NULL placement (`nullsLast`). The per-shard sort and the merge sort must use the same key order, direction, and NULL placement, or the merged top-N can differ from single-node evaluation.
* A row in the global top-N cannot be outranked by `n` rows within its own shard, so it survives that shard's local `ORDER BY … LIMIT n`. The global top-N is therefore a subset of the union of the per-shard top-Ns.
* A non-zero offset declines the bounded path, because each shard would skip its own first `m` rows and the union of the per-shard windows is not the global window. The declined wrapper renders `LIMIT n OFFSET m` on itself. `ScanSpec` has no offset field.
* The offset guard tests for a non-zero offset, not for the key's presence. Exasol pushes no `offset` key for `OFFSET 0`, but a presence test would decline every ordered LIMIT query on a build that attached `offset: 0`.
* A non-zero offset always arrives with a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`). The declined path then withholds the per-shard limit, because that rule is gated on a pushed `orderBy`.
* Exasol delegates a pushed `orderBy` and does not re-sort the returned rows, so the declined path renders the ordering itself.
* Exasol declares a result type for each `selectList` item but none for a sort-key expression. The declined path emits the expression's referenced base columns as hidden scan columns, each with its declared type from `involvedTables[0].columns`, and Exasol evaluates the expression over them.
* The Exasol-dialect renderer in `crates/vs-expression` translates a sort expression for an Exasol-evaluated clause. The qualified single-table wrapper and the N-scan join wrapper use the same renderer.
* Direction and NULL placement for every `ORDER BY` element the adapter emits render through one shared seam. An expression sort key reuses it with the rendered expression in place of a quoted column identifier.
* Hidden sort-key columns exist only to make the declined wrapper's outer `ORDER BY` resolvable (`pushdown-capabilities/pushdown-planning-order-by-capability`). They never appear in the returned result.

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
* *AND* a non-empty `orderBy` whose elements ALL fail to render SHALL take that SAME decline, and MUST NOT be treated as "no ordering was pushed" and returned as unwrapped scan-driving SQL — the two cases are one rule, not two, because dropping every key is the same silent-wrong-order outcome as dropping one (see the reconciled zero-parsed-keys clause in `pushdown-capabilities/pushdown-planning-order-by-capability`)
* *AND* an ABSENT or EMPTY `orderBy` SHALL remain outside this scenario entirely: no ordering was pushed, so the adapter emits no wrapper and no `ORDER BY`
* *AND* the decline SHALL be the ONLY alternative to a faithfully rendered ordering, so no reachable ordered shape can return a result that is both successful and silently unordered

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages
