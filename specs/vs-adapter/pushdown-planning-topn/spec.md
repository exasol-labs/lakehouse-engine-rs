# Feature: Pushdown Planning — Ordered Top-N

Pushes an ordered top-N query (single table, no GROUP BY, no aggregate, no OFFSET) down
as a decomposed partial/merge top-N: each shard computes its own bounded local top-N
over only its assigned files and emits at most `n` rows, and Exasol merges the per-shard
rows with a final `ORDER BY … LIMIT n`. The merge `ORDER BY … LIMIT n` attaches DIRECTLY
to the outer ungrouped scalar scan select over the nested distributor subquery (or the
from-less single-shard select), with no `SELECT * FROM (...)` wrapper. This replaces the
raw-emit-the-whole-table-then-sort-in-Exasol path for the common "top-N by a sort key"
query, so only `≤ (shard_count × n)` rows cross the UDF boundary instead of every
matching row.

## Background

* The per-shard bounded local top-N is carried once in the shard-invariant common spec; the merge `ORDER BY … LIMIT n` renders through the same order-by seam as the per-shard sort, so direction and NULL placement always agree.
* This path applies only when the `pushdown` request carries an `order_by` list AND a
  `limit` (with `numElements`), targets a single involved table, and carries NO `having`,
  NO `aggregationType`/aggregate select items, and NO group keys. Any other shape declines
  to the pre-existing behavior.
* A `limit` never carries an OFFSET: the adapter advertises `LIMIT` but NOT
  `LIMIT_WITH_OFFSET`, so Exasol never pushes an offset and one cannot structurally appear
  in the request. The adapter builds no offset-handling path.
* Sort keys are matched only when each `order_by` entry is a bare column reference that is
  also one of the query's projected output columns; the adapter advertises `ORDER_BY_COLUMN`
  but NOT `ORDER_BY_EXPRESSION`, so Exasol does not push expression sort keys.
* Each `order_by` entry carries a direction (`isAscending`) and a NULL placement
  (`nullsLast`); the per-shard bounded sort and the Exasol-side merge sort MUST use the
  identical key order, direction, and NULL placement, or the merged top-N can differ from
  single-node evaluation.
* The correctness of the per-shard bound is the standard distributed top-N argument: a row
  in the global top-N cannot be outranked by `n` or more rows across all shards, so it
  cannot be outranked by `n` rows within its OWN shard, so it survives that shard's local
  `ORDER BY … LIMIT n` cut — hence the global top-N is a subset of the union of the
  per-shard local top-Ns, and the outer `ORDER BY … LIMIT n` over that union is exact.
* Credentials MUST NOT appear in any returned SQL string or error message.

## Scenarios

### Scenario: Ordered top-N over a projected column is pushed down as per-shard bounded sort plus Exasol merge

* *GIVEN* a single-table pushdown request with no group keys, no aggregate select items, and no OFFSET, whose `order_by` entries are all bare column references present in the projection and whose `limit` carries `numElements` with no offset
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL carry the sort key list (each with its column, direction, and NULL placement) and the row limit `n` into the shard-invariant common scan spec spliced once as the scalar scan UDF's first argument, so every row-scan shard invocation computes the SAME bounded local top-N
* *AND* the generated scan-driving SQL SHALL attach the outer merge `ORDER BY <keys> LIMIT n` — using the identical key order, direction, and NULL placement as the per-shard sort — DIRECTLY to the outer ungrouped scalar scan select over the distributor subquery (or the from-less single-shard scalar select), never inside a `SELECT * FROM (...)` wrapper
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

* *GIVEN* a `pushdown` request that carries an `order_by` but does NOT match the ordered-top-N shape — because it has no `limit`, or a sort key that is not a bare projected column, or it carries group keys / aggregate select items / a `having`, or it involves more than one table
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL NOT emit a per-shard bounded top-N for that request
* *AND* the adapter SHALL fall back to the pre-existing scan plan for that shape (row scan or the aggregate/grouped path) without wrongly truncating or misordering the result, relying on Exasol to apply the ordering it retains
* *AND* the adapter MUST NOT emit a scan spec that would compute a different result than single-node evaluation
