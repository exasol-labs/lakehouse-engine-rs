# Feature: Pushdown Planning — Ordered Top-N

Pushes an ordered top-N query (single table, no GROUP BY, no aggregate, no OFFSET) down
as a decomposed partial/merge top-N: each shard computes its own bounded local top-N
over only its assigned files and emits at most `n` rows, and Exasol merges the per-shard
rows with a final `ORDER BY … LIMIT n`. The merge `ORDER BY … LIMIT n` attaches DIRECTLY
to the outer ungrouped scalar scan select over the nested distributor subquery (or the
from-less single-shard select), with no `SELECT * FROM (...)` wrapper.

## Background

* The per-shard bounded local top-N is carried once in the shard-invariant common spec; the merge `ORDER BY … LIMIT n` renders through the same order-by seam as the per-shard sort, so direction and NULL placement always agree.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Ordered top-N over a projected column is pushed down as per-shard bounded sort plus Exasol merge

* *GIVEN* a single-table pushdown request with no group keys, no aggregate select items, and no OFFSET, whose `order_by` entries are all bare column references present in the projection and whose `limit` carries `numElements` with no offset
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL carry the sort key list (each with its column, direction, and NULL placement) and the row limit `n` into the shard-invariant common scan spec spliced once as the scalar scan UDF's first argument, so every row-scan shard invocation computes the SAME bounded local top-N
* *AND* the generated scan-driving SQL SHALL attach the outer merge `ORDER BY <keys> LIMIT n` — using the identical key order, direction, and NULL placement as the per-shard sort — DIRECTLY to the outer ungrouped scalar scan select over the distributor subquery (or the from-less single-shard scalar select), never inside a `SELECT * FROM (...)` wrapper
* *AND* the merged result SHALL equal the same `ORDER BY … LIMIT n` evaluated over all matching rows on a single node
<!-- /DELTA:CHANGED -->
