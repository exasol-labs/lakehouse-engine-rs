# Feature: Pushdown Planning, Ordered Top-N

The adapter pushes an ordered top-N query (single table, no GROUP BY, no aggregate, no non-zero OFFSET) as a per-shard bounded top-N. Each shard emits at most `n` rows, and Exasol merges them with a final `ORDER BY … LIMIT n`. Every other ordered shape takes the declined path, which renders the ordering and the final window itself.

## Background

* The per-shard bounded top-N is carried once in the shard-invariant common spec. The merge `ORDER BY … LIMIT n` renders through the same order-by seam as the per-shard sort, so direction and NULL placement agree.
* The bounded path applies only to a request with an `order_by` list and a `limit` with `numElements`, a single involved table, and no `having`, no aggregates, and no group keys.
* A sort key is matched only when its `order_by` entry is a bare column reference that is also a projected output column. An expression or aggregate sort key never matches, because the per-shard sort key in the scan spec is a bare column.
* Each `order_by` entry carries a direction (`isAscending`) and a NULL placement (`nullsLast`), and Exasol states both on every entry. The per-shard sort and the merge sort must use the same key order, direction, and NULL placement, or the merged top-N can differ from single-node evaluation.
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

<!-- DELTA:NEW -->
### Scenario: A per-shard top-N ranks a stored empty string as NULL

* *GIVEN* a virtual table whose files span several shards and whose string column `label` stores `''` in some rows
* *WHEN* a single-table `ORDER BY label LIMIT n` is pushed down as a per-shard bounded sort
* *THEN* each shard SHALL rank a row that stores `''` as NULL, placed by the key's NULL placement
* *AND* the result SHALL equal the same query without pushdown, for `NULLS FIRST` and for `NULLS LAST`
<!-- /DELTA:NEW -->
