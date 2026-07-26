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

* This delta changes ONE scenario — the decline scenario — to state what the declined
  path actually renders and to fix the top-N eligibility input as the derived projection
  as it stands before any declined-path sort-key extension. The matched top-N path
  (eligibility gates, per-shard bounded sort, and merge rendering) is unchanged by this
  delta.
* A declined ordered shape may cause the adapter to emit extra hidden sort-key columns
  from the per-shard scan (see `vs-adapter/pushdown-planning-capability-extensions`).
  Those columns exist only to make the declined wrapper's outer `ORDER BY` resolvable;
  they are not a top-N optimization and are never visible in the returned result.
* Shapes whose sort key falls outside the derived projection now take the declined path
  rather than matching a bounded top-N over a widened projection. Those shapes never
  returned a usable result before (the widened match returned more columns than the select
  list, which Exasol rejects), so the declined path trades an unbounded per-shard scan for
  a correct answer; a bounded variant for them is possible future work, not a regression.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Unsupported ordered-query shapes decline the ordered-top-N path

* *GIVEN* a `pushdown` request that carries an `order_by` but does NOT match the ordered-top-N shape — because it has no `limit`, or a sort key that is not a bare projected column, or it carries group keys / aggregate select items / a `having`, or it involves more than one table
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL NOT emit a per-shard bounded top-N for that request
* *AND* the top-N eligibility check SHALL be evaluated against the adapter's derived projection as it stands BEFORE any declined-path sort-key extension, so a hidden sort-key column appended for the declined wrapper MUST NOT make an otherwise-ineligible shape eligible — the matched path renders the derived projection as the FINAL visible EMITS with no wrapping select, so a hidden column reaching it would leak into the result and break the returned column count — and a shape that already matched MUST NOT be disturbed
* *AND* the adapter SHALL fall back to the declined scan plan for that shape without wrongly truncating or misordering the result, rendering the retained ordering itself as a self-contained global `ORDER BY` (plus the request's `LIMIT`, if any) rather than relying on an Exasol-side backstop sort that no longer runs once `ORDER_BY_COLUMN` is advertised
* *AND* the adapter MUST NOT emit a scan spec that would compute a different result than single-node evaluation
<!-- /DELTA:CHANGED -->
