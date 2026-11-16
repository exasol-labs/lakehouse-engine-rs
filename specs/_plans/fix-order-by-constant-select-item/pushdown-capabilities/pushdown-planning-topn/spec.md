# Feature: Pushdown Planning, Ordered Top-N

The adapter pushes an ordered top-N query (single table, no GROUP BY, no aggregate, no non-zero OFFSET) as a per-shard bounded top-N. Each shard emits at most `n` rows, and Exasol merges them with a final `ORDER BY … LIMIT n`. Every other ordered shape takes the declined path, which renders the ordering and the final window itself.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/pushdown-capabilities/pushdown-planning-topn/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A constant sort key does not decide the bounded top-N path

* *GIVEN* a single-table row-scan query with `LIMIT n` whose `ORDER BY` mixes constant keys with bare projected column keys (`SELECT id, 1 FROM t ORDER BY 2 DESC, 1 LIMIT 3`)
* *WHEN* the adapter checks whether the query is an ordered top-N
* *THEN* the adapter SHALL check the non-constant keys only, so the query above runs as a per-shard bounded top-N on `id`
* *AND* the merged result SHALL equal the same query on a single node, ids 1, 2, 3
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: An ORDER BY of constants only runs as an unordered LIMIT

* *GIVEN* a single-table row-scan query whose every `ORDER BY` key is a constant, with `LIMIT n` and no offset (`SELECT id, 1 FROM t ORDER BY 2 LIMIT 3`)
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL plan the query as one without an `ORDER BY`, so each shard carries the row limit `n` and no sort, as for a bare `LIMIT`
* *AND* the query SHALL return `n` distinct rows of the table, because a constant ordering admits any `n` rows
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Per-shard row limit is emitted only alongside the matching per-shard sort

* *GIVEN* any `pushdown` request carrying both a `limit` and an `order_by` with at least one non-constant sort key, which the adapter has NOT matched as an ordered-top-N shape
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter MUST NOT push the row limit into a per-shard scan spec without also pushing the matching per-shard `ORDER BY`, because a bare per-shard `LIMIT` ahead of a global sort would let each shard return an arbitrary (not top-ranked) subset and silently truncate the true top-N
* *AND* the adapter SHALL instead withhold the per-shard limit (leaving row selection to the Exasol-side ordering) so the returned result cannot be wrongly truncated
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Unsupported ordered-query shapes decline the ordered-top-N path

* *GIVEN* a `pushdown` request that carries an `order_by` but does NOT match the ordered-top-N shape, because it has no `limit`, or a NON-ZERO `limit.offset`, or a non-constant sort key that is not a bare projected column, such as an expression or aggregate sort key, or it carries group keys / aggregate select items / a `having`, or it involves more than one table
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL NOT emit a per-shard bounded top-N for that request
* *AND* the top-N eligibility check SHALL be evaluated against the adapter's derived projection as it stands BEFORE any declined-path sort-key extension, so a hidden sort-key column appended for the declined wrapper MUST NOT make an otherwise-ineligible shape eligible, the matched path renders the derived projection as the FINAL visible EMITS with no wrapping select, so a hidden column reaching it would leak into the result and break the returned column count, and a shape that already matched MUST NOT be disturbed
* *AND* a non-constant expression or aggregate sort key SHALL decline the bounded path unconditionally, because the per-shard sort key carried in the scan spec is a bare column
* *AND* a NON-ZERO `limit.offset` SHALL decline the bounded path unconditionally, because a per-shard `LIMIT n OFFSET m` skips each shard's OWN first `m` rows and the union of those per-shard windows is not the global window
* *AND* the declining of a non-zero offset SHALL leave the bounded path's own row-scan SQL builder, the one that attaches the merge `ORDER BY … LIMIT n` directly to the fan-out, carrying NEITHER bound, because the request's limit is withheld once an `orderBy` is present and the shape did not match; that builder SHALL therefore gain NO offset rendering, and the unreachability SHALL be pinned by a test driven by the reachable request shape (a non-zero offset TOGETHER with a non-empty `orderBy`), never by a fixture omitting the `orderBy`, which Exasol cannot produce
* *AND* the adapter SHALL fall back to the declined scan plan for that shape without wrongly truncating, misordering, or mis-windowing the result, rendering the retained ordering itself as a self-contained global `ORDER BY` plus the request's full retained window (`LIMIT n`, and `OFFSET m` when the offset is non-zero) rather than relying on an Exasol-side backstop sort or backstop window that no longer runs once the sort-key and offset capabilities are advertised
* *AND* the adapter MUST NOT emit a scan spec that would compute a different result than single-node evaluation
<!-- /DELTA:CHANGED -->
