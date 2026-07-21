# Feature: Pushdown Planning — Single-Group COUNT(DISTINCT)

Extends single-group aggregate pushdown (`vs-adapter/pushdown-planning`) so a
`COUNT(DISTINCT col)` over the whole table (no GROUP BY) is decomposed into a dedicated
DISTINCT row-scan fan-out whose shard-local distinct values are counted by an outer
Exasol-native `COUNT(DISTINCT)`. Each shard streams one row per locally-distinct value
through the existing row-scan path; the outer wrapper's `COUNT(DISTINCT "V")` performs the
cross-shard deduplication, so distinct cardinality is bounded by Exasol's own aggregate
engine rather than a fixed per-shard serialization cap.

## Background

* Only the single-group (no GROUP BY) `COUNT(DISTINCT col)` case is in scope. A
  `COUNT(DISTINCT ...)` that appears inside a GROUP BY request MUST still cause the grouped
  detection to decline (fall back to row scanning); it is explicitly out of scope.
* The DISTINCT row-scan fan-out applies ONLY to a lone single-group `COUNT(DISTINCT col)` —
  exactly one distinct aggregate and no other select-list item (Case 1). A request carrying
  more than one distinct aggregate, or a distinct aggregate alongside any ordinary
  SUM/MIN/MAX/COUNT/AVG aggregate (Case 2/3), MUST decline the fan-out and route to a
  qualified single-table wrapper — the same shape the grouped-aggregate decline fallback uses
  (`vs-adapter/pushdown-planning-grouped-agg`). The wrapper renders the exact single-group
  select list, every aggregate (including each `COUNT(DISTINCT)`) spliced verbatim, over a
  materialized sharded raw scan aliased once, so the adapter's OWN SQL produces the one-row
  aggregated result and Exasol only passes it through. A BARE row scan MUST NOT be returned for
  a Case 2/3 request: Exasol never re-aggregates a declined pushdown, so returning the raw
  source columns where the request's `selectListDataTypes` expects the aggregate columns is
  rejected at pushdown-validation time (`sqlCode 04000`, column-count mismatch). An emitting UDF
  call is likewise only valid as a top-level FROM item; Exasol rejects it inside a SELECT-list
  scalar subquery at compile time (`sqlCode 04000`, "emitting function in expression"), so
  multiple distinct fan-outs cannot be composed as sibling scalar subqueries in one outer SELECT.
* `COUNT(DISTINCT col)` is NOT a partial aggregate. For the lone-distinct case the adapter
  plans it as a row-scan whose projection is the single distinct column (or rendered
  expression) with a `distinct` flag set, reusing the same fan-out, streaming, and per-column
  EMITS-type machinery the raw row-scan path uses. The scan spec MUST NOT carry a
  `CountDistinct` aggregate partial, and no per-shard JSON distinct-set is produced.
* Each distinct-column fan-out emits one row per shard-local distinct value using the
  column's actual Exasol EMITS type — the standard Arrow-to-Exasol mapping, including the
  JSON-string fallback for Exasol-incompatible types, the same type resolution MIN/MAX
  partials use — never a hardcoded `VARCHAR(2000000)` JSON array.
* `COUNT(DISTINCT col)` excludes NULLs, matching single-node SQL semantics: the pushed-down
  fan-out filter excludes NULL values.
* The cross-shard merge is a plain Exasol `COUNT(DISTINCT "V")` over the union of every
  shard's locally-distinct rows. The adapter MUST NOT generate a custom merge UDF, a
  `LISTAGG` concatenation, or bespoke SQL string-splitting. Distinct cardinality is bounded
  only by Exasol's own distinct-aggregate engine (its spill and resize behavior), NOT by a
  fixed per-shard element or byte cap.
* The distinct fan-out reuses the row-scan builder, which also renders `ORDER BY … LIMIT n`
  for matched top-N row scans. That top-N machinery MUST NOT engage for a distinct fan-out:
  the fan-out's shard-invariant common spec carries no LIMIT, OFFSET, or ORDER BY from the
  outer request. A per-shard LIMIT would drop shard-local distinct values before the merge
  counts them, producing a WRONG count — the same hazard the grouped-aggregate path already
  guards (`vs-adapter/pushdown-planning-grouped-agg`, "LIMIT is NOT pushed into per-shard
  scan for a grouped query"). Any request-level LIMIT or OFFSET applies only to the outer
  `COUNT(DISTINCT "V")` wrapper SELECT.
* Credentials MUST NOT appear in any returned SQL or error message.

## Scenarios

### Scenario: Single-group COUNT(DISTINCT) is decomposed into a dedicated DISTINCT row-scan fan-out

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list is a LONE `COUNT(DISTINCT col)` over the whole table with no GROUP BY and no other select-list item, e.g. `SELECT COUNT(DISTINCT L_SHIPMODE) FROM {vs_table}`
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL recognise the distinct aggregate, resolve the file list once, and build a row-scan-shaped scan spec whose projection is that single column (or rendered expression) with the `distinct` flag set and NULLs excluded
* *AND* the fan-out SHALL emit one row per shard-local distinct value through the existing row-scan streaming path, declaring that column with its actual Exasol EMITS type (never a `VARCHAR(2000000)` JSON array) and crossing the `.so` boundary as no Arrow type
* *AND* the scan spec MUST NOT carry a `CountDistinct` aggregate partial and the wrapper SQL MUST NOT invoke any custom distinct-merge UDF

### Scenario: Outer wrapper counts the unioned per-shard distinct rows with a native COUNT(DISTINCT)

* *GIVEN* a `COUNT(DISTINCT col)` fan-out over a file list partitioned into one or more shards, each streaming its locally-distinct values as rows aliased `"V"`
* *WHEN* the adapter builds the outer wrapper SQL
* *THEN* the outer wrapper SHALL compute the final count as a plain `COUNT(DISTINCT "V")` over the union of all shards' locally-distinct rows, with no custom merge UDF and no `LISTAGG` concatenation
* *AND* the merged distinct count SHALL equal `COUNT(DISTINCT col)` evaluated over all rows on a single node, including when the same value appears in more than one shard (deduplicated by Exasol) and when the column contains NULLs (NULLs never counted)
* *AND* an empty table SHALL yield a distinct count of zero

### Scenario: Multi-shard COUNT(DISTINCT) dedups across the shard boundary, excludes NULLs, and returns zero for an empty result

* *GIVEN* a virtual schema over an Iceberg table seeded across TWO data files, so the resolved file list partitions into at least two shards
* *AND* a target column where one value appears in BOTH files (both shards emit it as a locally-distinct row), some rows hold NULL, and a WHERE predicate can match zero rows without pruning every file
* *WHEN* `SELECT COUNT(DISTINCT col) FROM {vs_table}` and its zero-match and all-NULL variants are executed end-to-end against the Exasol Docker stack
* *THEN* the merged count SHALL equal `COUNT(DISTINCT col)` evaluated over all rows on a single node, proving the outer native `COUNT(DISTINCT "V")` deduplicates the shared value ACROSS the shard boundary rather than summing per-shard counts
* *AND* NULL values SHALL never be counted, and a zero-match or all-NULL result SHALL yield a distinct count of zero, not an error
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: High-cardinality COUNT(DISTINCT) completes past the former per-shard cap

* *GIVEN* an Exasol Docker container with the lakehouse VS adapter and scan UDF installed over MinIO
* *AND* an Iceberg table whose target column holds enough near-unique values per shard that the former per-shard distinct-set serialization would have exceeded its 1,048,576-byte cap
* *WHEN* `SELECT COUNT(DISTINCT col) FROM {vs_table}` is executed against the virtual schema
* *THEN* the query SHALL complete and its result MUST equal the same `COUNT(DISTINCT col)` executed on the raw Iceberg data via single-node DataFusion
* *AND* the scan SHALL NOT abort under any per-shard element or byte cap, because no such cap exists on this path
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: Multiple distinct columns or a distinct mixed with ordinary aggregates decline the fan-out and route to a qualified single-table wrapper

* *GIVEN* a single-group query whose select list carries more than one `COUNT(DISTINCT col)`, OR a `COUNT(DISTINCT col)` alongside one or more ordinary SUM/MIN/MAX/COUNT/AVG aggregates (optionally expression-argument), e.g. Q9b `SELECT COUNT(DISTINCT category), COUNT(DISTINCT region), SUM(LENGTH(comment)) FROM {vs_table}`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter MUST NOT build any DISTINCT row-scan fan-out or compose a distinct count as a SELECT-list scalar subquery — Exasol rejects an emitting UDF call nested in a scalar subquery at compile time (`sqlCode 04000`)
* *AND* the adapter MUST NOT return a bare row scan of the source columns — Exasol never re-aggregates a declined pushdown, so a raw-column response where `selectListDataTypes` expects N aggregate columns is rejected at pushdown-validation time (`sqlCode 04000`, column-count mismatch)
* *AND* the adapter SHALL decline the fan-out and return a qualified single-table wrapper — `SELECT <the exact select list, every aggregate incl. each COUNT(DISTINCT) spliced verbatim> FROM (<materialized sharded raw scan> AS <alias>)` — so the adapter's OWN SQL computes every aggregate, including every DISTINCT, and Exasol passes the one-row result through unchanged; the wrapper's output SHALL be N columns, one per select-list item
* *AND* the inner materialized scan's projection SHALL be narrowed to only the columns the request references — including columns nested inside aggregate arguments and CASE branches, plus filter, HAVING, and ORDER BY references — NEVER the full table schema, via the same referenced-column helper the grouped-aggregate qualified-wrapper fallback uses (`vs-adapter/pushdown-planning-grouped-agg`; issue #160)
* *AND* every returned aggregate value, including each `COUNT(DISTINCT col)`, SHALL equal the corresponding aggregate evaluated over all rows on a single node

### Scenario: LIMIT, OFFSET, and ORDER BY are NOT pushed into the distinct fan-out sub-scan

* *GIVEN* a lone single-group `COUNT(DISTINCT col)` request (Case 1) that also carries a request-level LIMIT, OFFSET, or ORDER BY, e.g. `SELECT COUNT(DISTINCT c) FROM t LIMIT 1`
* *WHEN* the adapter builds the distinct-column fan-out and its shard-invariant common spec
* *THEN* the distinct fan-out's shard-invariant common spec MUST NOT carry any LIMIT, OFFSET, or ORDER BY value from the outer request, so no per-shard DISTINCT sub-scan runs a bounded top-N that would drop shard-local distinct values before the merge counts them
* *AND* any request-level LIMIT or OFFSET SHALL appear only on the outer `COUNT(DISTINCT "V")` wrapper SELECT, never on the per-shard fan-out sub-scan, matching how the grouped-aggregate path confines LIMIT to the outer merge
* *AND* the merged distinct count SHALL therefore equal `COUNT(DISTINCT col)` evaluated over all rows on a single node, exactly, regardless of any outer LIMIT, OFFSET, or ORDER BY
* *AND* the Case 2/3 qualified single-table wrapper (see "Multiple distinct columns …") SHALL confine any request-level LIMIT to the OUTER wrapper SELECT, keeping the inner materialized sharded scan LIMIT-free and sort-free, exactly as the grouped-aggregate qualified-wrapper fallback confines LIMIT to its outer SELECT
