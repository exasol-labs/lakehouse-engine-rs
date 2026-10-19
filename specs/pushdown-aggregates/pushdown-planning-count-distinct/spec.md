# Feature: Pushdown Planning, Single-Group COUNT(DISTINCT)

The adapter pushes a lone `COUNT(DISTINCT col)` over the whole table (no GROUP BY) as a DISTINCT row-scan fan-out. Each shard streams its locally-distinct values, and an outer Exasol `COUNT(DISTINCT "V")` deduplicates across shards.

## Background

* Only the single-group case is in scope. A `COUNT(DISTINCT ...)` inside a GROUP BY request makes the grouped detection decline and fall back to row scanning.
* The fan-out applies to exactly one distinct aggregate with a bare-column argument and no other select-list item (Case 1). Any other single-group shape with a distinct aggregate (Case 2/3) routes to a qualified single-table wrapper: more than one distinct aggregate, a distinct aggregate beside an ordinary aggregate, or an expression argument.
* The wrapper renders the exact select list over a materialized sharded raw scan, so the adapter's own SQL produces the aggregated row. Exasol never re-aggregates a declined pushdown, so a bare row scan fails pushdown validation (`sqlCode 04000`, column-count mismatch).
* Exasol rejects an emitting UDF call inside a SELECT-list scalar subquery (`sqlCode 04000`), so several distinct fan-outs cannot be composed as sibling subqueries.
* `COUNT(DISTINCT col)` is not a partial aggregate. The scan spec is a row scan whose projection is the distinct column with a `distinct` flag set, and it carries no `CountDistinct` partial.
* The fan-out declares the column's actual Exasol EMITS type, the same type resolution MIN/MAX partials use, including the JSON-string fallback for Exasol-incompatible types. It never declares a `VARCHAR(2000000)` intermediate.
* The fan-out filter excludes NULLs.
* The cross-shard merge is a plain `COUNT(DISTINCT "V")`. Distinct cardinality is bounded only by Exasol's aggregate engine, not by a per-shard cap.
* A per-shard LIMIT would drop locally-distinct values before the merge, so the fan-out carries no LIMIT, OFFSET, or ORDER BY. A request-level LIMIT applies only to the outer wrapper SELECT.
* Exasol pushes no `orderBy` on an ungrouped aggregate request and rejects an `OFFSET` in an ungrouped aggregated select at parse time (`sqlCode 42000`). The wrapper therefore renders no OFFSET.

## Scenarios

### Scenario: Single-group COUNT(DISTINCT) is decomposed into a dedicated DISTINCT row-scan fan-out

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS
* *AND* a query whose select list is a LONE `COUNT(DISTINCT col)` over the whole table with no GROUP BY, no other select-list item, and a BARE COLUMN argument (Case 1), e.g. `SELECT COUNT(DISTINCT L_SHIPMODE) FROM {vs_table}`
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL recognise the distinct aggregate, resolve the file list once, and build a row-scan-shaped scan spec whose projection is that single bare column with the `distinct` flag set and NULLs excluded
* *AND* the fan-out SHALL emit one row per shard-local distinct value through the existing row-scan streaming path, declaring that column with its actual Exasol EMITS type, NEVER a `VARCHAR(2000000)` value or any other VARCHAR-typed intermediate, and crossing the `.so` boundary as no Arrow type
* *AND* the scan spec MUST NOT carry a `CountDistinct` aggregate partial, the wrapper SQL MUST NOT invoke any custom distinct-merge UDF, and a `COUNT(DISTINCT <expression>)` argument (e.g. `COUNT(DISTINCT UPPER(L_SHIPMODE))`) MUST NOT engage this fan-out even as the lone select-list item, it always declines to the Case 2/3 qualified single-table wrapper (see "Multiple distinct columns or a distinct mixed with ordinary aggregates decline the fan-out and route to a qualified single-table wrapper")

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

### Scenario: High-cardinality COUNT(DISTINCT) completes past the former per-shard cap

* *GIVEN* an Exasol Docker container with the lakehouse VS adapter and scan UDF installed over SeaweedFS
* *AND* an Iceberg table whose target column holds enough near-unique values per shard that a serialized per-shard distinct set would be large
* *WHEN* `SELECT COUNT(DISTINCT col) FROM {vs_table}` is executed against the virtual schema
* *THEN* the query SHALL complete and its result MUST equal the same `COUNT(DISTINCT col)` executed on the raw Iceberg data via single-node DataFusion
* *AND* the scan SHALL NOT abort under any per-shard element or byte cap, because no such cap exists on this path

### Scenario: Multiple distinct columns or a distinct mixed with ordinary aggregates decline the fan-out and route to a qualified single-table wrapper

* *GIVEN* a single-group query whose select list carries more than one `COUNT(DISTINCT col)`, OR a `COUNT(DISTINCT col)` alongside one or more ordinary SUM/MIN/MAX/COUNT/AVG aggregates (optionally expression-argument), e.g. Q9b `SELECT COUNT(DISTINCT category), COUNT(DISTINCT region), SUM(LENGTH(comment)) FROM {vs_table}`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter MUST NOT build any DISTINCT row-scan fan-out or compose a distinct count as a SELECT-list scalar subquery, Exasol rejects an emitting UDF call nested in a scalar subquery at compile time (`sqlCode 04000`)
* *AND* the adapter MUST NOT return a bare row scan of the source columns, Exasol never re-aggregates a declined pushdown, so a raw-column response where `selectListDataTypes` expects N aggregate columns is rejected at pushdown-validation time (`sqlCode 04000`, column-count mismatch)
* *AND* the adapter SHALL decline the fan-out and return a qualified single-table wrapper, `SELECT <the exact select list, every aggregate incl. each COUNT(DISTINCT) spliced verbatim> FROM (<materialized sharded raw scan> AS <alias>)`, so the adapter's OWN SQL computes every aggregate, including every DISTINCT, and Exasol passes the one-row result through unchanged; the wrapper's output SHALL be N columns, one per select-list item
* *AND* the inner materialized scan's projection SHALL be narrowed to only the columns the request references, including columns nested inside aggregate arguments and CASE branches, plus filter, HAVING, and ORDER BY references, NEVER the full table schema, via the same referenced-column helper the grouped-aggregate qualified-wrapper fallback uses (`pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`)
* *AND* every returned aggregate value, including each `COUNT(DISTINCT col)`, SHALL equal the corresponding aggregate evaluated over all rows on a single node

### Scenario: LIMIT, OFFSET, and ORDER BY are NOT pushed into the distinct fan-out sub-scan

* *GIVEN* a lone single-group `COUNT(DISTINCT col)` request (Case 1) that also carries a request-level LIMIT or ORDER BY, e.g. `SELECT COUNT(DISTINCT c) FROM t LIMIT 1` or `SELECT COUNT(DISTINCT c) FROM t ORDER BY 1 LIMIT 5`
* *WHEN* the adapter builds the distinct-column fan-out and its shard-invariant common spec
* *THEN* the distinct fan-out's shard-invariant common spec MUST NOT carry any LIMIT, OFFSET, or ORDER BY value from the outer request, so no per-shard DISTINCT sub-scan runs a bounded top-N that would drop shard-local distinct values before the merge counts them
* *AND* any request-level LIMIT SHALL appear only on the outer `COUNT(DISTINCT "V")` wrapper SELECT, never on the per-shard fan-out sub-scan, matching how the grouped-aggregate path confines LIMIT to the outer merge
* *AND* the wrapper SHALL receive the request's RAW `limit`, and MUST NOT have it withheld on the grounds that an `ORDER BY` the adapter did not render is present
* *AND* the wrapper SHALL render NO `OFFSET`, because a non-zero request `limit.offset` cannot reach it: Exasol rejects `SELECT COUNT(DISTINCT c) FROM t ORDER BY 1 LIMIT 5 OFFSET 2` with `sqlCode 42000` ("OFFSET not allowed in aggregated selects") before issuing a `pushdown` request, so the adapter adds no offset rendering, no collapse arithmetic, and no failure branch here
* *AND* the merged distinct count SHALL therefore equal `COUNT(DISTINCT col)` evaluated over all rows on a single node, exactly, whenever the request's `LIMIT` admits that row, and SHALL return zero rows for `LIMIT 0`
* *AND* the Case 2/3 qualified single-table wrapper (see "Multiple distinct columns …") SHALL confine any request-level LIMIT and OFFSET to the OUTER wrapper SELECT, keeping the inner materialized sharded scan LIMIT-free and sort-free, exactly as the grouped-aggregate qualified-wrapper fallback confines LIMIT to its outer SELECT

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages
