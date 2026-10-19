# Feature: Pushdown Planning, Broadcast Inner Equi-Join

Extends pushdown planning (`pushdown/pushdown-planning`) with the broadcast inner equi-join. When the smaller side of a two-table join is below the broadcast threshold, the adapter shards the larger (fact) side across the scan fan-out and replicates the smaller (dimension) side into the shard-invariant common spec, and a node-local DataFusion join runs inside the scan UDF (`scan-aggregation/scan-execution-join`). Every other join is served by the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`).

## Background

* The broadcast contract: exactly two involved tables, `join_type = "inner"`, an equi-join condition, disjoint column-name sets, no Exasol postprocessing, and a condition, filter, and projection that `crates/vs-expression` can render. Any deviation takes the fallback.
* Exasol postprocessing means one of four conditions: an aggregate select item (`function_aggregate`), a non-empty `groupBy`, `aggregationType == "group_by"`, or a non-null `having`. A pushed `LIMIT` and a pushed `ORDER BY` are not forcing conditions.
* The dimension side rides in the shard-invariant common spec. Only the fact side's per-shard file subset flows through the `LAKEHOUSE_DISTRIBUTE_FILES` distributor.
* A bare `LIMIT n` applies both per shard and once on the merge, because an unordered `LIMIT n` means any n qualifying rows. The full `n` goes into every shard's join block and onto the outer scalar select.
* An ordered broadcast window is served by one outer wrapper over the merged fan-out, and the fan-out's own scalar select carries no `LIMIT`. A `LIMIT` there would truncate the merged rows before the global sort.
* A zero-offset `ORDER BY ... LIMIT n` also bounds each shard to its own post-join top-n, which is correct under ties (`pushdown-capabilities/pushdown-planning-topn`).
* `ScanSpec` has no offset field, so an OFFSET is rendered only on the ordered wrapper.

## Scenarios

### Scenario: Adapter advertises inner equi-join capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`
* *AND* the capabilities list MUST NOT include `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, or any Cartesian-product capability
* *AND* each advertised join capability SHALL be backed by the join-planning path in this feature and its fallback counterpart, so an advertised capability is never a shape the planner cannot serve either by broadcast or by the unified unaccelerated fallback

### Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out

* *GIVEN* a virtual schema over a namespace whose tables are backed by SeaweedFS
* *AND* a `pushdown` request whose `from` clause is a `join` node over exactly two involved tables joined by an equi-condition
* *AND* the smaller side's table-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH sides through the SAME format-reader seam the single-table scan uses (`file-planning/pushdown-format-neutral-resolution`), obtaining each side's data-file list, per-file byte size, logical schema, table root, and effective storage exactly once, recovering each table's original-cased catalog identifier from the schema-metadata mapping by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards and driven through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, logical schema, and its OWN storage value (a connection REFERENCE or sealed envelope per `storage-access/scan-spec-credential-reference`) in the shard-invariant common spec's join block, and the fact side's storage and per-shard file subset through the distributor
* *AND* the generated scan-driving SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange, and with NO `SELECT * FROM (...)` wrapper for an UNORDERED request; an ORDERED request carries the outer wrapper this feature's ordering scenario specifies
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer
* *AND* a join whose two sides are DELTA tables reached through Unity Catalog SHALL take this same broadcast path with no Iceberg-specific step, because the resolution seam and the broadcast decision read only neutral resolved values

### Scenario: A bare LIMIT over a broadcast-eligible join is served by the broadcast path with a per-shard post-join cap

* *GIVEN* a `pushdown` request that is broadcast-eligible in every other respect, exactly two involved tables, a `predicate_equal` condition, the dimension side at or below the broadcast threshold, disjoint column names, a renderable condition and filter, a non-widened projection
* *AND* the request carries a `limit` with `numElements` = `n`, an ABSENT or EMPTY `orderBy`, and a zero or absent `limit.offset`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out for that request and MUST NOT route it to the unified unaccelerated fallback
* *AND* the shard-invariant common scan spec's JOIN BLOCK SHALL carry a post-join cap of `n`, so every fact-shard invocation truncates its OWN joined output at `n` inside its DataFusion session rather than emitting its complete local join
* *AND* the common scan spec's own shard-invariant row-limit field SHALL remain absent, so the cap exists only where a join block does and no fallback leg spec can express one (`pushdown-joins/pushdown-planning-join-fallback`)
* *AND* that per-shard cap SHALL be applied AFTER the join and its WHERE filter, never to the fact side's pre-join scan, because an input-side cap discards fact rows that would have matched and keeps fact rows that produce no output (`scan-aggregation/scan-execution-join`)
* *AND* the generated scan-driving SQL SHALL render the SAME undivided `LIMIT n` once on the outer ungrouped scalar select over the distributor subquery, through the row-scan builder's limit seam, so the merge of up to `G × n` shard rows is truncated to `n`
* *AND* the adapter MUST NOT divide `n` across shards, because a shard producing fewer than `n` joined rows would then make the merged result short
* *AND* the returned result SHALL be `n` rows whenever the unbounded join produces at least `n`, and SHALL be a subset of the unbounded join's rows in every case, equal to some valid single-node `LIMIT n` answer

### Scenario: A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out

* *GIVEN* a `pushdown` request that is broadcast-eligible in every other respect, carrying a non-empty `orderBy` whose every element is a bare `column` node that carries both `isAscending` and `nullsLast` and names a bare-column item of the broadcast projection, and an optional `limit` with `numElements` = `n` and an optional `limit.offset` = `m`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL serve the request with the broadcast fan-out, MUST NOT route it to the unified unaccelerated fallback, and SHALL wrap that fan-out in ONE outer `SELECT <visible projection items> FROM (<fan-out>) ORDER BY <keys>` plus the request's full retained window, rendering the window through the SAME shared limit-and-offset seam every other wrapper uses (` LIMIT n` byte-for-byte for a zero or absent offset, ` LIMIT n OFFSET m` for a non-zero one) and the select list and keys through the SAME shared emitted-identifier and direction/NULL-placement seams the declined row-scan wrapper uses (`pushdown-capabilities/pushdown-planning-topn`)
* *AND* the fan-out's own outer scalar select SHALL carry NO `LIMIT`, and the shard-invariant common scan spec SHALL carry NO `limit` and NO `order_by` key, so the wrapper is the ONLY place the global ordering and window are applied; what each shard's join block carries is set by this feature's two per-shard ordered-window scenarios
* *AND* an UNORDERED broadcast request, with or without a bare `LIMIT`, SHALL still be emitted as the bare fan-out with no wrapping select
* *AND* the adapter MUST NOT emit a broadcast ordered plan whose outer wrapper renders NO `ORDER BY`: a wrapper that returned the fan-out unchanged SHALL instead decline to the unified unaccelerated fallback rather than return rows that are silently unordered while `ORDER_BY_COLUMN` is advertised
* *AND* the returned result SHALL equal the same `ORDER BY … [LIMIT n [OFFSET m]]` evaluated over the same inner join on a single node

### Scenario: A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard to its own post-join top-N

* *GIVEN* a `pushdown` request that the ordered broadcast wrapper serves, carrying a `limit` with `numElements` = `n` and a zero or absent `limit.offset`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the shard-invariant common scan spec's JOIN BLOCK SHALL carry a post-join ordering equal to the request's sort keys, in pushed order, each with its column, direction, and NULL placement, together with a post-join cap of `n`, so every fact-shard invocation emits at most `n` joined rows, its own top-ranked ones
* *AND* the adapter MUST NOT divide `n` across shards, because every row of the global top-n can come from one shard
* *AND* a request carrying `limit.offset` EQUAL TO ZERO SHALL produce the same SQL as a request carrying no `offset` key, so the eligibility test is a non-zero test and not a key-presence test
* *AND* a request with no `limit` or a non-zero offset SHALL carry NEITHER post-join field, so every shard emits its complete local joined output for the wrapper to sort and window
* *AND* the returned result SHALL carry the same sort-key values, in the same order, as the same `ORDER BY … LIMIT n` evaluated over the same inner join on a single node, and every returned row SHALL be a row of that join

### Scenario: Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback

* *GIVEN* a `pushdown` request over a broadcast-eligible inner equi-join carrying one of: a `limit` with a NON-ZERO `limit.offset` and an absent or empty `orderBy`; an `orderBy` element that is an expression, an aggregate, or any non-`column` node; an `orderBy` element missing `isAscending` or `nullsLast`; or an `orderBy` element naming a column that is not a bare-column item of the broadcast projection
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL decline the broadcast plan and route the request to the unified unaccelerated fallback, whose qualified wrapper renders the ordering and the window table-qualified over the materialized join
* *AND* every arm EXCEPT projection membership SHALL be decided from the `pushdown` request ALONE, BEFORE either side is sized and before the broadcast render runs, so a request carrying an aggregate select item, a non-empty `groupBy`, `aggregationType == "group_by"`, or a non-null `having` MUST NOT reach the broadcast renderer at all, and pays neither its condition, filter, and projection render nor its no-column-metadata error arm
* *AND* the projection-membership arm SHALL be decided LATER, at the broadcast construction site against the RENDERED broadcast projection, because that projection does not exist until the render has run, and a sort key naming a column that is not a bare-column item of it SHALL downgrade the request to the SAME unified unaccelerated fallback, by the same fall-through
* *AND* that decline SHALL be a clean fall-through and MUST NOT be a hard client-facing `User` error, because, unlike the declined single-table row-scan path (`pushdown-capabilities/pushdown-planning-topn`), a renderer that CAN express the shape still exists
* *AND* the adapter MUST NOT drop a sort key, a limit, or an offset it declined to serve on the broadcast path, so no reachable shape returns rows that are silently unordered, unwindowed, or wrongly truncated
* *AND* the bare-`LIMIT`-with-non-zero-offset arm SHALL be a structural guard rather than a reachable branch, because a non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`)
* *AND* an aggregate select item, a non-empty `groupBy`, `aggregationType == "group_by"`, and a non-null `having` SHALL each force the unified unaccelerated fallback unconditionally

### Scenario: Small-side selection uses table-format metadata and the broadcast threshold

* *GIVEN* an inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter evaluates broadcast eligibility
* *THEN* the adapter SHALL compute each side's byte size as the sum of that side's resolved per-file byte sizes, read from the table's own format metadata, an Iceberg manifest's `file_size_in_bytes` for the resolved snapshot, a Delta `add` action's `size` for the resolved version, without opening any Parquet file
* *AND* the adapter SHALL choose the side with the smaller metadata byte size as the broadcast (dimension) side and the other as the sharded (fact) side
* *AND* when the smaller side's byte size is at or below `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL plan the broadcast fan-out
* *AND* when the smaller side's byte size exceeds `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL take the unified unaccelerated fallback instead
* *AND* the threshold SHALL be read from the persisted adapter note `JOIN_BROADCAST_MAX_BYTES`, defaulting to 134217728 when absent or unparseable
* *AND* the sum SHALL saturate, so a side whose byte total overflows `u64` is clamped to `u64::MAX` and is therefore never chosen as the broadcast side

### Scenario: Broadcast join projection and filter are rendered per involved table

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter resolves the projection and renders the WHERE filter
* *THEN* the adapter SHALL resolve each projected column's Exasol output type from the involved table it belongs to, matching the column against that table's involved-table column metadata
* *AND* the scan-driving SQL's declared EMITS column list SHALL match the projected join output columns in order and type
* *AND* a WHERE filter over columns of either side SHALL be rendered via the same path used for single-table filters, the type-rewrite pipeline over the union of both involved tables' column metadata, THEN the `crates/vs-expression` translator over the pipeline's REWRITTEN tree, and carried in the common spec
* *AND* that column-type universe SHALL be read only AFTER the disjoint-column-name guard has passed, because a bare column name resolves to exactly one Exasol type only once the two sides' names are known disjoint
* *AND* a filter that is PRESENT and non-trivial but that DECLINES, because the translator cannot express a node in the tree OR because the type-rewrite pipeline returned no tree, SHALL cause the adapter to decline the broadcast plan and take the unified unaccelerated fallback, exactly as an unrenderable join condition does, because the broadcast SQL carries no outer `WHERE` in which the predicate could be applied
* *AND* the adapter SHALL distinguish an ABSENT or trivially-true filter, which leaves the broadcast plan eligible and emits no scan-spec filter, from a DECLINED one, which forfeits the broadcast plan
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec omits a declined predicate, because the result would carry extra rows, see `pushdown/pushdown-declined-filter-self-apply`
* *AND* a filter the pipeline REWRITES rather than declines, a DATE LIKE subject rewrapped as CAST-to-VARCHAR, a governed string function's argument coerced, a DECIMAL stringification trimmed, SHALL keep the broadcast plan eligible and SHALL be carried in the common spec in its REWRITTEN form, never its raw form
* *AND* a filter the pipeline leaves untriggered SHALL render from the untouched filter
* *AND* the adapter SHALL strip Exasol's native `tableAlias` from every column reference in the filter and in any select-list expression BEFORE rendering, so the rendered SQL is bare-name BY CONSTRUCTION rather than only when Exasol happens not to send an alias, safe because the disjoint-column-name guard above has already proven bare-name resolution is unambiguous

### Scenario: Join condition is rendered via the vs-expression translator

* *GIVEN* a broadcast-eligible inner equi-join carrying an equi-join `condition` expression node
* *WHEN* the adapter renders the join condition
* *THEN* the adapter SHALL render the condition to a DataFusion SQL fragment using the `crates/vs-expression` translator, the same way filter predicates are translated
* *AND* the adapter SHALL strip Exasol's native `tableAlias` from every column reference in the condition BEFORE rendering, so the rendered condition is bare-name BY CONSTRUCTION and resolves against the scan's unaliased derived relations, safe because the disjoint-column-name guard has already proven bare-name resolution is unambiguous
* *AND* the rendered condition SHALL be carried in the common spec so every shard applies the identical join predicate
* *AND* a condition the translator cannot render SHALL cause the adapter to take the unified unaccelerated fallback rather than emit a broadcast plan with a mistranslated predicate
