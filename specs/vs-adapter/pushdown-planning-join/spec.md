# Feature: Pushdown Planning — Broadcast Inner Equi-Join

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the broadcast inner equi-join shape. When Exasol pushes a two-table join whose smaller side is below the broadcast threshold, the adapter resolves both sides' file lists once, shards only the larger (fact) side through the nested distributor + scalar scan fan-out, replicates the smaller (dimension) side's full file list into the shard-invariant common spec, and drives a node-local DataFusion join inside the scalar scan UDF (`datafusion-scan/scan-execution-join`). Every join outside this broadcast contract — above threshold, non-two-table, needing Exasol postprocessing, or otherwise ineligible — is served by the unified unaccelerated fallback renderer (`vs-adapter/pushdown-planning-join-fallback`), so a join is never wrong, only sometimes unaccelerated.

## Background

* The adapter advertises exactly `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`; `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, and any Cartesian-product capability stay unadvertised.
* Each side's data-file list and per-file byte size are resolved exactly once per pushdown, in the planning layer, through the SAME format-neutral resolve-once seam the single-table path uses (`vs-adapter/pushdown-format-neutral-resolution`); no scan UDF invocation discovers files itself.
* The broadcast threshold is read from a VS adapter note (`JOIN_BROADCAST_MAX_BYTES`, default 134217728) and compared against each side's table-metadata byte size — the sum of the per-file sizes that side's format reader resolved, with NO Parquet data read. What that sum MEANS per format (an Iceberg manifest `file_size_in_bytes` sum, a Delta `add`-action `size` sum) is the format reader's decision; this feature owns only the comparison.
* The broadcast contract is: exactly two involved tables, `join_type = "inner"`, an equi-join condition, disjoint column-name sets across the two tables, NO Exasol postprocessing in the request, and a condition/filter/projection the `crates/vs-expression` translator can render; any deviation is served by the unified unaccelerated fallback (`vs-adapter/pushdown-planning-join-fallback`) instead. Broadcast is an optimization selected within the single join path, never a second rendering implementation of that path.
* **Exasol postprocessing means exactly FOUR forcing conditions:** an aggregate select item (`function_aggregate`), a non-empty `groupBy`, `aggregationType == "group_by"`, or a non-null `having`. The in-UDF join renders only projection, filter, and join condition, so an aggregate or a grouping has to execute over the materialized join in Exasol's core engine. A pushed `LIMIT` and a pushed `ORDER BY` are NOT forcing conditions: a bare `LIMIT` and a bare-projected-column `ORDER BY` are served by the broadcast path under this feature's limit and ordering scenarios (issue #307).
* The dimension side rides once in the shard-invariant common spec (full file list, table root, logical schema, join condition); only the fact side's per-shard file subset flows through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor, so every shard joins its fact subset against the same replicated dimension side node-locally.
* **Each side carries its OWN effective storage (issue #294).** The fact side's rides in the whole-spec `storage` value and the dimension side's in the join block, each a connection REFERENCE or sealed envelope per `vs-adapter/scan-spec-credential-reference`, resolved inside the UDF through its own `ctx.connection()` read. The dimension side's entry is serialized ONCE, never per shard. A vended credential is scoped to the table it was loaded for, so neither side is read with the other side's grant. The N-scan fallback gives each leg its own spec and therefore its own storage.
* **Credential, backend-variant, and storage-account divergence between the two sides are served, not rejected.** Each side is read through a store built from its OWN backend, so a cross-variant join and a two-storage-account ADLS join each resolve to two DataFusion registry keys. The only unserveable shape, two ADLS containers of ONE account (which share a registry key while needing different stores), is refused by the scan's own store precondition (`datafusion-scan/scan-execution-join`). No plan-time same-backend guard exists.
* Broadcast SQL has no outer `WHERE`. Its projection is narrowed to the select-list items, so a
  filter-only column is not even in scope for one. Declining to the N-scan fallback — which owns a
  qualified outer `WHERE` — is therefore the only place the predicate can be applied without
  widening the projection, and widening already triggers the recorded projection-widened decline.
* **A present filter that declines forfeits the broadcast plan.** The broadcast renderer distinguishes an absent or trivially-true filter from one that is present but unrenderable, and declines the latter to the N-scan fallback. See `vs-adapter/pushdown-declined-filter-self-apply`.
* **The broadcast WHERE filter runs through the same path as a single-table filter (issue #215).** The type-rewrite pipeline (`apply_type_rewrites`, behind the same owner `classify_where_filter`) runs first, and the `crates/vs-expression` translator then renders the REWRITTEN tree. A type-rewrite decline takes the same `Ok(None)` fall-through as a syntactically unrenderable filter, and that outcome stays owned by `vs-adapter/pushdown-declined-filter-self-apply`. See `vs-adapter/pushdown-planning-like-type-coercion` for the per-surface type dispatch.
* The broadcast site's column-type universe is the UNION of both involved tables' columns, matched
  by bare column name, and it is read only AFTER `disjoint_schema_guard` has passed — which is
  exactly what makes a bare name resolve to one Exasol type. Broadcast rendering is bare-name by
  construction (issue #303, below), so a bare-name universe is the matching one. The ordering is therefore
  load-bearing, not incidental.
* A DATE-column LIKE is a REWRITE, not a decline, so it keeps the broadcast plan. Rendering the
  REWRITTEN tree rather than the raw one is what distinguishes "coerce and stay broadcast" from
  "decline and forfeit broadcast"; rendering the raw tree after a successful rewrite would silently
  discard the coercion and reintroduce the hard scan failure.
* **Broadcast rendering is bare-name BY CONSTRUCTION (issue #303).** `render_broadcast_join` strips Exasol's native `tableAlias` before it renders the join condition, the WHERE filter, and any select-list expression, using the same helper the single-table pushdown chokepoint applies. The scan's derived relations expose bare, unaliased column names, so an alias-qualified `"ALIAS"."NAME"` reference would fail to resolve at scan time. The disjoint-column-name guard makes bare-name resolution unambiguous.
* **The broadcast decline is decided in TWO places that share ONE outcome, and the split is load-bearing.** Which REQUEST shapes force Exasol postprocessing is decided once, from the `pushdown` request alone, BEFORE either side is sized or the broadcast render is attempted — so an aggregate-carrying join still short-circuits without paying a condition/filter/projection render. Whether the RENDERED broadcast projection can bind a given sort key is decided at the broadcast construction site, because the projection does not exist until the render has run. Both funnel into the SAME single decline outcome: fall through to the unified unaccelerated fallback. Neither site may decline for the other's reason.
* **A bare `LIMIT n` is safe to apply BOTH per shard and once on the merge, because an unordered `LIMIT n` means "any n qualifying rows".** This is the same argument, and the same shape, the single-table row-scan path has always used: the FULL, undivided `n` goes into every shard's scan spec AND is rendered again on the outer scalar select over the fan-out (`vs-adapter/pushdown-planning`). A shard that produces fewer than `n` joined rows cannot make the merged result short, because the merge is the union of every shard's output and the outer `LIMIT n` only ever truncates it.
* **A per-shard bound is applied POST-join only, as the join block's `post_join_limit` and `post_join_order_by` fields, never as a cap on the fact side's pre-join scan.** `datafusion-scan/scan-execution-join` states why an input-side cap would be incorrect.
* **An ordered broadcast window is always served by ONE outer wrapper over the merged fan-out.** One Exasol-side `SELECT <visible items> FROM (<broadcast fan-out>) ORDER BY <keys> [LIMIT n [OFFSET m]]` produces the final answer, and the fan-out's own outer scalar select carries no `LIMIT`. Rendering the request's `LIMIT` on the fan-out would truncate the merged rows to an arbitrary `n` BEFORE the global sort.
* **A zero-offset `ORDER BY … LIMIT n` also bounds each shard to its own post-join top-n (#309).** The join block carries the sort keys and `n`, so the wrapper merges at most `G × n` rows. Correctness is the distributed top-N argument of `vs-adapter/pushdown-planning-topn`, and it holds under ties. Every row ranked strictly ahead of the global n-th key survives its shard's cut. Each shard keeps at least as many rows tied at that key as the global answer needs. `datafusion-scan/scan-execution-join` owns how a shard ranks keys.
* **The ordered broadcast path accepts ONLY sort keys that are bare columns already present in the broadcast projection.** An expression sort key, an aggregate sort key, an element missing its direction or NULL-placement flag, and a bare column absent from the projection all DECLINE the broadcast plan and take the unified unaccelerated fallback, whose qualified wrapper renders every one of them table-qualified from its owning side. This is a clean fall-through, never the hard `User` decline `vs-adapter/pushdown-planning-topn` specifies for a row scan: on a row scan there is no other renderer left, whereas here the fallback renders the ordering correctly. Requiring projection membership is what lets the wrapper bind its `ORDER BY` to the fan-out's emitted identifiers without the hidden-sort-key-column machinery the row-scan decline path needs.
* **An OFFSET is only ever rendered on the ordered wrapper, never per shard.** `ScanSpec` has no offset field and gains none, matching the recorded invariant in `vs-adapter/pushdown-planning-topn`. A bare `LIMIT` carrying a NON-ZERO `limit.offset` and no `orderBy` declines the broadcast plan outright: the "any n qualifying rows" argument that licenses the per-shard cap does not extend to a window, and a per-shard `OFFSET m` would skip each shard's OWN first `m` rows. That shape is additionally unreachable in production — a non-zero offset never arrives without a non-empty `orderBy` (the offset-implies-ordering invariant, `vs-adapter/pushdown-planning-order-by-capability`) — so the guard is a structural decline, not a branch any request exercises.
* **Which of the forcing conditions declined a broadcast plan is not observable.** `EXPLAIN VIRTUAL` surfaces only the adapter's returned `sql`, and no log or trace distinguishes the reasons; a silently-suppressed broadcast looks like an ordinary fallback. This gap has no tracking issue yet, and a contributor who works on it files one first.

## Scenarios

### Scenario: Adapter advertises inner equi-join capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`
* *AND* the capabilities list MUST NOT include `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, or any Cartesian-product capability
* *AND* each advertised join capability SHALL be backed by the join-planning path in this feature and its fallback counterpart, so an advertised capability is never a shape the planner cannot serve either by broadcast or by the unified unaccelerated fallback

### Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a `pushdown` request whose `from` clause is a `join` node over exactly two involved tables joined by an equi-condition
* *AND* the smaller side's table-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH sides through the SAME format-reader seam the single-table scan uses (`vs-adapter/pushdown-format-neutral-resolution`) — obtaining each side's data-file list, per-file byte size, logical schema, table root, and effective storage exactly once — recovering each table's original-cased catalog identifier from the schema-metadata mapping by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards and driven through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, logical schema, and its OWN storage value (a connection REFERENCE or sealed envelope per `vs-adapter/scan-spec-credential-reference`) in the shard-invariant common spec's join block, and the fact side's storage and per-shard file subset through the distributor
* *AND* the generated scan-driving SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange, and with NO `SELECT * FROM (...)` wrapper for an UNORDERED request; an ORDERED request carries the outer wrapper this feature's ordering scenario specifies (issue #307)
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer
* *AND* a join whose two sides are DELTA tables reached through Unity Catalog SHALL take this same broadcast path with no Iceberg-specific step, because the resolution seam and the broadcast decision read only neutral resolved values

### Scenario: A bare LIMIT over a broadcast-eligible join is served by the broadcast path with a per-shard post-join cap

* *GIVEN* a `pushdown` request that is broadcast-eligible in every other respect — exactly two involved tables, a `predicate_equal` condition, the dimension side at or below the broadcast threshold, disjoint column names, a renderable condition and filter, a non-widened projection
* *AND* the request carries a `limit` with `numElements` = `n`, an ABSENT or EMPTY `orderBy`, and a zero or absent `limit.offset`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out for that request and MUST NOT route it to the unified unaccelerated fallback (issue #307)
* *AND* the shard-invariant common scan spec's JOIN BLOCK SHALL carry a post-join cap of `n`, so every fact-shard invocation truncates its OWN joined output at `n` inside its DataFusion session rather than emitting its complete local join
* *AND* the common scan spec's own shard-invariant row-limit field SHALL remain absent, so the cap exists only where a join block does and no fallback leg spec can express one (`vs-adapter/pushdown-planning-join-fallback`)
* *AND* that per-shard cap SHALL be applied AFTER the join and its WHERE filter, never to the fact side's pre-join scan, because an input-side cap discards fact rows that would have matched and keeps fact rows that produce no output (`datafusion-scan/scan-execution-join`)
* *AND* the generated scan-driving SQL SHALL render the SAME undivided `LIMIT n` once on the outer ungrouped scalar select over the distributor subquery, through the row-scan builder's existing limit seam, so the merge of up to `G × n` shard rows is truncated to `n`
* *AND* the adapter MUST NOT divide `n` across shards, because a shard producing fewer than `n` joined rows would then make the merged result short
* *AND* the returned result SHALL be `n` rows whenever the unbounded join produces at least `n`, and SHALL be a subset of the unbounded join's rows in every case, equal to some valid single-node `LIMIT n` answer

### Scenario: A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out

* *GIVEN* a `pushdown` request that is broadcast-eligible in every other respect, carrying a non-empty `orderBy` whose every element is a bare `column` node that carries both `isAscending` and `nullsLast` and names a bare-column item of the broadcast projection, and an optional `limit` with `numElements` = `n` and an optional `limit.offset` = `m`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL serve the request with the broadcast fan-out, MUST NOT route it to the unified unaccelerated fallback (issue #307), and SHALL wrap that fan-out in ONE outer `SELECT <visible projection items> FROM (<fan-out>) ORDER BY <keys>` plus the request's full retained window, rendering the window through the SAME shared limit-and-offset seam every other wrapper uses (` LIMIT n` byte-for-byte for a zero or absent offset, ` LIMIT n OFFSET m` for a non-zero one) and the select list and keys through the SAME shared emitted-identifier and direction/NULL-placement seams the declined row-scan wrapper uses (`vs-adapter/pushdown-planning-topn`)
* *AND* the fan-out's own outer scalar select SHALL carry NO `LIMIT`, and the shard-invariant common scan spec SHALL carry NO `limit` and NO `order_by` key, so the wrapper is the ONLY place the global ordering and window are applied; what each shard's join block carries is set by this feature's two per-shard ordered-window scenarios
* *AND* an UNORDERED broadcast request — with or without a bare `LIMIT` — SHALL still be emitted as the bare fan-out with no wrapping select, byte-identically to its pre-change output
* *AND* the adapter MUST NOT emit a broadcast ordered plan whose outer wrapper renders NO `ORDER BY`: a wrapper that returned the fan-out unchanged SHALL instead decline to the unified unaccelerated fallback rather than return rows that are silently unordered while `ORDER_BY_COLUMN` is advertised
* *AND* the returned result SHALL equal the same `ORDER BY … [LIMIT n [OFFSET m]]` evaluated over the same inner join on a single node

### Scenario: A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard to its own post-join top-N

* *GIVEN* a `pushdown` request that the ordered broadcast wrapper serves, carrying a `limit` with `numElements` = `n` and a zero or absent `limit.offset`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the shard-invariant common scan spec's JOIN BLOCK SHALL carry a post-join ordering equal to the request's sort keys, in pushed order, each with its column, direction, and NULL placement, together with a post-join cap of `n`, so every fact-shard invocation emits at most `n` joined rows, its own top-ranked ones (issue #309)
* *AND* the adapter MUST NOT divide `n` across shards, because every row of the global top-n can come from one shard
* *AND* a request carrying `limit.offset` EQUAL TO ZERO SHALL produce the same SQL as a request carrying no `offset` key, so the eligibility test is a non-zero test and not a key-presence test
* *AND* a request with no `limit` or a non-zero offset SHALL carry NEITHER post-join field, so every shard emits its complete local joined output for the wrapper to sort and window
* *AND* the returned result SHALL carry the same sort-key values, in the same order, as the same `ORDER BY … LIMIT n` evaluated over the same inner join on a single node, and every returned row SHALL be a row of that join

### Scenario: Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback

* *GIVEN* a `pushdown` request over a broadcast-eligible inner equi-join carrying one of: a `limit` with a NON-ZERO `limit.offset` and an absent or empty `orderBy`; an `orderBy` element that is an expression, an aggregate, or any non-`column` node; an `orderBy` element missing `isAscending` or `nullsLast`; or an `orderBy` element naming a column that is not a bare-column item of the broadcast projection
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL decline the broadcast plan and route the request to the unified unaccelerated fallback, whose qualified wrapper renders the ordering and the window table-qualified over the materialized join
* *AND* every arm EXCEPT projection membership SHALL be decided from the `pushdown` request ALONE, BEFORE either side is sized and before the broadcast render runs — so a request carrying an aggregate select item, a non-empty `groupBy`, `aggregationType == "group_by"`, or a non-null `having` MUST NOT reach the broadcast renderer at all, and pays neither its condition, filter, and projection render nor its no-column-metadata error arm
* *AND* the projection-membership arm SHALL be decided LATER, at the broadcast construction site against the RENDERED broadcast projection, because that projection does not exist until the render has run — and a sort key naming a column that is not a bare-column item of it SHALL downgrade the request to the SAME unified unaccelerated fallback, by the same fall-through
* *AND* that decline SHALL be a clean fall-through and MUST NOT be a hard client-facing `User` error, because — unlike the declined single-table row-scan path (`vs-adapter/pushdown-planning-topn`) — a renderer that CAN express the shape still exists
* *AND* the adapter MUST NOT drop a sort key, a limit, or an offset it declined to serve on the broadcast path, so no reachable shape returns rows that are silently unordered, unwindowed, or wrongly truncated
* *AND* the bare-`LIMIT`-with-non-zero-offset arm SHALL be a structural guard rather than a reachable branch, because a non-zero `limit.offset` never arrives without a non-empty `orderBy` (`vs-adapter/pushdown-planning-order-by-capability`)
* *AND* an aggregate select item, a non-empty `groupBy`, `aggregationType == "group_by"`, and a non-null `having` SHALL each continue to force the unified unaccelerated fallback unconditionally, unchanged by this delta

### Scenario: Small-side selection uses table-format metadata and the broadcast threshold

* *GIVEN* an inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter evaluates broadcast eligibility
* *THEN* the adapter SHALL compute each side's byte size as the sum of that side's resolved per-file byte sizes, read from the table's own format metadata — an Iceberg manifest's `file_size_in_bytes` for the resolved snapshot, a Delta `add` action's `size` for the resolved version — without opening any Parquet file
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
* *AND* a WHERE filter over columns of either side SHALL be rendered via the same path used for single-table filters — the type-rewrite pipeline over the union of both involved tables' column metadata, THEN the `crates/vs-expression` translator over the pipeline's REWRITTEN tree — and carried in the common spec
* *AND* that column-type universe SHALL be read only AFTER the disjoint-column-name guard has passed, because a bare column name resolves to exactly one Exasol type only once the two sides' names are known disjoint
* *AND* a filter that is PRESENT and non-trivial but that DECLINES — because the translator cannot express a node in the tree OR because the type-rewrite pipeline returned no tree — SHALL cause the adapter to decline the broadcast plan and take the unified unaccelerated fallback, exactly as an unrenderable join condition already does, because the broadcast SQL carries no outer `WHERE` in which the predicate could be applied
* *AND* the adapter SHALL distinguish an ABSENT or trivially-true filter, which leaves the broadcast plan eligible and emits no scan-spec filter, from a DECLINED one, which forfeits the broadcast plan
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec omits a declined predicate, because the result would carry extra rows — see `vs-adapter/pushdown-declined-filter-self-apply`
* *AND* a filter the pipeline REWRITES rather than declines — a DATE LIKE subject rewrapped as CAST-to-VARCHAR, a governed string function's argument coerced, a DECIMAL stringification trimmed — SHALL keep the broadcast plan eligible and SHALL be carried in the common spec in its REWRITTEN form, never its raw form
* *AND* a filter the pipeline leaves untriggered SHALL render byte-identically to its pre-change output, so no golden-SQL fixture over such a filter changes
* *AND* the adapter SHALL strip Exasol's native `tableAlias` from every column reference in the filter and in any select-list expression BEFORE rendering, so the rendered SQL is bare-name BY CONSTRUCTION rather than only when Exasol happens not to send an alias — safe because the disjoint-column-name guard above has already proven bare-name resolution is unambiguous

### Scenario: Join condition is rendered via the vs-expression translator

* *GIVEN* a broadcast-eligible inner equi-join carrying an equi-join `condition` expression node
* *WHEN* the adapter renders the join condition
* *THEN* the adapter SHALL render the condition to a DataFusion SQL fragment using the `crates/vs-expression` translator, the same way filter predicates are translated
* *AND* the adapter SHALL strip Exasol's native `tableAlias` from every column reference in the condition BEFORE rendering, so the rendered condition is bare-name BY CONSTRUCTION and resolves against the scan's unaliased derived relations — safe because the disjoint-column-name guard has already proven bare-name resolution is unambiguous
* *AND* the rendered condition SHALL be carried in the common spec so every shard applies the identical join predicate
* *AND* a condition the translator cannot render SHALL cause the adapter to take the unified unaccelerated fallback rather than emit a broadcast plan with a mistranslated predicate
