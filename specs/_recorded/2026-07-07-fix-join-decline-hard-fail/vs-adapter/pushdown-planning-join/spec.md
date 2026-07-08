# Feature: Pushdown Planning — Broadcast Inner Equi-Join

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the inner equi-join shape. When Exasol pushes a two-table join whose smaller side is below the broadcast threshold, the adapter resolves both sides' file lists once, shards only the larger (fact) side, replicates the smaller (dimension) side's full file list into the shard-invariant common spec, and drives a node-local DataFusion join inside the scan UDF (`datafusion-scan/scan-execution-join`). Every join whose smaller side exceeds the threshold, whose select list needs Exasol postprocessing, whose shape falls outside the broadcast contract, or that spans three or more involved tables is executed WITHOUT broadcast through a single unified per-table-scan fallback renderer (each involved table scanned independently, all N reconstructed by Exasol's core engine), so a join is never wrong, only sometimes unaccelerated. Broadcast is an optimization selected inside the one join path; the unaccelerated fallback has exactly one implementation and the two-involved-table case is simply N = 2.

## Background

* The adapter advertises exactly `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`; `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, and any Cartesian-product capability stay unadvertised.
* Both sides' Iceberg snapshot, data-file list, and per-file byte size are resolved exactly once per pushdown, in the planning layer; no scan UDF invocation discovers files itself.
* The broadcast threshold is read from a VS adapter note (`JOIN_BROADCAST_MAX_BYTES`, default 134217728) and compared against each side's Iceberg-metadata byte size — computed from manifest `file_size_in_bytes`, with NO Parquet data read.
* The broadcast contract is: exactly two involved tables, `join_type = "inner"`, an equi-join condition, disjoint column-name sets across the two tables, no Exasol postprocessing (aggregate / GROUP BY / HAVING / ORDER BY / LIMIT) in the request, and a condition/filter/projection the `crates/vs-expression` translator can render; any deviation is served by the unaccelerated fallback instead.
<!-- DELTA:CHANGED -->
* The unaccelerated fallback is a SINGLE unified renderer for every inner join with N ≥ 2 involved tables: the two-involved-table case is exactly N = 2, and there is only one implementation. Each involved table is scanned through its own sharded scan-UDF fan-out subquery, and all N subquery results are reconstructed into the original inner join by Exasol's core engine. Broadcast (strictly two-table, node-local in the scan UDF) is an optimization SELECTED WITHIN this one join path, not a second rendering implementation; when broadcast is unavailable for a two-table join it takes the same N = 2 unified fallback.
<!-- /DELTA:CHANGED -->
<!-- DELTA:NEW -->
* Because each advertised capability is served statically and Exasol never re-plans on an adapter error (a declined pushdown is erased by the `exasol-udf-macros` FFI shim into a hard `F-UDF-CL-RUST-9001` SQL error — there is NO native-retry response in the protocol), an advertised join capability MUST always be renderable by the join path: either by broadcast or by the unified unaccelerated fallback. "Decline at runtime and let Exasol retry natively" is not an available behavior. A hard error is a genuine last resort, raised ONLY for a shape the adapter cannot render at all — a non-inner join node in the tree, an involved table absent from `TABLE_MAP` or carrying no column metadata, or a join condition/clause the translator cannot render — and it is a hard client-facing error, not a retry.
<!-- /DELTA:NEW -->
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.

## Scenarios

### Scenario: Adapter advertises inner equi-join capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`
* *AND* the capabilities list MUST NOT include `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, or any Cartesian-product capability
* *AND* each advertised join capability SHALL be backed by the join-planning path in this feature, so an advertised capability is never a shape the planner cannot serve either by broadcast or by the unified unaccelerated fallback

### Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a `pushdown` request whose `from` clause is a `type: "join"`, `join_type: "inner"` node over exactly two involved tables joined by an equi-condition
* *AND* the smaller side's Iceberg-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH tables' Iceberg snapshot, data-file list, per-file byte size, and logical schema exactly once, recovering each table's original-cased Iceberg identifier from `TABLE_MAP` by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, and logical schema in the shard-invariant common spec (serialized once for the whole fan-out), and the fact side's per-shard file subset in the per-shard argument
* *AND* the generated scan-driving SQL SHALL drive the scan SET UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer — only file-level metadata crosses into the scan spec

### Scenario: Small-side selection uses Iceberg metadata and the broadcast threshold

* *GIVEN* an inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter evaluates broadcast eligibility
* *THEN* the adapter SHALL compute each side's byte size from its Iceberg manifest `file_size_in_bytes` sum for the resolved snapshot, without opening any Parquet file
* *AND* the adapter SHALL choose the side with the smaller metadata byte size as the broadcast (dimension) side and the other as the sharded (fact) side
* *AND* when the smaller side's byte size is at or below `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL plan the broadcast fan-out
* *AND* when the smaller side's byte size exceeds `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL take the unified unaccelerated fallback instead
* *AND* the threshold SHALL be read from the persisted adapter note `JOIN_BROADCAST_MAX_BYTES`, defaulting to 134217728 when absent or unparseable

<!-- DELTA:CHANGED -->
### Scenario: Join above the broadcast threshold falls back to the unified unaccelerated wrapper

* *GIVEN* an inner equi-join `pushdown` request over two involved tables whose smaller side exceeds `JOIN_BROADCAST_MAX_BYTES`
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit SQL through the SINGLE unified N-scan fallback renderer with N = 2 — scanning each table independently through its own sharded scan-UDF fan-out subquery and reconstructing the inner join over both subquery results in Exasol's core engine
* *AND* the two-involved-table case SHALL use the identical fallback code path as the three-or-more-table case (there is exactly one unaccelerated join renderer), differing only in the number of scanned sides
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
* *AND* the adapter MUST NOT push either side's rows through a broadcast replication for this shape
<!-- /DELTA:CHANGED -->

### Scenario: Join projection and EMITS span both involved tables

* *GIVEN* a broadcast-eligible inner equi-join whose select list projects columns from both involved tables
* *WHEN* the adapter builds the scan spec
* *THEN* the adapter SHALL resolve each projected column's Exasol output type from the involved table it belongs to, matching the column against that table's involved-table column metadata
* *AND* the scan-driving SQL's declared EMITS column list SHALL match the projected join output columns in order and type
* *AND* a WHERE filter over columns of either side SHALL be rendered via the same `crates/vs-expression` translator path used for single-table filters and carried in the common spec

### Scenario: Join condition is rendered via the vs-expression translator

* *GIVEN* a broadcast-eligible inner equi-join carrying an equi-join `condition` expression node
* *WHEN* the adapter renders the join condition
* *THEN* the adapter SHALL render the condition to a DataFusion SQL fragment using the `crates/vs-expression` translator, the same way filter predicates are translated
* *AND* the rendered condition SHALL be carried in the common spec so every shard applies the identical join predicate
* *AND* a condition the translator cannot render SHALL cause the adapter to take the unified unaccelerated fallback rather than emit a broadcast plan with a mistranslated predicate

<!-- DELTA:CHANGED -->
### Scenario: A join outside the broadcast contract is declined safely

* *GIVEN* a `pushdown` request whose `from` clause is a join
* *WHEN* the join is not a single broadcast-eligible two-table inner equi-join — it is above threshold, non-equi, spans more than two involved tables, has overlapping column names across tables, needs Exasol postprocessing, or carries a condition/filter/projection detail that keeps it off the broadcast path
* *THEN* the adapter SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL instead render the request through the SINGLE unified unaccelerated per-table-scan fallback (N ≥ 2 involved tables, the two-table case being N = 2), so Exasol's core engine produces the correct result
* *AND* spanning more than two involved tables, or having overlapping column names, or needing Exasol postprocessing SHALL by itself NEVER be a reason to return an error — every such inner join is served by the unified fallback
* *AND* the adapter SHALL return a HARD error — a client-facing `F-UDF-CL-RUST-9001`, NOT a request that Exasol retries natively (Exasol does not re-plan on an adapter error) — ONLY when it genuinely cannot render what it advertised: a non-inner join node in the tree, an involved table absent from `TABLE_MAP` or carrying no column metadata, or a condition/clause the translator cannot render at all
* *AND* the adapter MUST NOT emit any scan spec that would compute a different result than single-node evaluation
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over three or more involved tables (e.g. `supplier ⋈ nation ⋈ region`, `customer ⋈ orders ⋈ lineitem`, or `part ⋈ partsupp ⋈ supplier ⋈ nation`), every join node of which is `join_type = "inner"`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT return an error and SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL serve the request through the SAME single unified fallback renderer used for the two-table (N = 2) case, differing only in the number of involved tables
* *AND* the adapter SHALL resolve each involved table's Iceberg snapshot, data-file list, and logical schema exactly once — recovering each table's original-cased Iceberg identifier from `TABLE_MAP` by its involved-table name — and SHALL treat an involved table absent from `TABLE_MAP` as the same stale-virtual-schema hard error the single-table path reports
* *AND* the adapter SHALL emit SQL that scans EACH involved table independently through its own sharded scan-UDF fan-out subquery and reconstructs the original inner join over all N subquery results in Exasol's core engine, carrying every one of the tree's join conditions AND-conjoined in a table-qualified WHERE
* *AND* every join condition, WHERE filter, select-list item, GROUP BY, HAVING, and ORDER BY the wrapper renders SHALL use table-qualified column references resolved from each `column` node's `tableName` against the involved table that owns it, so the wrapper is correct whether or not any two involved tables share a column name
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same inner join evaluated on a single node
* *AND* the adapter MUST NOT read any involved table's Parquet row data in the planning layer — only file-level metadata crosses into each side's scan spec
<!-- /DELTA:CHANGED -->

### Scenario: Shared-column-name join uses qualified rendering, not bare-name broadcast rendering

* *GIVEN* an inner equi-join `pushdown` request over two involved tables that share a column name (e.g. both have an `id` column)
* *WHEN* the adapter builds the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL render the join condition, WHERE filter, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references resolved from each `column` node's `tableName` against the side that owns it — never against a combined bare-name schema
* *AND* the disjoint-column-name guard SHALL gate broadcast eligibility only, NOT the unified fallback's rendering path
* *AND* a disjoint-guard failure SHALL be treated as a plain reason the broadcast path is unavailable, not as an error, so the request falls through to the qualified unified fallback SQL instead of a hard `Err`
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node

<!-- DELTA:CHANGED -->
### Scenario: Aggregate over a join routes through the unified qualified wrapper

* *GIVEN* an inner equi-join `pushdown` request whose select list, GROUP BY, HAVING, ORDER BY, or LIMIT requires Exasol postprocessing (an aggregate, `GROUP BY`, `ORDER BY`, `LIMIT`, or `HAVING`)
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL route the request to the unified qualified fallback path unconditionally, regardless of whether the join would otherwise be broadcast-eligible, because the broadcast in-UDF join renders only projection, filter, and join condition
* *AND* the fallback wrapper SHALL render the aggregate select list as ordinary Exasol SQL over the materialized join (`SELECT <aggregates> FROM (side-0 fan-out) "LHS_T0", (side-1 fan-out) "LHS_T1", … WHERE <conditions> [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`), splicing Exasol's own aggregate function name verbatim while table-qualifying only its column argument(s)
* *AND* a select-list item that is a SCALAR FUNCTION WRAPPING one or more aggregates (e.g. `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)`) SHALL be rendered by recursing through the `crates/vs-expression` translator, which renders nested `function_aggregate` nodes by splicing the aggregate name verbatim and rendering the argument(s) — NOT declined
* *AND* the returned result SHALL equal the result of evaluating the same select list over the same inner join on a single node
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A scalar function wrapping aggregates in a grouped join select list is rendered, not declined

* *GIVEN* an inner equi-join `pushdown` request (two-table or three-or-more-table) whose grouped select list contains a select item that is a scalar function wrapping one or more aggregates — e.g. `ROUND(100.0 * SUM(CASE WHEN l_returnflag = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2)` — alongside plain aggregates such as `SUM(l_quantity)`, `SUM(CASE WHEN … END)`, and `AVG(l_extendedprice)`, with a GROUP BY, a HAVING, an ORDER BY, and a LIMIT
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT decline the request and SHALL NOT return an error for the scalar-over-aggregate select item
* *AND* the adapter SHALL render each such select item by recursing through the `crates/vs-expression` translator, which renders the outer scalar function (`ROUND`, arithmetic, `CASE`, …) around nested `function_aggregate` nodes whose aggregate names (`SUM`, `COUNT`, `AVG`, …) are spliced verbatim and whose column arguments are table-qualified from their `tableName`
* *AND* a top-level bare aggregate and a nested aggregate SHALL be rendered by the same aggregate-rendering path, so the two produce consistent SQL
* *AND* the emitted SQL SHALL be the unified qualified fallback wrapper (`LHS_T0`, `LHS_T1`, … subqueries) with the grouped select list, HAVING, ORDER BY, and LIMIT rendered over the materialized join
* *AND* the returned result SHALL equal the result of evaluating the same grouped, scalar-over-aggregate select list over the same inner join on a single node
<!-- /DELTA:NEW -->
