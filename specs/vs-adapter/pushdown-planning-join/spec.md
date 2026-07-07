# Feature: Pushdown Planning — Broadcast Inner Equi-Join

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the single-two-table inner equi-join shape. When Exasol pushes a join whose smaller side is below the broadcast threshold, the adapter resolves BOTH sides' file lists once, shards only the larger (fact) side, replicates the smaller (dimension) side's full file list into the shard-invariant common spec, and drives a node-local DataFusion join inside the scan UDF (`datafusion-scan/scan-execution-join`). A join whose smaller side exceeds the threshold — or whose shape falls outside the broadcast contract — is executed WITHOUT broadcast (each table scanned independently, joined by Exasol's core engine), so a join is never wrong, only sometimes unaccelerated.

## Background

* The adapter advertises exactly `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`; `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, and any Cartesian-product capability stay unadvertised.
* Both sides' Iceberg snapshot, data-file list, and per-file byte size are resolved exactly once per pushdown, in the planning layer; neither scan UDF invocation discovers files itself.
* The broadcast threshold is read from a VS adapter note (`JOIN_BROADCAST_MAX_BYTES`, default 134217728) and compared against each side's Iceberg-metadata byte size — computed from manifest `file_size_in_bytes`, with NO Parquet data read.
* The broadcast contract is: exactly two involved tables, `join_type = "inner"`, an equi-join condition, disjoint column-name sets across the two tables, and a condition/filter/projection the `crates/vs-expression` translator can render; any deviation takes the unaccelerated fallback (or, only when even the fallback cannot be built, an error so Exasol retries natively).
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.

## Scenarios

### Scenario: Adapter advertises inner equi-join capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`
* *AND* the capabilities list MUST NOT include `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, or any Cartesian-product capability
* *AND* each advertised join capability SHALL be backed by the join-planning path in this feature, so an advertised capability is never a shape the planner cannot serve either by broadcast or by the unaccelerated fallback

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
* *AND* when the smaller side's byte size exceeds `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL take the unaccelerated fallback instead
* *AND* the threshold SHALL be read from the persisted adapter note `JOIN_BROADCAST_MAX_BYTES`, defaulting to 134217728 when absent or unparseable

### Scenario: Join above the broadcast threshold falls back to an unaccelerated two-scan join

* *GIVEN* an inner equi-join `pushdown` request whose smaller side exceeds `JOIN_BROADCAST_MAX_BYTES`
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit SQL that scans EACH table independently through its own sharded scan-UDF fan-out subquery and joins the two subquery results with `INNER JOIN ... ON <condition>` executed by Exasol's core engine
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
* *AND* the adapter MUST NOT push either side's rows through a broadcast replication for this shape

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
* *AND* a condition the translator cannot render SHALL cause the adapter to take the unaccelerated fallback rather than emit a broadcast plan with a mistranslated predicate

### Scenario: A join outside the broadcast contract is declined safely

* *GIVEN* a `pushdown` request whose `from` clause is a join
* *WHEN* the join is not a single two-table inner equi-join — it is non-inner, non-equi, spans more than two involved tables, has overlapping column names across the two tables, or carries a condition/filter/projection the translator cannot render
* *THEN* the adapter SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL instead emit the unaccelerated two-scan join SQL when it can build one, so Exasol's core engine produces the correct result
* *AND* only when even the unaccelerated fallback cannot be built SHALL the adapter return an error so Exasol retries the query natively
* *AND* the adapter MUST NOT emit any scan spec that would compute a different result than single-node evaluation

### Scenario: Shared-column-name join uses qualified two-scan, not bare-name broadcast rendering

* *GIVEN* an inner equi-join `pushdown` request over two involved tables that share a column name (e.g. both have an `id` column)
* *WHEN* the adapter builds the unaccelerated two-scan fallback SQL
* *THEN* the adapter SHALL render the join condition, WHERE filter, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references (`"LHS_FACT"."COL"` / `"LHS_DIM"."COL"`), resolved from each `column` node's `tableName` against the side that owns it — never against a combined bare-name schema
* *AND* the disjoint-column-name guard SHALL gate broadcast eligibility only, NOT the two-scan fallback's rendering path
* *AND* a disjoint-guard failure SHALL be treated as a plain reason the broadcast path is unavailable, not as an error, so the request falls through to the qualified two-scan SQL instead of a hard `Err`
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node

### Scenario: Aggregate over a join routes through the qualified two-scan wrapper

* *GIVEN* an inner equi-join `pushdown` request whose select list, GROUP BY, HAVING, ORDER BY, or LIMIT requires Exasol postprocessing (an aggregate, `GROUP BY`, `ORDER BY`, `LIMIT`, or `HAVING`)
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL route the request to the qualified two-scan path unconditionally, regardless of whether the join would otherwise be broadcast-eligible, because the broadcast in-UDF join renders only projection, filter, and join condition
* *AND* the two-scan wrapper SHALL render the aggregate select list as ordinary Exasol SQL over the materialized join (`SELECT <aggregates> FROM (fact fan-out) JOIN (dim fan-out) ON … [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`), splicing Exasol's own aggregate function name verbatim while table-qualifying only its column argument
* *AND* the returned result SHALL equal the result of evaluating the same aggregate over the same inner equi-join on a single node
