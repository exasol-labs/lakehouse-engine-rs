# Feature: Pushdown Planning — Broadcast Inner Equi-Join

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the broadcast inner equi-join shape. When Exasol pushes a two-table join whose smaller side is below the broadcast threshold, the adapter resolves both sides' file lists once, shards only the larger (fact) side through the nested distributor + scalar scan fan-out, replicates the smaller (dimension) side's full file list into the shard-invariant common spec, and drives a node-local DataFusion join inside the scalar scan UDF (`datafusion-scan/scan-execution-join`). Every join outside this broadcast contract — above threshold, non-two-table, needing Exasol postprocessing, or otherwise ineligible — is served by the unified unaccelerated fallback renderer (`vs-adapter/pushdown-planning-join-fallback`), so a join is never wrong, only sometimes unaccelerated.

## Background

* The broadcast contract already requires "a condition/filter/projection the `crates/vs-expression`
  translator can render; any deviation is served by the unified unaccelerated fallback". This delta
  does not change that contract — it makes the FILTER half of it actually enforced. The broadcast
  renderer previously conflated an absent filter with a filter present but unrenderable, so a
  declined filter produced a broadcast plan carrying no filter at all, which no clause then applied.
  See `vs-adapter/pushdown-declined-filter-self-apply`.
* Broadcast SQL has no outer `WHERE`. Its projection is narrowed to the select-list items, so a
  filter-only column is not even in scope for one. Declining to the N-scan fallback — which owns a
  qualified outer `WHERE` — is therefore the only place the predicate can be applied without
  widening the projection, and widening already triggers the recorded projection-widened decline.
* The adapter advertises exactly `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`; `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, and any Cartesian-product capability stay unadvertised.
* Both sides' Iceberg snapshot, data-file list, and per-file byte size are resolved exactly once per pushdown, in the planning layer; no scan UDF invocation discovers files itself.
* The broadcast threshold is read from a VS adapter note (`JOIN_BROADCAST_MAX_BYTES`, default 134217728) and compared against each side's Iceberg-metadata byte size — computed from manifest `file_size_in_bytes`, with NO Parquet data read.
* The broadcast contract is: exactly two involved tables, `join_type = "inner"`, an equi-join condition, disjoint column-name sets across the two tables, no Exasol postprocessing (aggregate / GROUP BY / HAVING / ORDER BY / LIMIT) in the request, and a condition/filter/projection the `crates/vs-expression` translator can render; any deviation is served by the unified unaccelerated fallback (`vs-adapter/pushdown-planning-join-fallback`) instead. Broadcast is an optimization selected within the single join path, never a second rendering implementation of that path.
* The dimension side rides once in the shard-invariant common spec (full file list, table root, logical schema, join condition); only the fact side's per-shard file subset flows through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor, so every shard joins its fact subset against the same replicated dimension side node-locally.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.

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
* *AND* the smaller side's Iceberg-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH tables' Iceberg snapshot, data-file list, per-file byte size, and logical schema exactly once, recovering each table's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards and driven through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, and logical schema in the shard-invariant common spec (spliced once as the `LAKEHOUSE_SCAN` scalar UDF's first argument), and the fact side's per-shard file subset flowed through the distributor as the second argument
* *AND* the generated scan-driving SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange and no `SELECT * FROM (...)` wrapper
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer — only file-level metadata crosses into the scan spec

### Scenario: Small-side selection uses Iceberg metadata and the broadcast threshold

* *GIVEN* an inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter evaluates broadcast eligibility
* *THEN* the adapter SHALL compute each side's byte size from its Iceberg manifest `file_size_in_bytes` sum for the resolved snapshot, without opening any Parquet file
* *AND* the adapter SHALL choose the side with the smaller metadata byte size as the broadcast (dimension) side and the other as the sharded (fact) side
* *AND* when the smaller side's byte size is at or below `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL plan the broadcast fan-out
* *AND* when the smaller side's byte size exceeds `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL take the unified unaccelerated fallback instead
* *AND* the threshold SHALL be read from the persisted adapter note `JOIN_BROADCAST_MAX_BYTES`, defaulting to 134217728 when absent or unparseable

### Scenario: Broadcast join projection and filter are rendered per involved table

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter resolves the projection and renders the WHERE filter
* *THEN* the adapter SHALL resolve each projected column's Exasol output type from the involved table it belongs to, matching the column against that table's involved-table column metadata
* *AND* the scan-driving SQL's declared EMITS column list SHALL match the projected join output columns in order and type
* *AND* a WHERE filter over columns of either side SHALL be rendered via the same `crates/vs-expression` translator path used for single-table filters and carried in the common spec
* *AND* a filter that is PRESENT and non-trivial but that the translator DECLINES SHALL cause the adapter to decline the broadcast plan and take the unified unaccelerated fallback, exactly as an unrenderable join condition already does, because the broadcast SQL carries no outer `WHERE` in which the predicate could be applied
* *AND* the adapter SHALL distinguish an ABSENT or trivially-true filter, which leaves the broadcast plan eligible and emits no scan-spec filter, from a DECLINED one, which forfeits the broadcast plan
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec omits a declined predicate, because the result would carry extra rows — see `vs-adapter/pushdown-declined-filter-self-apply`

### Scenario: Join condition is rendered via the vs-expression translator

* *GIVEN* a broadcast-eligible inner equi-join carrying an equi-join `condition` expression node
* *WHEN* the adapter renders the join condition
* *THEN* the adapter SHALL render the condition to a DataFusion SQL fragment using the `crates/vs-expression` translator, the same way filter predicates are translated
* *AND* the rendered condition SHALL be carried in the common spec so every shard applies the identical join predicate
* *AND* a condition the translator cannot render SHALL cause the adapter to take the unified unaccelerated fallback rather than emit a broadcast plan with a mistranslated predicate
