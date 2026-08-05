# Feature: Pushdown Planning — Broadcast Inner Equi-Join

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the broadcast inner equi-join shape. When Exasol pushes a two-table join whose smaller side is below the broadcast threshold, the adapter resolves both sides' file lists once, shards only the larger (fact) side through the nested distributor + scalar scan fan-out, replicates the smaller (dimension) side's full file list into the shard-invariant common spec, and drives a node-local DataFusion join inside the scalar scan UDF (`datafusion-scan/scan-execution-join`). Every join outside this broadcast contract — above threshold, non-two-table, needing Exasol postprocessing, or otherwise ineligible — is served by the unified unaccelerated fallback renderer (`vs-adapter/pushdown-planning-join-fallback`), so a join is never wrong, only sometimes unaccelerated.

## Background

<!-- DELTA:NEW -->
* **Issue #294: the broadcast fan-out spec now carries each side's OWN effective storage.** `join_fan_out_scan_spec` set `CommonScanSpec.storage` from the fact side and the dimension side's `effective_storage` was dropped on the floor, even though `resolve_one_join_side` had already resolved it per table location. The broadcast SQL builder now places the dimension side's own `effective_storage` into the join block. The fact side's stays in the whole-spec `storage` value, and the N-scan fallback renderer — which already gives each leg its own spec and therefore its own storage — is unchanged.
* **The recorded claim that "both tables must be readable with the fact side's grant" is withdrawn, not narrowed.** It was the statement of the defect, not a contract: a Databricks-managed catalog vends a credential scoped to the table it loaded, so the fact side's grant is DENIED on the dimension side's prefix and the join fails to read.
* **Credential divergence between the two sides is now SERVED, not rejected.** It follows that the plan-time same-backend guard stays scoped to the backend VARIANT and, for ADLS, `account_name`, for a NEW reason: a variant or account difference is UNSERVEABLE by the scan (an S3 builder cannot address an `abfss://` URI, and two ADLS containers collapse onto one DataFusion registry key), whereas a credential difference is now carried per side and honoured. The former "tracked separately as `#294`" framing is discharged.
* **Nothing about shard selection, threshold evaluation, projection or filter rendering, condition rendering, or the emitted SQL's shape changes.** The only difference in the generated scan-driving SQL is the additional `storage` key inside the common blob's join block.
* **A broadcast join's emitted common blob now carries TWO credential sets instead of one, and that is stated rather than left implicit.** This feature's recorded Background bullet "Credentials MUST NOT appear in any returned SQL string or error message" is scoped to credential-REDACTED error surfacing (`redact_error_text` / `redact_credentials`, which cover error text only); the common blob spliced into the scan-driving SQL has always carried the storage credentials the scan UDF consumes, because that blob is how they reach the UDF. This delta doubles that credential material on a broadcast join, once per side, and does not change where it travels or who can read it. The dimension side's set is serialized ONCE for the whole fan-out, never per shard.
* **Issue `#303`: the broadcast renderer now strips Exasol's native `tableAlias` before rendering.** `render_broadcast_join` renders the join condition, the WHERE filter, and any select-list expression via the `crates/vs-expression` translator, which qualifies a column as `"ALIAS"."NAME"` whenever the incoming node carries a non-empty `tableAlias` — exactly what Exasol sends for a query that aliases a joined table (`FROM fact_orders o JOIN dim_customer c ON ...`, the common case). The scan's derived relations expose bare, unaliased column names, so an alias-qualified reference failed to resolve at scan time. The renderer now strips `tableAlias` before every render call, using the same helper the single-table pushdown chokepoint already applies for the identical reason.
* **The recorded Background claim "Broadcast rendering is side-agnostic bare-name" was already inaccurate before this delta, in the same way the #215 clause was.** The renderer PRESERVED a native `tableAlias` whenever the request carried one, so a table-aliased query rendered alias-qualified, not bare. This delta makes the clause true rather than adding a new requirement: it is now bare BY CONSTRUCTION — the renderer strips the alias — rather than merely bare whenever Exasol happened not to send one.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a `pushdown` request whose `from` clause is a `join` node over exactly two involved tables joined by an equi-condition
* *AND* the smaller side's Iceberg-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH tables' Iceberg snapshot, data-file list, per-file byte size, logical schema, and effective storage exactly once, recovering each table's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards and driven through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, logical schema, and its OWN effective storage backend in the shard-invariant common spec's join block (spliced once as the `LAKEHOUSE_SCAN` scalar UDF's first argument), and the fact side's per-shard file subset flowed through the distributor as the second argument
* *AND* the whole-spec `storage` value SHALL be the FACT side's own effective storage, so each side of the emitted spec names the storage backend resolved for that side's own table location and neither side's backend is dropped
* *AND* the generated scan-driving SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange and no `SELECT * FROM (...)` wrapper
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer — only file-level metadata and per-side storage credentials cross into the scan spec
* *AND* the dimension side's backend SHALL be serialized ONCE inside the shard-invariant common blob and MUST NOT be repeated per shard, exactly as the fact side's already is
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Join condition is rendered via the vs-expression translator

* *GIVEN* a broadcast-eligible inner equi-join carrying an equi-join `condition` expression node
* *WHEN* the adapter renders the join condition
* *THEN* the adapter SHALL render the condition to a DataFusion SQL fragment using the `crates/vs-expression` translator, the same way filter predicates are translated
* *AND* the adapter SHALL strip Exasol's native `tableAlias` from every column reference in the condition BEFORE rendering, so the rendered condition is bare-name BY CONSTRUCTION and resolves against the scan's unaliased derived relations — safe because the disjoint-column-name guard has already proven bare-name resolution is unambiguous
* *AND* the rendered condition SHALL be carried in the common spec so every shard applies the identical join predicate
* *AND* a condition the translator cannot render SHALL cause the adapter to take the unified unaccelerated fallback rather than emit a broadcast plan with a mistranslated predicate
<!-- /DELTA:CHANGED -->
