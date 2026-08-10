# Feature: DataFusion Scan Execution — Broadcast Join

Extends `datafusion-scan/scan-execution` with node-local broadcast inner equi-join execution. A join scan invocation receives, in addition to its per-shard fact-file subset, the FULL dimension-side file list carried once in the shard-invariant common spec. The UDF registers both sides as Iceberg tables in ONE DataFusion session, executes the inner equi-join with the pushed projection, filter, and LIMIT, and streams the joined rows back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

<!-- DELTA:NEW -->
* **The recorded note "the VS does not currently push a LIMIT alongside a join, but the path handles it identically for consistency" is superseded on BOTH counts (issue #307).** The VS now pushes a per-shard cap, and the path no longer handles it identically to the single-table scan: the cap is a field of the JOIN BLOCK (`post_join_limit`), not the shard-invariant row-limit field the single-table scan reads. It is a load-bearing, exercised contract rather than a consistency courtesy, and the guarantee it must meet is stated below rather than left implicit.
* **The cap's home in the join block is what makes "post-join" true by type rather than by convention.** The shard-invariant row-limit field means a PRE-join input cap on the single-table leg specs the unaccelerated join fallback emits, and a POST-join output cap here. Those two meanings MUST NOT share one field: a spec carrying no join block has no `post_join_limit` field at all, so a fallback leg cannot express a post-join cap and a join spec cannot inherit a pre-join one (`vs-adapter/pushdown-planning-join-fallback`). The scan therefore SHALL read the cap from the join block, and SHALL NOT consult the shard-invariant row-limit field on the join path.
* **The field is additive and defaulted, so the wire format stays backward-compatible.** `post_join_limit` is optional, defaults to absent, and is omitted from the serialized spec when absent — the same treatment every other optional field of the join block and the common spec already receives. A spec serialized before this delta deserializes with no cap, and an unordered broadcast request's serialized common blob is byte-identical to its pre-change output.
* **The limit is POST-join by construction at two independent levels, and both must hold.** At the SQL level, the join scan renders `LIMIT n` after the `INNER JOIN` and after the `WHERE`, so it bounds joined output rows, not either side's scanned input rows. At the optimizer level, DataFusion's `push_down_limit` rule refuses to push a fetch into the inputs of a non-cross inner join — `push_down_join` returns `(None, None)` for `JoinType::Inner` unless the join is a CROSS join (`on.is_empty() && filter.is_none()`), which a rendered equi-condition excludes (`datafusion-optimizer-54.1.0/src/push_down_limit.rs`). Either level alone would be enough; together they make an accidental pre-join cap a change that has to break both.
* **A pre-join cap would be silently wrong, not merely slower.** Capping the fact side's scan at `n` discards fact rows that would have matched the dimension side while keeping fact rows that produce zero joined output, so the emitted row count bounds nothing and the emitted rows are not a valid `LIMIT n` answer. The scan therefore MUST NOT gain a fact-side or dimension-side input limit, and the VS planning layer MUST NOT emit one (`vs-adapter/pushdown-planning-join-fallback`).
* **A pushed ordering is still NOT carried on the join path.** `spec.common.order_by` remains empty for every join spec: the VS renders an ordering for a broadcast join on an Exasol-side wrapper over the merged fan-out, never as a per-shard sort. A per-shard TopK over the joined output is tracked as `(#309)` and would be the change that first makes `order_by` meaningful here. Beyond the one additive `post_join_limit` field and the one read site that consumes it, nothing in the scan-spec wire format, the join block, or the scan UDF changes for this delta — registration, delete application, join execution, and the Arrow-IPC emit path are all untouched.
* **Early stream termination on a join spec was already accounted for.** The scan's opener-coverage diagnostic already treats any spec carrying a join block as `MayStopEarly`, independently of whether a limit is present, so a limit that now ends the stream before every assigned file is opened cannot make that diagnostic misreport.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Join projection, filter, and LIMIT are applied and rows streamed as Arrow IPC

* *GIVEN* a join scan spec carrying a projection spanning both sides, an optional filter, and an optional post-join row cap `n` carried in its JOIN BLOCK
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL emit only the projected join-output columns, in spec order, for rows satisfying both the join condition and the filter
* *AND* the UDF SHALL emit no more rows than the cap when one is carried, reading it from the join block and MUST NOT consult the spec's shard-invariant row-limit field on this path (issue #307)
* *AND* a join spec whose serialized join block omits the cap entirely SHALL deserialize with no cap and behave exactly as an unlimited join scan, so a spec produced before this delta is unaffected
* *AND* the cap SHALL bound JOINED OUTPUT rows and MUST NOT bound either side's scanned input rows, so for a fixture whose first `n` fact rows match no dimension row the UDF SHALL still emit `n` joined rows rather than zero (issue #307)
* *AND* the UDF MUST NOT register either side with an input-side fetch, and the executed plan MUST NOT carry a limit below the join node on either input
* *AND* the UDF SHALL emit each result batch via the SDK Arrow-batch emit path (`emit_batch`), fetching one batch, emitting it, and dropping it before the next, never materializing the entire joined result set
* *AND* no typed Arrow value SHALL cross the `.so` boundary — only the serialized IPC byte buffer
<!-- /DELTA:CHANGED -->
