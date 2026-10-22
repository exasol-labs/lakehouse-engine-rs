# Feature: Pushdown Planning, Unified Unaccelerated Join Fallback

One renderer serves every inner equi-join outside the two-table broadcast contract. It scans each involved table through its own sharded fan-out subquery (a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF) and rebuilds the join in Exasol's core engine as a left-to-right `INNER JOIN ... ON` chain, with leg-local `WHERE` conjuncts pushed into each leg.

## Background

* One implementation serves all N >= 2 legs. Broadcast (`pushdown-joins/pushdown-planning-join`) is an optimization selected within the join path, not a second renderer.
* A leg is one occurrence of a table in the FROM tree. Column-to-leg attribution is owned by `pushdown-joins/pushdown-planning-join-fallback-self-join`.
* Legs are numbered in the FROM tree's left-to-right traversal order, and subquery aliases follow that order.
* The adapter advertises `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` with no per-query opt-out, and Exasol never re-plans on an adapter error. Every advertised join shape must therefore render by broadcast or by this fallback. A hard client-facing error (`F-UDF-CL-RUST-9001`) is raised only for a shape the adapter cannot render at all.
* The wrapper has four entry points: an inner equi-join of N >= 2 legs, a grouped request that did not decompose into the partial/merge shape (`GroupByWrapper`), a single-group request with more than one `COUNT(DISTINCT)` or a distinct mixed with an ordinary aggregate (`pushdown-aggregates/pushdown-planning-count-distinct`), and a row scan whose derived projection was widened. All four share one `ORDER BY` and window rendering site.
* The wrapper resolves its own `LIMIT` and `OFFSET` and renders them on its outer SELECT over limit-free, sort-free fan-out legs. A row limit that leaked into a leg would cap that leg's pre-join input and return wrong rows. A non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`).
* The broadcast path declines, and this wrapper serves: a non-zero offset without `orderBy`, a non-column sort key, a sort key missing its direction or NULL-placement flag, a sort key absent from the broadcast projection, and the four Exasol-postprocessing conditions.
* A top-level WHERE conjunct reaches a leg only if it is local to that leg, accepted by the type-rewrite pipeline against that leg's own column metadata, and renderable for DataFusion in its rewritten form. Every other conjunct is residual and lands in the outer wrapper's `WHERE`. The leg and residual sets are exact complements (`pushdown/pushdown-declined-filter-self-apply`).
* The outer `WHERE` render has three outcomes. An absent residual set emits no clause. A residual set that renders trivially true emits no clause and is not an error. A residual set the non-suppressing qualified render cannot express returns the wrapper's client-facing error.

## Scenarios

### Scenario: Join above the broadcast threshold falls back to the unified unaccelerated wrapper

* *GIVEN* an inner equi-join `pushdown` request over two involved tables whose smaller side exceeds the broadcast threshold
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit SQL through the SINGLE unified N-scan fallback renderer with N = 2, scanning each table independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructing the inner join over both subquery results with an `INNER JOIN … ON` chain in Exasol's core engine
* *AND* the two-involved-table case SHALL use the identical fallback code path as the three-or-more-table case, differing only in the number of scanned sides
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
* *AND* the adapter MUST NOT push either side's rows through a broadcast replication for this shape

### Scenario: A join outside the broadcast contract is declined safely

* *GIVEN* a `pushdown` request whose `from` clause is a join
* *WHEN* the join is not a single broadcast-eligible two-table inner equi-join, it is above threshold, non-equi, spans more than two involved tables, has overlapping column names across tables, needs Exasol postprocessing, or carries a condition/filter/projection detail that keeps it off the broadcast path
* *THEN* the adapter SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL instead render the request through the SINGLE unified unaccelerated per-table-scan fallback (N ≥ 2 involved tables, the two-table case being N = 2), so Exasol's core engine produces the correct result
* *AND* spanning more than two involved tables, or having overlapping column names, or needing Exasol postprocessing SHALL by itself NEVER be a reason to return an error, every such inner join is served by the unified fallback
* *AND* the adapter SHALL return a HARD error, a client-facing `F-UDF-CL-RUST-9001`, NOT a request that Exasol retries natively (Exasol does not re-plan on an adapter error), ONLY when it genuinely cannot render what it advertised: a non-inner join node in the tree, an involved table carrying no readable identifier note or no column metadata, or a condition/clause the translator cannot render at all
* *AND* the adapter MUST NOT emit any scan spec that would compute a different result than single-node evaluation

### Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over three or more leaves, every join node of which is inner
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT return an error and SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL serve the request through the SAME single unified fallback renderer used for the two-leg (N = 2) case, differing only in the number of legs
* *AND* the adapter SHALL resolve each leg's Iceberg snapshot, data-file list, and logical schema exactly once, recovering each leg's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name, and SHALL treat an involved table absent from the mapping as the same stale-virtual-schema hard error the single-table path reports
* *AND* the adapter SHALL emit SQL that scans EACH leg independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructs the original inner join over all N subquery results with a left-to-right `INNER JOIN … ON` chain in Exasol's core engine, each join condition attached to the `ON` of the join point at which every LEG it references is in scope
* *AND* every join condition, WHERE filter, select-list item, GROUP BY, HAVING, and ORDER BY the wrapper renders SHALL use table-qualified column references resolved to each `column` node's own LEG, so the wrapper is correct whether or not any two legs share a column name AND whether or not any two legs are the same table
* *AND* the returned result SHALL equal, as an order-independent multiset, the result of the same inner join evaluated on a single node
* *AND* the adapter MUST NOT read any leg's Parquet row data in the planning layer, only file-level metadata crosses into each leg's scan spec

### Scenario: Join conditions attach greedily by LEG set and leg-local filters push into each leg

* *GIVEN* a unified unaccelerated fallback over N ≥ 2 legs with a set of join conditions and a WHERE filter
* *WHEN* the adapter renders the `INNER JOIN … ON` chain
* *THEN* the adapter SHALL attach each join condition to the earliest join point in the left-to-right chain at which every LEG the condition references is in scope, deciding scope by the SET of LEG INDEXES the condition touches, NEVER by column name, so shared column names across legs stay correctly qualified
* *AND* a join point at which no not-yet-attached condition becomes resolvable SHALL be rendered with `ON 1=1`
* *AND* a top-level WHERE conjunct SHALL be pushed INTO a leg's fan-out as a DataFusion filter if and only if it references only that ONE LEG AND the type-rewrite pipeline run against THAT LEG's own column metadata accepts it AND the DataFusion dialect can render that pipeline's REWRITTEN form of it
* *AND* the renderability condition SHALL be evaluated on the REWRITTEN conjunct, not the raw one, because the leg renders the rewritten tree; a conjunct that is type-accepted but whose REWRITTEN form is unrenderable SHALL become RESIDUAL in raw form and MUST NOT be omitted from both the leg and the residual, which would apply it nowhere and return extra rows with no error
* *AND* the type screen SHALL run PER LEG and PER CONJUNCT, AFTER leg-local attribution, and MUST NOT be folded into the pre-attribution syntactic screen, because two N-scan legs MAY declare the same column name with different Exasol types and only the owning leg's metadata resolves it correctly
* *AND* every OTHER conjunct SHALL be RESIDUAL and remain in the outer wrapper's `WHERE`: cross-leg, OR-spanning, untagged, column-free, leg-local-but-unrenderable, or leg-local-but-type-declined
* *AND* the partition SHALL stay total and disjoint under the added condition, so no conjunct is dropped and none is applied twice
* *AND* a type decline SHALL cost only the offending CONJUNCT's leg pushdown, so the same leg's other leg-local conjuncts SHALL still be pushed into its fan-out
* *AND* the filter each leg receives SHALL already be screened as type-accepted AND, in its REWRITTEN form, DataFusion-renderable, and SHALL BE that REWRITTEN tree, so the leg's own render cannot decline and no second renderability or type decision exists to drift from the first
* *AND* a leg-local conjunct the pipeline REWRITES rather than declines, for example a DATE-column LIKE rewrapped as CAST-to-VARCHAR, SHALL still be pushed into its leg in rewritten form, so it keeps per-leg row-group pruning and row filtering and SHALL NOT also appear in the outer `WHERE`
* *AND* each leg's Iceberg manifest-pruning predicate SHALL keep every conjunct attributed to THAT LEG in its RAW form, renderable or not and type-accepted or not, so neither a render decline nor a type decline nor a rewrite SHALL change which files are opened, and a conjunct attributed to a DIFFERENT leg of the same table MUST NOT reach it
* *AND* the residual conjunct the outer wrapper renders SHALL be the RAW conjunct in the Exasol dialect, which applies the implicit non-string-to-VARCHAR coercion DataFusion refuses
* *AND* a non-empty residual set that the NON-SUPPRESSING qualified Exasol render also cannot express SHALL return the wrapper's client-facing error, because the predicate can be applied nowhere and returning rows without it would be wrong
* *AND* a non-empty residual set that renders trivially true SHALL emit no outer `WHERE` and SHALL NOT error, so the error decision MUST NOT be taken from the trivially-true-suppressing render's empty result
* *AND* if the re-formed accepted-conjunct tree of a leg does not itself survive the pipeline, OR survives but is not DataFusion-renderable, that leg's ENTIRE leg-local set SHALL become residual, so no conjunct is ever applied nowhere
* *AND* an N-scan request in which no table occurs twice and whose filter triggers no rewrite and no type decline SHALL emit the SQL that the golden-SQL fixtures pin for that shape
* *AND* the returned result SHALL equal the result of the same inner join evaluated on a single node, for any assignment of conditions to join points

### Scenario: Aggregate over a join routes through the unified qualified wrapper

* *GIVEN* an inner equi-join `pushdown` request whose select list, GROUP BY, or HAVING requires Exasol postprocessing, an aggregate select item, a non-empty `groupBy`, `aggregationType` = `group_by`, or a non-null `having`, or whose `ORDER BY`/`LIMIT` shape the broadcast path declines per `pushdown-joins/pushdown-planning-join`
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL route the request to the unified qualified fallback path unconditionally, regardless of whether the join would otherwise be broadcast-eligible, because the broadcast in-UDF join renders only projection, filter, and join condition
* *AND* a bare `LIMIT` alone, and a bare-projected-column `ORDER BY` alone, SHALL NOT be members of that set, which names exactly the four unconditional forcing conditions
* *AND* the fallback wrapper SHALL render the aggregate select list as ordinary Exasol SQL over the materialized join (`SELECT <aggregates> FROM (side-0 fan-out) "LHS_T0", (side-1 fan-out) "LHS_T1", … WHERE <conditions> [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`), splicing Exasol's own aggregate function name verbatim while table-qualifying only its column argument(s)
* *AND* a select-list item that is a SCALAR FUNCTION WRAPPING one or more aggregates (e.g. `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)`) SHALL be rendered by recursing through the `crates/vs-expression` translator, which renders nested `function_aggregate` nodes by splicing the aggregate name verbatim and rendering the argument(s), NOT declined
* *AND* the returned result SHALL equal the result of evaluating the same select list over the same inner join on a single node

### Scenario: A fallback leg's fan-out scan spec never carries a row limit

* *GIVEN* any request served by the unified unaccelerated fallback, for any N ≥ 2, carrying a `limit` with `numElements` = `n` and any combination of `orderBy`, `groupBy`, `having`, and `limit.offset`
* *WHEN* the adapter builds each side's fan-out leg
* *THEN* every leg's scan spec SHALL carry `limit` = absent, an EMPTY `order_by`, and NO join block, so the leg emits every row the outer wrapper's join, ordering, and window still depend on
* *AND* that guarantee SHALL be structural rather than conventional, and SHALL be delivered by the TYPE rather than by a call signature, a field visibility, or a comment: the broadcast path's per-shard post-join cap SHALL be a field of the JOIN BLOCK, so a scan spec carrying no join block has NO field on which a post-join cap could be set. A leg builder constructs no join block, so it cannot express a post-join cap at all, in-crate and out-of-crate, in debug builds and in release
* *AND* the shard-invariant row-limit field the single-table scan paths use MUST NOT be set on any join-carrying spec, so the two meanings never share one field and no reader has to know which path a spec came from to know what a limit on it would mean
* *AND* that structural property SHALL be pinned by a test asserting every fallback leg spec carries `limit` = absent, an EMPTY `order_by`, and NO join block, for a request that DOES carry a limit, an offset, and an `orderBy`, so the runtime consequence is checked as well as the type
* *AND* the request's `LIMIT` and `OFFSET` SHALL be rendered ONLY on the outer wrapper SELECT, through the shared limit-and-offset seam

### Scenario: A scalar function wrapping aggregates in a grouped join select list is rendered, not declined

* *GIVEN* an inner equi-join `pushdown` request (two-table or three-or-more-table) whose grouped select list contains a select item that is a scalar function wrapping one or more aggregates, e.g. `ROUND(100.0 * SUM(CASE WHEN l_returnflag = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2)`, alongside plain aggregates such as `SUM(l_quantity)`, `SUM(CASE WHEN … END)`, and `AVG(l_extendedprice)`, with a GROUP BY, a HAVING, an ORDER BY, and a LIMIT
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT decline the request and SHALL NOT return an error for the scalar-over-aggregate select item
* *AND* the adapter SHALL render each such select item by recursing through the `crates/vs-expression` translator, which renders the outer scalar function (`ROUND`, arithmetic, `CASE`, …) around nested `function_aggregate` nodes whose aggregate names (`SUM`, `COUNT`, `AVG`, …) are spliced verbatim and whose column arguments are table-qualified from their `tableName`
* *AND* a top-level bare aggregate and a nested aggregate SHALL be rendered by the same aggregate-rendering path, so the two produce consistent SQL
* *AND* the emitted SQL SHALL be the unified qualified fallback wrapper (`LHS_T0`, `LHS_T1`, … subqueries) with the grouped select list, HAVING, ORDER BY, and LIMIT rendered over the materialized join
* *AND* the returned result SHALL equal the result of evaluating the same grouped, scalar-over-aggregate select list over the same inner join on a single node

### Scenario: The qualified wrapper renders a renderable expression sort key on every entry point that reaches it

* *GIVEN* a `pushdown` request routed to the unified qualified wrapper, whose `orderBy` carries a sort key that is an EXPRESSION node rather than a bare `column`, a scalar-function, arithmetic, aggregate, or CAST node, and which the qualified expression renderer CAN render
* *WHEN* the adapter builds the wrapper SQL, for each of the four entry points that reach this wrapper: an N-scan inner equi-join, a non-decomposing grouped request (`GroupByWrapper`), a multi-`COUNT(DISTINCT)` decline, and a widened-projection row scan
* *THEN* the wrapper's outer `ORDER BY` SHALL carry that expression rendered with table-qualified column references resolved from each `column` node's `tableName`, exactly as the wrapper renders its select list, WHERE, GROUP BY, and HAVING, so an expression sort key is rendered, NOT declined
* *AND* the element SHALL render its direction and NULL placement through the one shared direction/NULL seam, so the wrapper's ordering cannot drift from the other ordered paths
* *AND* an `orderBy` mixing a bare-column key with an expression key SHALL render EVERY element, in the pushed order, each with its own direction and NULL placement
* *AND* the wrapper's visible SELECT list SHALL be UNCHANGED by the ordering, the sort expression is rendered inline in the `ORDER BY`, so NO hidden output column is added and the returned column count still equals the `selectList` item count Exasol validates positionally
* *AND* the per-leg fan-out subqueries SHALL each project every column the sort expression names, via the SAME referenced-column narrowing helper that walks the `orderBy` node's full expression tree, so the wrapper never references a column a leg does not emit
* *AND* the returned result SHALL equal the same query with the same `ORDER BY` evaluated over all rows on a single node

### Scenario: The qualified wrapper renders the request's OFFSET on every entry point that reaches it

* *GIVEN* a `pushdown` request routed to the unified qualified wrapper, carrying a renderable non-empty `orderBy`, a `limit` with `numElements` = `n`, and a non-zero `limit.offset` = `m`
* *WHEN* the adapter builds the wrapper SQL, for each of the four entry points that reach this wrapper: an N-scan inner equi-join, a non-decomposing grouped request, a multi-`COUNT(DISTINCT)` decline, and a widened-projection row scan
* *THEN* the wrapper's outer SELECT SHALL render `ORDER BY <clause> LIMIT n OFFSET m`, in that clause order, through the SAME shared limit-and-offset seam every other wrapper uses
* *AND* the per-leg fan-out subqueries SHALL remain limit-free and sort-free, so the outer SELECT is the ONLY place either bound is applied and no leg drops a row that the join or the window still needs
* *AND* a request whose `limit.offset` is zero or absent SHALL render no `OFFSET` on all four entry points
* *AND* the wrapper's visible SELECT list SHALL be UNCHANGED by the offset, so the returned column count still equals the `selectList` item count Exasol validates positionally
* *AND* the adapter SHALL NOT render an `OFFSET` on a wrapper SELECT that renders no `ORDER BY`, because Exasol rejects that SQL with `sqlCode 42000` ("OFFSET not allowed in LIMIT without ORDER BY")
* *AND* that state SHALL NOT be given a failure branch of its own, because it is unreachable: this wrapper resolves no `ORDER BY` clause ONLY when the request's `orderBy` is absent or empty, and a request carrying a non-zero `limit.offset` always carries a non-empty `orderBy` (see the offset-implies-ordering invariant in `pushdown-capabilities/pushdown-planning-order-by-capability`), an `orderBy` this wrapper cannot RENDER takes the separate `User` decline stated in `pushdown-capabilities/pushdown-planning-topn`, which is a different state
* *AND* the adapter MUST NOT introduce a hard client-facing `User` decline for that unreachable state, because this seam serves all four entry points above: a decline here would turn a successful query into a hard failure on a branch no test exercises. The invariant SHALL instead be pinned by an assertion plus a unit test asserting that no emitted wrapper SQL contains an `OFFSET` token unpreceded by an `ORDER BY`, driven by the request shapes that actually reach this seam with an offset
* *AND* the returned result SHALL equal the same query with the same `ORDER BY … LIMIT n OFFSET m` evaluated over all rows on a single node

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages

### Scenario: Each join leg resolves its catalog identifier from its own identifier note

* *GIVEN* a join `pushdown` request whose `involvedTables` carries `CUSTOMER` and `ORDERS`, whose identifier notes name `lh.customer` and `lh.orders`
* *WHEN* the adapter plans the join
* *THEN* each leg SHALL take its catalog identifier from the identifier note of the `involvedTables` entry whose `name` equals the leg's table name
* *AND* every leg of a self-join, for which Exasol lists the table once per leg, SHALL resolve to the same catalog identifier
* *AND* a leg without a readable identifier note SHALL fail the request with the error of `file-planning/pushdown-planning-file-resolution`, and the adapter MUST NOT plan the join as a different shape
