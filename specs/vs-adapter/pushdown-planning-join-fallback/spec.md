# Feature: Pushdown Planning — Unified Unaccelerated Join Fallback

Extends pushdown planning with the SINGLE unified renderer that serves every inner
equi-join outside the two-table broadcast contract. Each involved table is scanned
independently through its own sharded fan-out subquery — nested `LAKEHOUSE_DISTRIBUTE_FILES`
distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF — and all N legs are
reconstructed into the original inner join by Exasol's core engine. The FROM clause is
rendered as a left-to-right `INNER JOIN … ON` chain (not a comma cross-join with one flat
`WHERE`): each join condition attaches to the `ON` of the join point at which every table
it references is in scope, and each leg's leg-local `WHERE` conjuncts are pushed into
that leg's fan-out so DataFusion prunes and filters per leg. Column references are
attributed to a JOIN LEG — one occurrence of a table in the FROM tree — never to a table
name; the leg-attribution binding itself, and its self-join-specific correctness properties,
are owned by the sibling feature `vs-adapter/pushdown-planning-join-fallback-self-join`. The
unaccelerated fallback has exactly one implementation for all N ≥ 2 legs.

## Background

* Column-to-leg attribution for a table joined to itself — including leaf alias retention, the
  `(tableName, alias)` leg key, and the unattributable-reference hard error — is owned by the
  sibling feature `vs-adapter/pushdown-planning-join-fallback-self-join`. Every attribution
  decision described here (expression rendering, leg-local WHERE attribution, join-condition
  attachment, per-leg projection narrowing) is resolved through that feature's single `JoinLegs`
  binding, which this feature's renderers consult rather than re-derive.
* **Leaf order is the FROM tree's left-to-right traversal, not the SQL text order.** A three-leg
  join arrives as ONE request with left-deep nesting (`join(join(A,B),C)`), and the existing
  recursive walk already flattens any nesting to N leaves in traversal order. Leg indexes, and
  therefore subquery aliases, are assigned in that order; no decision depends on it matching the
  written order.
* **The N = 1 qualified wrapper's name collapse is deliberate and unchanged.** That entry point
  maps EVERY involved table name onto its single `LHS_T0` subquery, because there is exactly one
  scan and no leg to disambiguate. It keeps that behavior, including leaving a column whose
  `tableName` is not an involved table unqualified.
* **The refusal check stays name-keyed, deliberately.** `ensure_no_side_refuses_a_referenced_column`
  charges a format reader's refused column to every side whose name matches, and already charges an
  untagged column to every side. Over-charging is the fail-safe direction for a refusal — it
  refuses a query that might read a refused column, never admits one that does — so name-keying is
  correct there and is left unchanged rather than narrowed to a leg.
* **The Iceberg table spec does not bear on this delta.** The change is Exasol-SQL column-to-leg
  attribution over the pushdown JSON and the generated wrapper SQL. It reads no manifest, no
  snapshot, no field id, and no type mapping, and it behaves identically on Iceberg and Delta
  tables — the established format-agnostic property of every SQL-shape pushdown decision. There is
  no normative spec section to quote and no deviation to track.
* This delta corrects which conjuncts reach a fan-out leg. A conjunct reaches a leg only if the leg
  can actually apply it, so a leg-local conjunct the DataFusion dialect cannot render is residual
  and lands in the outer wrapper's `WHERE` rather than being pushed into a leg that then drops it.
  The set the legs receive and the set the outer wrapper renders stay exact complements, so the
  partition remains total and disjoint. See `vs-adapter/pushdown-declined-filter-self-apply`.
* The renderability screen governs the RENDER path only. Each leg's Iceberg manifest-pruning
  predicate keeps EVERY conjunct attributed to that leg, renderable or not, because pruning only
  ever removes files that provably cannot match; narrowing that input would silently open more
  files and buy no correctness.
* The stale claim that the outer Exasol query "still applies the FULL `WHERE`" is corrected in the
  same change: it applies exactly the residual set, and the residual set is now defined so that
  every conjunct no leg applies is in it.
* The outer wrapper's `WHERE` is rendered from ONE renderer over ONE combined residual tree, and that
  render has THREE outcomes, not two. An ABSENT residual set emits no clause. A residual set that
  renders TRIVIALLY TRUE emits no clause and is not an error — the trivially-true-suppressing
  renderer returns nothing for it exactly as it does for an unrenderable one, so that renderer alone
  cannot decide the error. A residual set the NON-SUPPRESSING qualified render also cannot express is
  UNRENDERABLE and returns the wrapper's existing client-facing error. Only the third outcome errors.
* The adapter advertises exactly `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` (`vs-adapter/pushdown-planning-join`); there is no per-query opt-out, so Exasol pushes every inner equi-join of any arity — including a table joined to itself — and expects this fallback (or the broadcast path) to serve it.
* Each leg's Iceberg snapshot, data-file list, and logical schema are resolved exactly once per leg per pushdown, in the planning layer, recovering each leg's original-cased Iceberg identifier from `TABLE_MAP` by its involved-table name; no scan UDF invocation discovers files itself.
* The unaccelerated fallback is a SINGLE unified renderer for every inner join with N ≥ 2 legs: the two-leg case is exactly N = 2, and there is only one implementation. Each leg is scanned through its own nested-distributor + scalar-scan fan-out subquery, and all N subquery results are reconstructed into the original inner join by Exasol's core engine. Broadcast (strictly two-table, node-local in the scan UDF, `vs-adapter/pushdown-planning-join`) is an optimization SELECTED WITHIN the one join path, not a second rendering implementation; when broadcast is unavailable for a two-table join it takes the same N = 2 unified fallback.
* Because each advertised capability is served statically and Exasol never re-plans on an adapter error (a declined pushdown is erased by the `exasol-udf-macros` FFI shim into a hard `F-UDF-CL-RUST-9001` SQL error — there is NO native-retry response in the protocol), an advertised join capability MUST always be renderable by the join path: either by broadcast or by the unified unaccelerated fallback. "Decline at runtime and let Exasol retry natively" is not an available behavior. A hard error is a genuine last resort, raised ONLY for a shape the adapter cannot render at all — a non-inner join node in the tree, an involved table absent from `TABLE_MAP` or carrying no column metadata, a join condition/clause the translator cannot render, or a column reference no leg key matches — and it is a hard client-facing error, not a retry.
* Every clause the wrapper renders (join conditions, WHERE, select list, GROUP BY, HAVING, ORDER BY) uses table-qualified column references resolved to each `column` node's own LEG, so the wrapper is correct whether or not two legs share a column name and whether or not two legs are the same table.
* This ONE wrapper serves four production entry points, all reaching the same `ORDER BY`-rendering seam: a real inner equi-join of N ≥ 2 legs; a grouped request that did not decompose into the partial/merge shape (`GroupByWrapper`); a single-group request with more than one `COUNT(DISTINCT)` or a distinct mixed with an ordinary aggregate (the Case 2/3 count-distinct decline, `vs-adapter/pushdown-planning-count-distinct`); and a row scan whose derived projection the pre-existing fallback widened. Advertising `ORDER_BY_EXPRESSION` (`vs-adapter/pushdown-planning-order-by-capability`) makes an expression sort key reachable on ALL FOUR at once: the wrapper already renders arbitrary expressions for its own clauses through the qualified expression renderer, so relaxing the sort-key parser's bare-column gate is what makes an expression key render on every entry point, with no per-entry-point rendering work added. The `User` decline for an expression the renderer cannot render is stated once, in `vs-adapter/pushdown-planning-topn`, and covers this wrapper too.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.
* **This wrapper renders its own final window, so it owns the offset too (issue #191).** The
  wrapper resolves its `LIMIT` from the request independently of the row-scan dispatcher's
  withheld binding, and renders it on its own outer SELECT over limit-free, sort-free fan-out
  legs. The offset therefore belongs on that same outer SELECT, through the one shared
  limit-and-offset seam. All four entry points that reach this wrapper — an N-scan inner
  equi-join, a non-decomposing grouped request, a Case 2/3 `COUNT(DISTINCT)` decline, and a
  widened-projection row scan — share the single render site, so one seam change covers all
  four with no per-entry-point work.
* **Issue #307 withdraws the former "every LIMIT-carrying join reaches this wrapper, never broadcast" claim.** A bare `LIMIT` with a zero or absent `limit.offset`, and a bare-projected-column `ORDER BY` — with or without a `LIMIT`, and with or without an `OFFSET` — are now served by the broadcast path instead, per `vs-adapter/pushdown-planning-join`: the bare `LIMIT` as a per-shard post-join cap plus the outer merge limit, the ordering as an outer wrapper over the broadcast fan-out (`(#309)`). Those shapes were DECLINED by the broadcast gate, not unrenderable there, so "never broadcast", "every offset-bearing join shape lands here by construction", and "the broadcast renderer emits no `LIMIT` or `ORDER BY` at all and needs no offset handling" are each false after this delta. What still reaches this wrapper is the NARROWER set enumerated below. The offset-implies-ordering reasoning of this feature's recorded OFFSET scenario is UNCHANGED for the shapes that do reach here: that scenario reasons about which shapes arrive carrying an offset, and only the MEMBERSHIP of that set narrows — the invariant it rests on, that a non-zero `limit.offset` never arrives without a non-empty `orderBy` (`vs-adapter/pushdown-planning-order-by-capability`), is untouched, and a `LIMIT` carrying a non-zero `limit.offset` with no `orderBy` still lands here.
* **What still reaches this wrapper is now a NARROWER, explicitly enumerated set.** From the ordering-and-window family: a `limit` with a non-zero `limit.offset` and no `orderBy`; any non-`column` sort key; a sort key missing its direction or NULL-placement flag; and a sort key naming a column absent from the broadcast projection. From the postprocessing family, unchanged: an aggregate select item, a non-empty `groupBy`, `aggregationType == "group_by"`, and a non-null `having`. Everything else that reached this wrapper before — above threshold, non-equi, N ≥ 3, overlapping column names, a table joined to itself, unrenderable condition or filter, widened projection — is untouched by this delta.
* **This wrapper's own limit and offset handling is UNCHANGED.** It still resolves `LIMIT`/`OFFSET` from the request independently, renders them on its own outer SELECT after GROUP BY / HAVING / ORDER BY, through the one shared limit-and-offset seam, over limit-free and sort-free fan-out legs. No clause of the recorded offset scenario is narrowed or superseded; only which requests arrive here changes.
* **A fallback leg's fan-out spec must stay limit-free for a reason that is NOT merely "the outer SELECT applies it".** A row limit means something categorically different on the two paths the shared fan-out helper serves: on a broadcast spec the scan UDF applies it AFTER the node-local join, whereas a leg spec feeds a bare single-table scan whose rows Exasol has yet to join. A limit that leaked into a leg would cap that leg's pre-join INPUT — discarding rows that would have matched and keeping rows that produce no output — and silently return wrong rows with no error. The recorded "no leg drops a row that the join or the window still needs" clause states the consequence; this delta names the mechanism and pins it with a test, because a sibling path built by the same helper now carries a cap.
* **The two meanings are separated by the TYPE, not by a shared field plus a rule.** The broadcast per-shard cap is a field OF THE JOIN BLOCK (`post_join_limit`), not of the shard-invariant common spec, so it exists only where a join block exists. The shard-invariant `limit` field the single-table, TopN, and raw-scan paths use is left out of the join path entirely: the broadcast builder MUST NOT set it, and the scan reads the cap from the join block. That field's own doc comment names `post_join_limit` as the join path's cap; `post_join_limit`'s doc comment is the authoritative statement of what a post-join cap means, and the leg builder, the broadcast builder, and the scan-side renderer defer to it instead of restating the rule.
* The leg-eligibility test grows a THIRD condition. It was "local to exactly one leg AND
  syntactically renderable for DataFusion"; it becomes "…AND accepted by the type-rewrite pipeline
  run against THAT LEG's own column metadata AND whose REWRITTEN form the DataFusion dialect can
  render". Issue #215: the syntactic screen (`datafusion_renderable`) carries no column-type
  awareness, so a leg-local LIKE over a non-string column was classified leg-eligible and rendered
  bare into the leg's scan spec — the tree that hard-fails DataFusion's `type_coercion` planner at
  scan execution time.
* The renderability half of that condition MUST target the REWRITTEN tree, not the raw one, because
  the REWRITTEN tree is what the leg renders. Screening the raw tree for renderability and the
  rewritten tree only for type-acceptance would let a conjunct that is type-accepted but
  rewritten-unrenderable fall out of the leg half AND out of the residual half — applied nowhere,
  returning extra rows with no error. That is the defect #279 found at the broadcast site, and the
  single-table owner already carries the arm that prevents it
  (`classify_where_filter`'s `(Some(raw), Some(tree)) if !datafusion_renderable(tree) => (None, Some(raw))`).
  The N-scan site inherits that arm rather than diverging from it.
* The new condition CANNOT be folded into the existing pre-attribution screen, and that is a
  structural constraint rather than an implementation preference. `renderable_only` /
  `declined_only` partition the WHOLE top-level conjunct set with ONE predicate BEFORE
  leg-local attribution runs, whereas the type screen needs the OWNING leg's own `col_types` — and
  unlike broadcast, the N-scan path has NO disjoint-column-name precondition, so two legs MAY
  declare the same column name with different Exasol types, and two legs of the SAME table declare
  every column name twice. The type screen therefore runs PER LEG, PER CONJUNCT, AFTER attribution,
  and the leg/residual partition is computed in that order: per-leg legs first, residual last.
* The screen is per CONJUNCT, not per leg-local tree, so one type-declining conjunct does not
  forfeit the other leg-local conjuncts' pushdown. This is a deliberate difference from the
  single-table WHERE surface, where a decline is necessarily whole-filter because there is one
  filter and one wrapper; here the partition already exists and can absorb a single conjunct.
* The residual set gains a third disjoint component: the cross-leg complement of the leg-eligible
  half, the syntactically-declined half, and now the per-leg TYPE-declined conjuncts. All three
  are disjoint by construction — the type-declined set is drawn from conjuncts that are inside the
  leg-eligible half AND local to one leg, which is exactly the complement of the other two.
* The per-leg split is FAIL-CLOSED in BOTH directions: if the re-formed accepted-conjunct tree does
  not itself survive the pipeline, OR survives but is not renderable for DataFusion (either must hold,
  since each of its conjuncts satisfied both, but nothing in the type system forbids it), the WHOLE
  leg-local set goes to the residual rather than being silently lost. A conjunct applied nowhere
  returns wrong rows, so the safe direction is always "residual".
* The tree a leg receives is now the pipeline's REWRITTEN tree, not the raw one, so a leg-local
  DATE-column LIKE keeps its leg pushdown as `CAST(<col> AS VARCHAR) LIKE …` instead of becoming
  residual. Only what the LEG renders changes; each leg's Iceberg manifest-pruning predicate keeps
  every conjunct attributed to it in RAW form, unchanged by this delta.
* The residual conjunct rendered into the outer wrapper's `WHERE` is the RAW conjunct, in the
  Exasol dialect, because Exasol applies the implicit non-string-to-VARCHAR coercion DataFusion
  refuses — which is the whole reason the predicate is safe there and not in the leg.
* This delta adds no new residual MECHANISM and no new error path for a filter. The residual bucket, its
  qualified render, and its both-dialects-unrenderable error are owned by
  `vs-adapter/pushdown-declined-filter-self-apply`; only the set of triggers that reaches the bucket
  widens. See `vs-adapter/pushdown-planning-like-type-coercion` for the per-surface type dispatch.
* **Every offset-carrying request reaching this seam carries a resolvable ordering.** Verified
  live on the four shapes that reach it with a non-zero `limit.offset`: a grouped
  `COUNT(DISTINCT)` (`GROUP BY MOD(id,4)`, `ORDER BY 1 LIMIT 2 OFFSET 1`), a self-join ordered
  by an ordinal, a self-join ordered by a qualified column, and a self-join with a `GROUP BY`.
  All four pushed a non-empty `orderBy` alongside the offset. The wrapper therefore never has
  to choose between a bare `OFFSET` and a failure, and MUST NOT be given a failure branch for
  that choice.

## Scenarios

### Scenario: Join above the broadcast threshold falls back to the unified unaccelerated wrapper

* *GIVEN* an inner equi-join `pushdown` request over two involved tables whose smaller side exceeds the broadcast threshold
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit SQL through the SINGLE unified N-scan fallback renderer with N = 2 — scanning each table independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructing the inner join over both subquery results with an `INNER JOIN … ON` chain in Exasol's core engine
* *AND* the two-involved-table case SHALL use the identical fallback code path as the three-or-more-table case, differing only in the number of scanned sides
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
* *AND* the adapter MUST NOT push either side's rows through a broadcast replication for this shape

### Scenario: A join outside the broadcast contract is declined safely

* *GIVEN* a `pushdown` request whose `from` clause is a join
* *WHEN* the join is not a single broadcast-eligible two-table inner equi-join — it is above threshold, non-equi, spans more than two involved tables, has overlapping column names across tables, needs Exasol postprocessing, or carries a condition/filter/projection detail that keeps it off the broadcast path
* *THEN* the adapter SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL instead render the request through the SINGLE unified unaccelerated per-table-scan fallback (N ≥ 2 involved tables, the two-table case being N = 2), so Exasol's core engine produces the correct result
* *AND* spanning more than two involved tables, or having overlapping column names, or needing Exasol postprocessing SHALL by itself NEVER be a reason to return an error — every such inner join is served by the unified fallback
* *AND* the adapter SHALL return a HARD error — a client-facing `F-UDF-CL-RUST-9001`, NOT a request that Exasol retries natively (Exasol does not re-plan on an adapter error) — ONLY when it genuinely cannot render what it advertised: a non-inner join node in the tree, an involved table absent from `TABLE_MAP` or carrying no column metadata, or a condition/clause the translator cannot render at all
* *AND* the adapter MUST NOT emit any scan spec that would compute a different result than single-node evaluation

### Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over three or more leaves, every join node of which is inner
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT return an error and SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL serve the request through the SAME single unified fallback renderer used for the two-leg (N = 2) case, differing only in the number of legs
* *AND* the adapter SHALL resolve each leg's Iceberg snapshot, data-file list, and logical schema exactly once — recovering each leg's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name — and SHALL treat an involved table absent from the mapping as the same stale-virtual-schema hard error the single-table path reports
* *AND* the adapter SHALL emit SQL that scans EACH leg independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructs the original inner join over all N subquery results with a left-to-right `INNER JOIN … ON` chain in Exasol's core engine, each join condition attached to the `ON` of the join point at which every LEG it references is in scope
* *AND* every join condition, WHERE filter, select-list item, GROUP BY, HAVING, and ORDER BY the wrapper renders SHALL use table-qualified column references resolved to each `column` node's own LEG, so the wrapper is correct whether or not any two legs share a column name AND whether or not any two legs are the same table
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same inner join evaluated on a single node
* *AND* the adapter MUST NOT read any leg's Parquet row data in the planning layer — only file-level metadata crosses into each leg's scan spec

### Scenario: Join conditions attach greedily by LEG set and leg-local filters push into each leg

* *GIVEN* a unified unaccelerated fallback over N ≥ 2 legs with a set of join conditions and a WHERE filter
* *WHEN* the adapter renders the `INNER JOIN … ON` chain
* *THEN* the adapter SHALL attach each join condition to the earliest join point in the left-to-right chain at which every LEG the condition references is in scope, deciding scope by the SET of LEG INDEXES the condition touches — NEVER by column name, so shared column names across legs stay correctly qualified
* *AND* a join point at which no not-yet-attached condition becomes resolvable SHALL be rendered with `ON 1=1`
* *AND* a top-level WHERE conjunct SHALL be pushed INTO a leg's fan-out as a DataFusion filter if and only if it references only that ONE LEG AND the type-rewrite pipeline run against THAT LEG's own column metadata accepts it AND the DataFusion dialect can render that pipeline's REWRITTEN form of it
* *AND* the renderability condition SHALL be evaluated on the REWRITTEN conjunct, not the raw one, because the leg renders the rewritten tree; a conjunct that is type-accepted but whose REWRITTEN form is unrenderable SHALL become RESIDUAL in raw form and MUST NOT be omitted from both the leg and the residual, which would apply it nowhere and return extra rows with no error
* *AND* the type screen SHALL run PER LEG and PER CONJUNCT, AFTER leg-local attribution, and MUST NOT be folded into the pre-attribution syntactic screen, because two N-scan legs MAY declare the same column name with different Exasol types and only the owning leg's metadata resolves it correctly
* *AND* every OTHER conjunct SHALL be RESIDUAL and remain in the outer wrapper's `WHERE`: cross-leg, OR-spanning, untagged, column-free, leg-local-but-unrenderable, or leg-local-but-type-declined
* *AND* the partition SHALL stay total and disjoint under the added condition, so no conjunct is dropped and none is applied twice
* *AND* a type decline SHALL cost only the offending CONJUNCT's leg pushdown, so the same leg's other leg-local conjuncts SHALL still be pushed into its fan-out
* *AND* the filter each leg receives SHALL already be screened as type-accepted AND, in its REWRITTEN form, DataFusion-renderable, and SHALL BE that REWRITTEN tree, so the leg's own render cannot decline and no second renderability or type decision exists to drift from the first
* *AND* a leg-local conjunct the pipeline REWRITES rather than declines — for example a DATE-column LIKE rewrapped as CAST-to-VARCHAR — SHALL still be pushed into its leg in rewritten form, so it keeps per-leg row-group pruning and row filtering and SHALL NOT also appear in the outer `WHERE`
* *AND* each leg's Iceberg manifest-pruning predicate SHALL keep every conjunct attributed to THAT LEG in its RAW form, renderable or not and type-accepted or not, so neither a render decline nor a type decline nor a rewrite SHALL change which files are opened — and a conjunct attributed to a DIFFERENT leg of the same table MUST NOT reach it
* *AND* the residual conjunct the outer wrapper renders SHALL be the RAW conjunct in the Exasol dialect, which applies the implicit non-string-to-VARCHAR coercion DataFusion refuses
* *AND* a non-empty residual set that the NON-SUPPRESSING qualified Exasol render also cannot express SHALL return the wrapper's existing client-facing error, because the predicate can be applied nowhere and returning rows without it would be wrong
* *AND* a non-empty residual set that renders trivially true SHALL emit no outer `WHERE` and SHALL NOT error, so the error decision MUST NOT be taken from the trivially-true-suppressing render's empty result
* *AND* if the re-formed accepted-conjunct tree of a leg does not itself survive the pipeline, OR survives but is not DataFusion-renderable, that leg's ENTIRE leg-local set SHALL become residual, so no conjunct is ever applied nowhere
* *AND* an N-scan request in which no table occurs twice and whose filter triggers no rewrite and no type decline SHALL emit byte-identical SQL to its pre-change output, so no golden-SQL fixture over such a request changes
* *AND* the returned result SHALL equal the result of the same inner join evaluated on a single node, for any assignment of conditions to join points

### Scenario: Aggregate over a join routes through the unified qualified wrapper

* *GIVEN* an inner equi-join `pushdown` request whose select list, GROUP BY, or HAVING requires Exasol postprocessing — an aggregate select item, a non-empty `groupBy`, `aggregationType` = `group_by`, or a non-null `having` — or whose `ORDER BY`/`LIMIT` shape the broadcast path declines per `vs-adapter/pushdown-planning-join`
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL route the request to the unified qualified fallback path unconditionally, regardless of whether the join would otherwise be broadcast-eligible, because the broadcast in-UDF join renders only projection, filter, and join condition
* *AND* a bare `LIMIT` alone, and a bare-projected-column `ORDER BY` alone, SHALL NO LONGER be members of that set — the set names four unconditional forcing conditions, not six (issue #307)
* *AND* the fallback wrapper SHALL render the aggregate select list as ordinary Exasol SQL over the materialized join (`SELECT <aggregates> FROM (side-0 fan-out) "LHS_T0", (side-1 fan-out) "LHS_T1", … WHERE <conditions> [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`), splicing Exasol's own aggregate function name verbatim while table-qualifying only its column argument(s)
* *AND* a select-list item that is a SCALAR FUNCTION WRAPPING one or more aggregates (e.g. `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)`) SHALL be rendered by recursing through the `crates/vs-expression` translator, which renders nested `function_aggregate` nodes by splicing the aggregate name verbatim and rendering the argument(s) — NOT declined
* *AND* the returned result SHALL equal the result of evaluating the same select list over the same inner join on a single node

### Scenario: A fallback leg's fan-out scan spec never carries a row limit

* *GIVEN* any request served by the unified unaccelerated fallback, for any N ≥ 2, carrying a `limit` with `numElements` = `n` and any combination of `orderBy`, `groupBy`, `having`, and `limit.offset`
* *WHEN* the adapter builds each side's fan-out leg
* *THEN* every leg's scan spec SHALL carry `limit` = absent, an EMPTY `order_by`, and NO join block, so the leg emits every row the outer wrapper's join, ordering, and window still depend on
* *AND* that guarantee SHALL be structural rather than conventional, and SHALL be delivered by the TYPE rather than by a call signature, a field visibility, or a comment: the broadcast path's per-shard post-join cap SHALL be a field of the JOIN BLOCK, so a scan spec carrying no join block has NO field on which a post-join cap could be set. A leg builder constructs no join block, so it cannot express a post-join cap at all — in-crate and out-of-crate, in debug builds and in release
* *AND* the shard-invariant row-limit field the single-table scan paths use MUST NOT be set on any join-carrying spec, so the two meanings never share one field and no reader has to know which path a spec came from to know what a limit on it would mean
* *AND* that structural property SHALL be pinned by a test asserting every fallback leg spec carries `limit` = absent, an EMPTY `order_by`, and NO join block, for a request that DOES carry a limit, an offset, and an `orderBy` — so the runtime consequence is checked as well as the type
* *AND* the request's `LIMIT` and `OFFSET` SHALL be rendered ONLY on the outer wrapper SELECT, through the shared limit-and-offset seam, unchanged by this delta
* *AND* the emitted SQL for every fallback shape SHALL be byte-identical to its pre-change output, because this delta changes which requests arrive here and never how they are rendered once they do

### Scenario: A scalar function wrapping aggregates in a grouped join select list is rendered, not declined

* *GIVEN* an inner equi-join `pushdown` request (two-table or three-or-more-table) whose grouped select list contains a select item that is a scalar function wrapping one or more aggregates — e.g. `ROUND(100.0 * SUM(CASE WHEN l_returnflag = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2)` — alongside plain aggregates such as `SUM(l_quantity)`, `SUM(CASE WHEN … END)`, and `AVG(l_extendedprice)`, with a GROUP BY, a HAVING, an ORDER BY, and a LIMIT
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT decline the request and SHALL NOT return an error for the scalar-over-aggregate select item
* *AND* the adapter SHALL render each such select item by recursing through the `crates/vs-expression` translator, which renders the outer scalar function (`ROUND`, arithmetic, `CASE`, …) around nested `function_aggregate` nodes whose aggregate names (`SUM`, `COUNT`, `AVG`, …) are spliced verbatim and whose column arguments are table-qualified from their `tableName`
* *AND* a top-level bare aggregate and a nested aggregate SHALL be rendered by the same aggregate-rendering path, so the two produce consistent SQL
* *AND* the emitted SQL SHALL be the unified qualified fallback wrapper (`LHS_T0`, `LHS_T1`, … subqueries) with the grouped select list, HAVING, ORDER BY, and LIMIT rendered over the materialized join
* *AND* the returned result SHALL equal the result of evaluating the same grouped, scalar-over-aggregate select list over the same inner join on a single node

### Scenario: The qualified wrapper renders a renderable expression sort key on every entry point that reaches it

* *GIVEN* a `pushdown` request routed to the unified qualified wrapper, whose `orderBy` carries a sort key that is an EXPRESSION node rather than a bare `column` — a scalar-function, arithmetic, aggregate, or CAST node — and which the qualified expression renderer CAN render
* *WHEN* the adapter builds the wrapper SQL, for each of the four entry points that reach this wrapper: an N-scan inner equi-join, a non-decomposing grouped request (`GroupByWrapper`), a Case 2/3 `COUNT(DISTINCT)` decline, and a widened-projection row scan
* *THEN* the wrapper's outer `ORDER BY` SHALL carry that expression rendered with table-qualified column references resolved from each `column` node's `tableName`, exactly as the wrapper already renders its select list, WHERE, GROUP BY, and HAVING — so an expression sort key is rendered, NOT declined
* *AND* the element SHALL render its direction and NULL placement through the one shared direction/NULL seam, so the wrapper's ordering cannot drift from the other ordered paths
* *AND* an `orderBy` mixing a bare-column key with an expression key SHALL render EVERY element, in the pushed order, each with its own direction and NULL placement
* *AND* the wrapper's visible SELECT list SHALL be UNCHANGED by the ordering — the sort expression is rendered inline in the `ORDER BY`, so NO hidden output column is added and the returned column count still equals the `selectList` item count Exasol validates positionally
* *AND* the per-leg fan-out subqueries SHALL each project every column the sort expression names, via the SAME referenced-column narrowing helper that already walks the `orderBy` node's full expression tree, so the wrapper never references a column a leg does not emit
* *AND* the returned result SHALL equal the same query with the same `ORDER BY` evaluated over all rows on a single node

### Scenario: The qualified wrapper renders the request's OFFSET on every entry point that reaches it

* *GIVEN* a `pushdown` request routed to the unified qualified wrapper, carrying a renderable non-empty `orderBy`, a `limit` with `numElements` = `n`, and a non-zero `limit.offset` = `m`
* *WHEN* the adapter builds the wrapper SQL, for each of the four entry points that reach this wrapper: an N-scan inner equi-join, a non-decomposing grouped request, a Case 2/3 `COUNT(DISTINCT)` decline, and a widened-projection row scan
* *THEN* the wrapper's outer SELECT SHALL render `ORDER BY <clause> LIMIT n OFFSET m`, in that clause order, through the SAME shared limit-and-offset seam every other wrapper uses
* *AND* the per-leg fan-out subqueries SHALL remain limit-free and sort-free, so the outer SELECT is the ONLY place either bound is applied and no leg drops a row that the join or the window still needs
* *AND* a request whose `limit.offset` is zero or absent SHALL produce byte-identical SQL to the pre-change output on all four entry points
* *AND* the wrapper's visible SELECT list SHALL be UNCHANGED by the offset, so the returned column count still equals the `selectList` item count Exasol validates positionally
* *AND* the adapter SHALL NOT render an `OFFSET` on a wrapper SELECT that renders no `ORDER BY`, because Exasol rejects that SQL with `sqlCode 42000` ("OFFSET not allowed in LIMIT without ORDER BY")
* *AND* that state SHALL NOT be given a failure branch of its own, because it is unreachable: this wrapper resolves no `ORDER BY` clause ONLY when the request's `orderBy` is absent or empty, and a request carrying a non-zero `limit.offset` always carries a non-empty `orderBy` (see the offset-implies-ordering invariant in `vs-adapter/pushdown-planning-order-by-capability`) — an `orderBy` this wrapper cannot RENDER takes the separate `User` decline stated in `vs-adapter/pushdown-planning-topn`, which is a different state
* *AND* the adapter MUST NOT introduce a hard client-facing `User` decline for that unreachable state, because this seam serves all four entry points above: a decline here would turn a previously-successful query into a hard failure on a branch no test exercises. The invariant SHALL instead be pinned by an assertion plus a unit test asserting that no emitted wrapper SQL contains an `OFFSET` token unpreceded by an `ORDER BY`, driven by the request shapes that actually reach this seam with an offset
* *AND* the returned result SHALL equal the same query with the same `ORDER BY … LIMIT n OFFSET m` evaluated over all rows on a single node
