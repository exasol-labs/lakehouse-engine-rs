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
name, so a table joined to itself renders each occurrence as its own leg. The unaccelerated
fallback has exactly one implementation for all N ≥ 2 legs.

## Background

* **This delta fixes issue #361: a self-join returned a cross product.** Every attribution
  decision in the fallback keyed on a `column` node's `tableName`, and a self-join's legs all
  carry the SAME `tableName`, so the name-keyed alias map collapsed to one entry
  (last-write-wins) and both sides of the `ON` rendered against the same subquery alias.
  Captured live from `EXPLAIN VIRTUAL`'s `PUSHDOWN_JSON` column on the Docker container for
  `SELECT a.O_ORDERKEY, a.O_CUSTKEY FROM FACT_ORDERS a JOIN FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY`
  over a 10-row table, which returned 100 rows:
  `SELECT "LHS_T1"."O_ORDERKEY", "LHS_T1"."O_CUSTKEY" FROM (…) AS "LHS_T0" INNER JOIN (…) AS "LHS_T1" ON (("LHS_T1"."O_ORDERKEY" = "LHS_T1"."O_ORDERKEY"))`
  — a tautological `ON`, an unconstrained `LHS_T0`, and a select list wrongly all-`LHS_T1`. The
  three-leg shape returned 1000 rows instead of 10 and rendered `ON 1=1` at one join point plus
  the same tautology twice at the next.
* **SUPERSEDES every recorded clause that resolves a column reference "from its `tableName`".**
  `tableName` names a TABLE; a wrapper leg is an OCCURRENCE of a table in the FROM tree. The two
  coincide only while no table appears twice. The attribution signal is the JOIN LEG INDEX.
* **The signal was already in the request and was discarded at collection time.** Exasol stamps
  the per-occurrence SQL alias on BOTH ends of the pushdown JSON, under two DIFFERENT key names:
  a FROM-tree leaf carries `alias` (`{"type":"table","name":"FACT_ORDERS","alias":"A"}`), and a
  `column` node carries `tableAlias`
  (`{"type":"column","name":"O_ORDERKEY","tableName":"FACT_ORDERS","tableAlias":"A"}`). Both were
  captured live. `collect_join_tree` read only the leaf's `name` and dropped its `alias`, and
  `JoinLeaf` had no field to hold it — so the defect is two-part: identity is LOST at collection
  and then re-derived from `tableName` at render. Recovering the leaf alias is what makes leg
  resolution exact rather than reconstructed.
* **`(tableName, alias)` is an injective leg key, guaranteed by SQL itself.** Within one
  `tableName`, two occurrences cannot share an alias (`FROM T a JOIN T a` is illegal) and at most
  one occurrence can be alias-LESS (`FROM T JOIN T` is an ambiguous reference and is rejected).
  The pair therefore identifies exactly one leg, and no alias sorting, occurrence counting, or
  positional guess is needed. Aliases are compared VERBATIM: the leaf `alias` and the column
  `tableAlias` come from one parse of one statement, and both were observed identical (`a` → `"A"`,
  `o` → `"O"`).
* **One leg of a genuine self-join MAY carry no alias, and that is a valid identity.** Captured
  live: `FROM FACT_ORDERS JOIN FACT_ORDERS b ON FACT_ORDERS.O_ORDERKEY = b.O_ORDERKEY` yields
  leaves `{"name":"FACT_ORDERS"}` and `{"name":"FACT_ORDERS","alias":"B"}`, and the condition's
  two `column` nodes carry no `tableAlias` and `"B"` respectively. It returned 100 rows instead of
  10. An absent alias is part of the key, never a reason to fall back to a bare name.
* **A `tableName` naming exactly ONE leg resolves by name alone, so no existing SQL changes.**
  Exasol stamps no `tableAlias` at all when the user writes no alias — captured live for a
  two-table join, whose `column` nodes carry only `tableName` and whose leaves carry no `alias`
  key. Resolving a single-leg name without consulting an alias is what keeps that common case
  working and keeps the wrapper's output BYTE-IDENTICAL to its pre-change output for every request
  in which no table occurs twice. That covers every recorded golden-SQL fixture and every
  non-self-join E2E shape, and is asserted rather than assumed.
* **One owner, not four re-derivations.** The leg binding is derived from the detected join's
  leaves and is the SOLE resolver of column-to-leg attribution. Before this delta four call sites
  independently re-derived side identity from `tableName`: the qualified expression renderer's
  alias map, the leg-local WHERE attribution feeding each leg's manifest pruning and DataFusion
  filter, the FROM-chain's condition-attachment scope set, and the per-leg projection narrowing.
  Each was a separate instance of ONE defect. A single owner is what makes the three N-leg
  decisions — which alias a reference renders as, which leg a conjunct pushes into, which join
  point a condition attaches at — impossible to answer inconsistently.
* **The binding is reachable only THROUGH the detected join, never rebuilt by a caller.** It is
  derived from the same leaf list the join-tree walk produced, so no renderer can be handed a
  binding built from a different request and no call site can invent its own.
* **A leg no clause references is still scanned.** `SELECT a.ID FROM T a JOIN T b ON a.ID = a.ID`
  references one occurrence only. The other leg binds normally, nothing resolves to it, the
  existing join-point clamp attaches the one-leg condition at a real join point, and the result
  stays the cross product SQL means rather than becoming an error.
* **A column reference matching no leg key is unattributable and FAILS LOUDLY.** The wrapper
  raises its existing client-facing hard error rather than render the reference against an
  arbitrarily chosen leg. Wrong rows are the failure mode this delta removes; a hard error is not
  a regression from it. The state is unreachable for a well-formed request — Exasol stamps the
  alias on every `column` node of an aliased FROM clause, including columns written UNQUALIFIED
  (verified live, and the premise `strip_table_alias` already documents) — so it is pinned by a
  unit test over a synthesized request, not by any production shape.
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

<!-- DELTA:NEW -->
### Scenario: A table joined to itself renders each occurrence as its own leg

* *GIVEN* a `pushdown` request whose `from` clause is an inner join over TWO leaves naming the SAME virtual table — the shape `SELECT a.ID, b.ID FROM T a JOIN T b ON a.ID = b.ID` produces
* *AND* each FROM-tree leaf carries the per-occurrence SQL alias under its `alias` key, and every `column` node carries the same alias under its `tableAlias` key, including a column the user wrote unqualified
* *WHEN* the adapter renders the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL retain each FROM-tree leaf's `alias` when it collects the join tree, so leg identity is captured at COLLECTION time and never reconstructed later
* *AND* the adapter SHALL resolve each `column` node to a LEG INDEX by matching the pair (`tableName`, `tableAlias`) against the leaves' (`name`, `alias`) pairs, comparing the alias VERBATIM, and SHALL render the column qualified with THAT leg's subquery alias — so the two occurrences of `T` render as `"LHS_T0"` and `"LHS_T1"` and NEVER collapse onto one alias
* *AND* the emitted `ON` clause SHALL compare one leg to the OTHER leg — `ON (("LHS_T0"."ID" = "LHS_T1"."ID"))` — and MUST NOT emit a self-comparison of one alias to itself, which is trivially true and degenerates the join into a cross product (issue #361)
* *AND* the outer SELECT list SHALL qualify each item with the leg its own occurrence resolves to, so a select list naming both occurrences MUST NOT render every item against one leg
* *AND* leg resolution SHALL come from ONE binding derived from the detected join's leaves and consulted by EVERY attribution decision — expression rendering, leg-local WHERE attribution, join-condition attachment, and per-leg projection narrowing alike — so no two decisions can disagree about which leg a column belongs to, and no call site SHALL re-derive leg identity from `tableName`
* *AND* the (`tableName`, `alias`) pair SHALL be treated as identifying exactly one leg, which SQL guarantees: two occurrences of one table cannot share an alias, and at most one occurrence can carry no alias
* *AND* a leg no clause of the request references SHALL still be scanned, so a self-join written with a condition over one occurrence only stays the cross product SQL means rather than becoming an error
* *AND* the request MUST NOT reach the broadcast path, because a table joined to itself declares an identical column set on both sides and the disjoint-column-name guard already declines it here
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same self-join evaluated on a single node, for a primitive column and for a nested column rendered as JSON alike
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: One occurrence of a self-joined table carries no alias

* *GIVEN* a `pushdown` request whose `from` clause is an inner join over two leaves naming the SAME virtual table, where one leaf carries an `alias` and the other carries NO `alias` key — the shape `FROM T JOIN T b ON T.ID = b.ID` produces, whose two condition columns carry no `tableAlias` and `"B"` respectively
* *WHEN* the adapter renders the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL treat the ABSENT alias as part of the leg key rather than as a missing signal, so the unaliased occurrence and the aliased one resolve to DIFFERENT legs
* *AND* the adapter SHALL render the join condition against those two distinct leg aliases, and MUST NOT render a self-comparison
* *AND* the adapter MUST NOT resolve the unaliased column by `tableName` alone, which names both legs here
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same self-join evaluated on a single node
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A three-leg self-join attaches each condition to its own leg pair

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over THREE leaves naming the SAME virtual table, each with its own alias — the shape `SELECT a.ID, b.ID, c.ID FROM T a JOIN T b ON a.ID = b.ID JOIN T c ON b.ID = c.ID` produces, which arrives as ONE request with left-deep nesting
* *WHEN* the adapter renders the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL render THREE distinct fan-out legs with THREE distinct subquery aliases, one per FROM-tree leaf, in the tree's left-to-right traversal order
* *AND* the adapter SHALL attach each join condition to the earliest join point at which every LEG the condition references is in scope, decided by the SET of LEG INDEXES the condition touches — so the two conditions land at two DIFFERENT join points instead of both collapsing onto the last one and leaving the first rendered `ON 1=1`
* *AND* each rendered condition SHALL reference exactly the two distinct leg aliases its two occurrences resolve to, and no condition SHALL be rendered twice
* *AND* the N ≥ 3 same-table case SHALL use the identical code path as the N = 2 case and as the all-distinct-tables case, differing only in the number of legs and in which legs share a `tableName`
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same three-way self-join evaluated on a single node
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A WHERE conjunct local to one occurrence is pushed into only that occurrence's leg

* *GIVEN* a `pushdown` request that is an inner self-join over two occurrences of one table, carrying a top-level WHERE conjunct every `column` node of which resolves to ONE occurrence — the shape `WHERE b.SCORE < 5` produces
* *WHEN* the adapter builds each leg's fan-out subquery and each leg's format-level manifest-pruning predicate
* *THEN* the adapter SHALL push that conjunct into ONLY the leg its occurrence resolves to, and MUST NOT push it into the other leg, whose rows the conjunct does not constrain
* *AND* the adapter MUST NOT decide that attribution from the conjunct's `tableName`, which is identical on both legs and would attribute the conjunct to BOTH — over-filtering the unconstrained leg and silently dropping rows the join would have kept, with no error raised
* *AND* the same one-leg attribution SHALL govern BOTH consumers of that conjunct — the leg's DataFusion `ScanSpec.filter` and the leg's Iceberg manifest-pruning predicate — so a leg cannot prune files by a predicate its rows are not subject to
* *AND* a conjunct referencing BOTH occurrences SHALL be RESIDUAL and rendered in the outer wrapper's `WHERE`, exactly as a cross-table conjunct is
* *AND* the returned result SHALL equal the result of the same filtered self-join evaluated on a single node
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A column reference no leg key matches fails loudly

* *GIVEN* a `pushdown` request over an inner join in which some `tableName` names MORE THAN ONE leg
* *AND* a `column` node of that `tableName` whose (`tableName`, `tableAlias`) pair matches no leaf's (`name`, `alias`) pair
* *WHEN* the adapter renders the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL return the wrapper's existing HARD client-facing error rather than render that reference against an arbitrarily chosen leg, because an arbitrary choice returns wrong rows with no error and this delta exists to remove exactly that failure mode
* *AND* the error message SHALL name the unattributable column and its table, so the failure is diagnosable from the client
* *AND* the adapter MUST NOT fall back to bare, unqualified rendering for such a column, which Exasol would reject as ambiguous across the wrapper's subqueries
* *AND* this state SHALL be UNREACHABLE for a well-formed request — a table joined to itself is only legal SQL with distinct occurrences, and Exasol stamps each occurrence's alias on every `column` node of an aliased FROM clause, including columns written unqualified — so it SHALL be pinned by a unit test over a synthesized request rather than by any production shape
* *AND* a `column` node whose `tableName` names EXACTLY ONE leg SHALL resolve by name alone and SHALL NOT consult its alias, because Exasol stamps no `tableAlias` at all on an unaliased FROM clause — so a request in which no table occurs twice emits BYTE-IDENTICAL SQL to its pre-change output
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over three or more leaves, every join node of which is inner
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT return an error and SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL serve the request through the SAME single unified fallback renderer used for the two-leg (N = 2) case, differing only in the number of legs
* *AND* the adapter SHALL resolve each leg's Iceberg snapshot, data-file list, and logical schema exactly once — recovering each leg's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name — and SHALL treat an involved table absent from the mapping as the same stale-virtual-schema hard error the single-table path reports
* *AND* the adapter SHALL emit SQL that scans EACH leg independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructs the original inner join over all N subquery results with a left-to-right `INNER JOIN … ON` chain in Exasol's core engine, each join condition attached to the `ON` of the join point at which every LEG it references is in scope
* *AND* every join condition, WHERE filter, select-list item, GROUP BY, HAVING, and ORDER BY the wrapper renders SHALL use table-qualified column references resolved to each `column` node's own LEG — REPLACING the recorded "resolved from each `column` node's `tableName` against the involved table that owns it", which cannot distinguish two leaves naming ONE table (issue #361) — so the wrapper is correct whether or not any two legs share a column name AND whether or not any two legs are the same table
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same inner join evaluated on a single node
* *AND* the adapter MUST NOT read any leg's Parquet row data in the planning layer — only file-level metadata crosses into each leg's scan spec
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Join conditions attach greedily by table-name set and side-local filters push into each leg

Retired and RENAMED. Its heading states the defect issue #361 fixes — attribution by table-name
set — so it is removed rather than edited in place, and every clause it carried is restated,
corrected, in the scenario immediately below.

* *GIVEN* a unified unaccelerated fallback over N ≥ 2 involved tables with a set of join conditions and a WHERE filter
* *WHEN* the adapter renders the `INNER JOIN … ON` chain
* *THEN* the adapter SHALL attach each join condition to the earliest join point in the left-to-right chain at which every table the condition references is in scope, deciding scope by the SET of `tableName`s the condition touches — NEVER by column name, so shared column names across sides stay correctly qualified
* *AND* a join point at which no not-yet-attached condition becomes resolvable SHALL be rendered with `ON 1=1`
* *AND* a top-level WHERE conjunct SHALL be pushed INTO a side's fan-out leg as a DataFusion filter if and only if it references only that ONE table AND the type-rewrite pipeline run against THAT SIDE's own column metadata accepts it AND the DataFusion dialect can render that pipeline's REWRITTEN form of it
* *AND* every OTHER conjunct SHALL be RESIDUAL and remain in the outer wrapper's `WHERE`: cross-table, OR-spanning, untagged, column-free, side-local-but-unrenderable, or side-local-but-type-declined
* *AND* each side's Iceberg manifest-pruning predicate SHALL keep every side-local conjunct in its RAW form, renderable or not and type-accepted or not, so neither a render decline nor a type decline nor a rewrite SHALL change which files are opened
* *AND* the returned result SHALL equal the result of the same inner join evaluated on a single node, for any assignment of conditions to join points
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Join conditions attach greedily by LEG set and leg-local filters push into each leg

* *GIVEN* a unified unaccelerated fallback over N ≥ 2 legs with a set of join conditions and a WHERE filter
* *WHEN* the adapter renders the `INNER JOIN … ON` chain
* *THEN* the adapter SHALL attach each join condition to the earliest join point in the left-to-right chain at which every LEG the condition references is in scope, deciding scope by the SET of LEG INDEXES the condition touches — REPLACING the recorded "by the SET of `tableName`s the condition touches", which merges two leaves naming one table into a single scope entry and misplaces a condition once N ≥ 3 legs include a repeated table (issue #361) — and NEVER by column name, so shared column names across legs stay correctly qualified
* *AND* a join point at which no not-yet-attached condition becomes resolvable SHALL be rendered with `ON 1=1`
* *AND* a top-level WHERE conjunct SHALL be pushed INTO a leg's fan-out as a DataFusion filter if and only if it references only that ONE LEG — REPLACING the recorded "references only that ONE table", which attributes a conjunct written against one occurrence of a self-joined table to BOTH of its legs and over-filters the leg the conjunct does not constrain — AND the type-rewrite pipeline run against THAT LEG's own column metadata accepts it AND the DataFusion dialect can render that pipeline's REWRITTEN form of it
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
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Shared-column-name join uses qualified rendering, not bare-name broadcast rendering

* *GIVEN* an inner equi-join `pushdown` request over two legs that share a column name — two different tables that both carry an `id` column, or the SAME table joined to itself, which shares every column name
* *WHEN* the adapter builds the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL render the join condition, WHERE filter, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references resolved to each `column` node's own LEG — REPLACING the recorded "resolved from each `column` node's `tableName` against the side that owns it", under which two legs of one table resolve to the same alias — never against a combined bare-name schema
* *AND* the disjoint-column-name guard SHALL gate broadcast eligibility only, NOT the unified fallback's rendering path
* *AND* a disjoint-guard failure SHALL be treated as a plain reason the broadcast path is unavailable, not as an error, so the request falls through to the qualified unified fallback SQL instead of a hard `Err`
* *AND* a table joined to itself SHALL therefore always take the unified fallback, because its legs declare an identical column set and the guard cannot pass — and NO separate same-table broadcast guard SHALL be added, so that decision keeps its one owner
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
<!-- /DELTA:CHANGED -->
