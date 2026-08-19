# Feature: Pushdown Planning — Self-Join Leg Attribution

Extends the unified unaccelerated join fallback (`vs-adapter/pushdown-planning-join-fallback`) with
correct column-to-leg attribution for a table joined to itself. Every attribution decision —
expression rendering, leg-local WHERE attribution, join-condition attachment, and per-leg
projection narrowing — resolves a `column` node to a JOIN LEG (one occurrence of a table in the
FROM tree) by matching the pair (`tableName`, `tableAlias`) against the FROM-tree leaves' (`name`,
`alias`) pairs, never by `tableName` alone. `tableName` names a TABLE; a leg is an OCCURRENCE, and
the two coincide only while no table appears twice.

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

## Scenarios

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

### Scenario: One occurrence of a self-joined table carries no alias

* *GIVEN* a `pushdown` request whose `from` clause is an inner join over two leaves naming the SAME virtual table, where one leaf carries an `alias` and the other carries NO `alias` key — the shape `FROM T JOIN T b ON T.ID = b.ID` produces, whose two condition columns carry no `tableAlias` and `"B"` respectively
* *WHEN* the adapter renders the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL treat the ABSENT alias as part of the leg key rather than as a missing signal, so the unaliased occurrence and the aliased one resolve to DIFFERENT legs
* *AND* the adapter SHALL render the join condition against those two distinct leg aliases, and MUST NOT render a self-comparison
* *AND* the adapter MUST NOT resolve the unaliased column by `tableName` alone, which names both legs here
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same self-join evaluated on a single node

### Scenario: A three-leg self-join attaches each condition to its own leg pair

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over THREE leaves naming the SAME virtual table, each with its own alias — the shape `SELECT a.ID, b.ID, c.ID FROM T a JOIN T b ON a.ID = b.ID JOIN T c ON b.ID = c.ID` produces, which arrives as ONE request with left-deep nesting
* *WHEN* the adapter renders the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL render THREE distinct fan-out legs with THREE distinct subquery aliases, one per FROM-tree leaf, in the tree's left-to-right traversal order
* *AND* the adapter SHALL attach each join condition to the earliest join point at which every LEG the condition references is in scope, decided by the SET of LEG INDEXES the condition touches — so the two conditions land at two DIFFERENT join points instead of both collapsing onto the last one and leaving the first rendered `ON 1=1`
* *AND* each rendered condition SHALL reference exactly the two distinct leg aliases its two occurrences resolve to, and no condition SHALL be rendered twice
* *AND* the N ≥ 3 same-table case SHALL use the identical code path as the N = 2 case and as the all-distinct-tables case, differing only in the number of legs and in which legs share a `tableName`
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same three-way self-join evaluated on a single node

### Scenario: A WHERE conjunct local to one occurrence is pushed into only that occurrence's leg

* *GIVEN* a `pushdown` request that is an inner self-join over two occurrences of one table, carrying a top-level WHERE conjunct every `column` node of which resolves to ONE occurrence — the shape `WHERE b.SCORE < 5` produces
* *WHEN* the adapter builds each leg's fan-out subquery and each leg's format-level manifest-pruning predicate
* *THEN* the adapter SHALL push that conjunct into ONLY the leg its occurrence resolves to, and MUST NOT push it into the other leg, whose rows the conjunct does not constrain
* *AND* the adapter MUST NOT decide that attribution from the conjunct's `tableName`, which is identical on both legs and would attribute the conjunct to BOTH — over-filtering the unconstrained leg and silently dropping rows the join would have kept, with no error raised
* *AND* the same one-leg attribution SHALL govern BOTH consumers of that conjunct — the leg's DataFusion `ScanSpec.filter` and the leg's Iceberg manifest-pruning predicate — so a leg cannot prune files by a predicate its rows are not subject to
* *AND* a conjunct referencing BOTH occurrences SHALL be RESIDUAL and rendered in the outer wrapper's `WHERE`, exactly as a cross-table conjunct is
* *AND* the returned result SHALL equal the result of the same filtered self-join evaluated on a single node

### Scenario: A column reference no leg key matches fails loudly

* *GIVEN* a `pushdown` request over an inner join in which some `tableName` names MORE THAN ONE leg
* *AND* a `column` node of that `tableName` whose (`tableName`, `tableAlias`) pair matches no leaf's (`name`, `alias`) pair
* *WHEN* the adapter renders the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL return the wrapper's existing HARD client-facing error rather than render that reference against an arbitrarily chosen leg, because an arbitrary choice returns wrong rows with no error and this delta exists to remove exactly that failure mode
* *AND* the error message SHALL name the unattributable column and its table, so the failure is diagnosable from the client
* *AND* the adapter MUST NOT fall back to bare, unqualified rendering for such a column, which Exasol would reject as ambiguous across the wrapper's subqueries
* *AND* this state SHALL be UNREACHABLE for a well-formed request — a table joined to itself is only legal SQL with distinct occurrences, and Exasol stamps each occurrence's alias on every `column` node of an aliased FROM clause, including columns written unqualified — so it SHALL be pinned by a unit test over a synthesized request rather than by any production shape
* *AND* a `column` node whose `tableName` names EXACTLY ONE leg SHALL resolve by name alone and SHALL NOT consult its alias, because Exasol stamps no `tableAlias` at all on an unaliased FROM clause — so a request in which no table occurs twice emits BYTE-IDENTICAL SQL to its pre-change output

### Scenario: Shared-column-name join uses qualified rendering, not bare-name broadcast rendering

* *GIVEN* an inner equi-join `pushdown` request over two legs that share a column name — two different tables that both carry an `id` column, or the SAME table joined to itself, which shares every column name
* *WHEN* the adapter builds the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL render the join condition, WHERE filter, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references resolved to each `column` node's own LEG — never against a combined bare-name schema
* *AND* the disjoint-column-name guard SHALL gate broadcast eligibility only, NOT the unified fallback's rendering path
* *AND* a disjoint-guard failure SHALL be treated as a plain reason the broadcast path is unavailable, not as an error, so the request falls through to the qualified unified fallback SQL instead of a hard `Err`
* *AND* a table joined to itself SHALL therefore always take the unified fallback, because its legs declare an identical column set and the guard cannot pass — and NO separate same-table broadcast guard SHALL be added, so that decision keeps its one owner
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
