# Feature: Pushdown Planning, Self-Join Leg Attribution

Extends the unified unaccelerated join fallback (`pushdown-joins/pushdown-planning-join-fallback`) so a table joined to itself renders correctly. The adapter resolves each `column` node to a join leg (one occurrence of a table in the FROM tree) by the pair (`tableName`, `tableAlias`), never by `tableName` alone.

## Background

* A `column` node carries the per-occurrence alias under `tableAlias`, and a FROM-tree leaf carries it under `alias`. The adapter matches (`tableName`, `tableAlias`) against the leaves' (`name`, `alias`) pairs, comparing the alias verbatim.
* The pair identifies exactly one leg. SQL forbids two occurrences of a table with the same alias and allows at most one occurrence without an alias. An absent alias is part of the key.
* A `tableName` that names exactly one leg resolves by name alone, because Exasol stamps no `tableAlias` on an unaliased FROM clause.
* One binding, derived from the detected join's leaves, is the only resolver of column-to-leg attribution. Expression rendering, leg-local WHERE attribution, join-condition attachment, and per-leg projection narrowing all use it.
* A leg that no clause references is still scanned.
* A column reference that matches no leg key is a hard client error, because rendering it against an arbitrary leg returns wrong rows.

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
* *AND* this state SHALL be UNREACHABLE for a well-formed request, because a table joined to itself is only legal SQL with distinct occurrences, and Exasol stamps each occurrence's alias on every `column` node of an aliased FROM clause, including columns written unqualified
* *AND* a `column` node whose `tableName` names EXACTLY ONE leg SHALL resolve by name alone and SHALL NOT consult its alias, because Exasol stamps no `tableAlias` at all on an unaliased FROM clause — so a request in which no table occurs twice emits BYTE-IDENTICAL SQL to its pre-change output

### Scenario: Shared-column-name join uses qualified rendering, not bare-name broadcast rendering

* *GIVEN* an inner equi-join `pushdown` request over two legs that share a column name — two different tables that both carry an `id` column, or the SAME table joined to itself, which shares every column name
* *WHEN* the adapter builds the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL render the join condition, WHERE filter, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references resolved to each `column` node's own LEG — never against a combined bare-name schema
* *AND* the disjoint-column-name guard SHALL gate broadcast eligibility only, NOT the unified fallback's rendering path
* *AND* a disjoint-guard failure SHALL be treated as a plain reason the broadcast path is unavailable, not as an error, so the request falls through to the qualified unified fallback SQL instead of a hard `Err`
* *AND* a table joined to itself SHALL therefore always take the unified fallback, because its legs declare an identical column set and the guard cannot pass — and NO separate same-table broadcast guard SHALL be added, so that decision keeps its one owner
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
