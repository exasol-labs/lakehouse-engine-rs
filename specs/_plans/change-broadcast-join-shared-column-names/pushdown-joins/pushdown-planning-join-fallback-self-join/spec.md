# Feature: Pushdown Planning, Self-Join Leg Attribution

Extends the unified unaccelerated join fallback (`pushdown-joins/pushdown-planning-join-fallback`) so a table joined to itself renders correctly. The adapter resolves each `column` node to a join leg (one occurrence of a table in the FROM tree) by the pair (`tableName`, `tableAlias`), never by `tableName` alone.

<!-- DELTA:CHANGED -->
## Background

* A `column` node carries the per-occurrence alias under `tableAlias`, and a FROM-tree leaf carries it under `alias`. The adapter matches (`tableName`, `tableAlias`) against the leaves' (`name`, `alias`) pairs, comparing the alias verbatim.
* The pair identifies exactly one leg. SQL forbids two occurrences of a table with the same alias and allows at most one occurrence without an alias. An absent alias is part of the key.
* A `tableName` that names exactly one leg resolves by name alone, because Exasol stamps no `tableAlias` on an unaliased FROM clause.
* One binding, derived from the detected join's leaves, is the only resolver of column-to-leg attribution. Expression rendering, leg-local WHERE attribution, join-condition attachment, per-leg projection narrowing, and the broadcast join's per-occurrence relation aliases (`pushdown-joins/pushdown-planning-join`) all use it.
* A column reference that matches no leg key is a hard client error, because rendering it against an arbitrary leg returns wrong rows.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A table joined to itself renders each occurrence as its own leg

* *GIVEN* a `pushdown` request whose `from` clause is an inner join over TWO leaves naming the SAME virtual table, the shape `SELECT a.ID, b.ID FROM T a JOIN T b ON a.ID = b.ID` produces
* *AND* each FROM-tree leaf carries the per-occurrence SQL alias under its `alias` key, and every `column` node carries the same alias under its `tableAlias` key, including a column the user wrote unqualified
* *WHEN* the adapter renders the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL retain each FROM-tree leaf's `alias` when it collects the join tree, so leg identity is captured at COLLECTION time and never reconstructed later
* *AND* the adapter SHALL resolve each `column` node to a LEG INDEX by matching the pair (`tableName`, `tableAlias`) against the leaves' (`name`, `alias`) pairs, comparing the alias VERBATIM, and SHALL render the column qualified with THAT leg's subquery alias, so the two occurrences of `T` render as `"LHS_T0"` and `"LHS_T1"` and NEVER collapse onto one alias
* *AND* the emitted `ON` clause SHALL compare one leg to the OTHER leg, `ON (("LHS_T0"."ID" = "LHS_T1"."ID"))`, and MUST NOT emit a self-comparison of one alias to itself, which is trivially true and degenerates the join into a cross product
* *AND* the outer SELECT list SHALL qualify each item with the leg its own occurrence resolves to, so a select list naming both occurrences MUST NOT render every item against one leg
* *AND* leg resolution SHALL come from ONE binding derived from the detected join's leaves and consulted by EVERY attribution decision, expression rendering, leg-local WHERE attribution, join-condition attachment, and per-leg projection narrowing alike, so no two decisions can disagree about which leg a column belongs to, and no call site SHALL re-derive leg identity from `tableName`
* *AND* the (`tableName`, `alias`) pair SHALL be treated as identifying exactly one leg, which SQL guarantees: two occurrences of one table cannot share an alias, and at most one occurrence can carry no alias
* *AND* a leg no clause of the request references SHALL still be scanned, so a self-join written with a condition over one occurrence only stays the cross product SQL means rather than becoming an error
* *AND* a two-leg self-join SHALL reach this fallback only when it fails the broadcast contract, for example when the table is above the broadcast threshold, because the broadcast path serves a self-join that meets it (`pushdown-joins/pushdown-planning-join`)
* *AND* the returned result SHALL equal, as an order-independent multiset, the result of the same self-join evaluated on a single node, for a primitive column and for a nested column rendered as JSON alike
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Shared-column-name join uses qualified rendering, not bare-name broadcast rendering

* *GIVEN* an inner equi-join `pushdown` request over two legs that share a column name, two different tables that both carry an `id` column, or the SAME table joined to itself, which shares every column name
* *WHEN* the adapter builds the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL render the join condition, WHERE filter, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references resolved to each `column` node's own LEG, never against a combined bare-name schema
* *AND* the disjoint-column-name guard SHALL gate broadcast eligibility only, NOT the unified fallback's rendering path
* *AND* a disjoint-guard failure SHALL be treated as a plain reason the broadcast path is unavailable, not as an error, so the request falls through to the qualified unified fallback SQL instead of a hard `Err`
* *AND* a table joined to itself SHALL therefore always take the unified fallback, because its legs declare an identical column set and the guard cannot pass, and NO separate same-table broadcast guard SHALL be added, so that decision keeps its one owner
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A shared-column-name join on the fallback renders each column qualified by its leg

* *GIVEN* an inner join `pushdown` request that the unified unaccelerated fallback serves, over legs that share a column name: two different tables that both carry an `id` column, or the SAME table joined to itself
* *WHEN* the adapter builds the unified unaccelerated fallback SQL
* *THEN* the adapter SHALL render the join condition, WHERE filter, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references resolved to each `column` node's own LEG, never against a combined bare-name schema
* *AND* the returned result SHALL equal the result of the same inner join evaluated on a single node
<!-- /DELTA:NEW -->
