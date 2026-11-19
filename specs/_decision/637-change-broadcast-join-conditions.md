# Decisions: change-broadcast-join-conditions

## ADR: Split Inner Join Conditions into Join Equalities and WHERE Conjuncts

**ID:** split-inner-join-conditions-into-join-equalities-and-where-conjuncts
**Plan:** change-broadcast-join-conditions
**Status:** Accepted
**Supersedes:** per-side-predicate-pushdown-for-joins-by-tablename-conjunct-attribution

### Context

Exasol sends a join's whole `ON` clause as its condition and does not move a filter written there to `filter`. Per-side attribution reads only the top-level WHERE, so such a filter prunes no file, reaches no leg's scan, and keeps the join off the broadcast path.

### Decision

Each inner join's condition is split before WHERE attribution. Its join equalities stay in the condition and never reach a side. Its other conjuncts become top-level WHERE conjuncts that per-side attribution handles like any other: a conjunct whose columns all belong to one leg reaches that leg, and a cross-table conjunct or an OR spanning legs stays in the outer WHERE.
