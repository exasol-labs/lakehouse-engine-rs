# Decisions: fix-join-fallback-self-join-attribution

## ADR: Resolve legs from the FROM-tree leaf `alias`, not from column-node aliases

**ID:** join-leg-resolution-from-from-tree-leaf-alias
**Plan:** `fix-join-fallback-self-join-attribution`
**Status:** Accepted

### Context

Issue #361: a self-join returned a cross product. Every attribution decision in the unified
unaccelerated join fallback keyed a column reference on its `column` node's `tableName`, and a
self-join's two (or more) legs all carry the same `tableName`, so the name-keyed alias map
collapsed to one entry (last-write-wins). The going-into-planning research finding was that a
FROM-tree `table` leaf carried no alias (`{"name":…, "type":"table"}`), which would have forced a
reconstruction of the alias-to-leg mapping from the distinct `tableAlias` values a request's
`column` nodes reference. A live capture against the Docker Exasol container disproved that
finding: a leaf carries `alias` (`{"type":"table","name":"FACT_ORDERS","alias":"A"}`) alongside the
`column` node's own `tableAlias` key. `collect_join_tree` read only the leaf's `name` and discarded
`alias`, and `JoinLeaf` had no field to hold it, so leg identity was lost at collection and then
wrongly re-derived from `tableName` at render time.

### Decision

Retain each FROM-tree `table` leaf's `alias` in `collect_join_tree`, store it on `JoinLeaf`, and
resolve a `column` node to a leg by matching the pair (`tableName`, `tableAlias`) against the
leaves' (`name`, `alias`) pairs, comparing the alias verbatim.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read the leaf's own `alias` at collection time | ✓ Chosen — the signal is already in the request; reading it makes leg resolution exact rather than reconstructed |
| Reconstruct the mapping from the distinct `tableAlias` values referenced per `tableName`, bound to that name's leg positions in a deterministic order | ✗ Rejected — needs an arbitrary bijection plus a rule for alias-count mismatches, and rested on a since-disproven premise that the leaf carries no alias |

### Consequences

Leg identity is captured exactly once, where the FROM tree is walked, and never reconstructed
downstream. Task 1.1 re-verified the premise live before any production edit, closing the risk that
this decision rested on an unverified signal.

## ADR: `(tableName, alias)` is the leg key, with an absent alias part of the key

**ID:** join-leg-key-tablename-alias-pair
**Plan:** `fix-join-fallback-self-join-attribution`
**Status:** Accepted

### Context

A leg key needs to be total (it must resolve every well-formed reference) and exact (it must never
conflate two distinct occurrences of one table). Two live observations bear on this. First, a
genuine self-join may leave one occurrence unaliased — `FROM T JOIN T b`, captured live, still
returned 100 rows instead of 10 — so an alias-only key is not total. Second, Exasol stamps no
`tableAlias` at all when the user writes no alias, so a key that always requires an alias match
would break the common, currently-correct case of an unaliased join.

### Decision

Treat an absent alias as a distinct key value rather than as a missing signal. A `tableName` naming
exactly one leg resolves by name alone and never consults an alias.

### Options Considered

| Option | Verdict |
|--------|---------|
| `(tableName, alias)` pair, absent alias included as a key value; single-leg names resolve without consulting alias | ✓ Chosen — injective by SQL's own rules (two occurrences of one table cannot share an alias; at most one can be alias-less), and keeps every unaliased-join request byte-identical |
| Key on the alias alone | ✗ Rejected — not total; a genuine self-join can leave one occurrence unaliased |
| Always require an alias match on every column | ✗ Rejected — breaks every unaliased join, since Exasol stamps no `tableAlias` there |

### Consequences

The pair resolves every well-formed reference exactly, with no alias sorting, occurrence counting,
or positional guess, and every non-self-join request keeps emitting byte-identical SQL.

## ADR: One attribution owner rather than four corrected re-derivations

**ID:** join-legs-single-attribution-owner
**Plan:** `fix-join-fallback-self-join-attribution`
**Status:** Accepted

### Context

Four call sites in the unified unaccelerated join fallback each independently re-derived
column-to-leg identity from `tableName`: the qualified expression renderer's alias map, the
leg-local WHERE attribution feeding each leg's manifest pruning and DataFusion filter, the
FROM-chain's condition-attachment scope set, and the per-leg projection narrowing. Each was a
separate instance of the same root-cause defect, so a self-join produced four related but
distinct wrong-results symptoms (a tautological `ON`, silent over-filtering of an unconstrained
leg, misplaced join conditions at N ≥ 3, and inconsistent projection qualification).

### Decision

Introduce `JoinLegs` in a new `joins/attribution.rs` as the sole resolver of column-to-leg
attribution, reachable only through `DetectedJoin::legs()`, and delete the four `tableName`-keyed
derivations it replaces.

### Options Considered

| Option | Verdict |
|--------|---------|
| One `JoinLegs` owner threaded through all four call sites | ✓ Chosen — makes the three N-leg decisions (alias to render, leg to push a conjunct into, join point to attach a condition at) impossible to answer inconsistently |
| Correct each of the four call sites in place, each reading the leaf alias itself | ✗ Rejected — leaves the same four-derivation structure and the same drift risk that produced the original defect |

### Consequences

`attribution` depends on neither `planning` nor `rendering`, so the dependency direction stays
acyclic and the boundary is visible without reading internals. No caller can be handed a binding
built from a different request, and none can invent its own.
