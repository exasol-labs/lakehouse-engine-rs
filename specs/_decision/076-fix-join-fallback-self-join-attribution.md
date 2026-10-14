# Decisions: fix-join-fallback-self-join-attribution

## ADR: Resolve legs from the FROM-tree leaf `alias`, not from column-node aliases

**ID:** join-leg-resolution-from-from-tree-leaf-alias
**Plan:** `fix-join-fallback-self-join-attribution`
**Status:** Accepted

### Context

Issue #361: a self-join returned a cross product. The unaccelerated join fallback keyed column references on the column's `tableName`, which is identical for every leg of a self-join, so the alias map collapsed to one entry. A live capture against Exasol showed that each FROM-tree `table` leaf carries its own `alias`, which `collect_join_tree` discarded.

### Decision

`collect_join_tree` keeps each FROM-tree leaf's `alias` on `JoinLeaf`. A column resolves to a leg by matching its (`tableName`, `tableAlias`) pair against the leaves' (`name`, `alias`) pairs, with the alias compared verbatim.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reconstruct the mapping from the distinct `tableAlias` values per `tableName` | Rejected: needs an arbitrary bijection and a rule for count mismatches, and assumed leaves carry no alias |

### Consequences

Leg identity is captured once, where the FROM tree is walked, and never reconstructed downstream.

## ADR: `(tableName, alias)` is the leg key, with an absent alias part of the key

**ID:** join-leg-key-tablename-alias-pair
**Plan:** `fix-join-fallback-self-join-attribution`
**Status:** Accepted

### Context

A leg key must resolve every well-formed reference and never conflate two occurrences of one table. A self-join can leave one occurrence unaliased (`FROM T JOIN T b`), and Exasol stamps no `tableAlias` when the user writes none.

### Decision

An absent alias is a distinct key value. A `tableName` that names exactly one leg resolves by name alone and never consults an alias.

### Options Considered

| Option | Verdict |
|--------|---------|
| Key on the alias alone | Rejected: a self-join can leave one occurrence unaliased |
| Always require an alias match | Rejected: breaks every unaliased join |

### Consequences

The pair resolves every well-formed reference exactly, with no sorting, counting, or positional guess. Every non-self-join request still emits byte-identical SQL.

## ADR: One attribution owner rather than four corrected re-derivations

**ID:** join-legs-single-attribution-owner
**Plan:** `fix-join-fallback-self-join-attribution`
**Status:** Accepted

### Context

Four call sites in the join fallback each re-derived column-to-leg identity from `tableName`: the expression renderer's alias map, leg-local WHERE attribution, the FROM-chain condition scope, and per-leg projection narrowing. Each caused a distinct wrong-results symptom in a self-join.

### Decision

`JoinLegs`, in `joins/attribution.rs`, is the sole resolver of column-to-leg attribution and is reachable only through `DetectedJoin::legs()`. The four `tableName`-keyed derivations are deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Correct each of the four call sites in place | Rejected: keeps the four-derivation structure and its drift risk |

### Consequences

The alias to render, the leg to push a conjunct into, and the join point for a condition cannot be answered inconsistently. `attribution` depends on neither `planning` nor `rendering`, so the dependency direction stays acyclic.
