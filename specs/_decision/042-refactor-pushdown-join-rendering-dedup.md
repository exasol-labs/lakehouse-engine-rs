# Decisions: refactor-pushdown-join-rendering-dedup

## ADR: The two clause-walk routines share only their clause set, not a unified narrowing function

**ID:** pushdown-clause-walk-shared-set-not-unified-narrowing
**Plan:** refactor-pushdown-join-rendering-dedup
**Status:** Accepted

### Context

`referenced_side_columns` (`joins/rendering.rs`) and `referenced_column_projection`
(`joins/sql_builders.rs`) each hand-roll the same walk over the clause set whose rendered
SQL can name a source column. Issue #181 reads at first glance as calling for one unified
`collect_referenced_column_names(...)` used by both sites. The two routines actually differ
in five ways: the extra join-condition argument, per-table versus all-table attribution, the
absent-`selectList` short-circuit (`full_cols` immediately, never inspecting another clause)
versus always narrowing, the empty-result fallback (`full_cols` versus `all_cols.first()`),
and the return type. The decisive difference is case folding: `collect_all_column_names`
folds with Unicode `to_uppercase`, `collect_side_column_names` with ASCII-only
`to_ascii_uppercase`. Two sources state the two folds MUST NOT be reconciled — the
`walk_column_nodes` doc comment in `crates/lakehouse-engine/src/adapter/pushdown/support.rs`
("Case folding is deliberately NOT owned here … Those two MUST NOT be unified. They differ
for non-ASCII identifiers — `ß` folds to `SS` under Unicode but stays `ß` under ASCII"), and
`specs/vs-adapter/pushdown-module-structure/spec.md`'s §Background bullet plus the
case-folding *AND* of its "One blind traversal primitive backs every column-collecting walk"
scenario. No test in the crate used a non-ASCII identifier before this plan, so a merge would
have changed behavior while the entire suite still passed.

### Decision

Extract `referenced_clause_values(pushdown_req, visit)` in `joins/rendering.rs`, owning only
*which clauses can name a source column*. Each caller supplies its own per-node collector and
keeps its own filter, case folding, short-circuit, and empty-result fallback.

### Options Considered

| Option | Verdict |
|--------|---------|
| Caller-supplied collector over a shared clause walk | ✓ Chosen — passing the collector in makes reconciling the two case folds impossible by construction, while still removing the duplicated clause-set enumeration |
| One unified function returning the narrowed column list for both callers | ✗ Rejected — would have to reconcile five divergences, including the case-folding disagreement two independent sources forbid unifying |

### Consequences

Adding or removing a clause from the shared set (`filter`, `groupBy`, `orderBy`, `having`) is
now a one-function edit instead of two. `selectList` keeps two named sites by design —
`referenced_side_columns`' short-circuit guard is a fallback policy the walk deliberately does
not own. Each caller's case folding and fallback policy remain independently readable and
testable, at the cost of the walk not fully collapsing the two callers into one.

## ADR: The case-folding prohibition's source is the support.rs doc comment and the pushdown-module-structure spec, not decision 037

**ID:** pushdown-case-folding-source-correction
**Plan:** refactor-pushdown-join-rendering-dedup
**Status:** Accepted

### Context

Early planning drafts (plan.md §Context, §Consequences, task 1.1, and this plan's decision
log) attributed the case-folding non-reconciliation prohibition to
`specs/_decision/037-refactor-pushdown-collect-walk-dedup.md`. That ADR fragment contains
three decisions — the narrowed `walk_column_nodes` traversal, the wrapper-deletion precedent,
and the separation from issue #257's rewrite primitive — and does not mention case folding at
all. Plan review flagged this as a false-attribution risk: because the case-folding decision
was itself marked for ADR promotion, the false provenance would otherwise have been written
permanently into the library, and a later planner checking the cited authority would find no
constraint there and could reconcile the two folds believing nothing forbade it.

### Decision

Attribute the case-folding non-reconciliation constraint to its two actual sources: the
`walk_column_nodes` doc comment in `crates/lakehouse-engine/src/adapter/pushdown/support.rs`
("Case folding is deliberately NOT owned here … Those two MUST NOT be unified", with the `ß`
→ `SS` example), and `specs/vs-adapter/pushdown-module-structure/spec.md`'s §Background bullet
"The two case-folding calls this codebase uses are NOT interchangeable" plus the case-folding
*AND* of its "One blind traversal primitive backs every column-collecting walk" scenario.
`specs/_decision/037-refactor-pushdown-collect-walk-dedup.md` remains the correct source for
the wrapper-deletion precedent only.

### Options Considered

| Option | Verdict |
|--------|---------|
| Correct every case-folding attribution to the two real sources, leaving 037's wrapper-deletion citations untouched | ✓ Chosen — verified both quotations against the files at HEAD; each corrected site now states positively that 037 is silent on case folding |
| Leave the attribution as originally drafted | ✗ Rejected — would have written a false citation into the permanent decision log, invitable as grounds for a future reconciliation that breaks non-ASCII identifiers |

### Consequences

A future reader checking why the two folds cannot be unified finds the constraint at its
actual source instead of an ADR that is silent on the topic. Decision 037's remaining
citations (wrapper-deletion precedent, `walk_json` separation) are unaffected and stay
correctly attributed.
