# Decisions: refactor-pushdown-expr-rewrite-primitive

## ADR: One free function plus a per-node closure, not a visitor trait or typed AST

**ID:** rewrite-expr-tree-shared-post-order-primitive-not-visitor-or-typed-ast
**Plan:** refactor-pushdown-expr-rewrite-primitive
**Status:** Accepted

### Context

Three type-aware expression-tree rewriters in `pushdown/support.rs` — `like_subject_type_guard`,
`string_function_arg_type_guard`, and `rewrite_decimal_stringifications` — each hand-rolled the
same post-order recursion over the untyped `serde_json` pushdown expression grammar. Two of them
duplicated the curated child-field list verbatim, kept in sync only by comment. Each of three
type-blind fixes (#207, #210, #211) copied the traversal again; a fourth would copy it a third
time (#257).

### Decision

`fn rewrite_expr_tree(node: &Json, f: &impl Fn(&Json) -> Option<Json>) -> Option<Json>` — a private
free function plus two module-level consts (`EXPR_ARRAY_FIELDS`, `EXPR_SINGLE_FIELDS`) holding the
curated child-field lists. Each guard supplies its own per-node decision as the closure and owns no
traversal code.

### Options Considered

| Option | Verdict |
|--------|---------|
| Free function + per-node closure | ✓ Chosen — the honest size for the duplication actually observed: one traversal, three per-node decisions, over a deliberately untyped IR |
| `Visitor` trait with a method per node type | ✗ Rejected — adds a type surface per node kind and buys nothing over an untyped IR |
| Typed expression AST parsed from the JSON | ✗ Rejected — contradicts `vs-expression`'s stated no-SQL-parser property and would need a second grammar owner |
| A pass-ordering pipeline abstraction over the three guards | ✗ Rejected — out of scope per #257; the one production chain site keeps its explicit composition and load-bearing order comment |

### Consequences

One traversal and one field-list declaration replace three duplicated copies; extending the
curated field list for a future node type becomes a one-line change all three guards inherit.
Issue #177 reuses the same primitive for its two rebuild-shape join walks, while keeping its own
blind collect-style walk separate (see the companion ADR on that boundary).

## ADR: The blind collect-style JSON walker stays a separate primitive from the curated rewrite primitive

**ID:** walk-json-blind-collect-walker-stays-separate-from-curated-rewrite-primitive
**Plan:** refactor-pushdown-expr-rewrite-primitive
**Status:** Accepted

### Context

Issue #177 separately dedups a blind, collect-style `walk_json` that recurses over every
`map.values()` entry. `rewrite_expr_tree`'s curated field list must never descend into a node's
`dataType` or `name` sub-objects and rebuild them — a rewrite must touch expression children only.

### Decision

Do not merge `rewrite_expr_tree` with the blind collect-style `walk_json`. They stay two
primitives with two different reach contracts.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the curated and blind walkers separate | ✓ Chosen — #257 owns the curated rewrite primitive, #177 reuses it for its two rebuild-shape join walks and keeps its blind collect walk separate |
| One universal JSON walker serving both | ✗ Rejected — the blind walker recurses over every `map.values()` entry, which is exactly what the curated walker must not do; merging them would silently widen the rewrite surface of all three type guards |

### Consequences

The curated field list stays a documentable, auditable design decision rather than an
implementation detail folded into a general-purpose walker. #177 can build its rebuild-shape join
walks on `rewrite_expr_tree` without inheriting the blind walker's wider reach.

## ADR: The LIKE guard's widened reach may trade a working pushdown for a decline

**ID:** like-guard-widened-reach-may-trade-a-pushdown-for-a-decline
**Plan:** refactor-pushdown-expr-rewrite-primitive
**Status:** Accepted

### Context

Migrating `like_subject_type_guard` onto the shared primitive widens its traversal past the
former junction-only recursion (`predicate_and`/`predicate_or`/`predicate_not`), reaching a LIKE
nested inside a `function_scalar_case`, under a comparison operand, or inside a scalar function's
`arguments`. At those newly-reached positions a non-string or unresolvable subject now declines
the whole filter where it previously rendered.

### Decision

Accept the trade: a decline at a newly-reached position is unconditionally correct (Exasol
evaluates the predicate natively), so it is applied uniformly regardless of which of the two
sub-cases below produced it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Widen the traversal and accept the decline trade | ✓ Chosen — correctness first; a decline is always correct, never wrong |
| Keep the junction-only traversal, leave #207's blind spot open | ✗ Rejected — the blind spot's only remaining home was a code comment, since #207 is closed |
| Restrict the widened traversal to positions where no former pushdown existed | ✗ Rejected — unimplementable without re-deriving which shapes DataFusion happens to reject |

### Consequences

Where the subject type RESOLVES to a non-string type, the decline fixes a crash: the pre-change
render hard-failed the DataFusion scan. Where the subject name does NOT resolve, the decline may
cost a previously-working pushdown — slower, never wrong. The two sub-cases are recorded
separately (see the companion ADR on the unresolvable-subject correction) rather than conflated
into one absolute claim.

## ADR: An unresolvable LIKE subject's decline is a possible pushdown loss, not a fixed hard failure

**ID:** unresolvable-like-subject-decline-is-a-pushdown-loss-not-a-fixed-hard-failure
**Plan:** refactor-pushdown-expr-rewrite-primitive
**Status:** Accepted

### Context

The plan and the like-coercion delta initially recorded an absolute: every newly-reached LIKE
position "previously pushed down and hard-failed the DataFusion scan" / "was never a working
pushdown". That is false for one of the decline triggers the same scenario enumerates:
`extract_all_column_types` `filter_map`s over `involvedTables[0].columns` and silently drops any
entry missing `name` or `dataType`, reading the FIRST involved table only — so a genuinely VARCHAR
column can miss the lookup. At a newly-reached position that shape rendered `Utf8 LIKE Utf8` and
SUCCEEDED before this change; post-change it declines. No test could satisfy the absolute as
originally recorded.

### Decision

Split the reach clause into two sub-cases: a subject whose Exasol type RESOLVES to a non-string
type has its decline replace a hard scan failure (a fixed crash); a subject whose name does NOT
resolve MAY instead lose a pushdown that previously rendered and succeeded — an accepted cost that
SHALL NOT be recorded as a fixed hard failure.

### Options Considered

| Option | Verdict |
|--------|---------|
| Record the two sub-cases separately | ✓ Chosen — matches the verified behavior of `extract_all_column_types` exactly |
| Keep the single absolute claim | ✗ Rejected — provably false for the unresolvable-name sub-case; no test could satisfy it |

### Consequences

The trade recorded in the companion widened-reach ADR is unchanged; only its justification is
now accurate for both sub-cases it covers. Future edits to `extract_all_column_types`'s lookup
completeness change which sub-case a given subject falls into, not the correctness of either
branch.

## ADR: The leaf-equivalence characterization test must precede the migration it proves

**ID:** leaf-equivalence-characterization-test-must-precede-the-migration-it-proves
**Plan:** refactor-pushdown-expr-rewrite-primitive
**Status:** Accepted

### Context

The plan's proof that dropping each guard's `!node.is_object()` early return is
behavior-preserving rests on a characterization test asserting `rewrite_decimal_stringifications`
returns a non-object node unchanged. An earlier draft scheduled that test in the same parallel
group as the primitive extraction it was meant to validate — after the migration, not before. A
test written after a migration pins the new code's behavior; it proves nothing about equivalence
with the pre-migration code.

### Decision

The leaf-equivalence test is its own first parallel group, ordered before the primitive
extraction. It MUST be added and pass against the UNMIGRATED `rewrite_decimal_stringifications`,
with its `!node.is_object()` early return still in place, then re-run unchanged after the
migration. If it can only be made to pass after the migration, the "leaves are passed to `f` too"
simplification is not behavior-preserving and the migration task must stop.

### Options Considered

| Option | Verdict |
|--------|---------|
| Test-first, proven against the unmigrated code | ✓ Chosen — the only ordering that makes the test an equivalence proof rather than a description |
| Test written alongside or after the migration | ✗ Rejected — pins the new code's behavior only, proves no equivalence, and cannot serve as the plan's designated proof |

### Consequences

Any future primitive change that touches leaf handling inherits a real regression guard rather
than a test that would silently pass regardless of which behavior shipped first.
