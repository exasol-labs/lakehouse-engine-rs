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

Accept the trade: a decline at a newly-reached position is unconditionally correct (Exasol evaluates the predicate natively), so it is applied uniformly regardless of which of the two sub-cases below produced it.

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

