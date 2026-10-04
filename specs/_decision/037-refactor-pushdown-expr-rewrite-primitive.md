# Decisions: refactor-pushdown-expr-rewrite-primitive

## ADR: One free function plus a per-node closure, not a visitor trait or typed AST

**ID:** rewrite-expr-tree-shared-post-order-primitive-not-visitor-or-typed-ast
**Plan:** refactor-pushdown-expr-rewrite-primitive
**Status:** Accepted

### Context

Three type-aware rewriters (the LIKE-subject guard, the string-function argument guard, and the DECIMAL stringification rewrite) each hand-rolled the same post-order recursion over the untyped JSON pushdown grammar. Two kept their child-field lists in sync only by comment, and a fourth rewriter would copy the traversal again (#257).

### Decision

One private free function walks the tree post-order and takes a per-node closure. Two module constants hold the curated child-field lists. Each guard supplies only its own per-node decision.

### Options Considered

| Option | Verdict |
|--------|---------|
| `Visitor` trait with a method per node type | Rejected: adds a type surface per node kind and gains nothing over an untyped IR |
| Typed expression AST parsed from the JSON | Rejected: contradicts the no-SQL-parser property of `vs-expression` and needs a second grammar owner |
| Pass-ordering pipeline abstraction over the three guards | Rejected: out of scope per #257, and the production chain keeps its explicit, order-commented composition |

### Consequences

Extending the field list for a new node type is one line that all three guards inherit. Issue #177 reuses the primitive for its two rebuild-shape join walks. A decline at a newly reached position is correct, since Exasol evaluates the predicate natively, so it applies uniformly.

## ADR: The blind collect-style JSON walker stays a separate primitive from the curated rewrite primitive

**ID:** walk-json-blind-collect-walker-stays-separate-from-curated-rewrite-primitive
**Plan:** refactor-pushdown-expr-rewrite-primitive
**Status:** Accepted

### Context

Issue #177 dedups a blind collect-style `walk_json` that recurses over every map value. The curated rewrite must not descend into `dataType` or `name` sub-objects, because a rewrite may touch expression children only.

### Decision

The curated rewrite primitive and the blind collect walker stay separate, with different reach contracts.

### Options Considered

| Option | Verdict |
|--------|---------|
| One universal walker | Rejected: its blind recursion over every map value is what the curated walker must avoid, and merging would widen the rewrite surface of all three guards |

### Consequences

The curated field list stays an auditable design decision, and #177 builds on the rewrite primitive without the blind walker's wider reach.
