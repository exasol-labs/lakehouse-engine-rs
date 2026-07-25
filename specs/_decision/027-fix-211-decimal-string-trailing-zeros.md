# Decisions: fix-211-decimal-string-trailing-zeros

## ADR: Type-aware DECIMAL-to-string trim decision lives in the adapter; vs-expression stays type-blind

**ID:** decimal-string-trim-in-adapter-not-vs-expression
**Plan:** fix-211-decimal-string-trailing-zeros
**Status:** Accepted

### Context

Exasol trims trailing scale zeros when it converts a DECIMAL to text (`2912.00`→`'2912'`), but the
pushed-down DataFusion path's `CAST(decimal AS VARCHAR)` and implicit decimal→utf8 coercion both
render the full declared scale — a silent wrong-result divergence (issue #211), including a
demonstrated aggregate COUNT divergence. A stringified column's `dataType` never crosses the wire;
column Exasol types exist only in `involvedTables[0].columns`. `crates/vs-expression` is a pure,
stateless, sibling-shared JSON-to-SQL translator with no column-type context, so it cannot decide
type-dependent formatting itself.

### Decision

The adapter (`project_columns` and the `handle_pushdown` filter chain in `pushdown/support.rs`,
which already resolves column types via `extract_all_column_types`) decides where to inject the
DECIMAL→string trim; `vs-expression` gains only a pure primitive
(`format_decimal_exasol_style`) and a synthetic node it renders without inspecting types.

### Options Considered

| Option | Verdict |
|--------|---------|
| Type-aware guard in the adapter (`pushdown/support.rs`) | ✓ Chosen — the adapter already resolves column types per query; `vs-expression` stays a pure, stateless, shared translator |
| Add column-type awareness inside `vs-expression`'s CAST/string-function arms | ✗ Rejected — no column-type context on the wire, and the crate is stateless and sibling-shared |

### Consequences

Directly applies the accepted #207 ADR `like-guard-in-adapter-not-vs-expression`, which named #211
as its deferred follow-up, extending that precedent to the projection and WHERE-clause filter
paths. `vs-expression` remains reusable by the sibling VS-adapter project unchanged; any future
type-dependent pushdown gap must be fixed at the same adapter layer, not inside `vs-expression`.

## ADR: Inject an adapter-synthesized decimal_to_varchar_exasol node

**ID:** decimal-to-varchar-exasol-synthetic-node
**Plan:** fix-211-decimal-string-trailing-zeros
**Status:** Accepted

### Context

Once the adapter resolves a bare DECIMAL column at a stringification point (`CAST` to
VARCHAR/CHAR, `CONCAT`, `LENGTH` — including a DECIMAL column reachable only through a nested
`CONCAT` produced by chained `||`), it must rewrite that point so the rendered SQL reproduces
Exasol's trimmed formatting.

### Decision

The adapter rewrites a bare-DECIMAL-column stringification point into a one-argument
`decimal_to_varchar_exasol` JSON node; `vs-expression` renders it by rendering the argument then
applying `format_decimal_exasol_style`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Adapter-synthesized `decimal_to_varchar_exasol` node | ✓ Chosen — keeps nesting correct inside `CONCAT`, mirrors #207's `function_scalar_cast` injection for DATE, and is reused verbatim by #210 with zero new `vs-expression` code |
| Post-process the rendered SQL string to find and wrap the column | ✗ Rejected — fragile, and unable to target a nested `CONCAT` argument |
| A generic raw-SQL passthrough node | ✗ Rejected — a broader, less self-documenting surface |

### Consequences

`vs-expression` gains one narrow, purpose-built node type instead of a general escape hatch; issue
#210 can reuse the same node and the `format_decimal_exasol_style` primitive directly for its own
string-function DECIMAL handling with no further `vs-expression` changes.
