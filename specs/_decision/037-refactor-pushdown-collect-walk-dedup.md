# Decisions: refactor-pushdown-collect-walk-dedup

## ADR: `walk_column_nodes` visits `column` nodes only, narrowing the issue's suggested `walk_json`

**ID:** walk-column-nodes-narrow-traversal
**Plan:** refactor-pushdown-collect-walk-dedup
**Status:** Accepted

### Context

Three hand-rolled column-collecting walks (`collect_all_column_names`, `collect_column_tables`, `collect_side_column_names`) duplicate the same JSON recursion. Issue #177 suggests a generic `walk_json(expr, &mut impl FnMut(&Map))` firing on every object node. All three actual callers act only on `column`-typed nodes.

### Decision

Extract `pub(super) fn walk_column_nodes(expr: &Json, f: &mut impl FnMut(&Map<String, Json>))` in `adapter/pushdown/support.rs`. The primitive owns the recursion AND the `type == "column"` test; the callback receives only the matched `column` node's field map.

### Options Considered

| Option | Verdict |
|--------|---------|
| `walk_column_nodes` testing for `column` inside the primitive | ✓ Chosen — every caller acts only on `column` nodes, so the test belongs in the primitive; each caller's remaining body is 2–4 lines |
| Issue #177's literal `walk_json` over every object node | ✗ Rejected — pushes the `type == "column"` test back into all three closures, replacing one duplication with a smaller one three times over |
| A widest-form `FnMut(&Json)` seeing arrays and scalars too | ✗ Rejected — no current caller needs a non-column node, so every closure would immediately re-narrow |

### Consequences

One traversal primitive replaces three hand-rolled walks; the pushdown module tree holds exactly one blind column-collecting traversal. The narrowing is an intentional departure from the issue's suggested name and shape, recorded so a future reader does not "restore" the wider signature as a cleanup.

## ADR: Fold by deleting the wrapper, not by leaving a pass-through

**ID:** delete-wrapper-not-pass-through
**Plan:** refactor-pushdown-collect-walk-dedup
**Status:** Accepted

### Context

`str_prop`/`str_field` and `resolve_df_target_partitions`/`resolve_df_threads_per_udf` are each byte-identical pairs. Issue #177's literal wording ("both call it") would leave the original names as one-line pass-through wrappers around a new shared function.

### Decision

Delete `str_prop`, `str_field`, `resolve_df_target_partitions`, and `resolve_df_threads_per_udf` outright. Their call sites — roughly 40 across production and tests — call `nonempty_str` and `resolve_df_fixed_count` directly. `nonempty_str` stays private to `adapter`; `connection.rs` reaches it as `super::nonempty_str`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete all four names, migrate every call site | ✓ Chosen — a function whose whole body is one call with the same arguments is the pass-through red flag; the mechanical call-site edits buy the deletion of four names |
| Keep the original names as pass-through wrappers per the issue's literal wording | ✗ Rejected — trades two duplications for two shallow layers instead of removing indirection |

### Consequences

No visibility widens: a child module can name a private item of its parent, so hoisting `nonempty_str` to `adapter/mod.rs` keeps it private to `adapter`. Every resolver test keeps its name and expected value; only the callee spelling and the added key argument change.

## ADR: The collect primitive stays separate from issue #257's rewrite primitive

**ID:** collect-primitive-separate-from-rewrite-primitive
**Plan:** refactor-pushdown-collect-walk-dedup
**Status:** Accepted

### Context

Issue #257 owns a second, different JSON traversal: a curated-field, post-order rewrite walker backing three type-rewrite guards (`annotate_columns_with_alias`, `strip_table_alias`, and the `support` type-rewrite guards). A single shared walker serving both collect and rewrite callers was considered.

### Decision

This plan introduces no traversal shared with issue #257's rewrite primitive. It changes none of the three `support` type-rewrite guards and neither descoped transform walk, so it neither pre-empts nor blocks #257.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `walk_column_nodes` and #257's rewrite walker separate | ✓ Chosen — a rewrite MUST NOT descend into and rebuild `dataType`/`name` sub-objects, while a collect is read-only and must traverse every field; the two traversals have incompatible contracts |
| One shared walker for both collect and rewrite | ✗ Rejected — would force the rewrite walker's curated-field contract onto the collect side, or the collect side's blind traversal onto the rewrite side, breaking one of the two |

### Consequences

Issue #257's scope is untouched by this plan. The separation is substantive, not stylistic, and is stated in both issues so a future reader does not merge the two primitives as a "cleanup."
