# Decisions: refactor-pushdown-type-rewrite-pipeline

## ADR: Narrow the three type-rewrite passes to private, making the pipelines the only reachable entry points

**ID:** narrow-type-rewrite-passes-to-private-pipelines-sole-entry-point
**Plan:** refactor-pushdown-type-rewrite-pipeline
**Status:** Accepted

### Context

Two production sites sequenced the same three type-rewrite passes (`like_subject_type_guard`,
`string_function_arg_type_guard`, `rewrite_decimal_stringifications`) with different pass lists, and
six unit tests in `pushdown/mod.rs` hand-copied the production chain to assert rendered-SQL
behavior. The load-bearing order — `string_function_arg_type_guard` MUST precede
`rewrite_decimal_stringifications` — was recorded only as prose, with nothing enforcing it once
extracted into shared pipeline functions.

### Decision

After rewiring every caller onto the two new pipeline functions
(`apply_filter_type_rewrites`, `apply_select_item_type_rewrites`), narrow
`like_subject_type_guard`, `string_function_arg_type_guard`, and
`rewrite_decimal_stringifications` from `pub(super)` to private.

### Options Considered

| Option | Verdict |
|--------|---------|
| Narrow the three passes to private | ✓ Chosen — the call-site census proves zero callers outside `support` remain after rewiring, so the order becomes compiler-enforced rather than convention |
| Leave the three passes `pub(super)` | ✗ Rejected — the pipelines would be merely available, leaving the order prose-only and re-derivable by hand, exactly how the six test replicas arose |

### Consequences

A future caller cannot bypass the pipeline to re-sequence the passes; the guarantee is scoped
honestly as cross-module, not absolute — inside `support`, `project_columns` can still call a pass
directly. The narrowing is a deletion of surface, not an addition of machinery.

## ADR: Two fixed-body pipeline functions, not one function with a pass-selection parameter

**ID:** two-fixed-pass-list-functions-not-a-pass-selection-parameter
**Plan:** refactor-pushdown-type-rewrite-pipeline
**Status:** Accepted

### Context

The filter pipeline runs three passes; the select-list pipeline runs two, omitting the
LIKE-subject pass because that wiring is not yet done (tracked by issue #219). The two pass lists
differ today for a reason that needs a doc comment and an issue citation, not silent compression
into a caller-supplied flag.

### Decision

`apply_filter_type_rewrites` and `apply_select_item_type_rewrites` are two separate functions with
fixed pass-sequence bodies, each taking `(&Json, &[(String, String)]) -> Option<Json>`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two functions with fixed pass lists | ✓ Chosen — the pass-list difference is a tracked gap (#219) that belongs in a doc comment with an issue citation, not a boolean a caller can flip |
| One function taking `include_like_pass: bool` | ✗ Rejected — a configuration parameter is a decision the module declined to make; a reader sees a toggle and infers a supported configuration rather than a tracked gap |
| A `Vec<Box<dyn RewritePass>>` registry | ✗ Rejected — issue #259 explicitly rejects this; the pass list is never assembled at runtime, so dynamic dispatch buys nothing over a fixed body |

### Consequences

The LIKE-subject pass's absence from the select-list pipeline stays visibly a tracked gap rather
than an inferred invariant. Closing issue #219 becomes a one-line change inside one function body.
An earlier draft justified the two-function split on the false claim that a select-list item can
never be a LIKE-predicate subject; that claim was removed and the split now stands on the pass
lists differing today, not on that disproven premise.
