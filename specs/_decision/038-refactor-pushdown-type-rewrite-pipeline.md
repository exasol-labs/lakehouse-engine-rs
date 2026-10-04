# Decisions: refactor-pushdown-type-rewrite-pipeline

## ADR: Two fixed-body pipeline functions, not one function with a pass-selection parameter

**ID:** two-fixed-pass-list-functions-not-a-pass-selection-parameter
**Plan:** refactor-pushdown-type-rewrite-pipeline
**Status:** Superseded by one-type-rewrite-pipeline-function-for-both-render-surfaces

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
