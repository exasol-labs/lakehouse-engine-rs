# Decisions: fix-select-list-like-type-coercion

## ADR: The two pipeline functions collapse into one `apply_type_rewrites`

**ID:** one-type-rewrite-pipeline-function-for-both-render-surfaces
**Plan:** fix-select-list-like-type-coercion
**Status:** Accepted
**Supersedes:** two-fixed-pass-list-functions-not-a-pass-selection-parameter

### Context

Once the LIKE-subject guard is added to the select-list pipeline, its body becomes byte-identical
to the filter pipeline's — both run the same three passes in the same order. Two names for one
body is a redundancy nothing enforces; the split's original justification (differing pass lists)
no longer holds.

### Decision

Delete `apply_select_item_type_rewrites` outright and rename `apply_filter_type_rewrites` to
`pub(super) fn apply_type_rewrites`, so one function serves both render surfaces. The signature
`(&Json, &[(String, String)]) -> Option<Json>` is unchanged, so every call site is a bare
identifier swap the compiler verifies.

### Options Considered

| Option | Verdict |
|--------|---------|
| Collapse into one function, in this plan, as its last task | ✓ Chosen — the signature-preserving rename buys no safety by deferral, and doing the collapse in the same plan avoids a window where the library defends a redundancy nobody intends to keep |
| Defer the collapse to its own change | ✗ Rejected — buys no safety (compiler-verified rename), only a second plan/review/PR cycle for a rename over already-correct behavior |
| Keep `apply_select_item_type_rewrites` as a thin `pub(super)` alias | ✗ Rejected — module-private with one production caller, so no external consumer an alias could protect; a pass-through method with no purpose |

### Consequences

One pipeline function now owns the pass order for both render surfaces. The function's doc
comment states the two decline meanings abstractly rather than by caller name, which is what lets
it serve both. The narrowing to one `pub(super)` entry point applies uniformly instead of
differing per surface.
