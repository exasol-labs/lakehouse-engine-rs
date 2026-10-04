# Decisions: fix-select-list-like-type-coercion

## ADR: The two pipeline functions collapse into one `apply_type_rewrites`

**ID:** one-type-rewrite-pipeline-function-for-both-render-surfaces
**Plan:** fix-select-list-like-type-coercion
**Status:** Accepted
**Supersedes:** two-fixed-pass-list-functions-not-a-pass-selection-parameter

### Context

Once the select-list pipeline gains the LIKE-subject guard, both pipelines run the same three passes in the same order, so two functions have one body.

### Decision

One function, `apply_type_rewrites`, serves both render surfaces, and the select-list function is deleted. The signature is unchanged, so each call-site change is a rename the compiler verifies.

### Options Considered

| Option | Verdict |
|--------|---------|
| Defer the collapse to a later change | Rejected: the rename is compiler-verified, so deferral only adds a plan, review, and PR cycle |
| Keep the select-list function as an alias | Rejected: the function is module-private with one production caller, so an alias protects nothing |

### Consequences

One function owns the pass order. Its doc comment states the two decline meanings abstractly, not by caller name.
