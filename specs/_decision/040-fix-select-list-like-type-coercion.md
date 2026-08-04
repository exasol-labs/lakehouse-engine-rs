# Decisions: fix-select-list-like-type-coercion

## ADR: The LIKE-subject guard runs first in the select-list pipeline

**ID:** like-guard-first-in-select-list-pipeline
**Plan:** fix-select-list-like-type-coercion
**Status:** Accepted

### Context

`apply_select_item_type_rewrites` chained `string_function_arg_type_guard` →
`rewrite_decimal_stringifications`, omitting the LIKE-subject pass the filter pipeline already
ran. Closing issue #219 required adding that pass, and its position in the sequence needed one
rule rather than two.

### Decision

`apply_select_item_type_rewrites`' body becomes identical to `apply_filter_type_rewrites`':
`like_subject_type_guard` → `string_function_arg_type_guard` →
`rewrite_decimal_stringifications`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Mirror the filter pipeline's order (LIKE guard first) | ✓ Chosen — one documented ordering rule for both pipelines; also independently correct, since the LIKE guard neither produces nor consumes a `decimal_to_varchar_exasol` node |
| Append the LIKE pass last, after the decimal rewrite | ✗ Rejected — would give the two render surfaces two different ordering rules to document and defend, and would let `rewrite_decimal_stringifications` act on a subtree the LIKE guard would have declined |

### Consequences

The only load-bearing ordering constraint — `string_function_arg_type_guard` must precede
`rewrite_decimal_stringifications` — still holds. Equalizing the two pass lists is also what
makes the two pipeline functions byte-identical, enabling their later collapse into one.

## ADR: A select-list decline widens the projection; it does not omit a filter

**ID:** select-list-decline-widens-projection-not-omits-filter
**Plan:** fix-select-list-like-type-coercion
**Status:** Accepted

### Context

The WHERE-clause pipeline's decline means "omit the whole top-level filter". The select-list
pipeline's decline means something different — `project_columns` returns `Ok` with the projection
widened to the full base row, because no filter is involved. The spec text for the two surfaces
needed to state that difference rather than reuse one phrasing for both.

### Decision

The select-list scenarios state the decline outcome as "sets the existing full-base-row fallback
flag", explicitly NOT as "declines the whole top-level filter". The pipeline's doc comment names
neither outcome.

### Options Considered

| Option | Verdict |
|--------|---------|
| State the select-list decline as "sets the full-base-row fallback flag" | ✓ Chosen — factually accurate, and keeps the pipeline's doc comment caller-agnostic |
| Reuse the WHERE-clause scenarios' "decline the whole filter" phrasing for symmetry | ✗ Rejected — factually wrong on the select-list path, where no filter is involved |

### Consequences

The `pushdown-module-structure` rule that a pipeline reports a decline and the caller decides what
a decline means is upheld: the spec documents each caller's meaning at the caller, not at the
shared pipeline. This is also what makes one pipeline function able to serve two callers whose
decline meanings differ.

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
