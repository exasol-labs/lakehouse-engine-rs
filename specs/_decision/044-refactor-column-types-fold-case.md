# Decisions: refactor-column-types-fold-case

## ADR: Unify the folds now that no reachable input distinguishes them

**ID:** column-types-fold-case-unified
**Plan:** refactor-column-types-fold-case
**Status:** Accepted
**Supersedes:** col-types-fold-divergence-unreachable-design-preserved

### Context

`col-types-fold-divergence-unreachable-design-preserved` measured that no column name the adapter
can declare distinguishes `str::to_uppercase` from `str::to_ascii_uppercase`, and refused to unify
them anyway: unifying would make either builder's fold depend on `resolve_table_schema`'s
upstream uppercasing, which that ADR treated as one module's decision leaking into another
module's body. Issue #270 revisits that refusal now that the surviving fold's own module owns the
consumer it must agree with.

### Decision

`column_types` folds with `str::to_uppercase` in its own body. The `fold_case` parameter and
`str::to_ascii_uppercase` are removed from both wrappers.

### Options Considered

| Option | Verdict |
|--------|---------|
| Unify on `to_uppercase`, removing `fold_case` | ✓ Chosen — the surviving fold matches `column_exa_type`, the in-module consumer; `resolve_table_schema`'s uppercasing is a behavior-preservation premise guarded by a live E2E test, not the rule that selects the fold |
| Keep both folds and the `fold_case` parameter | ✗ Rejected — a parameter whose two arguments cannot produce different output is dead flexibility, not a preserved decision |

### Consequences

The one new cross-fold pairing the removal creates — `referenced_side_columns`' Unicode-folded
input against `collect_side_column_names`' ASCII-folded reference set — is named in
`vs-adapter/pushdown-col-types-consolidation` rather than left silent, with its actual failure mode
(a dropped column on a mixed-fold miss, not merely a wider projection). The characterization test
that pinned the two-fold divergence is deleted with the parameter; no replacement unified-fold test
is added, because it would restate `str::to_uppercase`'s stdlib behavior over already-uppercased
input.

## ADR: The builder drops `fold_case` and takes only the table selection

**ID:** column-types-builder-single-selection-param
**Plan:** refactor-column-types-fold-case
**Status:** Accepted
**Supersedes:** column-types-builder-separate-selection-and-fold-params

### Context

`column_types` originally took `(request, select_table, fold_case)` to let `extract_all_column_types`
and `involved_table_columns` each keep their historical fold alongside their own table selection.
`column-types-fold-case-unified` unifies the fold both wrappers apply, which removes the reason the
second parameter existed.

### Decision

`column_types(request, select_table)`. The builder applies `str::to_uppercase` in its own body;
`select_table` is the only remaining parameter besides `request`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drop `fold_case`; builder folds internally | ✓ Chosen — a fixed-point sweep over all 1,112,064 Unicode scalar values found zero inputs where unifying changes the output, so removal is byte-identical, not a behavior change |
| Keep `fold_case` and pass `str::to_uppercase` from both wrappers | ✗ Rejected — a parameter with one reachable argument is the dead flexibility this issue exists to delete |
| Reshape `select_table` into an `Option<&str>` mode argument now that it is the only parameter left | ✗ Rejected — out of scope; edits a parameter this change is not removing for no observable gain |

### Consequences

Both wrappers survive with unchanged signatures and declaration sites — each still supplies a table
selection the builder does not choose, so neither becomes a pass-through. Every comment asserting
the removed two-fold divergence is reworded or deleted, including the newest carrier added by a
prior review fix, so no stale citation survives the parameter's removal.
