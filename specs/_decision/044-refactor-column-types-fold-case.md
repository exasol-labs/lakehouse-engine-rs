# Decisions: refactor-column-types-fold-case

## ADR: Unify the folds now that no reachable input distinguishes them

**ID:** column-types-fold-case-unified
**Plan:** refactor-column-types-fold-case
**Status:** Accepted
**Supersedes:** col-types-fold-divergence-unreachable-design-preserved

### Context

No declarable column name distinguishes `to_uppercase` from `to_ascii_uppercase`. The earlier ADR refused to unify because the result would then depend on the upstream uppercasing in `resolve_table_schema`. Issue #270 revisits this now that the surviving fold's own module owns the consumer it must agree with.

### Decision

The builder folds with `str::to_uppercase` in its own body, and the `fold_case` parameter and ASCII fold are removed from both wrappers.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep both folds and `fold_case` | Rejected: a parameter whose arguments cannot produce different output is dead flexibility |

### Consequences

The removal creates one new pairing, `referenced_side_columns`' Unicode-folded input against `collect_side_column_names`' ASCII-folded reference set. It is named in `vs-adapter/pushdown-col-types-consolidation`, with its failure mode: a dropped column on a mixed-fold miss. The characterization test is deleted with the parameter, with no replacement, because it would restate stdlib behavior.

## ADR: The builder drops `fold_case` and takes only the table selection

**ID:** column-types-builder-single-selection-param
**Plan:** refactor-column-types-fold-case
**Status:** Accepted
**Supersedes:** column-types-builder-separate-selection-and-fold-params

### Context

The fold parameter existed so each wrapper could keep its own fold. Unifying the fold removes that reason.

### Decision

`column_types` takes the request and the table selector only, and folds internally.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `fold_case` and pass `to_uppercase` from both wrappers | Rejected: a parameter with one reachable argument is dead flexibility |
| Reshape the selector into an `Option<&str>` | Rejected: out of scope, with no observable gain |

### Consequences

Removal is byte-identical, since the Unicode sweep found no differing input. Both wrappers keep their signatures because each still supplies a table selection. Comments that cite the removed divergence are reworded or deleted.
