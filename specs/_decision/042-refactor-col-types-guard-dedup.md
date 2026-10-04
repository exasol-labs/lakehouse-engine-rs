# Decisions: refactor-col-types-guard-dedup

## ADR: The merged builder takes table selection and case fold as two separate parameters

**ID:** column-types-builder-separate-selection-and-fold-params
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted

### Context

`extract_all_column_types` and `involved_table_columns` walk `involvedTables` identically. They differ in table selection (first versus named) and case fold (Unicode versus ASCII-only).

### Decision

One builder takes the table selector and the case-fold function as separate parameters, and each wrapper passes its own.

### Options Considered

| Option | Verdict |
|--------|---------|
| One `Option<&str>` table name that derives the fold | Rejected: records an unreconciled divergence as intended behavior |
| Unify the fold | Rejected: a behavior change outside a pure refactor |
| Builder takes the selected table | Rejected: leaves the navigation duplicated |

### Consequences

`fold_case` only preserves a divergence that a tracked follow-up issue removes. The shape reads as a divergence with an end date, not as intended generality.

## ADR: The two builders' case-fold divergence is pinned and tracked, not reconciled

**ID:** col-types-fold-divergence-pinned-and-tracked
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted

### Context

The two builders fold column names differently, and the divergence was believed reachable through a non-ASCII name such as `straße`.

### Decision

Both folds stay byte-for-byte, a characterization test pins them before the merge, and a GitHub issue tracks reconciliation.

### Options Considered

| Option | Verdict |
|--------|---------|
| Unify the folds | Rejected: changes which non-ASCII join requests decline, outside a pure refactor |
| Preserve the divergence silently | Rejected: fails the never-a-silent-gap standard in CLAUDE.md |

### Consequences

The reachability claim was superseded by `col-types-fold-divergence-unreachable-design-preserved`. Preserving both folds and the test stands on new grounds.

## ADR: The fold divergence is unreachable, and preserved for a design reason rather than a behavioral one

**ID:** col-types-fold-divergence-unreachable-design-preserved
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted
**Supersedes:** col-types-fold-divergence-pinned-and-tracked

### Context

A live capture showed `straße` is served as `STRASSE`, because `resolve_table_schema` uppercases every Iceberg field name before Exasol sees it. A sweep of all 1,112,064 Unicode scalar values found no case where a second `to_uppercase` or `to_ascii_uppercase` changes the output, so both folds are no-ops.

### Decision

Both folds and the characterization test stay, because no declarable column name distinguishes the folds. The follow-up issue changes from reconciling a divergence to deleting the `fold_case` parameter as dead flexibility.

### Options Considered

| Option | Verdict |
|--------|---------|
| Unify the folds | Rejected: still a behavior change outside a pure refactor, and makes the result depend on another module's uppercasing |
| Keep the divergence and say nothing | Rejected: `fold_case` would read as intended generality, violating the never-a-silent-gap standard |

### Consequences

The test now relies on a constructed literal on which the two folds disagree. It is the only assertion that would catch a silent unification. The tracked issue is a low-priority simplification, not a correctness fix.
