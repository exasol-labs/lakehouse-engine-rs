# Decisions: refactor-col-types-guard-dedup

## ADR: The merged builder takes table selection and case fold as two separate parameters

**ID:** column-types-builder-separate-selection-and-fold-params
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted

### Context

`extract_all_column_types` and `involved_table_columns` perform byte-for-byte the same
`involvedTables` walk, differing only in which table they select (first, versus named) and which
case-fold they apply (Unicode `to_uppercase`, versus ASCII-only `to_ascii_uppercase`). Merging them
into one builder required deciding how to parameterize both differences.

### Decision

`column_types(request, select_table, fold_case)`. `extract_all_column_types` passes a first-table
selector plus `str::to_uppercase`; `involved_table_columns` passes a find-by-name selector plus
`str::to_ascii_uppercase`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two separate parameters, selection and fold | ✓ Chosen — the two decisions correlate today by accident, not by design; keeping them separate keeps a third combination expressible |
| One `Option<&str>` table-name argument, deriving the fold from it | ✗ Rejected — would record an unreconciled divergence as intended behavior |
| Unify the fold for both callers | ✗ Rejected — a behavior change outside a pure refactor's scope |
| Builder takes the already-selected table `&Json`, leaving navigation duplicated | ✗ Rejected — leaves the `involvedTables` navigation duplicated, buying back less than it costs |

### Consequences

`fold_case` exists only to preserve a divergence this plan itself schedules for removal via a
tracked follow-up issue that deletes `fold_case` once closed. The two-parameter shape reads as a
preserved divergence with a known end date rather than as intended generality.

## ADR: The two builders' case-fold divergence is pinned and tracked, not reconciled

**ID:** col-types-fold-divergence-pinned-and-tracked
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted

### Context

`extract_all_column_types` folds column names with the Unicode `to_uppercase`; `involved_table_columns`
folds with `to_ascii_uppercase`. At planning time this divergence was believed reachable through a
non-ASCII column name (`straße`), based on two live captures against the Docker Exasol container plus
one inference joining them: that Exasol applies the same fold to an adapter-declared JSON column name
as to a native unquoted DDL identifier.

### Decision

Preserve both folds byte-for-byte. Write the characterization test BEFORE the merge, and file a
GitHub issue tracking reconciliation, cited in the test.

### Options Considered

| Option | Verdict |
|--------|---------|
| Preserve both folds, pin with a test, track with an issue | ✓ Chosen — CLAUDE.md's "never a silent gap" standard requires naming a real consequence rather than reconciling or hiding it |
| Unify the folds in this plan | ✗ Rejected — changes which non-ASCII join requests decline, outside the "pure refactor" invariant |
| Preserve the divergence silently | ✗ Rejected — fails the never-a-silent-gap standard |

### Consequences

This decision's REACHABILITY claim was later superseded by the plan's task 3 live-capture gate,
which measured that no column name reaching either builder in production can distinguish the two
folds (see the superseding ADR). The decision to preserve both folds and pin them with a test
stands on new grounds.

## ADR: The fold divergence is unreachable, and preserved for a design reason rather than a behavioral one

**ID:** col-types-fold-divergence-unreachable-design-preserved
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted
**Supersedes:** col-types-fold-divergence-pinned-and-tracked

### Context

The plan's task 3 live-capture gate, run against the local Docker Exasol container, measured that
an Iceberg column `straße` is served as `STRASSE`, not the expected `STRAßE`. Root-cause analysis
traced this to this crate's own `resolve_table_schema` (`file_resolution.rs:610-644`), which maps
every Iceberg field through `f.name.to_uppercase()` before Exasol ever sees the name — not to any
Exasol-side normalization. Both folds are therefore no-ops on the result: a full Unicode sweep of
all 1,112,064 scalar values found zero cases where a second `to_uppercase` or a `to_ascii_uppercase`
alters `to_uppercase` output.

### Decision

Keep both folds byte-for-byte and keep the characterization test, on new grounds: no column name
the adapter can declare distinguishes the two folds. Rescope the follow-up issue from reconciling a
divergence to deleting `column_types`' `fold_case` parameter as dead flexibility.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep both folds, reframe the test and the issue as unreachable-input-domain / dead-flexibility | ✓ Chosen — unifying still changes `involved_table_columns`' output for a non-ASCII input outside the pure-refactor invariant, and its harmlessness would rest on `resolve_table_schema`'s fold — the information leakage this plan exists to remove |
| Unify the folds now that no reachable input distinguishes them | ✗ Rejected — still a behavior change outside a pure refactor's scope, and encodes a dependency on another module's decision |
| Keep the divergence and say nothing further | ✗ Rejected — `fold_case` preserving nothing observable reads as intended generality unless stated otherwise, violating the never-a-silent-gap standard |

### Consequences

The characterization test's justification changes from "the form Exasol delivers" to "a constructed
literal on which Rust's two folds disagree" — it remains the only assertion in the repository that
would catch a silent unification, since every column name reaching either builder in production is
already Unicode-uppercased. The tracked issue becomes a low-priority simplification, not a
correctness fix.

