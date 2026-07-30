# Decisions: refactor-col-types-guard-dedup

## ADR: The lookup helper does not test the node's `type` tag

**ID:** column-exa-type-helper-excludes-tag-test
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted

### Context

The three pushdown type-rewrite guards (`guard_like_subject`, `is_bare_decimal_column`,
`coerce_string_position_arg`) each hand-rolled the same `column`-node type resolve against
`col_types`. Extracting one shared helper required deciding whether the helper should also absorb
each guard's `type == "column"` tag test ahead of the lookup.

### Decision

`column_exa_type(node, col_types) -> Option<&str>` owns only the resolve — read `name`, fold, scan
`col_types`. Each of the three guards keeps its own `type == "column"` test ahead of the call.

### Options Considered

| Option | Verdict |
|--------|---------|
| Helper owns only the resolve; guards keep their own tag test | ✓ Chosen — preserves each guard's distinct pass-through-versus-decline handling |
| Fold the tag test into the helper | ✗ Rejected — `guard_like_subject` and `coerce_string_position_arg` return a non-`column` node UNCHANGED but decline on a lookup miss; a helper collapsing both to `None` would convert every literal and computed argument from a pass-through into a decline, silently dropping pushdown the guards accept today |

### Consequences

Each guard retains one extra early return, which is the correct price for keeping pass-through and
decline semantics distinct. The helper's contract stays simple: a node's tag is not its concern.

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

## ADR: The four-way guards match exhaustively; the two-way guard need not

**ID:** type-rewrite-guards-exhaustive-vs-two-way-match
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted

### Context

Substituting the three guards' hand-written `starts_with`/equality predicates onto the shared
`ExaTypeClass` classifier required deciding whether each guard's match over `Option<ExaTypeClass>`
should be exhaustive or carry a wildcard arm.

### Decision

`guard_like_subject` and `coerce_string_position_arg` match `Option<ExaTypeClass>` with no wildcard
arm. `is_bare_decimal_column` stays a two-way `matches!` test.

### Options Considered

| Option | Verdict |
|--------|---------|
| Exhaustive match for the two four-way guards; two-way test stays non-exhaustive | ✓ Chosen — for the four-way guards a variant added to `ExaTypeClass` later is a real unanswered question; for the two-way test a new family genuinely IS "not DECIMAL" |
| Wildcard arms everywhere | ✗ Rejected — a wildcard would silently answer a future variant as "decline" |
| Exhaustiveness in all three | ✗ Rejected — for the two-way guard, exhaustiveness would force an edit that could only restate the same answer |

### Consequences

A variant added to `ExaTypeClass` becomes a compile error at each four-way guard, rather than a
silent decline. The two-way guard stays as simple as its actual question warrants.

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

## ADR: The fold divergence is reachable via `ß`, not `ü` — captured live

**ID:** straße-fold-divergence-reachable-live-capture
**Plan:** refactor-col-types-guard-dedup
**Status:** Accepted

### Context

Plan review round 1 found that the plan asserted, from code inspection alone, that a non-ASCII
column name resolves on the single-table path and misses on the join path — without checking what
casing Exasol actually delivers in `involvedTables[].columns[].name`. CLAUDE.md § Verification
discipline requires such a claim be checked against a live Exasol system. The plan's own working
example (`nüm`) would in fact have pinned an unreachable input, since `NÜM` is a fixed point of both
Rust folds.

### Decision

Capture live evidence against the local Docker Exasol container before relying on the claim: (1)
`SYS.EXA_ALL_COLUMNS` shows lowercase Iceberg names served uppercased for existing virtual schemas,
so Exasol normalizes the declared name; (2) a native `CREATE TABLE FOLDPROBE.T (nüm INT, straße INT,
"nüm_q" INT)` stores `NÜM`, `STRAßE`, `nüm_q` — Exasol's native-identifier fold is Unicode SIMPLE
uppercasing, which leaves `ß` intact. Rust's two folds disagree on the surviving `STRAßE` form, so
the divergence is reachable, but only via `straße`, not `nüm`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Capture live evidence and correct the reachable example to `straße` | ✓ Chosen — satisfies CLAUDE.md § Verification discipline and grounds the claim in measurement rather than inference |
| Leave the `nüm` example and the code-inspection-only claim as originally written | ✗ Rejected — `nüm` is a fixed point of both folds and would have pinned an unreachable input, and an unverified SQL-capability claim is exactly the defect class CLAUDE.md's verification discipline exists to catch |

### Consequences

A new task (task 3) was added to the plan as a live gate confirming the composition end-to-end
through a real `createVirtualSchema` plus a captured pushdown request, ahead of both new
characterization tests. That same gate later refuted this entry's residual composition claim (see
the superseding "unreachable, design-preserved" ADR above) — the ordering discipline this entry
established is what let the refutation land before either test was written rather than after.
