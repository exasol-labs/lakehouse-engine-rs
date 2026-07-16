# Decisions: refactor-adapter-pushdown-modules

## ADR: Directory-module façade preserves the public API unchanged

**ID:** pushdown-directory-module-facade-preserves-public-api
**Plan:** `refactor-adapter-pushdown-modules`
**Status:** Accepted

### Context

Issue #129 flagged `adapter/pushdown.rs` (16,300 lines) as an extreme size outlier. Splitting
it into submodules risks silently narrowing, widening, adding, or removing an item on the
`crate::adapter::pushdown::<name>` public surface, which every consumer (`adapter`, `scan`,
`capabilities`) and external `tests/` crate rely on.

### Decision

Convert `pushdown.rs` into `pushdown/mod.rs` plus sibling files, keep `adapter/mod.rs`'s
`pub mod pushdown;`, and re-export every pre-refactor `pub`/`pub(crate)` item from `mod.rs`
at its original visibility. No consumer's `use crate::adapter::pushdown::...` path changes.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-export every pre-refactor `pub`/`pub(crate)` item at its original visibility | ✓ Chosen — keeps the import path stable and preserves the full API surface, the defensible behavior-preserving choice for a pure refactor |
| Re-export only the nine grep-verified externally-consumed items | ✗ Rejected — would silently narrow the public surface |
| Split into nested `foo/mod.rs` trees | ✗ Rejected — violates the crate's flat one-level module convention |

### Consequences

The import path `crate::adapter::pushdown::<name>` stays stable for every consumer with zero
edits. The façade carries some items with no in-repo caller, preserving surface area rather
than trimming it, which a later, separately-scoped cleanup could revisit.

## ADR: Single-responsibility submodule decomposition mirroring the `scan/` convention

**ID:** pushdown-single-responsibility-submodule-decomposition
**Plan:** `refactor-adapter-pushdown-modules`
**Status:** Accepted

### Context

The flat file already carried five banner-comment section dividers that agreed with a
natural capability decomposition. The crate's own convention (`scan/`, `adapter/`) is flat
sibling files one level deep under a `mod.rs` that holds real orchestration logic.

### Decision

Split into eight sibling files (`support`, `credentials`, `file_resolution`,
`single_group_agg`, `grouped_agg`, `joins`, `topn`, `namespace`) under a thin `mod.rs` holding
only `handle_pushdown`, `build_logical_schema`, and the re-export façade.

### Options Considered

| Option | Verdict |
|--------|---------|
| Eight capability-cluster siblings under a thin orchestrating `mod.rs` | ✓ Chosen — the file's five existing banner dividers already draw these boundaries; matches the crate's flat capability/lifecycle-sibling convention |
| A layer split (`types.rs`/`logic.rs`) | ✗ Rejected — does not match the crate's existing module convention |
| Fewer, coarser modules or nested module trees | ✗ Rejected — loses the single-responsibility boundary the banners already drew |

### Consequences

Each capability (credentials, file resolution, aggregation shapes, joins, top-N, namespace
listing) has one clear home, and a shared `support.rs` holds cross-cutting SQL-builder and
utility helpers instead of duplicating them.

## ADR: Cross-sibling private sharing widens to `pub(super)`, never broader

**ID:** pushdown-cross-sibling-sharing-widens-to-pub-super-only
**Plan:** `refactor-adapter-pushdown-modules`
**Status:** Accepted

### Context

Splitting one file into siblings forces some previously module-private helpers to become
visible across sibling boundaries, creating a risk of over-widening visibility beyond what
the refactor's "API unchanged" invariant permits.

### Decision

A private helper shared across sibling submodules widens to `pub(super)` — visible to the
parent `pushdown` module and its descendant siblings — never to `pub`/`pub(crate)` beyond its
pre-refactor visibility.

### Options Considered

| Option | Verdict |
|--------|---------|
| Widen shared helpers to `pub(super)` only | ✓ Chosen — narrowest visibility that lets siblings share code without enlarging the public surface |
| Widen shared helpers to `pub(crate)` | ✗ Rejected — unnecessarily broadens visibility beyond what sibling sharing requires |
| Duplicate helpers per module | ✗ Rejected — reintroduces the duplication the refactor is meant to remove |

### Consequences

Sibling submodules share implementation code without enlarging the crate's effective public
surface, keeping the "API unchanged" invariant machine-checkable via the visibility snapshot.

## ADR: Tests co-located per submodule with one shared `#[cfg(test)] mod test_support`

**ID:** pushdown-tests-colocated-per-submodule-with-shared-test-support
**Plan:** `refactor-adapter-pushdown-modules`
**Status:** Accepted

### Context

The flat file held 10,489 lines of tests (230 tests) in one central `#[cfg(test)] mod tests`
reaching across all capability clusters via private access, plus a shared "helpers used
across tests" block.

### Decision

Move each cluster's tests into its own submodule's `#[cfg(test)] mod tests`; hoist the shared
test-helper block into one `#[cfg(test)] mod test_support`; delete the central `mod tests`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Co-locate tests per submodule plus one shared `test_support` module | ✓ Chosen — matches the `scan/` crate's per-file test convention; gives cross-cluster fixtures one home instead of duplication |
| Keep one central test module | ✗ Rejected — defeats the purpose of splitting the file; keeps cross-cluster private-access coupling |
| Duplicate shared test helpers into each submodule | ✗ Rejected — duplicates fixture code the shared module avoids |

### Consequences

Each capability submodule owns and can be understood alongside its own tests. A mid-implementation
gap (51 tests initially stranded in the central block after moving `support.rs`'s production
code) required a follow-up task to triage those tests by actual call site (41 to `support.rs`,
10 to `mod.rs`'s own test module), confirming ownership must follow the consumer, not file order.

## ADR: One structural feature spec, no behavioral capability deltas

**ID:** pushdown-one-structural-spec-no-behavioral-deltas
**Plan:** `refactor-adapter-pushdown-modules`
**Status:** Accepted

### Context

The refactor changes code organization only — no query, pushdown, file-pruning, or
type-handling behavior changes — so the 16 existing `pushdown-planning*` and
`pushdown-file-pruning` behavioral specs stay accurate as written.

### Decision

Author one new feature spec, `vs-adapter/pushdown-module-structure`, capturing the
façade-preservation, behavior-unchanged, and test-co-location invariants at principle level.
Leave every behavioral spec untouched.

### Options Considered

| Option | Verdict |
|--------|---------|
| One new structural feature spec, behavioral specs untouched | ✓ Chosen — behavior is unchanged so the behavioral specs stay accurate; gives the plan verifiable, recordable scenarios; `packaging/single-so-two-entry-points` is existing precedent for a structural contract in this library |
| Edit the 16 behavioral specs to note the refactor | ✗ Rejected — nothing behavioral changed; would add noise with no verification value |
| Zero spec deltas | ✗ Rejected — leaves the plan with no recordable, verifiable artifact |

### Consequences

Future pure-refactor plans in this library have a precedent for specifying structural
invariants (façade stability, behavior parity, test co-location) separately from behavioral
capability specs.
